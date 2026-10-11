import asyncio
import hashlib
import html
import logging
import os
import re
import time

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from telethon import TelegramClient, errors, events, functions, types, utils
from telethon.sessions import StringSession

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logging.getLogger("telethon").setLevel(logging.WARNING)
log = logging.getLogger("userbot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]
OWNER_IDS = {int(x) for x in os.environ.get("OWNER_IDS", "").replace(" ", "").split(",") if x}
MONGO_URI = os.environ.get("MONGO_URI") or os.environ["MONGO_URL"]
MONGO_DB = os.environ.get("MONGO_DB", "banbot")
SEED_FILE = os.environ.get("SEED_FILE", "ids.txt")
LOG_CHAT_ID = os.environ.get("LOG_CHAT_ID")
PENDING_SECONDS = int(os.environ.get("PENDING_SECONDS", "45"))  # backup poll for join requests
SCAN_MINUTES = int(os.environ.get("SCAN_MINUTES", "360"))  # full member scan, 0 = off
RATE_DELAY = 0.7  # gentle pace: this is a real account, avoid FloodWait

client = None
db = None
BANNED: set[int] = set()
MODERATED: dict[int, object] = {}  # marked chat id -> Channel entity (chats where account can ban)
OWNERS: set[int] = set()
ME_ID = None
SCAN_LOCK = None

FULL_BAN = types.ChatBannedRights(until_date=None, view_messages=True)
NO_BAN = types.ChatBannedRights(until_date=None)


# ---------- Formatting: bold messages, monospace ids ----------
def esc(s) -> str:
    return html.escape(str(s if s is not None else ""))


def b(s) -> str:
    return f"<b>{esc(s)}</b>"


def code(s) -> str:
    return f"<code>{esc(s)}</code>"


def label(u) -> str:
    name = (utils.get_display_name(u) or "").strip()
    uname = f" @{u.username}" if getattr(u, "username", None) else ""
    text = f"{name}{uname}".strip()
    return f"{b(text)} {code(u.id)}" if text else code(u.id)


def parse_ids(text: str) -> list[int]:
    return list({int(x) for x in re.findall(r"\d{5,11}(?!\d)", text or "")})


async def send_log(text: str):
    if LOG_CHAT_ID:
        try:
            await client.send_message(int(LOG_CHAT_ID), text, parse_mode="html")
        except Exception as e:
            log.warning("log send failed: %s", e)


async def send_log_lines(header: str, lines: list):
    if not lines:
        return
    chunk = header
    for line in lines:
        if len(chunk) + len(line) + 1 > 3500:
            await send_log(chunk)
            chunk = header
        chunk += "\n" + line
    await send_log(chunk)


# ---------- Startup data ----------
async def seed():
    """Import ids.txt once per file version, so /unban survives restarts."""
    if not os.path.exists(SEED_FILE):
        return
    raw = open(SEED_FILE, encoding="utf-8").read()
    key = "seed:" + hashlib.sha256(raw.encode()).hexdigest()
    if await db.meta.find_one({"_id": key}):
        return
    ids = parse_ids(raw)
    now = int(time.time())
    if ids:
        await db.banned.bulk_write(
            [UpdateOne({"_id": i}, {"$setOnInsert": {"added_at": now}}, upsert=True) for i in ids]
        )
        BANNED.update(ids)
    await db.meta.insert_one({"_id": key, "count": len(ids)})
    log.info("Seeded %s ids from %s", len(ids), SEED_FILE)


async def refresh_chats():
    """Moderate every channel/supergroup where this account is owner or admin with Ban rights."""
    found = {}
    async for d in client.iter_dialogs():
        e = d.entity
        if not isinstance(e, types.Channel):
            continue
        if str(d.id) == str(LOG_CHAT_ID):
            continue
        if e.creator or (e.admin_rights and e.admin_rights.ban_users):
            found[d.id] = e
    new = [cid for cid in found if cid not in MODERATED]
    MODERATED.clear()
    MODERATED.update(found)
    for cid in new:
        await send_log(b(f"✅ Added to {found[cid].title}"))
        asyncio.create_task(scan_chat(cid, found[cid]))


# ---------- Telegram actions ----------
async def ban(entity, user) -> str:
    """user = User object or int id. Returns ok / invalid / forbidden / fail."""
    for _ in range(3):
        try:
            await client(functions.channels.EditBannedRequest(entity, user, FULL_BAN))
            return "ok"
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 1)
        except (ValueError, errors.UserIdInvalidError, errors.PeerIdInvalidError):
            return "invalid"  # id not known to this account yet
        except (errors.ChatAdminRequiredError, errors.ChatWriteForbiddenError):
            log.warning("No ban rights in %s", getattr(entity, "title", entity))
            return "forbidden"
        except Exception as e:
            log.warning("ban failed in %s: %s", getattr(entity, "title", entity), e)
            return "fail"
    return "fail"


async def decline(entity, user):
    try:
        await client(
            functions.messages.HideChatJoinRequestRequest(
                peer=entity, user_id=utils.get_input_user(user), approved=False
            )
        )
    except Exception as e:
        log.warning("decline failed: %s", e)


async def fetch_pending(entity) -> list:
    users = []
    offset_date = None
    offset_user = types.InputUserEmpty()
    while True:
        res = await client(
            functions.messages.GetChatInviteImportersRequest(
                peer=entity, offset_date=offset_date, offset_user=offset_user, limit=100, requested=True
            )
        )
        if not res.importers:
            break
        umap = {u.id: u for u in res.users}
        for imp in res.importers:
            if imp.user_id in umap:
                users.append(umap[imp.user_id])
        if len(res.importers) < 100:
            break
        last = res.importers[-1]
        offset_date = last.date
        offset_user = utils.get_input_user(umap[last.user_id])
    return users


async def process_pending(cid, entity):
    """Decline + ban banned users sitting in the join-request queue."""
    try:
        users = await fetch_pending(entity)
    except errors.FloodWaitError as e:
        await asyncio.sleep(e.seconds + 1)
        return
    except Exception as e:
        log.warning("pending fetch failed in %s: %s", entity.title, e)
        return
    for u in users:
        if u.id in BANNED:
            await decline(entity, u)
            await ban(entity, u)
            await send_log(f"{b('🚫 Join request declined + banned:')} {label(u)} {b('in ' + entity.title)}")
            await asyncio.sleep(RATE_DELAY)


async def scan_chat(cid, entity):
    """Pending requests + current members: ban anyone from the list."""
    await process_pending(cid, entity)
    lines = []
    try:
        async for u in client.iter_participants(entity):
            if u.id in BANNED:
                if await ban(entity, u) == "ok":
                    lines.append("• " + label(u))
                await asyncio.sleep(RATE_DELAY)
    except errors.FloodWaitError as e:
        await asyncio.sleep(e.seconds + 1)
    except Exception as e:
        log.warning("member scan failed in %s: %s", entity.title, e)
    await send_log_lines(b(f"🚫 Banned (scan) in {entity.title}:"), lines)


async def scan_all():
    async with SCAN_LOCK:
        for cid, ent in list(MODERATED.items()):
            await scan_chat(cid, ent)


async def ban_ids(ids):
    """Direct ban of ids we can already resolve, in every moderated chat."""
    ok = fail = 0
    for cid, ent in list(MODERATED.items()):
        lines = []
        for uid in ids:
            res = await ban(ent, uid)
            if res == "ok":
                ok += 1
                try:
                    lines.append("• " + label(await client.get_entity(uid)))
                except Exception:
                    lines.append("• " + code(uid))
            else:
                fail += 1
            await asyncio.sleep(RATE_DELAY)
        await send_log_lines(b(f"🚫 Banned in {ent.title}:"), lines)
    return ok, fail


async def unban_ids(ids):
    for cid, ent in list(MODERATED.items()):
        lines = []
        for uid in ids:
            try:
                await client(functions.channels.EditBannedRequest(ent, uid, NO_BAN))
                try:
                    lines.append("• " + label(await client.get_entity(uid)))
                except Exception:
                    lines.append("• " + code(uid))
            except Exception:
                pass
            await asyncio.sleep(RATE_DELAY)
        await send_log_lines(b(f"✅ Unbanned in {ent.title}:"), lines)


# ---------- Live events ----------
async def on_pending_update(update):
    cid = utils.get_peer_id(update.peer)
    ent = MODERATED.get(cid)
    if ent:
        await process_pending(cid, ent)


async def on_chat_action(event):
    if not (event.user_joined or event.user_added):
        return
    ent = MODERATED.get(event.chat_id)
    if not ent:
        return
    for u in await event.get_users():
        if u.id in BANNED and await ban(ent, u) == "ok":
            await send_log(f"{b('🚫 Banned on join:')} {label(u)} {b('in ' + ent.title)}")


async def on_message(event):
    if event.is_private:
        return await handle_command(event)
    ent = MODERATED.get(event.chat_id)
    if ent and event.sender_id in BANNED:
        sender = await event.get_sender()
        if sender and await ban(ent, sender) == "ok":
            try:
                await event.delete()
            except Exception:
                pass
            await send_log(f"{b('🚫 Banned (seen active):')} {label(sender)} {b('in ' + ent.title)}")


# ---------- Owner commands (private chat / Saved Messages) ----------
async def reply(event, text):
    await event.reply(text, parse_mode="html")


async def handle_command(event):
    if event.sender_id not in OWNERS:
        return
    if event.out and event.chat_id != ME_ID:
        return

    doc = event.message.document
    if doc:
        names = [a.file_name for a in doc.attributes if hasattr(a, "file_name")]
        if names and names[0].lower().endswith((".txt", ".csv")) and doc.size < 5_000_000:
            data = (await event.message.download_media(file=bytes)).decode("utf-8", errors="ignore")
            await do_ban(event, parse_ids(data))
        return

    m = re.match(r"^/(\w+)(?:@\w+)?\s*(.*)$", event.raw_text or "", re.S)
    if not m:
        return
    cmd, arg = m.group(1).lower(), m.group(2)
    ids = parse_ids(arg)

    if cmd in ("start", "help"):
        lines = [
            "Commands:",
            "/ban <ids> - ban everywhere",
            "/unban <ids>",
            "/check <id>",
            "/list - count + last 50",
            "/chats - chats I moderate",
            "/scan - scan members + join requests now",
            ".txt file bhejo = bulk ban",
        ]
        await reply(event, "\n".join(b(x) for x in lines))
    elif cmd == "ban":
        await do_ban(event, ids)
    elif cmd == "unban":
        await db.banned.delete_many({"_id": {"$in": ids}})
        BANNED.difference_update(ids)
        await reply(event, b(f"{len(ids)} id(s) unban ho rahe hain..."))
        asyncio.create_task(unban_ids(ids))
    elif cmd == "check" and ids:
        msg = "BANNED list me hai" if ids[0] in BANNED else "list me nahi hai"
        await reply(event, f"{code(ids[0])} {b(msg)}")
    elif cmd == "list":
        rows = [d["_id"] async for d in db.banned.find().sort("added_at", -1).limit(50)]
        await reply(event, b(f"Total: {len(BANNED)}") + "\n" + "\n".join(code(i) for i in rows))
    elif cmd == "chats":
        lines = [f"{b(e.title)} {code(cid)}" for cid, e in MODERATED.items()]
        await reply(event, b(f"{len(MODERATED)} chats:") + "\n" + "\n".join(lines))
    elif cmd == "scan":
        await reply(event, b("Scan shuru..."))

        async def job():
            await scan_all()
            await reply(event, b("✅ Scan done"))

        asyncio.create_task(job())


async def do_ban(event, ids):
    if not ids:
        await reply(event, b("Koi valid user id nahi mili."))
        return
    now = int(time.time())
    await db.banned.bulk_write(
        [UpdateOne({"_id": i}, {"$setOnInsert": {"added_at": now}}, upsert=True) for i in ids]
    )
    BANNED.update(ids)
    await reply(event, b(f"{len(ids)} id(s) add. {len(MODERATED)} chats me ban + scan ho raha hai..."))

    async def job():
        ok, fail = await ban_ids(ids)
        await scan_all()  # also catches members/requests the account could not resolve by id alone
        await reply(event, b(f"✅ Done. direct ok={ok}, fail={fail} (fail wale scan me pakde jate hain)"))

    asyncio.create_task(job())


# ---------- Background loops ----------
async def refresh_loop():
    while True:
        await asyncio.sleep(600)
        try:
            await refresh_chats()
        except Exception as e:
            log.warning("refresh failed: %s", e)


async def pending_loop():
    while True:
        for cid, ent in list(MODERATED.items()):
            await process_pending(cid, ent)
            await asyncio.sleep(1)
        await asyncio.sleep(PENDING_SECONDS)


async def scan_loop():
    await asyncio.sleep(30)
    while True:
        try:
            await scan_all()
        except Exception as e:
            log.warning("scan failed: %s", e)
        await asyncio.sleep(SCAN_MINUTES * 60)


async def main():
    global client, db, ME_ID, SCAN_LOCK
    SCAN_LOCK = asyncio.Lock()
    db = AsyncIOMotorClient(MONGO_URI)[MONGO_DB]
    BANNED.update([d["_id"] async for d in db.banned.find({}, {"_id": 1})])
    await seed()

    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        raise SystemExit("SESSION_STRING invalid/expired - generate a new one")
    me = await client.get_me()
    ME_ID = me.id
    OWNERS.update(OWNER_IDS | {me.id})

    client.add_event_handler(on_pending_update, events.Raw(types.UpdatePendingJoinRequests))
    client.add_event_handler(on_chat_action, events.ChatAction)
    client.add_event_handler(on_message, events.NewMessage)

    await refresh_chats()
    log.info("Userbot ready: %s banned ids, %s chats", len(BANNED), len(MODERATED))

    asyncio.create_task(refresh_loop())
    asyncio.create_task(pending_loop())
    if SCAN_MINUTES > 0:
        asyncio.create_task(scan_loop())
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
