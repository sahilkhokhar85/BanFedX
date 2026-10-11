"""
Telegram haters-ban system (ONE process, ONE file)

  USER account (Telethon)  = engine : bans, scans members, declines join requests, posts logs
  BOT (Telethon)           = panel  : you control everything from the bot chat (commands + buttons)
  MongoDB                  = ban list + counters
  LOG channel              = every action is reported here
"""
import asyncio
import hashlib
import html
import logging
import os
import re
import time

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from telethon import Button, TelegramClient, errors, events, functions, types, utils
from telethon.sessions import StringSession

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logging.getLogger("telethon").setLevel(logging.WARNING)
log = logging.getLogger("system")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION_STRING = os.environ["SESSION_STRING"]
OWNER_IDS = {int(x) for x in os.environ["OWNER_IDS"].replace(" ", "").split(",") if x}
MONGO_URI = os.environ.get("MONGO_URI") or os.environ["MONGO_URL"]
MONGO_DB = os.environ.get("MONGO_DB", "banbot")
LOG_ID = int(os.environ["LOG_CHAT_ID"])
SEED_FILE = os.environ.get("SEED_FILE", "ids.txt")
PENDING_SECONDS = int(os.environ.get("PENDING_SECONDS", "45"))  # backup poll for join requests
SCAN_MINUTES = int(os.environ.get("SCAN_MINUTES", "360"))  # full member scan, 0 = off
RATE_DELAY = 0.7  # gentle pace for a real account

user = None  # engine
bot = None  # panel
db = None
BANNED: set[int] = set()
MODERATED: dict[int, object] = {}  # marked chat id -> Channel entity
WARNED: dict = {}
STATE = {"last_scan": 0, "started": time.time()}
SCAN_LOCK = None

FULL_BAN = types.ChatBannedRights(until_date=None, view_messages=True)
NO_BAN = types.ChatBannedRights(until_date=None)


# ======================= formatting (bold text, monospace ids) =======================
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


# ======================= logging to the log channel =======================
async def send_log(text: str):
    try:
        await user.send_message(LOG_ID, text, parse_mode="html")
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


def warn_once(key, text: str, every: int = 3600):
    """Problems are reported to the log channel, but not more than once per hour per problem."""
    now = time.time()
    if now - WARNED.get(key, 0) < every:
        return
    WARNED[key] = now
    asyncio.create_task(send_log(text))


async def bump(field: str, n: int = 1):
    await db.meta.update_one({"_id": "stats"}, {"$inc": {field: n}}, upsert=True)


# ======================= data =======================
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
    log.info("Seeded %s ids", len(ids))


async def refresh_chats(initial: bool = False):
    """Moderate every channel/supergroup where the user account is owner/admin with Ban rights."""
    found = {}
    async for d in user.iter_dialogs():
        e = d.entity
        if not isinstance(e, types.Channel) or d.id == LOG_ID:
            continue
        if e.creator or (e.admin_rights and e.admin_rights.ban_users):
            found[d.id] = e
    added = [c for c in found if c not in MODERATED]
    removed_titles = [MODERATED[c].title for c in MODERATED if c not in found]
    MODERATED.clear()
    MODERATED.update(found)
    if initial:
        return
    for c in added:
        await send_log(f"{b('✅ Naya chat add hua:')} {b(found[c].title)} {code(c)}")
        asyncio.create_task(scan_chat(found[c]))
    for t in removed_titles:
        await send_log(b(f"⚠️ Chat hata (rights chali gayi ya account nikal gaya): {t}"))


# ======================= engine: actions =======================
async def ban(entity, target) -> str:
    """target = User object or int id. Returns ok / invalid / forbidden / fail."""
    for _ in range(3):
        try:
            await user(functions.channels.EditBannedRequest(entity, target, FULL_BAN))
            return "ok"
        except errors.FloodWaitError as e:
            warn_once(("flood", entity.id), b(f"⏳ Telegram ne rok diya (FloodWait {e.seconds}s) - {entity.title}"), 600)
            await asyncio.sleep(e.seconds + 1)
        except (ValueError, errors.UserIdInvalidError, errors.PeerIdInvalidError):
            return "invalid"  # id not known to the account yet
        except (errors.ChatAdminRequiredError, errors.ChatWriteForbiddenError):
            warn_once(("noright", entity.id), b(f"⚠️ Ban rights nahi hain: {entity.title}"))
            return "forbidden"
        except Exception as e:
            log.warning("ban failed in %s: %s", entity.title, e)
            return "fail"
    return "fail"


async def decline(entity, target):
    try:
        await user(
            functions.messages.HideChatJoinRequestRequest(
                peer=entity, user_id=utils.get_input_user(target), approved=False
            )
        )
    except Exception as e:
        log.warning("decline failed: %s", e)


async def fetch_pending(entity) -> list:
    users = []
    offset_date = None
    offset_user = types.InputUserEmpty()
    while True:
        res = await user(
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


async def process_pending(entity) -> int:
    """Decline + ban banned users waiting in the join-request queue. Returns how many."""
    try:
        users = await fetch_pending(entity)
    except errors.FloodWaitError as e:
        await asyncio.sleep(e.seconds + 1)
        return 0
    except errors.ChatAdminRequiredError:
        warn_once(("invite", entity.id), b(f"⚠️ {entity.title}: join requests check nahi ho pa rahe (Invite users right chahiye)"))
        return 0
    except Exception as e:
        log.warning("pending fetch failed in %s: %s", entity.title, e)
        return 0
    n = 0
    for u in users:
        if u.id in BANNED:
            await decline(entity, u)
            await ban(entity, u)
            n += 1
            await bump("declined")
            await bump("bans")
            await send_log(f"{b('🚫 Join request decline + ban:')} {label(u)} {b('in ' + entity.title)}")
            await asyncio.sleep(RATE_DELAY)
    return n


async def scan_chat(entity):
    """Pending requests + current members. Returns (checked, banned, declined)."""
    declined = await process_pending(entity)
    checked = banned = 0
    lines = []
    try:
        async for u in user.iter_participants(entity):
            checked += 1
            if u.id in BANNED:
                if await ban(entity, u) == "ok":
                    banned += 1
                    lines.append("• " + label(u))
                await asyncio.sleep(RATE_DELAY)
    except errors.FloodWaitError as e:
        await asyncio.sleep(e.seconds + 1)
    except Exception as e:
        warn_once(("scan", entity.id), b(f"⚠️ {entity.title}: members scan nahi ho paya ({type(e).__name__})"))
    if banned:
        await bump("bans", banned)
    await send_log_lines(b(f"🚫 Scan me ban hue - {entity.title}:"), lines)
    return checked, banned, declined


async def scan_all(reason: str):
    async with SCAN_LOCK:
        t0 = time.time()
        await send_log(b(f"🔍 Scan shuru ({reason}) - {len(MODERATED)} chats"))
        rows = []
        for ent in list(MODERATED.values()):
            c, bn, dc = await scan_chat(ent)
            rows.append(b(f"• {ent.title}: checked {c}, banned {bn}, declined {dc}"))
        STATE["last_scan"] = time.time()
        await send_log("\n".join([b(f"✅ Scan khatam ({int(time.time() - t0)}s)")] + rows))


async def ban_ids(ids):
    """Direct ban of ids the account can already resolve, in every moderated chat."""
    ok = fail = 0
    invalid = set()
    for ent in list(MODERATED.values()):
        lines = []
        for uid in ids:
            if uid in invalid:
                fail += 1
                continue
            res = await ban(ent, uid)
            if res == "ok":
                ok += 1
                try:
                    lines.append("• " + label(await user.get_entity(uid)))
                except Exception:
                    lines.append("• " + code(uid))
            else:
                fail += 1
                if res == "invalid":
                    invalid.add(uid)
            if res != "invalid":
                await asyncio.sleep(RATE_DELAY)
        if lines:
            await bump("bans", len(lines))
        await send_log_lines(b(f"🚫 Ban hue - {ent.title}:"), lines)
    return ok, fail


async def unban_ids(ids):
    idset = set(ids)
    total = 0
    for ent in list(MODERATED.values()):
        lines = []
        try:
            async for u in user.iter_participants(ent, filter=types.ChannelParticipantsKicked):
                if u.id in idset:
                    await user(functions.channels.EditBannedRequest(ent, u, NO_BAN))
                    lines.append("• " + label(u))
                    await asyncio.sleep(RATE_DELAY)
        except Exception as e:
            warn_once(("unban", ent.id), b(f"⚠️ {ent.title}: unban list nahi mili ({type(e).__name__})"))
        total += len(lines)
        await send_log_lines(b(f"✅ Unban hue - {ent.title}:"), lines)
    if total:
        await bump("unbans", total)
    await send_log(b(f"✅ Unban khatam: {total} unban entries"))


# ======================= engine: live events =======================
async def on_pending_update(update):
    ent = MODERATED.get(utils.get_peer_id(update.peer))
    if ent:
        await process_pending(ent)


async def on_chat_action(event):
    if not (event.user_joined or event.user_added):
        return
    ent = MODERATED.get(event.chat_id)
    if not ent:
        return
    for u in await event.get_users():
        if u.id in BANNED and await ban(ent, u) == "ok":
            await bump("bans")
            await send_log(f"{b('🚫 Join pe ban:')} {label(u)} {b('in ' + ent.title)}")


async def on_group_message(event):
    ent = MODERATED.get(event.chat_id)
    if not ent or event.sender_id not in BANNED:
        return
    sender = await event.get_sender()
    if sender and await ban(ent, sender) == "ok":
        try:
            await event.delete()
        except Exception:
            pass
        await bump("bans")
        await send_log(f"{b('🚫 Active dikha, ban + msg delete:')} {label(sender)} {b('in ' + ent.title)}")


# ======================= panel (bot) =======================
def panel():
    return [
        [Button.inline("📊 Stats", b"stats"), Button.inline("📋 Chats", b"chats")],
        [Button.inline("🔍 Scan now", b"scan"), Button.inline("📄 Banned list", b"list")],
    ]


def help_text() -> str:
    lines = [
        "🛡 Control panel",
        "/ban <ids> - sab chats me ban",
        "/unban <ids>",
        "/check <id>",
        "/list - last 50 banned ids",
        "/chats - moderated chats",
        "/stats - status",
        "/scan - members + join requests abhi check",
        ".txt file bhejo = bulk ban",
    ]
    return "\n".join(b(x) for x in lines)


def stats_text(s: dict) -> str:
    last = time.strftime("%d %b %H:%M UTC", time.gmtime(STATE["last_scan"])) if STATE["last_scan"] else "abhi tak nahi"
    up = int(time.time() - STATE["started"]) // 60
    lines = [
        "📊 Stats",
        f"Banned list: {len(BANNED)} IDs",
        f"Chats: {len(MODERATED)}",
        f"Total ban: {s.get('bans', 0)}",
        f"Join requests decline: {s.get('declined', 0)}",
        f"Unban: {s.get('unbans', 0)}",
        f"Last scan: {last}",
        f"Uptime: {up} min",
    ]
    return "\n".join(b(x) for x in lines)


async def get_stats() -> str:
    return stats_text(await db.meta.find_one({"_id": "stats"}) or {})


async def chats_text() -> str:
    lines = [f"{b(e.title)} {code(c)}" for c, e in MODERATED.items()]
    return b(f"{len(MODERATED)} chats:") + "\n" + "\n".join(lines)


async def list_text() -> str:
    rows = [d["_id"] async for d in db.banned.find().sort("added_at", -1).limit(50)]
    return b(f"Total: {len(BANNED)} (last 50)") + "\n" + "\n".join(code(i) for i in rows)


async def start_scan(event):
    await event.respond(b("Scan shuru. Result log channel me aayega."), parse_mode="html")
    asyncio.create_task(scan_all("manual"))


async def do_ban(event, ids):
    if not ids:
        await event.respond(b("Koi valid user id nahi mili."), parse_mode="html")
        return
    sender = await event.get_sender()
    now = int(time.time())
    await db.banned.bulk_write(
        [UpdateOne({"_id": i}, {"$setOnInsert": {"added_at": now}}, upsert=True) for i in ids]
    )
    BANNED.update(ids)
    await send_log(f"{b('📝 Ban command:')} {label(sender)} {b(f'- {len(ids)} id(s)')}")
    await event.respond(b(f"{len(ids)} id(s) add. Ban + scan chal raha hai, result log channel me."), parse_mode="html")

    async def job():
        ok, fail = await ban_ids(ids)
        await scan_all("ban ke baad")
        await send_log(b(f"✅ Ban command khatam: direct ok={ok}, abhi-pehchane-nahi={fail} (wo join/scan pe pakde jayenge)"))

    asyncio.create_task(job())


async def on_command(event):
    f = event.file
    if f and f.name and f.name.lower().endswith((".txt", ".csv")) and (f.size or 0) < 5_000_000:
        data = (await event.message.download_media(file=bytes)).decode("utf-8", errors="ignore")
        return await do_ban(event, parse_ids(data))
    m = re.match(r"^/(\w+)(?:@\w+)?\s*(.*)$", event.raw_text or "", re.S)
    if not m:
        return
    cmd, ids = m.group(1).lower(), parse_ids(m.group(2))
    if cmd in ("start", "help", "panel"):
        await event.respond(help_text(), parse_mode="html", buttons=panel())
    elif cmd == "ban":
        await do_ban(event, ids)
    elif cmd == "unban":
        if not ids:
            return await event.respond(b("Usage: /unban <id>"), parse_mode="html")
        sender = await event.get_sender()
        await db.banned.delete_many({"_id": {"$in": ids}})
        BANNED.difference_update(ids)
        await send_log(f"{b('📝 Unban command:')} {label(sender)} {b(f'- {len(ids)} id(s)')}")
        await event.respond(b(f"{len(ids)} id(s) list se hata di. Unban ho rahe hain, result log channel me."), parse_mode="html")
        asyncio.create_task(unban_ids(ids))
    elif cmd == "check" and ids:
        msg = "BANNED list me hai" if ids[0] in BANNED else "list me nahi hai"
        await event.respond(f"{code(ids[0])} {b(msg)}", parse_mode="html")
    elif cmd == "list":
        await event.respond(await list_text(), parse_mode="html")
    elif cmd == "chats":
        await event.respond(await chats_text(), parse_mode="html")
    elif cmd == "stats":
        await event.respond(await get_stats(), parse_mode="html")
    elif cmd == "scan":
        await start_scan(event)


async def on_callback(event):
    data = event.data.decode()
    await event.answer()
    if data == "stats":
        await event.respond(await get_stats(), parse_mode="html")
    elif data == "chats":
        await event.respond(await chats_text(), parse_mode="html")
    elif data == "list":
        await event.respond(await list_text(), parse_mode="html")
    elif data == "scan":
        await start_scan(event)


# ======================= background loops =======================
async def refresh_loop():
    while True:
        await asyncio.sleep(600)
        try:
            await refresh_chats()
        except Exception as e:
            log.warning("refresh failed: %s", e)


async def pending_loop():
    while True:
        for ent in list(MODERATED.values()):
            await process_pending(ent)
            await asyncio.sleep(1)
        await asyncio.sleep(PENDING_SECONDS)


async def scan_loop():
    while True:
        await asyncio.sleep(SCAN_MINUTES * 60)
        try:
            await scan_all("auto")
        except Exception as e:
            log.warning("scan failed: %s", e)


async def report_loop():
    while True:
        await asyncio.sleep(24 * 3600)
        await send_log(await get_stats())


# ======================= start =======================
async def main():
    global user, bot, db, SCAN_LOCK
    SCAN_LOCK = asyncio.Lock()
    db = AsyncIOMotorClient(MONGO_URI)[MONGO_DB]
    BANNED.update([d["_id"] async for d in db.banned.find({}, {"_id": 1})])
    await seed()

    user = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    await user.connect()
    if not await user.is_user_authorized():
        raise SystemExit("SESSION_STRING invalid/expired - generate a new one")
    bot = TelegramClient(StringSession(), API_ID, API_HASH)
    await bot.start(bot_token=BOT_TOKEN)

    await refresh_chats(initial=True)  # also loads the dialog cache needed to post logs
    try:
        await user.get_input_entity(LOG_ID)
    except Exception:
        raise SystemExit("LOG_CHAT_ID not reachable: add the USER account to the log channel (as admin) and check the id")

    user.add_event_handler(on_pending_update, events.Raw(types.UpdatePendingJoinRequests))
    user.add_event_handler(on_chat_action, events.ChatAction)
    user.add_event_handler(on_group_message, events.NewMessage(incoming=True, func=lambda e: not e.is_private))
    bot.add_event_handler(
        on_command,
        events.NewMessage(incoming=True, from_users=list(OWNER_IDS), func=lambda e: e.is_private),
    )
    bot.add_event_handler(on_callback, events.CallbackQuery(func=lambda e: e.sender_id in OWNER_IDS))

    chat_lines = [b(f"• {e.title}") for e in MODERATED.values()] or [b("• (koi chat nahi - account ko admin banao)")]
    await send_log("\n".join([b("🟢 System online"), b(f"Banned IDs: {len(BANNED)}"), b(f"Chats: {len(MODERATED)}")] + chat_lines))
    log.info("System ready: %s banned ids, %s chats", len(BANNED), len(MODERATED))

    asyncio.create_task(scan_all("startup"))
    asyncio.create_task(refresh_loop())
    asyncio.create_task(pending_loop())
    asyncio.create_task(report_loop())
    if SCAN_MINUTES > 0:
        asyncio.create_task(scan_loop())
    await asyncio.gather(user.run_until_disconnected(), bot.run_until_disconnected())


if __name__ == "__main__":
    asyncio.run(main())
