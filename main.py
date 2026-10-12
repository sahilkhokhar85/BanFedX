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
SCAN_MINUTES = int(os.environ.get("SCAN_MINUTES", "30"))  # full member scan, 0 = off
RATE_DELAY = 0.7  # gentle pace for a real account

user = None  # engine
bot = None  # panel
db = None
BANNED: set[int] = set()
CANDIDATES: dict[int, object] = {}  # every chat where the account can ban
MODERATED: dict[int, object] = {}  # only chats YOU enabled with /add
ENABLED: dict[int, str] = {}  # enabled chat id -> title (stored in MongoDB)
HIDDEN: set[int] = set()  # chats you removed from the lists forever (stored in MongoDB)
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
    """CANDIDATES = chats where the account can ban. MODERATED = only the ones enabled via /add."""
    found = {}
    async for d in user.iter_dialogs():
        e = d.entity
        if not isinstance(e, types.Channel) or d.id == LOG_ID:
            continue
        if e.creator or (e.admin_rights and e.admin_rights.ban_users):
            found[d.id] = e
    lost = [(c, ENABLED[c]) for c in ENABLED if c not in found]
    CANDIDATES.clear()
    CANDIDATES.update(found)
    MODERATED.clear()
    MODERATED.update({c: e for c, e in found.items() if c in ENABLED})
    if initial:
        return
    for c, t in lost:
        warn_once(("lost", c), b(f"⚠️ Lost ban rights in an active chat: {t}"))


# ======================= engine: actions =======================
async def ban(entity, target) -> str:
    """target = User object or int id. Returns ok / invalid / forbidden / fail."""
    for _ in range(3):
        try:
            await user(functions.channels.EditBannedRequest(entity, target, FULL_BAN))
            return "ok"
        except errors.FloodWaitError as e:
            warn_once(("flood", entity.id), b(f"⏳ Telegram rate limit (FloodWait {e.seconds}s) - {entity.title}"), 600)
            await asyncio.sleep(e.seconds + 1)
        except (ValueError, errors.UserIdInvalidError, errors.PeerIdInvalidError):
            return "invalid"  # id not known to the account yet
        except (errors.ChatAdminRequiredError, errors.ChatWriteForbiddenError):
            warn_once(("noright", entity.id), b(f"⚠️ No ban rights in: {entity.title}"))
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
        warn_once(("invite", entity.id), b(f"⚠️ {entity.title}: cannot check join requests (Invite users right needed)"))
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
            await send_log(f"{b('🚫 Join request declined + banned:')} {label(u)} {b('in ' + entity.title)}")
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
        warn_once(("scan", entity.id), b(f"⚠️ {entity.title}: member scan failed ({type(e).__name__})"))
    if banned:
        await bump("bans", banned)
    await send_log_lines(b(f"🚫 Banned by scan - {entity.title}:"), lines)
    return checked, banned, declined


async def scan_all(reason: str, quiet: bool = False):
    """quiet=True (auto scans): the log channel only hears about it when something was found."""
    async with SCAN_LOCK:
        t0 = time.time()
        if not quiet:
            await send_log(b(f"🔍 Scan started ({reason}) - {len(MODERATED)} chats"))
        rows = []
        total = 0
        for ent in list(MODERATED.values()):
            c, bn, dc = await scan_chat(ent)
            total += bn + dc
            rows.append(b(f"• {ent.title}: checked {c}, banned {bn}, declined {dc}"))
        STATE["last_scan"] = time.time()
        if not quiet or total:
            await send_log("\n".join([b(f"✅ Scan finished ({int(time.time() - t0)}s)")] + rows))


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
        await send_log_lines(b(f"🚫 Banned - {ent.title}:"), lines)
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
            warn_once(("unban", ent.id), b(f"⚠️ {ent.title}: could not read banned list ({type(e).__name__})"))
        total += len(lines)
        await send_log_lines(b(f"✅ Unbanned - {ent.title}:"), lines)
    if total:
        await bump("unbans", total)
    await send_log(b(f"✅ Unban finished: {total} user(s) unbanned"))


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
            await send_log(f"{b('🚫 Banned on join:')} {label(u)} {b('in ' + ent.title)}")


# ======================= panel (bot): clean menu, everything by buttons =======================
AWAIT: dict[int, str] = {}  # owner id -> "ban" / "unban" (bot is waiting for ids)
MENU_BTN = [Button.inline("⬅️ Menu", b"menu")]


def ago(ts) -> str:
    if not ts:
        return "never"
    m = int((time.time() - ts) // 60)
    return "just now" if m < 1 else (f"{m} min ago" if m < 120 else f"{m // 60} h ago")


async def show(event, text: str, buttons=None):
    """Button taps edit the same message (clean chat); typed messages get a new one."""
    try:
        if isinstance(event, events.CallbackQuery.Event):
            await event.edit(text, parse_mode="html", buttons=buttons)
        else:
            await event.respond(text, parse_mode="html", buttons=buttons)
    except errors.MessageNotModifiedError:
        pass


def table(rows: list) -> str:
    """Aligned two-column block (monospace) for dashboards."""
    w = max(len(k) for k, _ in rows)
    return "<pre>" + "\n".join(f"{esc(k).ljust(w)}  {esc(v)}" for k, v in rows) + "</pre>"


def last_scan_text() -> str:
    if SCAN_LOCK is not None and SCAN_LOCK.locked():
        return "running now..."
    return ago(STATE["last_scan"])


async def menu(event):
    s = await db.meta.find_one({"_id": "stats"}) or {}
    rows = [
        ("Active chats", str(len(MODERATED))),
        ("Banned IDs", str(len(BANNED))),
        ("Total bans", str(s.get("bans", 0))),
        ("Last scan", last_scan_text()),
    ]
    text = "\n".join(
        [
            "🛡 " + b("Haters Ban System"),
            "🟢 " + b("Running") + " · fully automatic",
            "",
            table(rows),
            "<i>Tap a button below</i>",
        ]
    )
    buttons = [
        [Button.inline("🚫 Ban", b"ban"), Button.inline("✅ Unban", b"unban")],
        [Button.inline("📋 Chats", b"chats"), Button.inline("📊 Stats", b"stats")],
        [Button.inline("🔍 Scan now", b"scan"), Button.inline("📄 Banned list", b"list")],
    ]
    await show(event, text, buttons)


def stats_text(s: dict) -> str:
    up = int(time.time() - STATE["started"]) // 60
    rows = [
        ("Active chats", str(len(MODERATED))),
        ("Banned IDs", str(len(BANNED))),
        ("Total bans", str(s.get("bans", 0))),
        ("Declined requests", str(s.get("declined", 0))),
        ("Unbans", str(s.get("unbans", 0))),
        ("Last scan", last_scan_text()),
        ("Uptime", f"{up // 60}h {up % 60}m"),
    ]
    return "\n".join(["📊 " + b("Stats"), "", table(rows)])


async def get_stats() -> str:
    return stats_text(await db.meta.find_one({"_id": "stats"}) or {})


async def stats_screen(event):
    await show(event, await get_stats(), [MENU_BTN])


async def list_screen(event):
    rows = [d["_id"] async for d in db.banned.find().sort("added_at", -1).limit(50)]
    text = b(f"📄 Banned list: {len(BANNED)} IDs (last 50)") + "\n" + "\n".join(code(i) for i in rows)
    await show(event, text, [MENU_BTN])


async def chats_screen(event):
    if MODERATED:
        lines = [b(f"🟢 Active chats: {len(MODERATED)}")] + [f"{b(e.title)} {code(c)}" for c, e in MODERATED.items()]
        text = "\n".join(lines)
    else:
        text = b("No active chats yet. Tap ➕ Add chat.")
    buttons = [[Button.inline("➕ Add chat", b"add"), Button.inline("➖ Remove chat", b"remove")], MENU_BTN]
    await show(event, text, buttons)


async def add_screen(event):
    await refresh_chats(initial=True)  # pick up chats where the account just became admin
    rows = [
        [Button.inline(e.title[:35], f"add:{c}".encode()), Button.inline("🗑", f"hide:{c}".encode())]
        for c, e in CANDIDATES.items()
        if c not in ENABLED and c not in HIDDEN
    ]
    if rows:
        text = b("Tap a chat to activate it. 🗑 hides it from this list forever:")
    else:
        text = b("No chats found. Make the account an admin there (Ban users right), then tap again.")
    if HIDDEN:
        rows.append([Button.inline("♻️ Show hidden chats", b"unhide")])
    rows.append([Button.inline("⬅️ Back", b"chats")])
    await show(event, text, rows[:50])


async def remove_screen(event):
    rows = [[Button.inline(t[:35], f"rm:{c}".encode())] for c, t in ENABLED.items()]
    text = b("Which chat should be deactivated? Tap one:") if rows else b("No active chats.")
    rows.append([Button.inline("⬅️ Back", b"chats")])
    await show(event, text, rows[:50])


async def enable_chat(event, cid: int):
    ent = CANDIDATES.get(cid)
    if ent:
        ENABLED[cid] = ent.title
        await db.chats.update_one({"_id": cid}, {"$set": {"title": ent.title}}, upsert=True)
        MODERATED[cid] = ent
        sender = await event.get_sender()
        await send_log(f"{b('➕ Chat activated:')} {b(ent.title)} {code(cid)} {b('by')} {label(sender)}")
        asyncio.create_task(scan_chat(ent))
    await chats_screen(event)


async def disable_chat(event, cid: int):
    title = ENABLED.pop(cid, None)
    if title is not None:
        await db.chats.delete_one({"_id": cid})
        MODERATED.pop(cid, None)
        sender = await event.get_sender()
        await send_log(f"{b('➖ Chat deactivated:')} {b(title)} {code(cid)} {b('by')} {label(sender)}")
    await chats_screen(event)


async def hide_chat(event, cid: int):
    ent = CANDIDATES.get(cid)
    HIDDEN.add(cid)
    await db.hidden.update_one({"_id": cid}, {"$set": {"title": ent.title if ent else str(cid)}}, upsert=True)
    await add_screen(event)


async def unhide_all(event):
    HIDDEN.clear()
    await db.hidden.delete_many({})
    await add_screen(event)


async def start_scan(event):
    asyncio.create_task(scan_all("manual"))
    await show(event, b("🔍 Scan started. Results will appear in the log channel."), [MENU_BTN])


async def ask_ids(event, mode: str):
    AWAIT[event.sender_id] = mode
    what = "ban" if mode == "ban" else "unban"
    text = b(f"Send the user ID(s) to {what} (one or many, separated by space or new line). A .txt file works too.")
    await show(event, text, [[Button.inline("❌ Cancel", b"menu")]])


async def do_ban(event, ids):
    if not ids:
        return await show(event, b("No valid user ID found."), [MENU_BTN])
    sender = await event.get_sender()
    now = int(time.time())
    await db.banned.bulk_write(
        [UpdateOne({"_id": i}, {"$setOnInsert": {"added_at": now}}, upsert=True) for i in ids]
    )
    BANNED.update(ids)
    await send_log(f"{b('📝 Ban command:')} {label(sender)} {b(f'- {len(ids)} ID(s)')}")
    await show(event, b(f"✅ {len(ids)} ID(s) added to the list. Banning in progress, results in the log channel."), [MENU_BTN])

    async def job():
        ok, fail = await ban_ids(ids)
        await scan_all("after ban")
        await send_log(b(f"✅ Ban finished: direct ok={ok}, not resolved yet={fail} (they will be caught on join or by scan)"))

    asyncio.create_task(job())


async def do_unban(event, ids):
    if not ids:
        return await show(event, b("No valid user ID found."), [MENU_BTN])
    sender = await event.get_sender()
    await db.banned.delete_many({"_id": {"$in": ids}})
    BANNED.difference_update(ids)
    await send_log(f"{b('📝 Unban command:')} {label(sender)} {b(f'- {len(ids)} ID(s)')}")
    await show(event, b(f"✅ {len(ids)} ID(s) removed from the list. Unbanning in progress, results in the log channel."), [MENU_BTN])
    asyncio.create_task(unban_ids(ids))


async def on_command(event):
    mode = AWAIT.pop(event.sender_id, None)
    f = event.file
    if f and f.name and f.name.lower().endswith((".txt", ".csv")) and (f.size or 0) < 5_000_000:
        data = (await event.message.download_media(file=bytes)).decode("utf-8", errors="ignore")
        ids = parse_ids(data)
        return await (do_unban(event, ids) if mode == "unban" else do_ban(event, ids))

    text = event.raw_text or ""
    m = re.match(r"^/(\w+)(?:@\w+)?\s*(.*)$", text, re.S)
    if not m:  # plain message: if we asked for ids, use them, otherwise just show the menu
        if mode in ("ban", "unban"):
            ids = parse_ids(text)
            return await (do_unban(event, ids) if mode == "unban" else do_ban(event, ids))
        return await menu(event)

    cmd, ids = m.group(1).lower(), parse_ids(m.group(2))
    if cmd == "ban":
        await (do_ban(event, ids) if ids else ask_ids(event, "ban"))
    elif cmd == "unban":
        await (do_unban(event, ids) if ids else ask_ids(event, "unban"))
    elif cmd == "check" and ids:
        msg = "is in the BANNED list" if ids[0] in BANNED else "is not in the list"
        await show(event, f"{code(ids[0])} {b(msg)}", [MENU_BTN])
    elif cmd == "list":
        await list_screen(event)
    elif cmd == "chats":
        await chats_screen(event)
    elif cmd == "stats":
        await stats_screen(event)
    elif cmd == "scan":
        await start_scan(event)
    elif cmd == "add":
        await add_screen(event)
    elif cmd in ("remove", "rm"):
        await remove_screen(event)
    else:
        await menu(event)


async def on_callback(event):
    data = event.data.decode()
    await event.answer()
    AWAIT.pop(event.sender_id, None)
    if data == "menu":
        await menu(event)
    elif data in ("ban", "unban"):
        await ask_ids(event, data)
    elif data == "chats":
        await chats_screen(event)
    elif data == "stats":
        await stats_screen(event)
    elif data == "list":
        await list_screen(event)
    elif data == "scan":
        await start_scan(event)
    elif data == "add":
        await add_screen(event)
    elif data == "remove":
        await remove_screen(event)
    elif data == "unhide":
        await unhide_all(event)
    elif data.startswith("add:"):
        await enable_chat(event, int(data[4:]))
    elif data.startswith("rm:"):
        await disable_chat(event, int(data[3:]))
    elif data.startswith("hide:"):
        await hide_chat(event, int(data[5:]))


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
            await scan_all("auto", quiet=True)
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
    ENABLED.update({d["_id"]: d.get("title", "") async for d in db.chats.find()})
    HIDDEN.update([d["_id"] async for d in db.hidden.find({}, {"_id": 1})])

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
    bot.add_event_handler(
        on_command,
        events.NewMessage(incoming=True, from_users=list(OWNER_IDS), func=lambda e: e.is_private),
    )
    bot.add_event_handler(on_callback, events.CallbackQuery(func=lambda e: e.sender_id in OWNER_IDS))

    chat_lines = [b(f"• {e.title}") for e in MODERATED.values()] or [b("• (no active chats - open Chats > Add chat in the bot)")]
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
