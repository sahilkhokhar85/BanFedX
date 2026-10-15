"""
Telegram haters-ban system (ONE process, ONE file)

  USER account (Telethon)  = engine : bans, scans members, declines join requests, posts logs
  BOT (Telethon)           = panel  : you control everything from the bot chat (commands + buttons)
  MongoDB                  = ban list + counters
  LOG channel              = every action is reported here
"""
import asyncio
import csv
import hashlib
import html
import io
import logging
import os
import re
import time

import httpx
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from telethon import Button, TelegramClient, errors, events, functions, types, utils
from telethon.sessions import StringSession

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logging.getLogger("telethon").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)  # httpx would print the bot token
logging.getLogger("httpcore").setLevel(logging.WARNING)
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
EXEMPT: dict[int, set[int]] = {}  # chat id -> users on the ban list who must NOT be banned in that chat
HASHES: dict[int, int] = {}  # banned user id -> access_hash learned by the account (lets us pre-ban non-members)
DONE: set = set()  # (chat id, user id) pairs already banned by us, so work is never repeated
PREBAN_Q = None  # users whose hash was just learned -> banned in all other chats
RESOLVE = {"running": False, "again": False}
LAST_ERR = {"ban": "", "decline": ""}  # last error name, shown in the log channel
RECENT: dict = {}  # (chat id, user id) -> time we last handled a join request (avoids repeats)
HTTP = None  # Bot API client (fallback pre-ban through the bot)
BOT_SKIP: dict = {}  # chat id -> time; chats where the bot has no ban rights (not retried for an hour)
BOT_INVALID: set = set()  # ids the Bot API cannot resolve either
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


def parse_file(text: str, fname: str):
    """Returns (ids, {id: username}). A CSV with an 'id' column is read column-wise."""
    if fname.lower().endswith(".csv"):
        try:
            rows = list(csv.DictReader(io.StringIO(text)))
            if rows and "id" in rows[0]:
                ids, names = {}, {}
                for r in rows:
                    raw = (r.get("id") or "").strip()
                    if raw.isdigit() and 5 <= len(raw) <= 11:
                        ids[int(raw)] = 1
                        un = (r.get("username") or "").strip().lstrip("@")
                        if un:
                            names[int(raw)] = un
                return list(ids), names
        except Exception:
            pass
    return parse_ids(text), {}


def is_target(entity, uid: int) -> bool:
    """True if this user must be banned in this chat: on the ban list and not exempted for this chat."""
    return uid in BANNED and uid not in EXEMPT.get(utils.get_peer_id(entity), ())


def parse_ids(text: str) -> list[int]:
    text = re.sub(r"@\w+", " ", text or "")  # usernames can contain digits, never treat them as ids
    return list({int(x) for x in re.findall(r"(?<!\d)\d{5,11}(?!\d)", text)})


def parse_pairs(text: str) -> dict:
    """'123456789 @name' pairs -> {123456789: 'name'}"""
    out = {}
    for m in re.finditer(r"(?<!\d)(\d{5,11})(?!\d)\s*[,:\-]?\s*@([A-Za-z0-9_]{4,32})", text or ""):
        out[int(m.group(1))] = m.group(2)
    return out


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
async def bot_api_ban(cid: int, uid: int) -> str:
    """Fallback pre-ban through the Bot API. Works for ids the Bot API can resolve, in chats where the
    bot is admin with Ban rights. Returns ok / invalid / skip / fail."""
    if HTTP is None or uid in BOT_INVALID or time.time() - BOT_SKIP.get(cid, 0) < 3600:
        return "skip"
    for _ in range(2):
        try:
            r = await HTTP.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/banChatMember",
                json={"chat_id": cid, "user_id": uid, "revoke_messages": False},
            )
            data = r.json()
        except Exception as e:
            LAST_ERR["botapi"] = type(e).__name__
            return "fail"
        if data.get("ok"):
            return "ok"
        desc = (data.get("description") or "").lower()
        if r.status_code == 429:
            await asyncio.sleep((data.get("parameters") or {}).get("retry_after", 5) + 1)
            continue
        if "participant_id_invalid" in desc or "user not found" in desc or "user_id_invalid" in desc:
            BOT_INVALID.add(uid)
            return "invalid"
        if any(k in desc for k in ("not enough rights", "administrator rights", "chat_admin_required",
                                   "chat not found", "not a member", "was kicked")):
            BOT_SKIP[cid] = time.time()
            return "skip"
        LAST_ERR["botapi"] = desc[:80]
        return "fail"
    return "fail"


async def mark_done(entity, uid):
    cid = utils.get_peer_id(entity)
    if uid is None or (cid, uid) in DONE:
        return
    DONE.add((cid, uid))
    await db.done.update_one({"_id": f"{cid}:{uid}"}, {"$set": {"chat": cid, "user": uid}}, upsert=True)


def remember(u):
    """Store the access_hash of a listed user the first time we see him anywhere.
    From then on he can be pre-banned in every chat, even where he never joined."""
    uid = getattr(u, "id", None)
    h = getattr(u, "access_hash", None)
    if uid in BANNED and h and not getattr(u, "min", False) and HASHES.get(uid) != h:
        is_new = uid not in HASHES
        HASHES[uid] = h
        BOT_INVALID.discard(uid)
        asyncio.create_task(db.banned.update_one({"_id": uid}, {"$set": {"hash": h}}))
        if is_new and PREBAN_Q is not None:
            PREBAN_Q.put_nowait(uid)


async def ban(entity, target) -> str:
    """target = User object or int id. Returns ok / invalid / forbidden / fail."""
    uid = target if isinstance(target, int) else (getattr(target, "id", None) or getattr(target, "user_id", None))
    if isinstance(target, int) and target in HASHES:
        target = types.InputPeerUser(target, HASHES[target])  # known hash: works even if never joined
    for _ in range(3):
        try:
            await user(functions.channels.EditBannedRequest(entity, target, FULL_BAN))
            await mark_done(entity, uid)
            return "ok"
        except errors.FloodWaitError as e:
            warn_once(("flood", entity.id), b(f"⏳ Telegram rate limit (FloodWait {e.seconds}s) - {entity.title}"), 600)
            await asyncio.sleep(e.seconds + 1)
        except (ValueError, errors.UserIdInvalidError, errors.PeerIdInvalidError) as e:
            LAST_ERR["ban"] = type(e).__name__
            if uid and await bot_api_ban(utils.get_peer_id(entity), uid) == "ok":  # try through the bot
                await mark_done(entity, uid)
                return "ok"
            return "invalid"  # unknown to the account (and to the bot)
        except (errors.ChatAdminRequiredError, errors.ChatWriteForbiddenError) as e:
            LAST_ERR["ban"] = type(e).__name__
            warn_once(("noright", entity.id), b(f"⚠️ No ban rights in: {entity.title}"))
            return "forbidden"
        except Exception as e:
            LAST_ERR["ban"] = f"{type(e).__name__}: {str(e)[:80]}"
            log.warning("ban failed in %s: %s", entity.title, e)
            return "fail"
    return "fail"


async def decline(entity, target) -> bool:
    try:
        await user(
            functions.messages.HideChatJoinRequestRequest(
                peer=entity, user_id=utils.get_input_user(target), approved=False
            )
        )
        return True
    except Exception as e:
        LAST_ERR["decline"] = type(e).__name__
        log.warning("decline failed: %s", e)
        return False


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
    now = time.time()
    for u in users:
        remember(u)
        if not is_target(entity, u.id):
            continue
        key = (utils.get_peer_id(entity), u.id)
        if now - RECENT.get(key, 0) < 600:
            continue  # handled a moment ago (request may still show as pending)
        RECENT[key] = now
        res = await ban(entity, u)  # ban first: even if declining fails, the user is out
        if res != "ok":
            await asyncio.sleep(3)
            try:  # one retry with the input peer cached from the request list
                res = await ban(entity, await user.get_input_entity(u.id))
            except Exception:
                pass
        declined = await decline(entity, u)
        where = b("in " + entity.title)
        if res == "ok":
            n += 1
            await bump("bans")
            if declined:
                await bump("declined")
                await send_log(f"{b('🚫 Join request declined + banned:')} {label(u)} {where}")
            else:
                await send_log(f"{b('🚫 Banned (request could not be declined: ' + LAST_ERR['decline'] + '):')} {label(u)} {where}")
        else:
            state = "declined" if declined else "NOT declined"
            await send_log(f"{b('⚠️ Join request ' + state + ' but NOT banned (' + LAST_ERR['ban'] + '):')} {label(u)} {where}")
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
            remember(u)
            if is_target(entity, u.id):
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
            await send_log_lines(b(f"✅ Scan finished ({int(time.time() - t0)}s)"), rows)


async def ent_label(uid: int) -> str:
    try:
        ref = types.InputPeerUser(uid, HASHES[uid]) if uid in HASHES else uid
        return label(await user.get_entity(ref))
    except Exception:
        return code(uid)


async def ban_ids(ids, detail: bool = True):
    """Ban every id in every active chat, even if the user never joined (pre-ban): through the account
    (stored hash) or, failing that, through the bot. Pairs already done are skipped.
    detail=False posts one summary line instead of names. Returns (bans done, set of ids that failed)."""
    ok = 0
    invalid = set()
    attempted = set()
    succeeded = set()
    for ent in list(MODERATED.values()):
        cid = utils.get_peer_id(ent)
        lines = []
        for uid in ids:
            if not is_target(ent, uid) or (cid, uid) in DONE or uid in invalid:
                continue
            attempted.add(uid)
            res = await ban(ent, uid)
            if res == "ok":
                ok += 1
                succeeded.add(uid)
                if detail:
                    lines.append("• " + await ent_label(uid))
            elif res == "invalid" and uid in BOT_INVALID:
                invalid.add(uid)  # unknown to the account AND to the Bot API: stop trying elsewhere
            if res != "invalid":
                await asyncio.sleep(RATE_DELAY)
        if lines:
            await bump("bans", len(lines))
        await send_log_lines(b(f"🚫 Banned - {ent.title}:"), lines)
    if ok and not detail:
        await bump("bans", ok)
        await send_log(b(f"🛡 Pre-ban: {ok} new ban(s) done in the background"))
    return ok, attempted - succeeded


# ---------- usernames -> access hashes (so IDs can be pre-banned) ----------
async def save_usernames(names: dict):
    await db.banned.bulk_write(
        [UpdateOne({"_id": i}, {"$set": {"username": u}, "$unset": {"name_bad": ""}}) for i, u in names.items()]
    )


async def lookup(uid: int, uname: str):
    """Find a user by @username, check it is really that id, store his hash. Returns (status, detail)."""
    try:
        ent = await user.get_entity(uname)
    except errors.FloodWaitError as e:
        return "flood", e.seconds
    except (ValueError, errors.UsernameNotOccupiedError, errors.UsernameInvalidError):
        return "notfound", ""
    except Exception as e:
        return "error", type(e).__name__
    if not isinstance(ent, types.User):
        return "mismatch", "not a user account"
    if ent.id != uid:
        return "mismatch", ent.id
    if not ent.access_hash:
        return "error", "no access hash"
    HASHES[uid] = ent.access_hash
    BOT_INVALID.discard(uid)
    await db.banned.update_one({"_id": uid}, {"$set": {"hash": ent.access_hash}})
    return "ok", ""


def lookup_reason(status: str, detail, uid: int, uname: str) -> str:
    if status == "notfound":
        return f"@{uname} does not exist (check the spelling)"
    if status == "mismatch":
        if detail == "not a user account":
            return f"@{uname} is not a user account"
        return f"@{uname} belongs to a different account ({detail}), not {uid}"
    if status == "flood":
        return f"Telegram limit while looking up @{uname}, try again in {detail}s"
    return f"@{uname}: lookup error ({detail})"


async def resolve_now(names: dict) -> list:
    """Immediate lookup for usernames given together with /ban. Returns readable result lines."""
    lines = []
    for uid, uname in names.items():
        status, detail = await lookup(uid, uname)
        if status == "ok":
            lines.append(b(f"🔎 @{uname} found:") + " " + code(uid))
        else:
            if status in ("notfound", "mismatch"):
                await db.banned.update_one({"_id": uid}, {"$set": {"name_bad": True}})
            lines.append(b("⚠️ " + lookup_reason(status, detail, uid, uname)))
        await asyncio.sleep(1)
    return lines


async def resolve_pass():
    todo = [
        d
        async for d in db.banned.find(
            {"username": {"$exists": True, "$ne": ""}, "hash": {"$exists": False}, "name_bad": {"$exists": False}},
            {"username": 1},
        )
    ]
    ok = 0
    fails = []
    fresh = []
    for d in todo:
        uid, uname = d["_id"], d["username"]
        status, detail = await lookup(uid, uname)
        if status == "ok":
            ok += 1
            fresh.append(uid)
        elif status in ("notfound", "mismatch"):
            await db.banned.update_one({"_id": uid}, {"$set": {"name_bad": True}})
            fails.append(f"• {code(uid)} {b(lookup_reason(status, detail, uid, uname))}")
        elif status == "flood":
            if detail > 900:
                break  # too long: the rest is retried on the next start
            await asyncio.sleep(detail + 1)
        else:
            log.warning("username lookup failed: %s %s", uname, detail)
        await asyncio.sleep(2)
    if todo:
        left = len(todo) - ok - len(fails)
        header = b(f"🔎 Username lookup: {ok} found, {len(fails)} failed, {left} left for later")
        if fails:
            await send_log_lines(header, fails[:25])
        else:
            await send_log(header)
    if fresh:
        await ban_ids(fresh, detail=False)


async def resolve_usernames():
    """Bulk lookup of all saved usernames. A call made while it is running makes it run once more after."""
    if RESOLVE["running"]:
        RESOLVE["again"] = True
        return
    RESOLVE["running"] = True
    try:
        while True:
            RESOLVE["again"] = False
            await resolve_pass()
            if not RESOLVE["again"]:
                break
    finally:
        RESOLVE["running"] = False


async def preban_worker():
    """Users whose hash was just learned (e.g. seen in a scan) get banned in all other chats too."""
    while True:
        batch = [await PREBAN_Q.get()]
        await asyncio.sleep(10)
        while not PREBAN_Q.empty():
            batch.append(PREBAN_Q.get_nowait())
        try:
            await ban_ids(batch, detail=False)
        except Exception as e:
            log.warning("preban failed: %s", e)


async def startup_prep():
    await asyncio.sleep(20)
    try:
        await resolve_usernames()
        await ban_ids(list(BANNED), detail=False)
    except Exception as e:
        log.warning("startup prep failed: %s", e)


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


async def on_private_message(event):
    """A private chat with a listed user (he writes to the account, or you message him from it) teaches the
    account his access hash. He can then be banned in every chat, even where he never joined."""
    if event.chat_id in BANNED:
        try:
            remember(await event.get_chat())
        except Exception:
            pass


async def on_chat_action(event):
    if not (event.user_joined or event.user_added):
        return
    ent = MODERATED.get(event.chat_id)
    if not ent:
        return
    for u in await event.get_users():
        remember(u)
        if is_target(ent, u.id) and await ban(ent, u) == "ok":
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
        [Button.inline("🛡 Exceptions", b"exc")],
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
        ("Exceptions", str(sum(len(v) for v in EXEMPT.values()))),
        ("Pre-bannable IDs", f"{sum(1 for i in HASHES if i in BANNED)}/{len(BANNED)}"),
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
    text = b(f"📄 Banned list: {len(BANNED)} IDs in total (showing last 50)") + "\n" + "\n".join(code(i) for i in rows)
    await show(event, text, [[Button.inline("📥 Export all", b"export")], MENU_BTN])


async def export_list(event):
    """Send the full ban list as a .txt file (one ID per line)."""
    ids = [d["_id"] async for d in db.banned.find().sort("added_at", 1)]
    if not ids:
        return await show(event, b("The ban list is empty."), [MENU_BTN])
    f = io.BytesIO("\n".join(map(str, ids)).encode())
    f.name = "banned_ids.txt"
    await bot.send_file(event.chat_id, f, caption=b(f"📥 Banned list: {len(ids)} IDs"), parse_mode="html", buttons=[MENU_BTN])


PAGE = 10  # chats per page (Telegram allows max ~100 buttons per message)


def nav_row(page: int, pages: int, prefix: str) -> list:
    if pages <= 1:
        return []
    row = []
    if page > 0:
        row.append(Button.inline("◀️ Prev", f"{prefix}:{page - 1}".encode()))
    row.append(Button.inline(f"{page + 1}/{pages}", b"noop"))
    if page < pages - 1:
        row.append(Button.inline("Next ▶️", f"{prefix}:{page + 1}".encode()))
    return row


def paginate(items: list, page: int):
    pages = max(1, -(-len(items) // PAGE))
    page = max(0, min(page, pages - 1))
    return items[page * PAGE:(page + 1) * PAGE], page, pages


async def chats_screen(event, page: int = 0):
    items = list(MODERATED.items())
    nav = []
    if items:
        chunk, page, pages = paginate(items, page)
        title = f"🟢 Active chats: {len(items)}" + (f"  (page {page + 1}/{pages})" if pages > 1 else "")
        text = "\n".join([b(title)] + [f"{b(e.title)} {code(c)}" for c, e in chunk])
        nav = nav_row(page, pages, "chp")
    else:
        text = b("No active chats yet. Tap ➕ Add chat.")
    buttons = ([nav] if nav else []) + [
        [Button.inline("➕ Add chat", b"add"), Button.inline("➖ Remove chat", b"remove")],
        MENU_BTN,
    ]
    await show(event, text, buttons)


async def add_screen(event, page: int = 0):
    if page == 0:
        await refresh_chats(initial=True)  # pick up chats where the account just became admin
    cands = [(c, e) for c, e in CANDIDATES.items() if c not in ENABLED and c not in HIDDEN]
    chunk, page, pages = paginate(cands, page)
    rows = [
        [Button.inline(e.title[:35], f"add:{c}".encode()), Button.inline("🗑", f"hide:{c}".encode())]
        for c, e in chunk
    ]
    nav = nav_row(page, pages, "addp")
    if nav:
        rows.append(nav)
    if cands:
        text = b("Tap a chat to activate it. 🗑 hides it from this list forever:")
    else:
        text = b("No chats found. Make the account an admin there (Ban users right), then tap again.")
    if HIDDEN:
        rows.append([Button.inline("♻️ Show hidden chats", b"unhide")])
    rows.append([Button.inline("⬅️ Back", b"chats")])
    await show(event, text, rows)


async def remove_screen(event, page: int = 0):
    items = list(ENABLED.items())
    chunk, page, pages = paginate(items, page)
    rows = [[Button.inline(t[:35], f"rm:{c}".encode())] for c, t in chunk]
    nav = nav_row(page, pages, "rmp")
    if nav:
        rows.append(nav)
    text = b("Which chat should be deactivated? Tap one:") if items else b("No active chats.")
    rows.append([Button.inline("⬅️ Back", b"chats")])
    await show(event, text, rows)


async def enable_chat(event, cid: int):
    ent = CANDIDATES.get(cid)
    if ent:
        ENABLED[cid] = ent.title
        await db.chats.update_one({"_id": cid}, {"$set": {"title": ent.title}}, upsert=True)
        MODERATED[cid] = ent
        sender = await event.get_sender()
        await send_log(f"{b('➕ Chat activated:')} {b(ent.title)} {code(cid)} {b('by')} {label(sender)}")
        asyncio.create_task(scan_chat(ent))
        asyncio.create_task(ban_ids(list(BANNED), detail=False))
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
    if mode == "ban":
        text += "\n" + b("Tip: add the @username after an ID (e.g. 123456789 @name) so he can be banned even if he never joined.")
    await show(event, text, [[Button.inline("❌ Cancel", b"menu")]])


async def do_ban(event, ids, names=None):
    if not ids:
        return await show(event, b("No valid user ID found. Send numeric IDs like 123456789."), [[Button.inline("🔁 Try again", b"ban")], MENU_BTN])
    names = {i: u for i, u in (names or {}).items() if i in ids}
    new = [i for i in ids if i not in BANNED]
    already = [i for i in ids if i in BANNED]
    retry = [i for i in already if i in names]  # already listed, but a username was given: try again now
    more_btn = [[Button.inline("🚫 Ban more", b"ban")], MENU_BTN]

    parts = []
    if new:
        parts.append(b(f"✅ {len(new)} new ID(s) added. Banning in progress, results in the log channel."))
    if already:
        shown = " ".join(code(i) for i in already[:10])
        more = b(f" +{len(already) - 10} more") if len(already) > 10 else ""
        parts.append(b(f"ℹ️ {len(already)} already in the list:") + "\n" + shown + more)
    if retry:
        parts.append(b(f"🔁 Using the username to ban {len(retry)} listed ID(s) now."))
    if not new and not retry:
        parts.append(b("Nothing to add. The automatic scan keeps checking them."))
        return await show(event, "\n\n".join(parts), more_btn)

    sender = await event.get_sender()
    if new:
        now = int(time.time())
        await db.banned.bulk_write(
            [UpdateOne({"_id": i}, {"$setOnInsert": {"added_at": now}}, upsert=True) for i in new]
        )
        BANNED.update(new)
        skipped = f", {len(already)} already listed" if already else ""
        await send_log(f"{b('📝 Ban command:')} {label(sender)} {b(f'- {len(new)} new ID(s){skipped}')}")
    if names:
        await save_usernames(names)
    await show(event, "\n\n".join(parts), more_btn)

    targets = new + retry

    async def job():
        notes = await resolve_now(names) if names else []  # look usernames up first, so the ban can use them
        if notes:
            await send_log_lines(b("🔎 Username lookup:"), notes)
        ok, failed = await ban_ids(targets)
        pending = sorted(i for i in failed if i not in HASHES)
        text = b(f"✅ Ban finished: {ok} ban(s) done")
        if pending:
            text += "\n" + b(
                f"⚠️ {len(pending)} ID(s) cannot be banned yet (last error: {LAST_ERR['ban'] or 'unknown id'}). "
                "They are banned automatically the moment they send a join request, join, or show up in a scan. "
                "To ban them now, send the ID with the correct @username (e.g. 123456789 @name)."
            ) + "\n" + " ".join(code(i) for i in pending[:30])
        await send_log(text)
        if names:  # the owner also gets the result here, not only in the log channel
            reply = notes + [b(f"✅ {ok} ban(s) done")]
            if pending:
                reply.append(b(f"⚠️ {len(pending)} ID(s) could not be banned yet. Details are in the log channel."))
            await event.respond("\n".join(reply), parse_mode="html", buttons=[MENU_BTN])
        await scan_all("after ban")

    asyncio.create_task(job())


async def do_unban(event, ids):
    if not ids:
        return await show(event, b("No valid user ID found. Send numeric IDs like 123456789."), [[Button.inline("🔁 Try again", b"unban")], MENU_BTN])
    sender = await event.get_sender()
    await db.banned.delete_many({"_id": {"$in": ids}})
    await db.done.delete_many({"user": {"$in": ids}})
    BANNED.difference_update(ids)
    DONE.difference_update({p for p in DONE if p[1] in set(ids)})
    for i in ids:
        HASHES.pop(i, None)
    await send_log(f"{b('📝 Unban command:')} {label(sender)} {b(f'- {len(ids)} ID(s)')}")
    await show(event, b(f"✅ {len(ids)} ID(s) removed from the list. Unbanning in progress, results in the log channel."), [[Button.inline("✅ Unban more", b"unban")], MENU_BTN])
    asyncio.create_task(unban_ids(ids))


# ---------- exceptions: users on the ban list who must NOT be banned in a specific chat ----------
async def unban_in_chat(ent, ids):
    idset = set(ids)
    lines = []
    try:
        async for u in user.iter_participants(ent, filter=types.ChannelParticipantsKicked):
            if u.id in idset:
                await user(functions.channels.EditBannedRequest(ent, u, NO_BAN))
                lines.append("• " + label(u))
                await asyncio.sleep(RATE_DELAY)
    except Exception as e:
        warn_once(("unban-chat", ent.id), b(f"⚠️ {ent.title}: could not read banned list ({type(e).__name__})"))
    await send_log_lines(b(f"🔓 Exception - unbanned in {ent.title}:"), lines)


async def exc_chats_screen(event, page: int = 0):
    if not MODERATED:
        return await show(event, b("No active chats yet. Add a chat first."), [MENU_BTN])
    chunk, page, pages = paginate(list(MODERATED.items()), page)
    rows = [
        [Button.inline(f"{e.title[:28]} ({len(EXEMPT.get(c, ()))})", f"exc:{c}".encode())]
        for c, e in chunk
    ]
    nav = nav_row(page, pages, "excp")
    if nav:
        rows.append(nav)
    rows.append(MENU_BTN)
    text = "\n".join(
        [
            "🛡 " + b("Exceptions"),
            b("Users on the ban list who should NOT be banned in a specific chat."),
            b("Pick a chat:"),
        ]
    )
    await show(event, text, rows)


async def exc_chat_screen(event, cid: int, note: str = ""):
    ent = MODERATED.get(cid)
    if not ent:
        return await show(event, b("This chat is not active."), [[Button.inline("⬅️ Back", b"exc")], MENU_BTN])
    ids = sorted(EXEMPT.get(cid, ()))
    lines = [note, ""] if note else []
    lines.append("🛡 " + b(f"Exceptions - {ent.title}"))
    if ids:
        lines.append(b(f"{len(ids)} user(s) are NOT banned here:"))
        lines += [code(i) for i in ids[:40]]
    else:
        lines.append(b("No exceptions. Everyone on the ban list is banned here."))
    first = [Button.inline("➕ Add exception", f"excadd:{cid}".encode())]
    if ids:
        first.append(Button.inline("➖ Remove", f"excrm:{cid}".encode()))
    buttons = [first, [Button.inline("⬅️ Back", b"exc"), Button.inline("⬅️ Menu", b"menu")]]
    await show(event, "\n".join(lines), buttons)


async def exc_remove_screen(event, cid: int):
    ids = sorted(EXEMPT.get(cid, ()))
    rows = [[Button.inline(f"🗑 {i}", f"excdel:{cid}:{i}".encode())] for i in ids[:50]]
    rows.append([Button.inline("⬅️ Back", f"exc:{cid}".encode())])
    text = b("Tap an ID to remove its exception (the user will be banned in this chat again):")
    await show(event, text, rows)


async def ask_exc(event, cid: int):
    ent = MODERATED.get(cid)
    if not ent:
        return await show(event, b("This chat is not active."), [MENU_BTN])
    AWAIT[event.sender_id] = f"exc:{cid}"
    text = b(f"Send the user ID(s) that must NOT be banned in {ent.title} (one or many). A .txt file works too.")
    await show(event, text, [[Button.inline("❌ Cancel", f"exc:{cid}".encode())]])


async def do_exempt(event, cid: int, ids):
    ent = MODERATED.get(cid)
    if not ent:
        return await show(event, b("This chat is not active."), [MENU_BTN])
    if not ids:
        buttons = [[Button.inline("🔁 Try again", f"excadd:{cid}".encode())], [Button.inline("⬅️ Back", f"exc:{cid}".encode())]]
        return await show(event, b("No valid user ID found. Send numeric IDs like 123456789."), buttons)
    EXEMPT.setdefault(cid, set()).update(ids)
    DONE.difference_update({(cid, i) for i in ids})
    await db.done.delete_many({"_id": {"$in": [f"{cid}:{i}" for i in ids]}})
    await db.exempt.bulk_write(
        [UpdateOne({"_id": f"{cid}:{i}"}, {"$set": {"chat": cid, "user": i}}, upsert=True) for i in ids]
    )
    sender = await event.get_sender()
    head = f"{b('🛡 Exception added:')} {label(sender)} {b('- ' + str(len(ids)) + ' ID(s) in ' + ent.title)}"
    await send_log(head + "\n" + " ".join(code(i) for i in ids[:20]))
    asyncio.create_task(unban_in_chat(ent, ids))  # lift any existing ban in this chat
    note = b(f"✅ {len(ids)} ID(s) will NOT be banned in this chat. Existing bans here are being lifted.")
    await exc_chat_screen(event, cid, note=note)


async def del_exception(event, cid: int, uid: int):
    EXEMPT.get(cid, set()).discard(uid)
    await db.exempt.delete_one({"_id": f"{cid}:{uid}"})
    ent = MODERATED.get(cid)
    title = ent.title if ent else str(cid)
    sender = await event.get_sender()
    await send_log(f"{b('🛡 Exception removed:')} {label(sender)} {b('-')} {code(uid)} {b('in ' + title)}")
    if ent and uid in BANNED:
        if await ban(ent, uid) == "ok":
            await bump("bans")
            await send_log(f"{b('🚫 Banned again (exception removed):')} {code(uid)} {b('in ' + title)}")
        else:
            asyncio.create_task(scan_chat(ent))  # not resolvable by id: find them among members
    await exc_chat_screen(event, cid, note=b("✅ Exception removed. This user is banned in this chat again."))


async def dispatch_ids(event, mode, ids, names=None):
    if mode == "unban":
        return await do_unban(event, ids)
    if mode and mode.startswith("exc:"):
        return await do_exempt(event, int(mode[4:]), ids)
    return await do_ban(event, ids, names)


async def on_command(event):
    mode = AWAIT.pop(event.sender_id, None)
    f = event.file
    if f and f.name and f.name.lower().endswith((".txt", ".csv")) and (f.size or 0) < 5_000_000:
        data = (await event.message.download_media(file=bytes)).decode("utf-8", errors="ignore")
        ids, names = parse_file(data, f.name)
        await dispatch_ids(event, mode, ids)
        if names and mode in (None, "ban"):
            await save_usernames(names)
            asyncio.create_task(resolve_usernames())
            await event.respond(
                b(f"🔎 {len(names)} usernames saved. The account will look them up so those users can be banned in every chat even if they never joined. Progress is posted in the log channel."),
                parse_mode="html",
            )
        return

    text = event.raw_text or ""
    m = re.match(r"^/(\w+)(?:@\w+)?\s*(.*)$", text, re.S)
    if not m:  # plain message: if we asked for ids, use them, otherwise just show the menu
        if mode:
            return await dispatch_ids(event, mode, parse_ids(text), parse_pairs(text) if mode == "ban" else None)
        return await menu(event)

    cmd, ids = m.group(1).lower(), parse_ids(m.group(2))
    if cmd == "ban":
        if ids:
            await do_ban(event, ids, parse_pairs(m.group(2)))
        else:
            await ask_ids(event, "ban")
    elif cmd == "unban":
        await (do_unban(event, ids) if ids else ask_ids(event, "unban"))
    elif cmd == "check" and ids:
        msg = "is in the BANNED list" if ids[0] in BANNED else "is not in the list"
        text = f"{code(ids[0])} {b(msg)}"
        ex = [MODERATED[c].title for c, s in EXEMPT.items() if ids[0] in s and c in MODERATED]
        if ex:
            text += "\n" + b("Not banned in: " + ", ".join(ex))
        await show(event, text, [MENU_BTN])
    elif cmd == "list":
        await list_screen(event)
    elif cmd == "chats":
        await chats_screen(event)
    elif cmd == "stats":
        await stats_screen(event)
    elif cmd == "export":
        await export_list(event)
    elif cmd in ("exceptions", "exc"):
        await exc_chats_screen(event)
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
    elif data == "export":
        await export_list(event)
    elif data == "scan":
        await start_scan(event)
    elif data == "add":
        await add_screen(event)
    elif data == "remove":
        await remove_screen(event)
    elif data == "unhide":
        await unhide_all(event)
    elif data == "noop":
        pass
    elif data.startswith("chp:"):
        await chats_screen(event, int(data[4:]))
    elif data.startswith("addp:"):
        await add_screen(event, int(data[5:]))
    elif data.startswith("rmp:"):
        await remove_screen(event, int(data[4:]))
    elif data.startswith("excp:"):
        await exc_chats_screen(event, int(data[5:]))
    elif data == "exc":
        await exc_chats_screen(event)
    elif data.startswith("exc:"):
        await exc_chat_screen(event, int(data[4:]))
    elif data.startswith("excadd:"):
        await ask_exc(event, int(data[7:]))
    elif data.startswith("excrm:"):
        await exc_remove_screen(event, int(data[6:]))
    elif data.startswith("excdel:"):
        _, c, u = data.split(":")
        await del_exception(event, int(c), int(u))
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
    global user, bot, db, SCAN_LOCK, PREBAN_Q, HTTP
    HTTP = httpx.AsyncClient(timeout=20)
    SCAN_LOCK = asyncio.Lock()
    PREBAN_Q = asyncio.Queue()
    db = AsyncIOMotorClient(MONGO_URI)[MONGO_DB]
    BANNED.update([d["_id"] async for d in db.banned.find({}, {"_id": 1})])
    await seed()
    ENABLED.update({d["_id"]: d.get("title", "") async for d in db.chats.find()})
    HIDDEN.update([d["_id"] async for d in db.hidden.find({}, {"_id": 1})])
    async for d in db.exempt.find():
        EXEMPT.setdefault(d["chat"], set()).add(d["user"])
    async for d in db.banned.find({"hash": {"$exists": True}}, {"hash": 1}):
        HASHES[d["_id"]] = d["hash"]
    async for d in db.done.find():
        DONE.add((d["chat"], d["user"]))

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
    user.add_event_handler(on_private_message, events.NewMessage(func=lambda e: e.is_private))
    bot.add_event_handler(
        on_command,
        events.NewMessage(incoming=True, from_users=list(OWNER_IDS), func=lambda e: e.is_private),
    )
    bot.add_event_handler(on_callback, events.CallbackQuery(func=lambda e: e.sender_id in OWNER_IDS))

    chat_lines = [b(f"• {e.title}") for e in MODERATED.values()] or [b("• (no active chats - open Chats > Add chat in the bot)")]
    await send_log_lines("\n".join([b("🟢 System online"), b(f"Banned IDs: {len(BANNED)}"), b(f"Chats: {len(MODERATED)}")]), chat_lines)
    log.info("System ready: %s banned ids, %s chats", len(BANNED), len(MODERATED))

    asyncio.create_task(scan_all("startup"))
    asyncio.create_task(startup_prep())
    asyncio.create_task(preban_worker())
    asyncio.create_task(refresh_loop())
    asyncio.create_task(pending_loop())
    asyncio.create_task(report_loop())
    if SCAN_MINUTES > 0:
        asyncio.create_task(scan_loop())
    await asyncio.gather(user.run_until_disconnected(), bot.run_until_disconnected())


if __name__ == "__main__":
    asyncio.run(main())
