import asyncio
import hashlib
import logging
import os
import re
import time

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from telegram import Update
from telegram.constants import ChatMemberStatus, ChatType
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    ChatJoinRequestHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)  # httpx logs would print the bot token
logging.getLogger("httpcore").setLevel(logging.WARNING)
log = logging.getLogger("banbot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_IDS = {int(x) for x in os.environ.get("OWNER_IDS", "").replace(" ", "").split(",") if x}
MONGO_URI = os.environ.get("MONGO_URI") or os.environ["MONGO_URL"]
MONGO_DB = os.environ.get("MONGO_DB", "banbot")
SEED_FILE = os.environ.get("SEED_FILE", "ids.txt")
SWEEP_MINUTES = int(os.environ.get("SWEEP_MINUTES", "0"))  # 0 = off
LOG_CHAT_ID = os.environ.get("LOG_CHAT_ID")  # optional: chat id where bot posts logs

RATE_DELAY = 0.06  # stay under Telegram's ~30 req/sec

db = None  # set in post_init (needs the running event loop)
BANNED: set[int] = set()  # in-memory cache for instant checks
CHATS: dict[int, str] = {}


def parse_ids(text: str) -> list[int]:
    # Telegram user ids are at most 11 digits; longer numbers (phone numbers etc.) are ignored
    return list({int(x) for x in re.findall(r"\d{5,11}(?!\d)", text or "")})


# ---------- Startup ----------
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


async def post_init(app: Application):
    global db
    db = AsyncIOMotorClient(MONGO_URI)[MONGO_DB]
    BANNED.update([d["_id"] async for d in db.banned.find({}, {"_id": 1})])
    CHATS.update({d["_id"]: d.get("title", "") async for d in db.chats.find()})
    if LOG_CHAT_ID:  # log channel is not a moderated chat
        CHATS.pop(int(LOG_CHAT_ID), None)
        await db.chats.delete_one({"_id": int(LOG_CHAT_ID)})
    await seed()
    log.info("Loaded %s banned ids, %s chats", len(BANNED), len(CHATS))


# ---------- Telegram helpers ----------
def fmt_user(u) -> str:
    name = (u.full_name or "").strip()
    uname = f" @{u.username}" if u.username else ""
    return f"{name}{uname} [{u.id}]".strip()


async def user_label(bot, chat_id: int, uid: int):
    """Returns (readable label, status in chat). Falls back to just the id."""
    try:
        cm = await bot.get_chat_member(chat_id, uid)
        return fmt_user(cm.user), cm.status
    except TelegramError:
        return f"[{uid}]", None


async def try_ban(bot, chat_id: int, uid: int) -> str:
    """Returns 'ok', 'invalid' (id unknown to Telegram/bot), 'forbidden' or 'fail'."""
    for _ in range(3):
        try:
            await bot.ban_chat_member(chat_id, uid, revoke_messages=False)
            return "ok"
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
        except Forbidden:
            log.warning("No rights in chat %s", chat_id)
            return "forbidden"
        except BadRequest as e:
            if "participant_id_invalid" in str(e).lower():
                return "invalid"
            log.warning("ban %s in %s failed: %s", uid, chat_id, e)
            return "fail"
        except TelegramError as e:
            log.warning("ban %s in %s failed: %s", uid, chat_id, e)
            return "fail"
    return "fail"


async def ban_in_chat(bot, chat_id: int, uid: int) -> bool:
    return await try_ban(bot, chat_id, uid) == "ok"


async def send_log(bot, text: str):
    if LOG_CHAT_ID:
        try:
            await bot.send_message(int(LOG_CHAT_ID), text)
        except TelegramError as e:
            log.warning("log send failed: %s", e)


async def send_log_lines(bot, header: str, lines: list):
    if not lines:
        return
    chunk = header
    for line in lines:
        if len(chunk) + len(line) + 1 > 3800:
            await send_log(bot, chunk)
            chunk = header + " (contd.)"
        chunk += "\n" + line
    await send_log(bot, chunk)


async def ban_everywhere(bot, uids, chat_ids=None, notify=False) -> tuple[int, int]:
    """Ban every uid in every chat (works even if the user is not a member).
    notify=True posts the list of banned users per chat to the log channel."""
    chat_ids = list(CHATS) if chat_ids is None else chat_ids
    ok = fail = 0
    invalid = set()  # ids Telegram can't resolve: don't retry them in other chats this run
    for chat_id in chat_ids:
        lines = []
        for uid in uids:
            if uid in invalid:
                fail += 1
                continue
            res = await try_ban(bot, chat_id, uid)
            if res == "ok":
                ok += 1
                if notify:
                    label, _ = await user_label(bot, chat_id, uid)
                    lines.append("• " + label)
            else:
                fail += 1
                if res == "invalid":
                    invalid.add(uid)
            await asyncio.sleep(RATE_DELAY)
        if notify:
            await send_log_lines(bot, f"🚫 Banned in {CHATS.get(chat_id, chat_id)}:", lines)
    return ok, fail


# ---------- Auto-moderation ----------
async def on_join_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    req = update.chat_join_request
    uid = req.from_user.id
    if uid in BANNED:
        try:
            await req.decline()
        except TelegramError as e:
            log.warning("decline failed: %s", e)
        await ban_in_chat(ctx.bot, req.chat.id, uid)
        await send_log(ctx.bot, f"🚫 Join request declined + banned: {fmt_user(req.from_user)} in {req.chat.title}")


async def on_chat_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cm = update.chat_member
    uid = cm.new_chat_member.user.id
    if uid in BANNED and cm.new_chat_member.status in (ChatMemberStatus.MEMBER, ChatMemberStatus.RESTRICTED):
        if await ban_in_chat(ctx.bot, cm.chat.id, uid):
            await send_log(ctx.bot, f"🚫 Banned on join: {fmt_user(cm.new_chat_member.user)} in {cm.chat.title}")


async def on_my_chat_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Auto-register a chat when the bot is made admin; remove it when the bot is removed."""
    m = update.my_chat_member
    chat = m.chat
    if chat.type == ChatType.PRIVATE or str(chat.id) == str(LOG_CHAT_ID):
        return
    if m.new_chat_member.status == ChatMemberStatus.ADMINISTRATOR:
        await register_chat(ctx, chat.id, chat.title)
    else:
        CHATS.pop(chat.id, None)
        await db.chats.delete_one({"_id": chat.id})


async def register_chat(ctx, chat_id: int, title: str):
    CHATS[chat_id] = title
    await db.chats.update_one({"_id": chat_id}, {"$set": {"title": title}}, upsert=True)

    async def job():
        ok, fail = await ban_everywhere(ctx.bot, list(BANNED), [chat_id], notify=True)
        await send_log(ctx.bot, f"✅ Added to {title}. Synced {len(BANNED)} bans (ok={ok}, fail={fail})")

    ctx.application.create_task(job())


async def on_group_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Catches already-joined banned users the moment they talk, plus service-message joins."""
    msg = update.effective_message
    if not msg:
        return
    chat = update.effective_chat
    targets = []
    if msg.from_user and msg.from_user.id in BANNED:
        targets.append(msg.from_user)
    targets += [u for u in (msg.new_chat_members or []) if u.id in BANNED]
    for u in targets:
        await ban_in_chat(ctx.bot, chat.id, u.id)
        try:
            await msg.delete()
        except TelegramError:
            pass
        await send_log(ctx.bot, f"🚫 Banned (seen active): {fmt_user(u)} in {chat.title}")


# ---------- Owner commands (private chat only) ----------
def owner_only(fn):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user and update.effective_user.id in OWNER_IDS:
            return await fn(update, ctx)
    return wrapper


@owner_only
async def cmd_start(update, ctx):
    await update.message.reply_text(
        "Commands:\n"
        "/ban <ids> - ban everywhere\n"
        "/unban <ids>\n"
        "/check <id>\n"
        "/list - count + last 50\n"
        "/chats - chats where I'm admin\n"
        "/addchat <chat_id> - manually register a chat\n"
        "/sync - re-ban whole list in all chats\n"
        ".txt file bhejo = bulk ban"
    )


async def _do_ban(update, ctx, ids):
    if not ids:
        await update.message.reply_text("Koi valid user id nahi mili.")
        return
    now = int(time.time())
    await db.banned.bulk_write(
        [UpdateOne({"_id": i}, {"$setOnInsert": {"added_at": now}}, upsert=True) for i in ids]
    )
    BANNED.update(ids)
    await update.message.reply_text(f"{len(ids)} id(s) add. {len(CHATS)} chats me ban ho rahe hain...")

    async def job():
        ok, fail = await ban_everywhere(ctx.bot, ids, notify=True)
        await update.message.reply_text(f"✅ Done. ok={ok}, fail={fail}")

    ctx.application.create_task(job())


@owner_only
async def cmd_ban(update, ctx):
    await _do_ban(update, ctx, parse_ids(" ".join(ctx.args)))


@owner_only
async def on_document(update, ctx):
    doc = update.message.document
    if not (doc.file_name or "").lower().endswith((".txt", ".csv")) or doc.file_size > 5_000_000:
        return
    f = await doc.get_file()
    data = (await f.download_as_bytearray()).decode("utf-8", errors="ignore")
    await _do_ban(update, ctx, parse_ids(data))


@owner_only
async def cmd_unban(update, ctx):
    ids = parse_ids(" ".join(ctx.args))
    await db.banned.delete_many({"_id": {"$in": ids}})
    BANNED.difference_update(ids)
    for chat_id in list(CHATS):
        lines = []
        for uid in ids:
            label, status = await user_label(ctx.bot, chat_id, uid)
            try:
                await ctx.bot.unban_chat_member(chat_id, uid, only_if_banned=True)
                if status == ChatMemberStatus.BANNED:
                    lines.append("• " + label)
            except TelegramError:
                pass
            await asyncio.sleep(RATE_DELAY)
        await send_log_lines(ctx.bot, f"✅ Unbanned in {CHATS.get(chat_id, chat_id)}:", lines)
    await update.message.reply_text(f"{len(ids)} id(s) unban + list se hata di.")


@owner_only
async def cmd_check(update, ctx):
    ids = parse_ids(" ".join(ctx.args))
    if ids:
        await update.message.reply_text(f"{ids[0]}: {'BANNED list me hai' if ids[0] in BANNED else 'list me nahi hai'}")


@owner_only
async def cmd_list(update, ctx):
    rows = [d["_id"] async for d in db.banned.find().sort("added_at", -1).limit(50)]
    await update.message.reply_text(f"Total: {len(BANNED)}\n" + "\n".join(map(str, rows)))


@owner_only
async def cmd_chats(update, ctx):
    await update.message.reply_text(f"{len(CHATS)} chats:\n" + "\n".join(f"{t} ({c})" for c, t in CHATS.items()))


@owner_only
async def cmd_addchat(update, ctx):
    try:
        chat_id = int(ctx.args[0])
        chat = await ctx.bot.get_chat(chat_id)
        me = await ctx.bot.get_chat_member(chat_id, ctx.bot.id)
        if me.status != ChatMemberStatus.ADMINISTRATOR:
            await update.message.reply_text("Bot is chat me admin nahi hai.")
            return
        await register_chat(ctx, chat_id, chat.title)
        await update.message.reply_text(f"Added: {chat.title}")
    except (IndexError, ValueError, TelegramError) as e:
        await update.message.reply_text(f"Usage: /addchat -100xxxxxxxxxx ({e})")


@owner_only
async def cmd_sync(update, ctx):
    await update.message.reply_text("Full sync shuru...")

    async def job():
        ok, fail = await ban_everywhere(ctx.bot, list(BANNED))
        await update.message.reply_text(f"✅ Sync done. ok={ok}, fail={fail}")

    ctx.application.create_task(job())


async def sweep(ctx: ContextTypes.DEFAULT_TYPE):
    if BANNED and CHATS:
        await ban_everywhere(ctx.bot, list(BANNED))
        log.info("Sweep done: %s ids x %s chats", len(BANNED), len(CHATS))


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    private = filters.ChatType.PRIVATE

    app.add_handler(CommandHandler("start", cmd_start, private))
    app.add_handler(CommandHandler("ban", cmd_ban, private))
    app.add_handler(CommandHandler("unban", cmd_unban, private))
    app.add_handler(CommandHandler("check", cmd_check, private))
    app.add_handler(CommandHandler("list", cmd_list, private))
    app.add_handler(CommandHandler("chats", cmd_chats, private))
    app.add_handler(CommandHandler("addchat", cmd_addchat, private))
    app.add_handler(CommandHandler("sync", cmd_sync, private))
    app.add_handler(MessageHandler(filters.Document.ALL & private, on_document))

    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ChatType.GROUPS, on_group_message))

    if SWEEP_MINUTES > 0:
        app.job_queue.run_repeating(sweep, interval=SWEEP_MINUTES * 60, first=30)

    log.info("Bot starting")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
