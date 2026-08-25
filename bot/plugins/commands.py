from hydrogram import filters
from hydrogram.types import Message
from bot import TelegramBot
from bot.config import Telegram, Server, Stream
from bot.modules.static import WelcomeText, PrivacyText
from bot.modules.decorators import verify_user
from bot.stream import chunk_cache, worker_pool

@TelegramBot.on_message(filters.command(['start', 'help']) & filters.private)
@verify_user
async def start_command(_, msg: Message):
    first_name = msg.from_user.first_name if msg.from_user else "User"
    await msg.reply(
        text=WelcomeText % {'first_name': first_name},
        quote=True,
        disable_web_page_preview=True
    )

@TelegramBot.on_message(filters.command('status') & filters.private)
@verify_user
async def status_command(_, msg: Message):
    stats = chunk_cache.get_stats()
    worker_count = len(worker_pool.clients)
    
    text = (
        "📊 **FileStream Performance Dashboard**\n\n"
        f"⚡ **Stream Engine:** Parallel Prefetch ({Stream.PREFETCH_CHUNKS}x concurrency)\n"
        f"🤖 **Active Worker Clients:** `{worker_count}`\n"
        f"💾 **LRU Chunk Cache:** `{stats['cached_size_mb']} MB / {stats['max_size_mb']} MB`\n"
        f"📦 **Cached Chunks:** `{stats['cached_chunks']}`\n"
        f"🎯 **Cache Hit Ratio:** `{stats['hit_ratio_percent']}%` (`{stats['hits']}` hits, `{stats['misses']}` misses)\n"
        f"🌐 **Server Port:** `{Server.PORT}`\n"
    )
    await msg.reply_text(text, quote=True)

@TelegramBot.on_message(filters.command('privacy') & filters.private)
@verify_user
async def privacy_command(_, msg: Message):
    await msg.reply(text=PrivacyText, quote=True, disable_web_page_preview=True)

@TelegramBot.on_message(filters.command('log') & filters.chat(Telegram.OWNER_ID))
async def log_command(_, msg: Message):
    try:
        await msg.reply_document('event-log.txt', quote=True)
    except Exception as e:
        await msg.reply_text(f"Error reading log file: {e}", quote=True)
