from hydrogram import filters
from hydrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from bot import TelegramBot
from bot.config import Telegram, Server
from bot.modules.decorators import verify_user
from bot.modules.telegram import format_bytes, get_file_properties
from bot.stream.virtual import encode_batch, clean_virtual_name

@TelegramBot.on_message(filters.command(['merge', 'stitch', 'join']) & filters.private)
@verify_user
async def merge_command(client, msg: Message):
    """
    Merges multiple 2GB Telegram parts into a single virtual 10GB+ stream link.
    100% Stateless: No database required. Links are permanent and survive all restarts.
    Usage:
      1. Reply to the first file message with: /merge <count> (e.g. /merge 5)
      2. Or send: /merge <start_msg_id> <count>
    """
    args = msg.text.split()[1:]
    start_id = None
    count = 0

    if msg.reply_to_message:
        if not args:
            await msg.reply_text(
                "❌ **Usage:** Reply to the first part and specify count.\n"
                "Example: `/merge 5` (merges this file + next 4 parts into a 10GB single stream).",
                quote=True
            )
            return
        try:
            count = int(args[0])
        except ValueError:
            await msg.reply_text("❌ Count must be a valid number (e.g. `/merge 5`).", quote=True)
            return
        
        replied = msg.reply_to_message
        if not (replied.document or replied.video or replied.audio):
            await msg.reply_text("❌ The replied message does not contain a media file.", quote=True)
            return
        
        # Forward to storage channel to get channel message ID
        fwd = await replied.copy(chat_id=Telegram.CHANNEL_ID)
        start_id = fwd.id
    else:
        if len(args) < 2:
            await msg.reply_text(
                "ℹ️ **Stateless 10GB+ Multi-Part Stream Stitcher**\n\n"
                "**How to merge split 2GB parts into 1 single link:**\n"
                "1. Forward part 1 to the bot\n"
                "2. Reply to it with: `/merge <total_parts>` (e.g. `/merge 5`)\n\n"
                "Or specify message IDs: `/merge <start_id> <count>`",
                quote=True
            )
            return
        try:
            start_id = int(args[0])
            count = int(args[1])
        except ValueError:
            await msg.reply_text("❌ Invalid format. Use `/merge <start_id> <count>`.", quote=True)
            return

    if count < 2 or count > 50:
        await msg.reply_text("❌ Count must be between 2 and 50 parts.", quote=True)
        return

    status_msg = await msg.reply_text(f"⏳ Scanning and stitching {count} parts...", quote=True)

    msg_ids = list(range(start_id, start_id + count))

    try:
        # Fetch all messages in a single call to verify parts & sizes
        messages = await client.get_messages(Telegram.CHANNEL_ID, message_ids=msg_ids)
        if not isinstance(messages, list):
            messages = [messages]

        total_size = 0
        unified_name = ""

        for idx, m in enumerate(messages):
            if not m or m.empty:
                await status_msg.edit(f"❌ Part {idx + 1} (Msg ID {msg_ids[idx]}) not found in storage channel.")
                return

            p_name, p_size, _ = get_file_properties(m)
            if p_size <= 0:
                await status_msg.edit(f"❌ Part {idx + 1} (Msg ID {msg_ids[idx]}) is not a valid file.")
                return

            if idx == 0:
                unified_name = clean_virtual_name(p_name)

            total_size += p_size

    except Exception as e:
        await status_msg.edit(f"❌ Error verifying parts: {e}")
        return

    # Encode message sequence into URL token (stateless: no database required!)
    batch_token = encode_batch(msg_ids)

    base_url = Server.BASE_URL.rstrip('/') if Server.BASE_URL else f"http://127.0.0.1:{Server.PORT}"
    stream_url = f"{base_url}/inline/v/{batch_token}/{unified_name}"
    download_url = f"{base_url}/dl/v/{batch_token}/{unified_name}"
    player_url = f"{base_url}/v/{batch_token}/{unified_name}"

    formatted_size = format_bytes(total_size)
    text = (
        f"🎬 **Virtual Multi-Part Stream Created!**\n\n"
        f"📁 **Merged Name:** `{unified_name}`\n"
        f"📦 **Total Size:** `{formatted_size}` ({count} Parts)\n"
        f"⚡ **Stream Protocol:** `RFC 7233 Virtual Stitching (Stateless)`\n\n"
        f"🔗 **Single Seamless Links:**\n"
        f"• **Direct Stream:** [Click Here]({stream_url})\n"
        f"• **Web & VLC Player:** [Click Here]({player_url})\n"
        f"• **Single 10GB Download:** [Click Here]({download_url})\n\n"
        f"💡 _All {count} parts stream and seek seamlessly as one continuous 10GB+ file without any database!_"
    )

    buttons = [
        [
            InlineKeyboardButton("⚡ Stream Merged Video", url=stream_url),
            InlineKeyboardButton("📥 Download (Single 10GB)", url=download_url)
        ],
        [
            InlineKeyboardButton("🎬 Open in Web / VLC Player", url=player_url)
        ]
    ]

    await status_msg.edit(text=text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)
