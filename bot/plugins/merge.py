import re
import secrets
from hydrogram import filters
from hydrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from bot import TelegramBot
from bot.config import Telegram, Server
from bot.modules.decorators import verify_user
from bot.modules.telegram import format_bytes, get_file_properties
from bot.stream import virtual_db

def clean_virtual_name(raw_name: str) -> str:
    """Strips .001, .part1, etc. to produce a clean merged media filename."""
    # Match patterns like .001, .002 or .part01, .part1
    cleaned = re.sub(r'(\.00[1-9]|\.0[1-9][0-9]|\.part0*[1-9]|\.z0[1-9])+$', '', raw_name, flags=re.IGNORECASE)
    cleaned = re.sub(r'[\\/*?:"<>|]', '_', cleaned).replace(" ", "-")
    return cleaned if cleaned else raw_name

@TelegramBot.on_message(filters.command(['merge', 'stitch', 'join']) & filters.private)
@verify_user
async def merge_command(client, msg: Message):
    """
    Merges multiple 2GB Telegram parts into a single virtual 10GB+ stream link.
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
        
        # When replying, copy the replied file to storage if not already there
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
                "ℹ️ **Virtual 10GB+ Multi-Part Stream Stitcher**\n\n"
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

    parts = []
    total_size = 0
    unified_name = ""
    mime_type = "video/x-matroska"

    # Fetch parts from storage channel sequentially
    for offset in range(count):
        curr_id = start_id + offset
        try:
            part_msg = await client.get_messages(Telegram.CHANNEL_ID, message_ids=curr_id)
            if not part_msg or part_msg.empty:
                await status_msg.edit(f"❌ Part {offset + 1} (Msg ID {curr_id}) not found in storage channel.")
                return

            p_name, p_size, p_mime = get_file_properties(part_msg)
            if p_size <= 0:
                await status_msg.edit(f"❌ Part {offset + 1} (Msg ID {curr_id}) is not a valid file.")
                return

            if offset == 0:
                unified_name = clean_virtual_name(p_name)
                mime_type = p_mime

            parts.append({
                "message_id": curr_id,
                "size": p_size,
                "name": p_name
            })
            total_size += p_size
        except Exception as e:
            await status_msg.edit(f"❌ Error reading part {offset + 1}: {e}")
            return

    batch_id = secrets.token_hex(6)
    await virtual_db.save_batch(
        batch_id=batch_id,
        filename=unified_name,
        total_size=total_size,
        mime_type=mime_type,
        parts=parts
    )

    base_url = Server.BASE_URL.rstrip('/') if Server.BASE_URL else f"http://127.0.0.1:{Server.PORT}"
    stream_url = f"{base_url}/inline/v/{batch_id}/{unified_name}"
    download_url = f"{base_url}/dl/v/{batch_id}/{unified_name}"
    player_url = f"{base_url}/v/{batch_id}/{unified_name}"

    formatted_size = format_bytes(total_size)
    text = (
        f"🎬 **Virtual Multi-Part Stream Created!**\n\n"
        f"📁 **Merged Name:** `{unified_name}`\n"
        f"📦 **Total Size:** `{formatted_size}` ({count} Parts)\n"
        f"⚡ **Stream Protocol:** `RFC 7233 Virtual Stitching`\n\n"
        f"🔗 **Single Seamless Links:**\n"
        f"• **Direct Stream:** [Click Here]({stream_url})\n"
        f"• **Web & VLC Player:** [Click Here]({player_url})\n"
        f"• **Single 10GB Download:** [Click Here]({download_url})\n\n"
        f"💡 _All {count} parts will stream and seek seamlessly as one continuous 10GB+ file!_"
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
