import os
import re
from hydrogram import filters
from hydrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from bot import TelegramBot
from bot.config import Telegram, Server
from bot.modules.decorators import verify_user
from bot.modules.telegram import format_bytes, is_streamable_media

@TelegramBot.on_message(
    filters.private
    & (
        filters.document
        | filters.video
        | filters.video_note
        | filters.audio
        | filters.voice
        | filters.photo
    )
)
@verify_user
async def handle_user_file(_, msg: Message):
    # Forward/copy uploaded media to private storage channel
    stored_file = await msg.copy(chat_id=Telegram.CHANNEL_ID)

    media = (
        stored_file.document
        or stored_file.video
        or stored_file.video_note
        or stored_file.audio
        or stored_file.voice
        or stored_file.photo
    )

    file_name = getattr(media, "file_name", None)
    file_size = getattr(media, "file_size", 0)

    if not file_name:
        if stored_file.photo:
            file_name = f"photo_{stored_file.id}.jpg"
        elif stored_file.voice:
            file_name = f"voice_{stored_file.id}.ogg"
        elif stored_file.video_note:
            file_name = f"video_note_{stored_file.id}.mp4"
        elif stored_file.audio:
            file_name = f"audio_{stored_file.id}.mp3"
        elif stored_file.video:
            file_name = f"video_{stored_file.id}.mp4"
        else:
            file_name = f"file_{stored_file.id}"

    # Clean filename for URL safety
    clean_name = re.sub(r'[\\/*?:"<>|]', '_', file_name).replace(" ", "-")
    
    base_url = Server.BASE_URL.rstrip('/') if Server.BASE_URL else f"http://127.0.0.1:{Server.PORT}"
    
    download_link = f"{base_url}/dl/{stored_file.id}/{clean_name}"
    stream_link = f"{base_url}/inline/{stored_file.id}/{clean_name}"
    player_link = f"{base_url}/stream/{stored_file.id}/{clean_name}"

    formatted_size = format_bytes(file_size)
    can_stream = is_streamable_media(stored_file)

    caption = (
        f"📁 **File Name:** `{clean_name}`\n"
        f"📦 **File Size:** `{formatted_size}`\n"
        f"⚡ **Stream Protocol:** `RFC 7233 (Parallel Prefetch)`\n\n"
        f"🔗 **Direct Links:**\n"
        f"• **Download:** [Click Here]({download_link})\n"
    )

    if can_stream:
        caption += f"• **Fast Stream:** [Click Here]({stream_link})\n"
        caption += f"• **Web Player:** [Click Here]({player_link})\n"

        buttons = [
            [
                InlineKeyboardButton("⚡ Direct Stream", url=stream_link),
                InlineKeyboardButton("📥 Fast Download", url=download_link)
            ],
            [
                InlineKeyboardButton("🎬 Open Web Player", url=player_link)
            ],
            [
                InlineKeyboardButton("🟠 Open in VLC", url=f"vlc://{stream_link}"),
                InlineKeyboardButton("🟣 MX Player", url=f"intent:{stream_link}#Intent;package=com.mxtech.videoplayer.ad;type=video/*;end")
            ]
        ]
    else:
        buttons = [
            [
                InlineKeyboardButton("📥 Fast Download", url=download_link)
            ]
        ]

    await msg.reply_text(
        text=caption,
        quote=True,
        disable_web_page_preview=True,
        reply_markup=InlineKeyboardMarkup(buttons)
    )