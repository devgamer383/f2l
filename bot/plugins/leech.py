import os
import re
import time
import math
import asyncio
import aiohttp
import aiofiles
from urllib.parse import unquote, urlparse

from hydrogram import filters
from hydrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from bot import TelegramBot
from bot.config import Telegram, Server
from bot.modules.decorators import verify_user
from bot.modules.telegram import format_bytes
from bot.stream.virtual import encode_batch, clean_virtual_name

# 1.95 GB slice size to stay safely within Telegram's 2,000 MB limit
PART_SIZE = 1950 * 1024 * 1024
DOWNLOAD_DIR = "downloads"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def extract_filename_from_url(url: str, content_disposition: str | None = None) -> str:
    """Extracts clean filename from Content-Disposition header or URL path."""
    if content_disposition:
        m = re.search(r'filename\*?=(?:UTF-8\'\')?["\']?([^"\';\r\n]+)', content_disposition, re.IGNORECASE)
        if m:
            return unquote(m.group(1)).strip()

    parsed = urlparse(url)
    base = os.path.basename(parsed.path)
    if base and '.' in base:
        return unquote(base).strip()

    return "downloaded_file.mkv"

def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r'[\\/*?:"<>|]', '_', name).replace(" ", "-")
    return cleaned if cleaned else "video.mkv"

@TelegramBot.on_message(filters.command(['leech', 'download', 'fetch']) & filters.private)
@verify_user
async def leech_command(client, msg: Message):
    """
    Downloads files from any internet URL and automatically splits files >2GB
    into 2GB parts in Telegram, outputting a single seamless 10GB+ stream link.
    Usage:
      /leech <URL> [optional_custom_filename.mkv]
    """
    args = msg.text.split()[1:]
    if not args:
        await msg.reply_text(
            "ℹ️ **High-Speed 10GB+ Cloud Leech Engine**\n\n"
            "**Usage:** `/leech <direct_url> [custom_name.mkv]`\n\n"
            "• Downloads from any direct HTTP/HTTPS link.\n"
            "• If file is **> 2 GB** (e.g. 10 GB), it automatically slices it into 2 GB parts and generates **one single 10 GB stream link**!",
            quote=True
        )
        return

    url = args[0]
    custom_name = " ".join(args[1:]) if len(args) > 1 else None

    status_msg = await msg.reply_text("⏳ Connecting to URL and inspecting stream...", quote=True)

    try:
        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=300)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # 1. Inspect URL with HEAD or streaming GET
            async with session.get(url, allow_redirects=True) as resp:
                if resp.status not in (200, 206):
                    await status_msg.edit(f"❌ Failed to connect to URL (HTTP {resp.status})")
                    return

                total_size = int(resp.headers.get('Content-Length') or 0)
                cd = resp.headers.get('Content-Disposition')
                raw_filename = custom_name if custom_name else extract_filename_from_url(str(resp.url), cd)
                filename = sanitize_filename(raw_filename)

                if total_size <= 0:
                    await status_msg.edit("❌ Remote server did not report file size (Content-Length missing).")
                    return

                formatted_total = format_bytes(total_size)

                # Case A: File is <= 2GB (Single file upload)
                if total_size <= PART_SIZE:
                    await status_msg.edit(f"📥 Downloading single file: `{filename}` ({formatted_total})...")
                    local_path = os.path.join(DOWNLOAD_DIR, filename)

                    last_edit = time.time()
                    downloaded = 0

                    async with aiofiles.open(local_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(1024 * 1024):
                            await f.write(chunk)
                            downloaded += len(chunk)
                            if time.time() - last_edit > 5:
                                pct = (downloaded / total_size * 100) if total_size > 0 else 0
                                try:
                                    await status_msg.edit(
                                        f"📥 Downloading: `{filename}`\n"
                                        f"📊 Progress: `{format_bytes(downloaded)} / {formatted_total}` ({pct:.1f}%)"
                                    )
                                    last_edit = time.time()
                                except Exception:
                                    pass

                    # Upload to storage channel
                    await status_msg.edit(f"📤 Uploading to Telegram channel: `{filename}`...")
                    sent = await client.send_document(
                        chat_id=Telegram.CHANNEL_ID,
                        document=local_path,
                        file_name=filename,
                        caption=f"📁 `{filename}`\n📦 `{formatted_total}`"
                    )

                    if os.path.exists(local_path):
                        os.remove(local_path)

                    base_url = Server.BASE_URL.rstrip('/') if Server.BASE_URL else f"http://127.0.0.1:{Server.PORT}"
                    stream_url = f"{base_url}/inline/{sent.id}/{filename}"
                    player_url = f"{base_url}/stream/{sent.id}/{filename}"
                    download_url = f"{base_url}/dl/{sent.id}/{filename}"

                    text = (
                        f"🎬 **Leech Complete!**\n\n"
                        f"📁 **File Name:** `{filename}`\n"
                        f"📦 **File Size:** `{formatted_total}`\n"
                        f"⚡ **Stream Protocol:** `RFC 7233 (High Speed)`\n\n"
                        f"🔗 **Direct Links:**\n"
                        f"• **Direct Stream:** [Click Here]({stream_url})\n"
                        f"• **Web & VLC Player:** [Click Here]({player_url})\n"
                        f"• **Fast Download:** [Click Here]({download_url})"
                    )

                    buttons = [
                        [
                            InlineKeyboardButton("⚡ Direct Stream", url=stream_url),
                            InlineKeyboardButton("📥 Fast Download", url=download_url)
                        ],
                        [
                            InlineKeyboardButton("🎬 Open in VLC / Web Player", url=player_url)
                        ]
                    ]
                    await status_msg.edit(text=text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)
                    return

                # Case B: Large 10GB+ File (Automatic Slicing into 2GB parts)
                num_parts = math.ceil(total_size / PART_SIZE)
                await status_msg.edit(
                    f"🧩 **Large {formatted_total} File Detected!**\n"
                    f"Slicing into **{num_parts} parts** (~1.95 GB each) and uploading to Telegram..."
                )

                msg_ids = []
                clean_name = clean_virtual_name(filename)

                for part_idx in range(num_parts):
                    part_num = part_idx + 1
                    part_start = part_idx * PART_SIZE
                    part_end = min(total_size - 1, (part_idx + 1) * PART_SIZE - 1)
                    part_len = part_end - part_start + 1
                    part_filename = f"{clean_name}.{part_num:03d}"
                    part_path = os.path.join(DOWNLOAD_DIR, part_filename)

                    range_hdr = {'Range': f'bytes={part_start}-{part_end}'}
                    await status_msg.edit(
                        f"📥 Downloading **Part {part_num}/{num_parts}** ({format_bytes(part_len)})...\n"
                        f"📁 `{part_filename}`"
                    )

                    async with session.get(url, headers=range_hdr, allow_redirects=True) as part_resp:
                        if part_resp.status not in (200, 206):
                            await status_msg.edit(f"❌ Failed to download slice {part_num} (HTTP {part_resp.status})")
                            return

                        async with aiofiles.open(part_path, 'wb') as pf:
                            async for chunk in part_resp.content.iter_chunked(1024 * 1024):
                                await pf.write(chunk)

                    await status_msg.edit(
                        f"📤 Uploading **Part {part_num}/{num_parts}** to Telegram...\n"
                        f"📁 `{part_filename}`"
                    )

                    sent_part = await client.send_document(
                        chat_id=Telegram.CHANNEL_ID,
                        document=part_path,
                        file_name=part_filename,
                        caption=f"📁 `{part_filename}` (Part {part_num}/{num_parts})\n📦 `{format_bytes(part_len)}`"
                    )

                    msg_ids.append(sent_part.id)

                    if os.path.exists(part_path):
                        os.remove(part_path)

                # All parts uploaded! Generate seamless 10GB+ virtual link
                batch_token = encode_batch(msg_ids)
                base_url = Server.BASE_URL.rstrip('/') if Server.BASE_URL else f"http://127.0.0.1:{Server.PORT}"
                stream_url = f"{base_url}/inline/v/{batch_token}/{clean_name}"
                player_url = f"{base_url}/v/{batch_token}/{clean_name}"
                download_url = f"{base_url}/dl/v/{batch_token}/{clean_name}"

                text = (
                    f"🎬 **10GB+ Virtual Leech Complete!**\n\n"
                    f"📁 **Merged Name:** `{clean_name}`\n"
                    f"📦 **Total Size:** `{formatted_total}` ({num_parts} Parts)\n"
                    f"⚡ **Stream Protocol:** `RFC 7233 Virtual Stitching`\n\n"
                    f"🔗 **Single Seamless 10GB Links:**\n"
                    f"• **Direct Stream:** [Click Here]({stream_url})\n"
                    f"• **Web & VLC Player:** [Click Here]({player_url})\n"
                    f"• **Single 10GB Download:** [Click Here]({download_url})\n\n"
                    f"💡 _All {num_parts} parts stream and seek seamlessly as one single 10GB+ file!_"
                )

                buttons = [
                    [
                        InlineKeyboardButton("⚡ Stream 10GB Video", url=stream_url),
                        InlineKeyboardButton("📥 Download (Single 10GB)", url=download_url)
                    ],
                    [
                        InlineKeyboardButton("🎬 Open in Web / VLC Player", url=player_url)
                    ]
                ]

                await status_msg.edit(text=text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)

    except Exception as e:
        await status_msg.edit(f"❌ Leech failed: {e}")
