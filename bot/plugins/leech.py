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
from logging import getLogger
from bot.modules.decorators import verify_user
from bot.modules.telegram import format_bytes, render_progress, make_progress_bar, format_eta
from bot.stream.virtual import encode_batch, clean_virtual_name
from bot.stream.worker import worker_pool

logger = getLogger('bot')

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

                available_bots = [c for c in worker_pool.clients if getattr(c, "is_connected", False)] if worker_pool.clients else [client]
                if not available_bots:
                    available_bots = [client]
                num_bots = len(available_bots)

                # Only split if file exceeds Telegram's single-file limit (PART_SIZE ~1.95 GB)
                if total_size <= PART_SIZE:
                    num_parts = 1
                else:
                    num_parts = math.ceil(total_size / PART_SIZE)

                slice_size = math.ceil(total_size / num_parts)
                clean_name = clean_virtual_name(filename)
                base_url = Server.BASE_URL.rstrip('/') if Server.BASE_URL else f"http://127.0.0.1:{Server.PORT}"

                # Case 1: Single Part File (<= 2 GB, upload as normal using single bot without splitting)
                if num_parts == 1:
                    local_path = os.path.join(DOWNLOAD_DIR, filename)
                    dl_start = time.time()
                    dl_state = {"last_edit": 0.0, "last_text": ""}
                    downloaded = 0

                    async with aiofiles.open(local_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(1024 * 1024):
                            await f.write(chunk)
                            downloaded += len(chunk)
                            now = time.time()
                            if now - dl_state["last_edit"] >= 3.5:
                                p_text = render_progress(
                                    "📥 **Downloading from URL**",
                                    filename,
                                    downloaded,
                                    total_size,
                                    dl_start
                                )
                                if p_text != dl_state["last_text"]:
                                    try:
                                        await status_msg.edit_text(p_text)
                                        dl_state["last_edit"] = now
                                        dl_state["last_text"] = p_text
                                    except Exception:
                                        pass

                    up_start = time.time()
                    up_state = {"last_edit": 0.0, "last_text": ""}

                    async def upload_progress(current, total):
                        now = time.time()
                        if now - up_state["last_edit"] < 3.5 and current != total:
                            return
                        p_text = render_progress(
                            "📤 **Uploading to Telegram**",
                            filename,
                            current,
                            total,
                            up_start
                        )
                        if p_text != up_state["last_text"]:
                            try:
                                await status_msg.edit_text(p_text)
                                up_state["last_edit"] = now
                                up_state["last_text"] = p_text
                            except Exception:
                                pass

                    sent = await client.send_document(
                        chat_id=Telegram.CHANNEL_ID,
                        document=local_path,
                        file_name=filename,
                        caption=f"📁 `{filename}`\n📦 `{formatted_total}`",
                        progress=upload_progress
                    )

                    if os.path.exists(local_path):
                        os.remove(local_path)

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
                    await status_msg.edit_text(text=text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)
                    return

                # Case 2: Multi-Bot Parallel Pipelined Leech (> 2GB to 20GB+)
                active_workers = min(num_bots, num_parts)
                await status_msg.edit_text(
                    f"🚀 **Multi-Bot Parallel Accelerator Engaged!**\n\n"
                    f"📁 `{clean_name}` ({formatted_total})\n"
                    f"🤖 **Worker Bots:** `{active_workers} Bots uploading simultaneously`\n"
                    f"🧩 **Slices:** `{num_parts} parts` (~{format_bytes(slice_size)} each)"
                )

                part_progress = {i: 0 for i in range(num_parts)}
                up_start = time.time()
                up_state = {"last_edit": 0.0, "last_text": ""}
                up_lock = asyncio.Lock()

                async def update_aggregate_progress(part_idx, current_part_bytes):
                    part_progress[part_idx] = current_part_bytes
                    now = time.time()
                    if now - up_state["last_edit"] < 3.5:
                        return
                    
                    total_uploaded = sum(part_progress.values())
                    elapsed = max(0.1, now - up_start)
                    speed = total_uploaded / elapsed
                    eta = max(0, total_size - total_uploaded) / speed if speed > 0 else 0
                    bar, pct = make_progress_bar(total_uploaded, total_size)

                    text = (
                        f"📤 **Multi-Bot Parallel Upload:** `{clean_name}`\n"
                        f"{bar} **{pct:.1f}%**\n\n"
                        f"⚡ **Combined Speed:** `{format_bytes(int(speed))}/s` | ⏱️ **ETA:** `{format_eta(eta)}`\n"
                        f"📦 **Uploaded:** `{format_bytes(total_uploaded)} / {formatted_total}`\n"
                        f"🤖 **Workers:** `{active_workers} Bots active in parallel` ({num_parts} parts)"
                    )
                    async with up_lock:
                        if text != up_state["last_text"]:
                            try:
                                await status_msg.edit_text(text)
                                up_state["last_edit"] = now
                                up_state["last_text"] = text
                            except Exception:
                                pass

                msg_ids = [None] * num_parts
                parts_queue = asyncio.Queue()
                for i in range(num_parts):
                    await parts_queue.put(i)

                async def bot_worker_loop(bot_worker, worker_idx):
                    while True:
                        try:
                            part_idx = parts_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break

                        part_num = part_idx + 1
                        part_start = part_idx * slice_size
                        part_end = min(total_size - 1, (part_idx + 1) * slice_size - 1)
                        part_len = part_end - part_start + 1
                        part_filename = f"{clean_name}.{part_num:03d}"
                        part_path = os.path.join(DOWNLOAD_DIR, part_filename)

                        try:
                            # 1. Download slice from URL
                            range_hdr = {'Range': f'bytes={part_start}-{part_end}'}
                            async with session.get(url, headers=range_hdr, allow_redirects=True) as part_resp:
                                if part_resp.status not in (200, 206):
                                    raise RuntimeError(f"Failed to download slice {part_num} (HTTP {part_resp.status})")

                                async with aiofiles.open(part_path, 'wb') as pf:
                                    async for chunk in part_resp.content.iter_chunked(1024 * 1024):
                                        await pf.write(chunk)

                            # 2. Upload slice to Telegram
                            async def p_cb(curr, tot):
                                await update_aggregate_progress(part_idx, curr)

                            try:
                                sent_slice = await bot_worker.send_document(
                                    chat_id=Telegram.CHANNEL_ID,
                                    document=part_path,
                                    file_name=part_filename,
                                    caption=f"📁 `{part_filename}` (Part {part_num}/{num_parts})\n📦 `{format_bytes(part_len)}`",
                                    progress=p_cb
                                )
                            except Exception as e:
                                logger.warning(f"Worker bot failed to send document ({e}), falling back to primary bot...")
                                sent_slice = await client.send_document(
                                    chat_id=Telegram.CHANNEL_ID,
                                    document=part_path,
                                    file_name=part_filename,
                                    caption=f"📁 `{part_filename}` (Part {part_num}/{num_parts})\n📦 `{format_bytes(part_len)}`",
                                    progress=p_cb
                                )

                            msg_ids[part_idx] = sent_slice.id
                            part_progress[part_idx] = part_len
                            await update_aggregate_progress(part_idx, part_len)

                        finally:
                            # 3. Clean up slice from disk immediately
                            if os.path.exists(part_path):
                                try:
                                    os.remove(part_path)
                                except Exception:
                                    pass
                            parts_queue.task_done()

                worker_tasks = [
                    asyncio.create_task(bot_worker_loop(available_bots[i], i))
                    for i in range(active_workers)
                ]

                # Wait for all multi-bot upload workers to complete
                await asyncio.gather(*worker_tasks)

                # Generate seamless 10GB+ virtual links
                batch_token = encode_batch([m for m in msg_ids if m])
                stream_url = f"{base_url}/inline/v/{batch_token}/{clean_name}"
                player_url = f"{base_url}/v/{batch_token}/{clean_name}"
                download_url = f"{base_url}/dl/v/{batch_token}/{clean_name}"

                text = (
                    f"🎬 **Multi-Bot Parallel Leech Complete!**\n\n"
                    f"📁 **Merged Name:** `{clean_name}`\n"
                    f"📦 **Total Size:** `{formatted_total}` ({num_parts} Parts)\n"
                    f"🤖 **Speed Boost:** Uploaded with {active_workers} bots in parallel!\n"
                    f"⚡ **Stream Protocol:** `RFC 7233 Virtual Stitching`\n\n"
                    f"🔗 **Single Seamless Links:**\n"
                    f"• **Direct Stream:** [Click Here]({stream_url})\n"
                    f"• **Web & VLC Player:** [Click Here]({player_url})\n"
                    f"• **Single Download:** [Click Here]({download_url})\n\n"
                    f"💡 _All {num_parts} parts stream and seek seamlessly as one single contiguous file!_"
                )

                buttons = [
                    [
                        InlineKeyboardButton("⚡ Stream Merged Video", url=stream_url),
                        InlineKeyboardButton("📥 Fast Download", url=download_url)
                    ],
                    [
                        InlineKeyboardButton("🎬 Open in Web / VLC Player", url=player_url)
                    ]
                ]

                await status_msg.edit_text(text=text, reply_markup=InlineKeyboardMarkup(buttons), disable_web_page_preview=True)

    except Exception as e:
        await status_msg.edit(f"❌ Leech failed: {e}")
