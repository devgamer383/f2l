import os
import re
from datetime import datetime
from mimetypes import guess_type
from hydrogram.types import Message
from bot import TelegramBot
from bot.config import Telegram

# Additional common media mime types mapping
MEDIA_MIME_TYPES = {
    '.mkv': 'video/x-matroska',
    '.mp4': 'video/mp4',
    '.webm': 'video/webm',
    '.avi': 'video/x-msvideo',
    '.mov': 'video/quicktime',
    '.flv': 'video/x-flv',
    '.wmv': 'video/x-ms-wmv',
    '.ts': 'video/mp2t',
    '.3gp': 'video/3gpp',
    '.mp3': 'audio/mpeg',
    '.m4a': 'audio/mp4',
    '.flac': 'audio/flac',
    '.wav': 'audio/wav',
    '.ogg': 'audio/ogg',
    '.opus': 'audio/opus',
    '.aac': 'audio/aac',
}

def format_bytes(size_bytes: int) -> str:
    """Format bytes to human-readable string (e.g. 1.45 GB)."""
    if not size_bytes or size_bytes <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    size = float(size_bytes)
    while size >= 1024.0 and i < len(units) - 1:
        size /= 1024.0
        i += 1
    return f"{size:.2f} {units[i]}"

def make_progress_bar(current: int, total: int, width: int = 10) -> tuple[str, float]:
    """Generates a visual progress bar e.g. [▰▰▰▰▱▱▱▱▱▱] 40.0%"""
    if total <= 0:
        return "[▱▱▱▱▱▱▱▱▱▱]", 0.0
    pct = min(100.0, max(0.0, (current / total) * 100))
    filled = int(round(width * current / float(total)))
    filled = min(width, max(0, filled))
    bar = "▰" * filled + "▱" * (width - filled)
    return f"[{bar}]", pct

def format_eta(seconds: float) -> str:
    """Formats seconds into 00m 00s or 00h 00m format."""
    if seconds < 0 or seconds > 86400 * 30:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h:02d}h {m:02d}m"
    return f"{m:02d}m {s:02d}s"

def render_progress(
    action: str,
    filename: str,
    current: int,
    total: int,
    start_time: float,
    extra_line: str = ""
) -> str:
    """Renders a beautiful, compact progress card for Telegram messages."""
    import time
    elapsed = max(0.1, time.time() - start_time)
    speed = current / elapsed
    rem_bytes = max(0, total - current)
    eta = rem_bytes / speed if speed > 0 else 0
    bar, pct = make_progress_bar(current, total)

    lines = [
        f"{action}: `{filename}`",
        f"{bar} **{pct:.1f}%**",
        "",
        f"⚡ **Speed:** `{format_bytes(int(speed))}/s` | ⏱️ **ETA:** `{format_eta(eta)}`",
        f"📦 **Processed:** `{format_bytes(current)} / {format_bytes(total)}`"
    ]
    if extra_line:
        lines.append(f"🧩 **Info:** {extra_line}")

    return "\n".join(lines)

async def get_message(message_id: int) -> Message | None:
    message = None
    try:
        message = await TelegramBot.get_messages(Telegram.CHANNEL_ID, message_ids=message_id)
        if message and message.empty:
            message = None
    except Exception:
        pass
    return message

async def send_message(msg: Message, send_to: int = Telegram.CHANNEL_ID) -> Message:
    return await TelegramBot.send_message(chat_id=send_to, text=msg.text)

def get_file_properties(msg: Message):
    attributes = (
        'document',
        'video',
        'audio',
        'voice',
        'photo',
        'video_note'
    )
    media = None
    file_type = None
    for attribute in attributes:
        media = getattr(msg, attribute, None)
        if media:
            file_type = attribute
            break

    if not media:
        raise ValueError('Unknown or unsupported file type.')

    file_name = getattr(media, 'file_name', None)
    file_size = getattr(media, 'file_size', 0)

    if not file_name:
        file_format = {
            'video': 'mp4',
            'audio': 'mp3',
            'voice': 'ogg',
            'photo': 'jpg',
            'video_note': 'mp4'
        }.get(file_type, 'bin')
        date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_name = f'{file_type}-{msg.id}-{date}.{file_format}'

    # Sanitize filename
    file_name = re.sub(r'[\\/*?:"<>|]', '_', file_name)
    ext = os.path.splitext(file_name)[1].lower()

    mime_type = getattr(media, 'mime_type', None)
    if not mime_type or mime_type == 'application/octet-stream':
        if ext in MEDIA_MIME_TYPES:
            mime_type = MEDIA_MIME_TYPES[ext]
        else:
            mime_type = guess_type(file_name)[0] or 'application/octet-stream'

    return file_name, file_size, mime_type

def is_streamable_media(file: Message) -> bool:
    """Check if media can be streamed in video/audio players."""
    if file.video or file.video_note or file.audio or file.voice:
        return True
    
    if file.document:
        mime = (file.document.mime_type or '').lower()
        if mime.startswith('video/') or mime.startswith('audio/'):
            return True
        filename = (file.document.file_name or '').lower()
        ext = os.path.splitext(filename)[1]
        if ext in MEDIA_MIME_TYPES:
            return True
    return False
