from hydrogram import filters
from hydrogram.types import Message

from bot import TelegramBot
from bot.config import Telegram, Server
from bot.modules.decorators import verify_user


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
    # Copy the uploaded file to the storage channel
    file = await msg.copy(chat_id=Telegram.CHANNEL_ID)

    # Get the media object
    media = (
        file.document
        or file.video
        or file.video_note
        or file.audio
        or file.voice
        or file.photo
    )

    # Determine filename
    filename = getattr(media, "file_name", None)

    if not filename:
        if file.photo:
            filename = f"photo_{file.id}.jpg"
        elif file.voice:
            filename = f"voice_{file.id}.ogg"
        elif file.video_note:
            filename = f"video_note_{file.id}.mp4"
        elif file.audio:
            filename = f"audio_{file.id}.mp3"
        elif file.video:
            filename = f"video_{file.id}.mp4"
        else:
            filename = f"file_{file.id}"

    # Download link (available for every file)
    download_link = f"{Server.BASE_URL}/dl/{file.id}/{filename}"

    # Check whether the file can be streamed
    is_streamable = (
        file.video
        or file.video_note
        or file.audio
        or file.voice
        or (
            file.document
            and file.document.mime_type
            and (
                file.document.mime_type.startswith("video/")
                or file.document.mime_type.startswith("audio/")
            )
        )
    )

    if is_streamable:
        stream_link = f"{Server.BASE_URL}/stream/{file.id}/{filename}"

        text = (
            f"📥 Download:\n"
            f"{download_link}\n\n"
            f"▶️ Stream:\n"
            f"{stream_link}"
        )
    else:
        text = (
            f"📥 Download:\n"
            f"{download_link}"
        )

    await msg.reply_text(text, quote=True)