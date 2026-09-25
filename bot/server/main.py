import re
from math import ceil
from quart import Blueprint, Response, request, render_template, redirect, jsonify
from .error import abort
from bot.config import Telegram, Server, Stream
from bot.modules.telegram import get_message, get_file_properties, format_bytes, is_streamable_media
from bot.stream import fast_streamer, chunk_cache

bp = Blueprint('main', __name__)

def get_base_url() -> str:
    """Returns configured BASE_URL or dynamically resolves from incoming request."""
    if Server.BASE_URL:
        return Server.BASE_URL.rstrip('/')
    return request.host_url.rstrip('/')

@bp.route('/')
async def home():
    if Telegram.BOT_USERNAME:
        return redirect(f'https://t.me/{Telegram.BOT_USERNAME}')
    return jsonify({"status": "running", "service": "FileStreamBot", "version": "2.0"})

@bp.route('/health')
@bp.route('/ping')
async def health_check():
    """Heroku Keep-Alive / Health Check endpoint."""
    return jsonify({
        "status": "healthy",
        "cache": chunk_cache.get_stats()
    }), 200

@bp.route('/info/<int:file_id>')
async def file_info(file_id: int):
    """File metadata JSON API."""
    file = await get_message(file_id)
    if not file:
        abort(404, 'File not found')

    try:
        file_name, file_size, mime_type = get_file_properties(file)
    except ValueError as e:
        abort(400, str(e))

    base = get_base_url()
    
    return jsonify({
        "file_id": file_id,
        "filename": file_name,
        "size_bytes": file_size,
        "size_formatted": format_bytes(file_size),
        "mime_type": mime_type,
        "streamable": is_streamable_media(file),
        "stream_url": f"{base}/inline/{file_id}/{file_name}",
        "download_url": f"{base}/dl/{file_id}/{file_name}",
        "web_player_url": f"{base}/stream/{file_id}/{file_name}"
    })

async def handle_media_transmission(file_id: int, filename: str, is_inline: bool):
    """
    High-performance RFC 7233 partial-content media transmitter with HEAD handling,
    parallel prefetching, and in-memory LRU chunk caching.
    """
    file = await get_message(file_id)
    if not file:
        abort(404, 'File not found or revoked')

    try:
        file_name, file_size, mime_type = get_file_properties(file)
    except ValueError as e:
        abort(400, str(e))
    
    # In case the URL filename differs from stored filename, preserve format
    if not filename:
        filename = file_name

    disposition = "inline" if is_inline else "attachment"
    
    # Handle HEAD requests instantly (0 bytes transferred)
    if request.method == 'HEAD':
        headers = {
            'Content-Type': mime_type,
            'Content-Length': str(file_size),
            'Accept-Ranges': 'bytes',
            'Content-Disposition': f'{disposition}; filename="{filename}"',
            'Cache-Control': 'public, max-age=31536000, immutable',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Expose-Headers': 'Content-Range, Content-Length, Accept-Ranges',
        }
        return Response('', headers=headers, status=200)

    range_header = request.headers.get('Range')
    start = 0
    end = file_size - 1 if file_size > 0 else 0
    status_code = 200

    if range_header:
        # Parse RFC 7233 range header: bytes=START-END, bytes=START-, bytes=-SUFFIX
        range_match = re.match(r'bytes=(\d*)-(\d*)', range_header.strip())
        if range_match:
            start_str, end_str = range_match.groups()
            
            if start_str and end_str:
                start = int(start_str)
                end = int(end_str)
            elif start_str:
                start = int(start_str)
                end = file_size - 1
            elif end_str:
                # Suffix byte range: e.g. bytes=-500 (last 500 bytes)
                suffix_len = int(end_str)
                start = max(0, file_size - suffix_len)
                end = file_size - 1

            if start > end or start >= file_size:
                abort(416, 'Requested range not satisfiable')
            
            status_code = 206
        else:
            abort(400, 'Invalid Range header format')

    content_length = (end - start + 1) if file_size > 0 else 0

    headers = {
        'Content-Type': mime_type,
        'Content-Disposition': f'{disposition}; filename="{filename}"',
        'Accept-Ranges': 'bytes',
        'Cache-Control': 'public, max-age=31536000, immutable',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
        'Access-Control-Allow-Headers': '*',
        'Access-Control-Expose-Headers': 'Content-Range, Content-Length, Accept-Ranges',
    }

    if status_code == 206:
        headers['Content-Range'] = f'bytes {start}-{end}/{file_size}'
        headers['Content-Length'] = str(content_length)
    else:
        headers['Content-Length'] = str(file_size)

    stream_generator = fast_streamer.stream_file_range(
        file_id=file_id,
        start_byte=start,
        end_byte=end,
        content_length=content_length,
        file_message=file,
        chunk_size=Stream.CHUNK_SIZE,
    )

    return Response(stream_generator, headers=headers, status=status_code)

@bp.route('/dl/<int:file_id>/<path:filename>', methods=['GET', 'HEAD'])
async def download_file(file_id: int, filename: str):
    """Direct Download endpoint (Content-Disposition: attachment)."""
    return await handle_media_transmission(file_id, filename, is_inline=False)

@bp.route('/inline/<int:file_id>/<path:filename>', methods=['GET', 'HEAD'])
@bp.route('/play/<int:file_id>/<path:filename>', methods=['GET', 'HEAD'])
async def inline_stream_file(file_id: int, filename: str):
    """Direct Streaming endpoint (Content-Disposition: inline) for VLC, MPV, Safari, MX Player."""
    return await handle_media_transmission(file_id, filename, is_inline=True)

@bp.route('/stream/<int:file_id>/<path:filename>')
@bp.route('/watch/<int:file_id>/<path:filename>')
async def web_player(file_id: int, filename: str):
    """Universal Web Player page with embedded Plyr and 1-click external player launchers."""
    file = await get_message(file_id)
    if not file:
        abort(404, 'File not found')

    try:
        file_name, file_size, mime_type = get_file_properties(file)
    except ValueError as e:
        abort(400, str(e))

    base = get_base_url()
    
    media_link = f'{base}/inline/{file_id}/{filename}'
    download_link = f'{base}/dl/{file_id}/{filename}'
    
    import os
    ext = os.path.splitext(filename)[1].lower()
    is_audio = mime_type.startswith('audio/')
    is_mkv = ext in ['.mkv', '.avi', '.flv', '.wmv', '.mov', '.ts'] or 'matroska' in mime_type
    
    return await render_template(
        'player.html',
        filename=filename,
        filesize=format_bytes(file_size),
        mimeType=mime_type,
        mediaLink=media_link,
        downloadLink=download_link,
        isAudio=is_audio,
        isMkv=is_mkv,
        ext=ext.upper().lstrip('.')
    )
