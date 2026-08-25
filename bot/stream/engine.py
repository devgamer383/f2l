import asyncio
from math import ceil
from logging import getLogger
from typing import AsyncGenerator
from bot.config import Telegram, Stream
from .cache import chunk_cache
from .worker import worker_pool

logger = getLogger('bot')

class FastStreamEngine:
    """
    High-Performance MTProto Stream Engine.
    Uses persistent MTProto streaming pipelines with client-specific file reference resolution,
    smart RAM chunk caching for instant 0ms probes, and multi-worker failover.
    """

    async def stream_file_range(
        self,
        file_id: int,
        start_byte: int,
        end_byte: int,
        content_length: int,
        chunk_size: int = Stream.CHUNK_SIZE,
    ) -> AsyncGenerator[bytes, None]:
        """
        Streams a byte range [start_byte, end_byte] with fresh per-client file references.
        """
        offset_chunks = start_byte // chunk_size
        total_bytes = end_byte - start_byte + 1
        chunks_to_stream = ceil(total_bytes / chunk_size)
        
        # Check cache if single chunk request (e.g. video player metadata probe)
        cache_key = f"{file_id}_{offset_chunks}"
        if chunks_to_stream == 1:
            cached = await chunk_cache.get(cache_key)
            if cached is not None:
                trim_start = start_byte % chunk_size
                chunk = cached[trim_start:]
                if len(chunk) > content_length:
                    chunk = chunk[:content_length]
                if chunk:
                    yield chunk
                return

        # Prepare client list to try (selected worker, then primary bot as fallback)
        selected_client = await worker_pool.get_client()
        clients_to_try = [selected_client]
        if worker_pool.primary_bot and worker_pool.primary_bot != selected_client:
            clients_to_try.append(worker_pool.primary_bot)

        bytes_streamed = 0
        chunk_index = 0

        for client in clients_to_try:
            try:
                # Fetch fresh message specifically for this client so it gets a valid, authenticated file_reference
                client_file = await client.get_messages(Telegram.CHANNEL_ID, message_ids=file_id)
                if not client_file or client_file.empty:
                    continue

                bytes_streamed = 0
                chunk_index = 0

                async for chunk in client.stream_media(
                    client_file,
                    offset=offset_chunks,
                    limit=chunks_to_stream,
                ):
                    if not chunk:
                        break

                    # Cache initial metadata chunks and single chunks in memory
                    current_chunk_pos = offset_chunks + chunk_index
                    if current_chunk_pos == 0 or chunks_to_stream == 1:
                        await chunk_cache.set(f"{file_id}_{current_chunk_pos}", chunk)

                    # Trim first chunk if starting in the middle
                    if chunk_index == 0:
                        trim_start = start_byte % chunk_size
                        if trim_start > 0:
                            chunk = chunk[trim_start:]

                    remaining_bytes = content_length - bytes_streamed
                    if remaining_bytes <= 0:
                        break

                    # Trim last chunk if ending in the middle
                    if len(chunk) > remaining_bytes:
                        chunk = chunk[:remaining_bytes]

                    if not chunk:
                        break

                    yield chunk
                    bytes_streamed += len(chunk)
                    chunk_index += 1
                
                # If streaming completed or was started successfully, finish
                if bytes_streamed > 0 or chunks_to_stream == 0:
                    break

            except asyncio.CancelledError:
                # Client disconnected or seeked
                break
            except Exception as e:
                logger.warning(f"Client {client.name} stream error on msg {file_id}: {e}. Retrying fallback...")
                if client == clients_to_try[-1]:
                    logger.error(f"All clients failed for msg {file_id}: {e}")
                    raise

fast_streamer = FastStreamEngine()
