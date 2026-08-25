import asyncio
from math import ceil
from logging import getLogger
from typing import AsyncGenerator
from hydrogram.types import Message
from bot.config import Stream
from .cache import chunk_cache
from .worker import worker_pool

logger = getLogger('bot')

class FastStreamEngine:
    """
    High-Performance MTProto Stream Engine.
    Uses continuous persistent MTProto streaming pipelines, smart RAM chunk caching
    for instant 0ms media header probes, and multi-worker client round-robin.
    """

    async def stream_file_range(
        self,
        file_message: Message,
        start_byte: int,
        end_byte: int,
        content_length: int,
        chunk_size: int = Stream.CHUNK_SIZE,
    ) -> AsyncGenerator[bytes, None]:
        """
        Streams a byte range [start_byte, end_byte] with 0ms cache hits and continuous MTProto streaming.
        """
        offset_chunks = start_byte // chunk_size
        total_bytes = end_byte - start_byte + 1
        chunks_to_stream = ceil(total_bytes / chunk_size)
        
        # Check cache if single chunk request (e.g. video player metadata probe)
        cache_key = f"{file_message.id}_{offset_chunks}"
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

        client = await worker_pool.get_client()
        bytes_streamed = 0
        chunk_index = 0

        try:
            async for chunk in client.stream_media(
                file_message,
                offset=offset_chunks,
                limit=chunks_to_stream,
            ):
                if not chunk:
                    break

                # Cache initial metadata chunks and single chunks in memory
                current_chunk_pos = offset_chunks + chunk_index
                if current_chunk_pos == 0 or chunks_to_stream == 1:
                    await chunk_cache.set(f"{file_message.id}_{current_chunk_pos}", chunk)

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
        except asyncio.CancelledError:
            # Client disconnected or seeked to another position
            pass
        except Exception as e:
            logger.error(f"Error streaming msg {file_message.id}: {e}")
            raise

fast_streamer = FastStreamEngine()
