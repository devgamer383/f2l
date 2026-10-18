import asyncio
from math import ceil
from logging import getLogger
from typing import AsyncGenerator
from hydrogram.types import Message
from bot.config import Telegram, Stream
from .cache import chunk_cache
from .worker import worker_pool

logger = getLogger('bot')

class FastStreamEngine:
    """
    High-Performance MTProto Stream Engine with True Sliding-Window Prefetching.
    Downloads chunks ahead in an asynchronous producer-consumer queue to eliminate
    stop-and-wait latency, with 0ms probe caching and instant seek cancellation.
    """

    async def stream_file_range(
        self,
        file_id: int,
        start_byte: int,
        end_byte: int,
        content_length: int,
        file_message: Message | None = None,
        chunk_size: int = Stream.CHUNK_SIZE,
    ) -> AsyncGenerator[bytes, None]:
        """
        Streams a byte range [start_byte, end_byte] with a background prefetch pipeline.
        """
        offset_chunks = start_byte // chunk_size
        total_bytes = end_byte - start_byte + 1
        chunks_to_stream = ceil(total_bytes / chunk_size)
        
        # Check cache if single chunk request (e.g. video player metadata probe / moov atom)
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

        # Select client: Primary bot has the authenticated MTProto session across all DCs
        client = worker_pool.primary_bot or await worker_pool.get_client()

        # Fast path: Re-use cached file message for primary bot (0ms message lookup)
        if file_message is not None:
            client_file = file_message
        else:
            client_file = await client.get_messages(Telegram.CHANNEL_ID, message_ids=file_id)
        
        if not client_file or client_file.empty:
            return

        # Sliding-window prefetch queue (holds up to PREFETCH_CHUNKS in memory)
        prefetch_limit = max(2, getattr(Stream, 'PREFETCH_CHUNKS', 4))
        chunk_queue: asyncio.Queue[bytes | Exception | None] = asyncio.Queue(maxsize=prefetch_limit)
        
        # Sentinel to indicate end of stream
        sentinel = object()

        async def prefetch_producer():
            """Background worker that continuously pulls chunks ahead of the player."""
            try:
                async for raw_chunk in client.stream_media(
                    client_file,
                    offset=offset_chunks,
                    limit=chunks_to_stream,
                ):
                    if not raw_chunk:
                        break
                    await chunk_queue.put(raw_chunk)
                await chunk_queue.put(sentinel)
            except asyncio.CancelledError:
                # Stream was seeked or aborted by client
                raise
            except Exception as exc:
                logger.warning(f"Prefetch producer error for msg {file_id}: {exc}")
                await chunk_queue.put(exc)

        producer_task = asyncio.create_task(prefetch_producer())

        bytes_streamed = 0
        chunk_index = 0

        try:
            while True:
                item = await chunk_queue.get()
                
                if item is sentinel:
                    break
                
                if isinstance(item, Exception):
                    raise item

                chunk: bytes = item

                # Cache metadata chunks and single probes in memory for instant reuse
                current_chunk_pos = offset_chunks + chunk_index
                if current_chunk_pos == 0 or chunks_to_stream == 1:
                    await chunk_cache.set(f"{file_id}_{current_chunk_pos}", chunk)

                # Trim first chunk if starting in the middle of a boundary
                if chunk_index == 0:
                    trim_start = start_byte % chunk_size
                    if trim_start > 0:
                        chunk = chunk[trim_start:]

                remaining_bytes = content_length - bytes_streamed
                if remaining_bytes <= 0:
                    break

                # Trim last chunk if ending in the middle of a boundary
                if len(chunk) > remaining_bytes:
                    chunk = chunk[:remaining_bytes]

                if not chunk:
                    break

                yield chunk
                bytes_streamed += len(chunk)
                chunk_index += 1

        except asyncio.CancelledError:
            # Video player sought to a new position or closed the tab
            return
        except Exception as e:
            logger.warning(f"Stream error on msg {file_id}: {e}")
            raise
        finally:
            # Clean up: If player seeks or disconnects, instantly abort background prefetcher
            if not producer_task.done():
                producer_task.cancel()
                try:
                    await producer_task
                except (asyncio.CancelledError, Exception):
                    pass

fast_streamer = FastStreamEngine()
