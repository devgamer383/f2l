import asyncio
from logging import getLogger
from typing import AsyncGenerator
from hydrogram.types import Message
from bot.config import Stream
from .cache import chunk_cache
from .worker import worker_pool

logger = getLogger('bot')

class FastStreamEngine:
    """
    High-Performance MTProto Parallel Prefetch Streamer.
    Uses asynchronous worker pooling and prefetch buffering to achieve
    maximum throughput, zero stuttering, and instant seek response times.
    """

    async def _fetch_chunk(self, file_message: Message, chunk_idx: int) -> bytes:
        cache_key = f"{file_message.id}_{chunk_idx}"
        
        # Check LRU in-memory cache first (0ms latency)
        cached_data = await chunk_cache.get(cache_key)
        if cached_data is not None:
            return cached_data

        client = await worker_pool.get_client()
        chunk_data = bytearray()

        try:
            async for chunk in client.stream_media(file_message, offset=chunk_idx, limit=1):
                if chunk:
                    chunk_data.extend(chunk)
        except Exception as e:
            logger.error(f"Error fetching chunk #{chunk_idx} for msg {file_message.id}: {e}")
            raise

        data_bytes = bytes(chunk_data)
        
        # Cache chunk (especially headers and hot chunks)
        if data_bytes:
            await chunk_cache.set(cache_key, data_bytes)

        return data_bytes

    async def stream_file_range(
        self,
        file_message: Message,
        start_byte: int,
        end_byte: int,
        chunk_size: int = Stream.CHUNK_SIZE,
        prefetch_count: int = Stream.PREFETCH_CHUNKS,
    ) -> AsyncGenerator[bytes, None]:
        """
        Streams a byte range [start_byte, end_byte] with concurrent chunk prefetching.
        """
        start_chunk = start_byte // chunk_size
        end_chunk = end_byte // chunk_size
        total_bytes_to_send = end_byte - start_byte + 1

        active_tasks: dict[int, asyncio.Task[bytes]] = {}
        pending_chunks = list(range(start_chunk, end_chunk + 1))
        
        # Semaphore to limit concurrent background downloads
        semaphore = asyncio.Semaphore(prefetch_count)

        async def _download_worker(c_idx: int) -> bytes:
            async with semaphore:
                return await self._fetch_chunk(file_message, c_idx)

        # Pre-seed initial tasks
        for _ in range(min(prefetch_count, len(pending_chunks))):
            next_idx = pending_chunks.pop(0)
            active_tasks[next_idx] = asyncio.create_task(_download_worker(next_idx))

        bytes_streamed = 0

        try:
            for current_chunk_idx in range(start_chunk, end_chunk + 1):
                if bytes_streamed >= total_bytes_to_send:
                    break

                # Spawn next task to maintain continuous pipeline buffer
                if pending_chunks:
                    next_idx = pending_chunks.pop(0)
                    active_tasks[next_idx] = asyncio.create_task(_download_worker(next_idx))

                task = active_tasks.pop(current_chunk_idx, None)
                if task is None:
                    task = asyncio.create_task(_download_worker(current_chunk_idx))

                try:
                    chunk_data = await task
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error awaiting chunk {current_chunk_idx}: {e}")
                    raise

                if not chunk_data:
                    break

                # Trim first chunk if starting in the middle of a chunk
                if current_chunk_idx == start_chunk:
                    trim_offset = start_byte % chunk_size
                    if trim_offset > 0:
                        chunk_data = chunk_data[trim_offset:]

                # Trim last chunk if ending in the middle
                remaining_bytes = total_bytes_to_send - bytes_streamed
                if len(chunk_data) > remaining_bytes:
                    chunk_data = chunk_data[:remaining_bytes]

                if not chunk_data:
                    break

                yield chunk_data
                bytes_streamed += len(chunk_data)

        finally:
            # Clean up all in-flight prefetch tasks immediately on disconnect/completion
            for t in active_tasks.values():
                if not t.done():
                    t.cancel()
            
            # Allow cancelled tasks to clean up quietly
            if active_tasks:
                await asyncio.gather(*active_tasks.values(), return_exceptions=True)

fast_streamer = FastStreamEngine()
