import asyncio
from typing import AsyncGenerator, Dict, Any, List
from logging import getLogger
from bot.config import Stream
from .engine import fast_streamer
from .virtual_db import virtual_db

logger = getLogger('bot')

class VirtualStreamEngine:
    """
    Virtual Multi-Part Concatenation Engine.
    Enables seamless single-link streaming and downloading across multiple 2GB Telegram parts
    (e.g., streaming a 10GB+ file composed of 5x 2GB parts) with transparent HTTP range routing.
    """

    async def stream_virtual_range(
        self,
        batch_id: str,
        start_byte: int,
        end_byte: int,
        content_length: int,
        chunk_size: int = Stream.CHUNK_SIZE
    ) -> AsyncGenerator[bytes, None]:
        """
        Streams a byte range across multiple split parts seamlessly.
        """
        batch = await virtual_db.get_batch(batch_id)
        if not batch:
            raise FileNotFoundError(f"Virtual batch {batch_id} not found")

        parts: List[Dict[str, Any]] = batch["parts"]
        if not parts:
            raise ValueError(f"Virtual batch {batch_id} has no parts")

        bytes_streamed = 0
        part_global_start = 0

        for part in parts:
            part_size = part["size"]
            part_global_end = part_global_start + part_size - 1

            # Check if requested range overlaps with this part
            overlap_start = max(start_byte, part_global_start)
            overlap_end = min(end_byte, part_global_end)

            if overlap_start <= overlap_end:
                # Calculate local offsets inside this part
                local_start = overlap_start - part_global_start
                local_end = overlap_end - part_global_start
                slice_len = local_end - local_start + 1

                logger.debug(
                    f"Streaming virtual batch {batch_id} part {part.get('message_id')}: "
                    f"bytes {local_start}-{local_end} (length: {slice_len})"
                )

                async for chunk in fast_streamer.stream_file_range(
                    file_id=part["message_id"],
                    start_byte=local_start,
                    end_byte=local_end,
                    content_length=slice_len,
                    chunk_size=chunk_size
                ):
                    if not chunk:
                        break
                    yield chunk
                    bytes_streamed += len(chunk)
                    if bytes_streamed >= content_length:
                        return

            part_global_start += part_size
            if bytes_streamed >= content_length:
                break

virtual_streamer = VirtualStreamEngine()
