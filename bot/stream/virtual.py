import re
import asyncio
from typing import AsyncGenerator, Dict, Any, List, Optional
from logging import getLogger
from bot.config import Telegram, Stream
from bot.modules.telegram import get_file_properties
from .engine import fast_streamer
from .worker import worker_pool

logger = getLogger('bot')

# In-memory LRU cache for virtual file metadata (0ms lookup on repeat chunk requests)
_meta_cache: Dict[str, Dict[str, Any]] = {}
_meta_lock = asyncio.Lock()

def clean_virtual_name(raw_name: str) -> str:
    """Strips .001, .part1, etc. to produce a clean merged media filename."""
    cleaned = re.sub(r'(\.00[1-9]|\.0[1-9][0-9]|\.part0*[1-9]|\.z0[1-9])+$', '', raw_name, flags=re.IGNORECASE)
    cleaned = re.sub(r'[\\/*?:"<>|]', '_', cleaned).replace(" ", "-")
    return cleaned if cleaned else raw_name

def encode_batch(msg_ids: List[int]) -> str:
    """Encodes list of message IDs into a compact URL-safe token."""
    if not msg_ids:
        raise ValueError("Cannot encode empty message IDs list")
    # If consecutive, encode as s{start}c{count} e.g. s1050c5
    if len(msg_ids) > 1 and msg_ids == list(range(msg_ids[0], msg_ids[0] + len(msg_ids))):
        return f"s{msg_ids[0]}c{len(msg_ids)}"
    return "_".join(str(i) for i in msg_ids)

def decode_batch(batch_token: str) -> List[int]:
    """Decodes a compact URL-safe token into a list of message IDs."""
    m = re.match(r"^s(\d+)c(\d+)$", batch_token)
    if m:
        start = int(m.group(1))
        count = int(m.group(2))
        return list(range(start, start + count))
    m2 = re.match(r"^(\d+)-(\d+)$", batch_token)
    if m2:
        start = int(m2.group(1))
        end = int(m2.group(2))
        return list(range(start, end + 1))
    parts = [int(x) for x in batch_token.split("_") if x.isdigit()]
    if parts:
        return parts
    raise ValueError(f"Invalid batch token format: {batch_token}")

class VirtualStreamEngine:
    """
    Stateless 10GB+ Virtual Multi-Part Concatenation Engine.
    No database required: all state is encoded directly in the URL token,
    making links permanently valid across all server restarts and redeployments.
    """

    async def get_virtual_metadata(self, batch_token: str) -> Dict[str, Any]:
        """Resolves metadata for a virtual multi-part file from Telegram with RAM caching."""
        async with _meta_lock:
            if batch_token in _meta_cache:
                return _meta_cache[batch_token]

        msg_ids = decode_batch(batch_token)
        client = await worker_pool.get_client()

        # Batch fetch all messages in one single MTProto call
        messages = await client.get_messages(Telegram.CHANNEL_ID, message_ids=msg_ids)
        if not isinstance(messages, list):
            messages = [messages]

        parts: List[Dict[str, Any]] = []
        total_size = 0
        unified_name = ""
        mime_type = "video/x-matroska"

        for idx, m in enumerate(messages):
            if not m or m.empty:
                raise FileNotFoundError(f"Message ID {msg_ids[idx]} not found in storage channel")
            
            p_name, p_size, p_mime = get_file_properties(m)
            if p_size <= 0:
                raise ValueError(f"Message ID {msg_ids[idx]} has no valid media file")

            if idx == 0:
                unified_name = clean_virtual_name(p_name)
                mime_type = p_mime

            parts.append({
                "message_id": m.id,
                "size": p_size,
                "name": p_name
            })
            total_size += p_size

        meta = {
            "batch_id": batch_token,
            "filename": unified_name,
            "total_size": total_size,
            "mime_type": mime_type,
            "parts": parts
        }

        async with _meta_lock:
            # Keep cache bounded to 100 virtual file descriptors
            if len(_meta_cache) > 100:
                _meta_cache.pop(next(iter(_meta_cache)))
            _meta_cache[batch_token] = meta

        return meta

    async def stream_virtual_range(
        self,
        batch_id: str,
        start_byte: int,
        end_byte: int,
        content_length: int,
        chunk_size: int = Stream.CHUNK_SIZE
    ) -> AsyncGenerator[bytes, None]:
        """
        Streams a byte range across multiple split parts seamlessly without a database.
        """
        batch = await self.get_virtual_metadata(batch_id)
        parts: List[Dict[str, Any]] = batch["parts"]

        bytes_streamed = 0
        part_global_start = 0

        for part in parts:
            part_size = part["size"]
            part_global_end = part_global_start + part_size - 1

            # Check if requested range overlaps with this part
            overlap_start = max(start_byte, part_global_start)
            overlap_end = min(end_byte, part_global_end)

            if overlap_start <= overlap_end:
                local_start = overlap_start - part_global_start
                local_end = overlap_end - part_global_start
                slice_len = local_end - local_start + 1

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
