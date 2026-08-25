import time
import asyncio
from collections import OrderedDict
from logging import getLogger
from bot.config import Stream

logger = getLogger('bot')

class ChunkCache:
    """
    High-performance in-memory LRU cache for Telegram file chunks.
    Ensures 0ms instant media playback starts and rapid seeking by
    caching critical media container headers (moov/ftyp) and hot video segments.
    """
    def __init__(self, max_size_mb: int = Stream.CACHE_SIZE_MB, ttl: int = Stream.CACHE_TTL_SEC):
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.ttl = ttl
        self._cache: OrderedDict[str, tuple[bytes, float]] = OrderedDict()
        self._current_size = 0
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    async def get(self, key: str) -> bytes | None:
        async with self._lock:
            if key not in self._cache:
                self._misses += 1
                return None
            
            data, timestamp = self._cache[key]
            if self.ttl > 0 and (time.time() - timestamp) > self.ttl:
                # Expired
                self._current_size -= len(data)
                del self._cache[key]
                self._misses += 1
                return None

            # Move to end (most recently used)
            self._cache.move_to_end(key)
            self._hits += 1
            return data

    async def set(self, key: str, data: bytes):
        data_len = len(data)
        if data_len > self.max_size_bytes:
            return  # Chunk larger than entire cache

        async with self._lock:
            if key in self._cache:
                old_data, _ = self._cache[key]
                self._current_size -= len(old_data)
                del self._cache[key]

            # Evict least recently used items until space is available
            while (self._current_size + data_len) > self.max_size_bytes and self._cache:
                _, (evicted_data, _) = self._cache.popitem(last=False)
                self._current_size -= len(evicted_data)

            self._cache[key] = (data, time.time())
            self._current_size += data_len

    def get_stats(self) -> dict:
        total_requests = self._hits + self._misses
        hit_ratio = (self._hits / total_requests * 100) if total_requests > 0 else 0.0
        return {
            "cached_chunks": len(self._cache),
            "cached_size_mb": round(self._current_size / (1024 * 1024), 2),
            "max_size_mb": round(self.max_size_bytes / (1024 * 1024), 2),
            "hits": self._hits,
            "misses": self._misses,
            "hit_ratio_percent": round(hit_ratio, 2)
        }

# Global chunk cache instance
chunk_cache = ChunkCache()
