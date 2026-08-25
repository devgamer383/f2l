import asyncio
from logging import getLogger
from hydrogram import Client
from bot.config import Telegram, Stream

logger = getLogger('bot')

class WorkerPool:
    """
    Multi-Client MTProto Worker Pool.
    Distributes chunk downloads across multiple bot tokens to bypass
    Telegram per-bot download bandwidth limits and achieve 10-50+ MB/s throughput.
    """
    def __init__(self):
        self.clients: list[Client] = []
        self._index = 0
        self._lock = asyncio.Lock()
        self._is_started = False

    def register_primary(self, primary_client: Client):
        if primary_client not in self.clients:
            self.clients.append(primary_client)

    async def start_workers(self):
        if self._is_started:
            return
        
        secondary_tokens = Telegram.SECONDARY_BOT_TOKENS
        if secondary_tokens:
            logger.info(f"Initializing {len(secondary_tokens)} secondary worker bot(s)...")
            for i, token in enumerate(secondary_tokens):
                try:
                    worker = Client(
                        name=f"worker_{i + 1}",
                        api_id=Telegram.API_ID,
                        api_hash=Telegram.API_HASH,
                        bot_token=token,
                        in_memory=True,
                        sleep_threshold=-1,
                        max_concurrent_transmissions=Stream.MAX_CONCURRENT_TRANSMISSIONS,
                    )
                    await worker.start()
                    self.clients.append(worker)
                    logger.info(f"Secondary worker #{i + 1} started successfully.")
                except Exception as e:
                    logger.error(f"Failed to start secondary worker #{i + 1}: {e}")
        
        self._is_started = True
        logger.info(f"Worker pool active with {len(self.clients)} total Telegram client(s).")

    async def stop_workers(self):
        # Stop secondary workers (primary bot is managed separately by app lifespan)
        for client in self.clients[1:]:
            try:
                if client.is_connected:
                    await client.stop()
            except Exception as e:
                logger.warning(f"Error stopping worker client: {e}")
        self._is_started = False

    async def get_client(self) -> Client:
        if not self.clients:
            raise RuntimeError("No Telegram clients available in worker pool.")
        if len(self.clients) == 1:
            return self.clients[0]
        
        async with self._lock:
            client = self.clients[self._index % len(self.clients)]
            self._index += 1
            return client

worker_pool = WorkerPool()
