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
        self.primary_bot: Client | None = None
        self._index = 0
        self._lock = asyncio.Lock()
        self._is_started = False

    def register_primary(self, primary_client: Client):
        self.primary_bot = primary_client
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
                        sleep_threshold=60,
                        max_concurrent_transmissions=Stream.MAX_CONCURRENT_TRANSMISSIONS,
                    )
                    await worker.start()
                    
                    # Verify channel access
                    try:
                        await worker.get_chat(Telegram.CHANNEL_ID)
                        self.clients.append(worker)
                        logger.info(f"✅ Secondary worker #{i + 1} verified & active in storage channel.")
                    except Exception as ch_err:
                        logger.warning(
                            f"⚠️ Secondary worker #{i + 1} cannot access channel {Telegram.CHANNEL_ID}: {ch_err}. "
                            "Please add this bot as an ADMIN in your storage channel to enable multi-bot speedup!"
                        )
                        # Stop worker if it has no channel access to avoid stream failures
                        await worker.stop()
                except Exception as e:
                    logger.error(f"Failed to start secondary worker #{i + 1}: {e}")
        
        self._is_started = True
        logger.info(f"Worker pool ready with {len(self.clients)} active Telegram client(s).")

    async def stop_workers(self):
        for client in self.clients:
            if client != self.primary_bot:
                try:
                    if client.is_connected:
                        await client.stop()
                except Exception as e:
                    logger.warning(f"Error stopping worker client: {e}")
        self._is_started = False

    async def get_client(self) -> Client:
        if not self.clients:
            if self.primary_bot:
                return self.primary_bot
            raise RuntimeError("No Telegram clients available in worker pool.")
        if len(self.clients) == 1:
            return self.clients[0]
        
        async with self._lock:
            client = self.clients[self._index % len(self.clients)]
            self._index += 1
            return client

worker_pool = WorkerPool()
