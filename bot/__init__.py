from hydrogram import Client
from logging import getLogger
from logging.config import dictConfig
from .config import Telegram, Stream, LOGGER_CONFIG_JSON

dictConfig(LOGGER_CONFIG_JSON)

version = "2.0"
logger = getLogger('bot')

TelegramBot = Client(
    name='bot',
    api_id=Telegram.API_ID,
    api_hash=Telegram.API_HASH,
    bot_token=Telegram.BOT_TOKEN,
    plugins={'root': 'bot/plugins'},
    sleep_threshold=60,  # Automatically wait up to 60s on FloodWait without breaking stream
    max_concurrent_transmissions=Stream.MAX_CONCURRENT_TRANSMISSIONS,
)
