import os
from os import environ as env

def _get_env_int(key: str, default: int = 0) -> int:
    val = env.get(key)
    if val:
        try:
            return int(val.strip())
        except ValueError:
            pass
    return default

def _get_env_str(key: str, default: str = "") -> str:
    return env.get(key, default).strip()

class Telegram:
    API_ID = _get_env_int("TELEGRAM_API_ID") or _get_env_int("API_ID")
    API_HASH = _get_env_str("TELEGRAM_API_HASH") or _get_env_str("API_HASH")
    OWNER_ID = _get_env_int("OWNER_ID")
    ALLOWED_USER_IDS = [int(x) for x in env.get("ALLOWED_USER_IDS", "").replace(",", " ").split() if x.isdigit()]
    BOT_USERNAME = _get_env_str("TELEGRAM_BOT_USERNAME") or _get_env_str("BOT_USERNAME")
    BOT_TOKEN = _get_env_str("TELEGRAM_BOT_TOKEN") or _get_env_str("BOT_TOKEN")
    CHANNEL_ID = _get_env_int("TELEGRAM_CHANNEL_ID") or _get_env_int("CHANNEL_ID")
    
    # Secondary bot tokens for high-speed multi-worker parallel chunk streaming
    _multi_tokens = env.get("MULTI_BOT_TOKENS", "").replace(",", " ").split()
    SECONDARY_BOT_TOKENS = [t.strip() for t in _multi_tokens if t.strip()]

class Stream:
    # 1MB MTProto optimal chunk size
    CHUNK_SIZE = 1024 * 1024
    # Number of chunks to prefetch ahead in memory buffer
    PREFETCH_CHUNKS = max(2, _get_env_int("PREFETCH_CHUNKS", 4))
    # LRU Chunk Cache memory limit in MB
    CACHE_SIZE_MB = max(16, _get_env_int("CACHE_SIZE_MB", 64))
    # Cache time-to-live in seconds
    CACHE_TTL_SEC = _get_env_int("CACHE_TTL_SEC", 1800)
    # Maximum concurrent client chunk downloaders
    MAX_CONCURRENT_TRANSMISSIONS = max(10, _get_env_int("MAX_CONCURRENT_TRANSMISSIONS", 20))

class Server:
    BASE_URL = _get_env_str("BASE_URL", "").rstrip("/")
    BIND_ADDRESS = _get_env_str("BIND_ADDRESS", "0.0.0.0")
    # Heroku automatically injects $PORT
    PORT = _get_env_int("PORT", 8080)
    KEEP_ALIVE = 60

# LOGGING CONFIGURATION
LOGGER_CONFIG_JSON = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': '[%(asctime)s][%(name)s][%(levelname)s] -> %(message)s',
            'datefmt': '%d/%m/%Y %H:%M:%S'
        },
    },
    'handlers': {
        'file_handler': {
            'class': 'logging.FileHandler',
            'filename': 'event-log.txt',
            'formatter': 'default',
            'encoding': 'utf-8'
        },
        'stream_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'default'
        }
    },
    'loggers': {
        'uvicorn': {
            'level': 'INFO',
            'handlers': ['file_handler', 'stream_handler'],
            'propagate': False
        },
        'uvicorn.error': {
            'level': 'WARNING',
            'handlers': ['file_handler', 'stream_handler'],
            'propagate': False
        },
        'bot': {
            'level': 'INFO',
            'handlers': ['file_handler', 'stream_handler'],
            'propagate': False
        },
        'hydrogram': {
            'level': 'WARNING',
            'handlers': ['file_handler', 'stream_handler'],
            'propagate': False
        }
    }
}
