from quart import Quart
from uvicorn import Server as UvicornServer, Config
from logging import getLogger
from bot.config import Server, LOGGER_CONFIG_JSON
from bot.stream import worker_pool
from . import main, error

logger = getLogger('uvicorn')
instance = Quart(__name__)
instance.config['RESPONSE_TIMEOUT'] = None
instance.config['MAX_CONTENT_LENGTH'] = 999999999999999

@instance.before_serving
async def before_serve():
    logger.info('🚀 High-Speed Web Server is starting...')
    logger.info(f'📡 Server listening on {Server.BIND_ADDRESS}:{Server.PORT}')
    try:
        await worker_pool.start_workers()
    except Exception as e:
        logger.warning(f'Worker pool startup notice: {e}')

@instance.after_serving
async def after_serve():
    logger.info('Shutting down stream workers...')
    try:
        await worker_pool.stop_workers()
    except Exception as e:
        logger.warning(f'Worker pool shutdown notice: {e}')

instance.register_blueprint(main.bp)

instance.register_error_handler(400, error.invalid_request)
instance.register_error_handler(404, error.not_found)
instance.register_error_handler(405, error.invalid_method)
instance.register_error_handler(error.HTTPError, error.http_error)

server = UvicornServer(
    Config(
        app=instance,
        host=Server.BIND_ADDRESS,
        port=Server.PORT,
        log_config=LOGGER_CONFIG_JSON,
        timeout_keep_alive=Server.KEEP_ALIVE,
        access_log=False  # Reduce logging overhead on heavy multi-part streaming
    )
)
