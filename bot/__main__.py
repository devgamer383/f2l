from bot import TelegramBot
from bot.server import server
from bot.stream import worker_pool

if __name__ == '__main__':
    # Register primary Telegram bot in worker pool
    worker_pool.register_primary(TelegramBot)
    
    # Launch Quart + Uvicorn server in bot event loop
    TelegramBot.loop.create_task(server.serve())
    
    # Run Telegram Bot
    TelegramBot.run()
