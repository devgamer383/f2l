import asyncio
import time
import os
from bot import TelegramBot
from bot.config import Telegram
from bot.stream.uploader import fast_upload

async def main():
    await TelegramBot.start()
    print("Bot started!")
    
    # Create 10MB test file
    test_path = "test_10mb.dat"
    with open(test_path, "wb") as f:
        f.write(os.urandom(10 * 1024 * 1024))
    
    print("Uploading 10MB test file with fast_upload...")
    t0 = time.time()
    
    async def p(c, t):
        pct = (c / t) * 100
        el = max(0.01, time.time() - t0)
        print(f"Uploaded {c}/{t} ({pct:.1f}%) - Speed: {(c/el)/(1024*1024):.2f} MB/s")
    
    doc = await fast_upload(TelegramBot, test_path, progress=p, concurrency=8)
    dt = time.time() - t0
    speed = (10 / dt)
    print(f"fast_upload finished in {dt:.2f}s -> {speed:.2f} MB/s")
    
    print("Sending document to channel...")
    sent = await TelegramBot.send_document(
        chat_id=Telegram.CHANNEL_ID,
        document=doc,
        file_name="test_10mb.dat"
    )
    print(f"Sent successfully! Message ID: {sent.id}")
    
    if os.path.exists(test_path):
        os.remove(test_path)
    
    await TelegramBot.stop()

if __name__ == "__main__":
    asyncio.run(main())
