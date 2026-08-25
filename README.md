<div align="center">
  <h1>⚡ Ultra-Fast File Stream Bot</h1>
  <p><b>High-Performance Telegram File Streaming & Direct Download Platform with Parallel Prefetching, In-Memory Caching & Universal Web Player</b></p>
  
  <p>
    <a href="https://heroku.com/deploy?template=https://github.com/TheCaduceus/FileStreamBot">
      <img src="https://www.herokucdn.com/deploy/button.svg" alt="Deploy to Heroku">
    </a>
  </p>
</div>

---

## 🚀 Key Features

- ⚡ **Parallel Chunk Prefetch Pipeline:** Downloads MTProto chunks ahead in parallel, eliminating stop-and-wait latency for maximum streaming throughput (3x–10x faster).
- 💾 **In-Memory LRU Header Cache:** Caches video container headers (ftyp/moov) and hot chunks in RAM for instantaneous (0ms) playback start and zero-lag seeking.
- 🤖 **Multi-Bot Worker Pool:** Support for multiple bot tokens (`MULTI_BOT_TOKENS`) to bypass Telegram's per-bot download bandwidth limits and achieve 10–50+ MB/s.
- 🌐 **Full RFC 7233 Range Support:** Compatible with VLC, MPV, MX Player, Infuse, Kodi, Smart TVs, IDM, Chrome, Safari, and Apple TV.
- 🎬 **Universal Web Player:** Sleek Dark / Glassmorphic UI with Plyr player, playback speed controls (0.25x - 2x), theater mode, picture-in-picture, and 1-click external player launchers.
- 📱 **1-Click Deep Links:** Direct launch buttons for VLC (`vlc://`), MX Player (`intent:...`), MPV (`mpv://`), and PotPlayer (`potplayer://`).
- ☁️ **Heroku 1-Click Ready:** Includes `app.json`, `Procfile`, `runtime.txt`, dynamic `$PORT` binding, and `/health` keep-alive ping endpoint.

---

## 🕹️ Deployment to Heroku

### Option A: 1-Click Deploy (Easiest)
1. Click the **Deploy to Heroku** button above.
2. Fill in your environment variables in the Heroku web interface.
3. Click **Deploy App**.
4. Set up a free ping monitor (e.g. on [UptimeRobot](https://uptimerobot.com)) pointing to `https://your-app.herokuapp.com/health` to keep your dyno awake 24/7.

### Option B: Deploy via Heroku CLI / Git
```bash
# 1. Login to Heroku
heroku login

# 2. Create a Heroku App
heroku create your-file-stream-bot

# 3. Set Config Vars
heroku config:set TELEGRAM_API_ID=1234567
heroku config:set TELEGRAM_API_HASH="your_api_hash"
heroku config:set TELEGRAM_BOT_TOKEN="your_bot_token"
heroku config:set TELEGRAM_BOT_USERNAME="your_bot_username"
heroku config:set OWNER_ID=12345678
heroku config:set TELEGRAM_CHANNEL_ID=-1001234567890

# 4. Push to Heroku
git add .
git commit -m "Deploy ultra-fast stream bot"
git push heroku main

# 5. Scale web dyno
heroku ps:scale web=1
```

---

## 📝 Environment Variables

| Variable | Description | Required | Default |
| :--- | :--- | :---: | :---: |
| `TELEGRAM_API_ID` | Telegram API ID from [my.telegram.org](https://my.telegram.org) | **Yes** | - |
| `TELEGRAM_API_HASH` | Telegram API Hash from [my.telegram.org](https://my.telegram.org) | **Yes** | - |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token from [@BotFather](https://t.me/BotFather) | **Yes** | - |
| `TELEGRAM_BOT_USERNAME`| Username of your Telegram bot (without `@`) | **Yes** | - |
| `OWNER_ID` | Telegram User ID of the Bot Owner | **Yes** | - |
| `TELEGRAM_CHANNEL_ID` | Private Telegram Storage Channel ID (must start with `-100`) | **Yes** | - |
| `BASE_URL` | App public URL (e.g. `https://your-app.herokuapp.com`). Auto-detected if empty. | No | `""` |
| `ALLOWED_USER_IDS` | Space-separated User IDs allowed to use the bot. Leave empty for all. | No | `""` |
| `MULTI_BOT_TOKENS` | Space or comma-separated secondary bot tokens for multi-worker streaming. | No | `""` |
| `CACHE_SIZE_MB` | Size of in-memory chunk cache in MB (for instant seeking). | No | `64` |
| `PREFETCH_CHUNKS` | Number of 1MB chunks to prefetch ahead in parallel. | No | `4` |
| `PORT` | Web server port (Heroku injects this automatically). | No | `8080` |

---

## 💻 Local / Docker Deployment

### Run Locally:
```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the bot
python -m bot
```

### Run with Docker:
```bash
docker build -t filestreambot .
docker run -p 8080:8080 --env-file .env filestreambot
```

---

## 📡 API Endpoints

- `GET /stream/{file_id}/{filename}`: Web Video Player interface.
- `GET /inline/{file_id}/{filename}`: Direct stream link (for VLC, MPV, MX Player, Smart TVs).
- `GET /dl/{file_id}/{filename}`: High-speed direct file download.
- `GET /info/{file_id}`: JSON file metadata API.
- `GET /health` or `GET /ping`: Health check & keep-alive endpoint.

---

## ❤️ Credits
- Developed and optimized for maximum speed and efficiency.
