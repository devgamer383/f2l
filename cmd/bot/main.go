package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"f2l/internal/cache"
	"f2l/internal/bot"
	"f2l/internal/config"
	"f2l/internal/server"
	"f2l/internal/stream"
	"f2l/internal/telegram"
)

func main() {
	// 1. Structured logging setup
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slog.SetDefault(slog.New(logHandler))

	slog.Info("🚀 Initializing Telegram File-to-Link Service (Go Edition)")

	// 2. Load Configuration
	cfg := config.Load()

	if cfg.Telegram.APIID == 0 || cfg.Telegram.APIHash == "" || cfg.Telegram.BotToken == "" {
		slog.Error("FATAL: Missing required Telegram credentials (TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_BOT_TOKEN)")
		os.Exit(1)
	}

	if cfg.Telegram.ChannelID == 0 {
		slog.Error("FATAL: Missing TELEGRAM_CHANNEL_ID")
		os.Exit(1)
	}

	slog.Info("Configuration loaded",
		"port", cfg.Server.Port,
		"bind", cfg.Server.BindAddress,
		"channel_id", cfg.Telegram.ChannelID,
		"secondary_bots", len(cfg.Telegram.SecondaryBotTokens),
		"cache_mb", cfg.Stream.CacheSizeMB,
		"base_url", cfg.Server.BaseURL,
	)

	// 3. Graceful shutdown context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 4. Memory LRU Cache
	chunkCache := cache.NewLRUCache(cfg.Stream.CacheSizeMB, cfg.Stream.CacheTTLSec)

	// 5. Bot Pool & MTProto Client
	pool := telegram.NewBotPool(cfg)

	// 6. Fast Stream & Virtual Multi-Part Engines
	fastStreamer := stream.NewFastStreamEngine(
		pool,
		chunkCache,
		cfg.Stream.ChunkSize,
		cfg.Stream.PrefetchChunks,
	)
	virtualStreamer := stream.NewVirtualStreamEngine(fastStreamer, pool)

	// 7. Bot Update Dispatcher & Handlers
	botEngine := bot.NewBot(cfg, pool, virtualStreamer, chunkCache)

	// 8. Start Telegram Bot Pool
	slog.Info("Starting Telegram MTProto bot pool...")
	if err := pool.Start(ctx, botEngine.Dispatcher()); err != nil {
		slog.Error("Failed to start bot pool", "error", err)
		os.Exit(1)
	}

	// 9. HTTP Web & Stream Server
	webServer := server.NewServer(cfg, chunkCache, fastStreamer, virtualStreamer)
	addr := fmt.Sprintf("%s:%d", cfg.Server.BindAddress, cfg.Server.Port)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           webServer.Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Launch HTTP Server
	go func() {
		slog.Info("🌐 HTTP Server listening", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server failed", "error", err)
			stop()
		}
	}()

	// Wait for pool to report ready in background
	go func() {
		readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := pool.WaitReady(readyCtx); err != nil {
			slog.Warn("Bot pool wait ready timed out or canceled", "error", err)
		} else {
			slog.Info("✨ Bot pool is fully ready and connected to storage channel!")
		}
	}()

	// 10. Wait for OS Termination Signal
	<-ctx.Done()
	slog.Info("Shutting down gracefully...")

	// 11. Drain HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP server shutdown error", "error", err)
	}

	slog.Info("👋 Service terminated successfully")
}
