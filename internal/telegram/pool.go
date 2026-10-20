package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"f2l/internal/config"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

type WorkerClient struct {
	ID        int
	Client    *telegram.Client
	API       *tg.Client
	IsPrimary bool
	IsReady   atomic.Bool
	Token     string
}

type BotPool struct {
	cfg          *config.Config
	resolver     *ChannelResolver
	primary      *WorkerClient
	workers      []*WorkerClient
	workerIndex  uint64
	mu           sync.RWMutex
	readySignal  chan struct{}
	readyOnce    sync.Once
}

func NewBotPool(cfg *config.Config) *BotPool {
	return &BotPool{
		cfg:         cfg,
		resolver:    NewChannelResolver(cfg.Telegram.ChannelID),
		readySignal: make(chan struct{}),
	}
}

func (p *BotPool) Resolver() *ChannelResolver {
	return p.resolver
}

func (p *BotPool) Primary() *WorkerClient {
	return p.primary
}

func (p *BotPool) WaitReady(ctx context.Context) error {
	select {
	case <-p.readySignal:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start launches the primary bot and all secondary worker bots in background goroutines
func (p *BotPool) Start(ctx context.Context, dispatcher tg.UpdateDispatcher) error {
	// Ensure data directory for session storage
	_ = os.MkdirAll("data", 0755)

	// 1. Initialize Primary Bot with in-memory session
	primaryOpts := telegram.Options{
		SessionStorage: &session.StorageMemory{},
		UpdateHandler:  dispatcher,
	}

	primaryClient := telegram.NewClient(p.cfg.Telegram.APIID, p.cfg.Telegram.APIHash, primaryOpts)
	p.primary = &WorkerClient{
		ID:        0,
		Client:    primaryClient,
		API:       primaryClient.API(),
		IsPrimary: true,
		Token:     p.cfg.Telegram.BotToken,
	}
	p.workers = append(p.workers, p.primary)

	// Launch primary bot in background
	go func() {
		err := primaryClient.Run(ctx, func(runCtx context.Context) error {
			slog.Info("Authenticating primary bot token...")
			if _, err := primaryClient.Auth().Bot(runCtx, p.cfg.Telegram.BotToken); err != nil {
				return fmt.Errorf("primary bot auth failed: %w", err)
			}

			self, err := primaryClient.Self(runCtx)
			if err != nil {
				return fmt.Errorf("get primary self: %w", err)
			}
			slog.Info("✅ Primary bot logged in as @" + self.Username)

			// Resolve storage channel
			if _, err := p.resolver.GetInputChannel(runCtx, primaryClient.API()); err != nil {
				slog.Warn("Could not resolve storage channel on startup", "error", err)
			}

			p.primary.IsReady.Store(true)
			p.readyOnce.Do(func() {
				close(p.readySignal)
			})

			<-runCtx.Done()
			return nil
		})
		if err != nil && ctx.Err() == nil {
			slog.Error("Primary bot crashed", "error", err)
		}
	}()

	// 2. Initialize Secondary Worker Bots (in-memory sessions for upload acceleration)
	for i, token := range p.cfg.Telegram.SecondaryBotTokens {
		workerID := i + 1
		workerToken := token
		workerOpts := telegram.Options{
			SessionStorage: &session.StorageMemory{},
		}

		workerClient := telegram.NewClient(p.cfg.Telegram.APIID, p.cfg.Telegram.APIHash, workerOpts)
		worker := &WorkerClient{
			ID:        workerID,
			Client:    workerClient,
			API:       workerClient.API(),
			IsPrimary: false,
			Token:     workerToken,
		}

		p.mu.Lock()
		p.workers = append(p.workers, worker)
		p.mu.Unlock()

		go func(w *WorkerClient, tok string) {
			err := w.Client.Run(ctx, func(runCtx context.Context) error {
				slog.Info("Authenticating secondary worker...", "id", w.ID)
				if _, err := w.Client.Auth().Bot(runCtx, tok); err != nil {
					return fmt.Errorf("secondary worker %d auth: %w", w.ID, err)
				}
				self, _ := w.Client.Self(runCtx)
				username := ""
				if self != nil {
					username = self.Username
				}
				w.IsReady.Store(true)
				slog.Info("✅ Secondary worker ready", "id", w.ID, "username", username)
				<-runCtx.Done()
				return nil
			})
			if err != nil && ctx.Err() == nil {
				slog.Warn("Secondary worker failed", "id", w.ID, "error", err)
			}
		}(worker, workerToken)
	}

	return nil
}

// GetAvailableWorkers returns all currently connected & ready worker clients
func (p *BotPool) GetAvailableWorkers() []*WorkerClient {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var ready []*WorkerClient
	for _, w := range p.workers {
		if w.IsReady.Load() {
			ready = append(ready, w)
		}
	}
	if len(ready) == 0 && p.primary != nil {
		ready = append(ready, p.primary)
	}
	return ready
}

// NextWorker returns a worker in round-robin fashion for uploading
func (p *BotPool) NextWorker() *WorkerClient {
	workers := p.GetAvailableWorkers()
	if len(workers) == 0 {
		return p.primary
	}
	idx := atomic.AddUint64(&p.workerIndex, 1) % uint64(len(workers))
	return workers[idx]
}
