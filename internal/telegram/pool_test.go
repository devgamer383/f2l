package telegram

import (
	"sync/atomic"
	"testing"
)

func TestGetDownloadWorkers(t *testing.T) {
	primary := &WorkerClient{
		ID:        0,
		IsPrimary: true,
	}
	primary.IsReady.Store(true)
	primary.HasChannelAccess.Store(true)

	worker1 := &WorkerClient{
		ID:        1,
		IsPrimary: false,
	}
	worker1.IsReady.Store(true)
	worker1.HasChannelAccess.Store(false) // No channel access

	worker2 := &WorkerClient{
		ID:        2,
		IsPrimary: false,
	}
	worker2.IsReady.Store(true)
	worker2.HasChannelAccess.Store(true) // Has channel access

	pool := &BotPool{
		primary: primary,
		workers: []*WorkerClient{primary, worker1, worker2},
	}

	dlWorkers := pool.GetDownloadWorkers()
	if len(dlWorkers) != 2 {
		t.Fatalf("expected 2 download workers, got %d", len(dlWorkers))
	}
	if dlWorkers[0].ID != 0 || dlWorkers[1].ID != 2 {
		t.Errorf("expected workers [0, 2], got [%d, %d]", dlWorkers[0].ID, dlWorkers[1].ID)
	}

	// When only worker1 is ready (no channel access), should fall back to primary
	worker2.HasChannelAccess.Store(false)
	primary.HasChannelAccess.Store(false) // even if primary has false, it should fallback to primary
	fallbackWorkers := pool.GetDownloadWorkers()
	if len(fallbackWorkers) != 1 || fallbackWorkers[0].ID != 0 {
		t.Errorf("expected fallback to primary, got %v", fallbackWorkers)
	}
}

func init() {
	_ = atomic.Bool{}
}
