package memstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
	"github.com/bakhod1r/cachex/storetest"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func TestConformance(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	storetest.Run(t, func(t *testing.T) cachex.Store { return memstore.NewWithClock(clk.Now) }, clk.Advance)
}

func TestFaultInjection(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	boom := errors.New("boom")
	s.FailNext(2, boom)
	for i := 0; i < 2; i++ {
		if err := s.Set(ctx, "k", []byte("v"), 0); !errors.Is(err, boom) {
			t.Fatalf("op %d: err=%v, want boom", i, err)
		}
	}
	if err := s.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("after FailNext exhausted: %v", err)
	}
	s.SetDown(true)
	if _, err := s.Get(ctx, "k"); !errors.Is(err, cachex.ErrL2Unavailable) {
		t.Fatalf("down: err=%v", err)
	}
	s.SetDown(false)
	if _, err := s.Get(ctx, "k"); err != nil {
		t.Fatalf("up: %v", err)
	}
	if got := s.Ops(); got != 5 {
		t.Fatalf("Ops=%d want 5", got)
	}
	_ = s.Close()
	if _, err := s.Get(ctx, "k"); !errors.Is(err, cachex.ErrClosed) {
		t.Fatalf("closed: err=%v", err)
	}
}
