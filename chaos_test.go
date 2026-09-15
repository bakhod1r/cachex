package cachex_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
)

// After an L2 outage trips the breaker, restoring L2 must close it again and
// writes must reach L2, so a second instance sees them.
func TestChaosBreakerRecoversAfterOutage(t *testing.T) {
	e := newEnv(t)
	e.l2.SetDown(true)
	for i := range 40 {
		_, _ = e.c.Get(ctx, fmt.Sprintf("k%d", i))
	}
	if s := e.c.Stats(); s.Breaker != "open" {
		t.Fatalf("breaker %q, want open", s.Breaker)
	}

	e.l2.SetDown(false)
	e.clock.Advance(2 * time.Minute) // past any backed-off cooldown
	for i := 0; i < 5 && e.c.Stats().Breaker != "closed"; i++ {
		_, _ = e.c.Get(ctx, "probe")
	}
	if s := e.c.Stats(); s.Breaker != "closed" {
		t.Fatalf("breaker %q after recovery, want closed", s.Breaker)
	}

	if err := e.c.Set(ctx, "after", []byte("v2"), time.Hour); err != nil {
		t.Fatalf("Set after recovery: %v", err)
	}
	other, err := cachex.New(cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if v, err := other.Get(ctx, "after"); err != nil || string(v) != "v2" {
		t.Fatalf("other instance Get = %q, %v; want v2", v, err)
	}
}

// Concurrent traffic while L2 flaps between up, down and injected errors must
// never panic, deadlock or return unexpected errors; once L2 is stable again the
// cache must converge: a write is readable from a fresh instance.
func TestChaosFlappingL2UnderLoad(t *testing.T) {
	e := newEnv(t, cachex.WithLoadTimeout(time.Second))
	boom := errors.New("boom")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var unexpected atomic.Value

	allowed := func(err error) bool {
		return err == nil || errors.Is(err, cachex.ErrMiss) || errors.Is(err, cachex.ErrL2Unavailable) ||
			errors.Is(err, boom) || errors.Is(err, context.DeadlineExceeded)
	}
	check := func(op string, err error) {
		if !allowed(err) {
			unexpected.CompareAndSwap(nil, fmt.Errorf("%s: %w", op, err))
		}
	}

	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("k%d", (i+w)%32)
				switch i % 5 {
				case 0:
					check("Set", e.c.Set(ctx, k, []byte(k), time.Minute))
				case 1:
					_, err := e.c.Get(ctx, k)
					check("Get", err)
				case 2:
					_, err := e.c.GetOrLoad(ctx, k, time.Minute, func(context.Context) ([]byte, error) { return []byte(k), nil })
					check("GetOrLoad", err)
				case 3:
					check("Delete", e.c.Delete(ctx, k))
				case 4:
					_, err := e.c.GetMulti(ctx, []string{k, "k0", "k1"})
					check("GetMulti", err)
				}
			}
		}()
	}

	deadline := time.Now().Add(time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		switch i % 3 {
		case 0:
			e.l2.SetDown(true)
		case 1:
			e.l2.SetDown(false)
			e.l2.FailNext(5, boom)
		case 2:
			e.l2.SetDown(false)
		}
		e.clock.Advance(3 * time.Second)
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if err, _ := unexpected.Load().(error); err != nil {
		t.Fatalf("unexpected error under chaos: %v", err)
	}

	e.l2.SetDown(false)
	e.l2.FailNext(0, nil)
	e.clock.Advance(2 * time.Minute)
	for i := 0; i < 5 && e.c.Stats().Breaker != "closed"; i++ {
		_, _ = e.c.Get(ctx, "probe")
	}
	if err := e.c.Set(ctx, "final", []byte("ok"), time.Hour); err != nil {
		t.Fatalf("Set after chaos: %v (breaker %s)", err, e.c.Stats().Breaker)
	}
	fresh, err := cachex.New(cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if v, err := fresh.Get(ctx, "final"); err != nil || string(v) != "ok" {
		t.Fatalf("fresh instance Get = %q, %v; want ok", v, err)
	}
}

var _ cachex.Store = (*memstore.Store)(nil)
