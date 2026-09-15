package cachex_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock { return &fakeClock{now: time.Unix(1_700_000_000, 0)} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

type env struct {
	c     *cachex.Cache
	l2    *memstore.Store
	clock *fakeClock
}

func newEnv(t *testing.T, opts ...cachex.Option) env {
	t.Helper()
	clock := newClock()
	l2 := memstore.NewWithClock(clock.Now)
	base := []cachex.Option{
		cachex.WithClock(clock), cachex.WithL2(l2), cachex.WithSweepInterval(0),
		cachex.WithRand(func() float64 { return 1 }), // ln(1)=0: no early refresh unless a test sets it
	}
	c, err := cachex.New(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return env{c: c, l2: l2, clock: clock}
}

var ctx = context.Background()

func TestSetGet(t *testing.T) {
	e := newEnv(t)
	if _, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("want miss, got %v", err)
	}
	if err := e.c.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := e.c.Get(ctx, "k")
	if err != nil || string(got) != "v" {
		t.Fatalf("got %q %v", got, err)
	}
	got[0] = 'x' // caller mutation must not reach the cache
	if again, _ := e.c.Get(ctx, "k"); string(again) != "v" {
		t.Fatalf("cache aliased caller slice: %q", again)
	}
}

func TestTTLExpiry(t *testing.T) {
	e := newEnv(t, cachex.WithL1TTL(time.Hour))
	_ = e.c.Set(ctx, "k", []byte("v"), 5*time.Second)
	e.clock.Advance(4 * time.Second)
	if _, err := e.c.Get(ctx, "k"); err != nil {
		t.Fatalf("before expiry: %v", err)
	}
	e.clock.Advance(2 * time.Second)
	if _, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("after expiry want miss, got %v", err)
	}
}

func TestL2BackfillsL1(t *testing.T) {
	e := newEnv(t)
	_ = e.c.Set(ctx, "k", []byte("v"), time.Minute)
	// Second process sharing the same L2.
	other, err := cachex.New(cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	for range 3 {
		if v, err := other.Get(ctx, "k"); err != nil || string(v) != "v" {
			t.Fatalf("got %q %v", v, err)
		}
	}
	s := other.Stats()
	if s.L2Hits != 1 || s.L1Hits != 2 {
		t.Fatalf("want 1 L2 hit then 2 L1 hits, got %+v", s)
	}
}

func TestL1TTLBoundsCrossNodeStaleness(t *testing.T) {
	e := newEnv(t, cachex.WithL1TTL(2*time.Second))
	other, _ := cachex.New(cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithSweepInterval(0),
		cachex.WithL1TTL(2*time.Second))
	defer other.Close()
	_ = e.c.Set(ctx, "k", []byte("old"), time.Minute)
	_, _ = other.Get(ctx, "k") // other now holds "old" in L1
	_ = e.c.Delete(ctx, "k")
	if v, _ := other.Get(ctx, "k"); string(v) != "old" {
		t.Fatalf("expected stale L1 copy inside window, got %q", v)
	}
	e.clock.Advance(3 * time.Second)
	if _, err := other.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("stale copy must be gone after L1 TTL, got %v", err)
	}
}

func TestDeleteBothTiers(t *testing.T) {
	e := newEnv(t)
	_ = e.c.Set(ctx, "k", []byte("v"), time.Minute)
	if err := e.c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := secondNode(t, e).Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("L2 still serves key to other nodes: %v", err)
	}
	if _, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("want miss, got %v", err)
	}
}

func TestDeleteReturnsL2Failure(t *testing.T) {
	e := newEnv(t)
	e.l2.SetDown(true)
	if err := e.c.Delete(ctx, "k"); !errors.Is(err, cachex.ErrL2Unavailable) {
		t.Fatalf("want ErrL2Unavailable, got %v", err)
	}
}

func TestL2DownDegradesToL1(t *testing.T) {
	e := newEnv(t, cachex.WithDegradedL1TTL(time.Second))
	e.l2.SetDown(true)
	if err := e.c.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("Set must be best effort, got %v", err)
	}
	if v, err := e.c.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("L1 should serve during outage: %q %v", v, err)
	}
	e.clock.Advance(2 * time.Second)
	if _, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("degraded copy should expire fast, got %v", err)
	}
	if e.c.Stats().L2Errors == 0 {
		t.Fatal("L2Errors not counted")
	}
}

func TestBreakerOpensAndSkipsL2(t *testing.T) {
	e := newEnv(t)
	e.l2.SetDown(true)
	for i := range 40 {
		_, _ = e.c.Get(ctx, "k"+string(rune('a'+i%26)))
	}
	before := e.l2.Ops()
	for range 10 {
		_, _ = e.c.Get(ctx, "zz")
	}
	if e.l2.Ops() != before {
		t.Fatalf("open breaker must not call L2 (ops %d -> %d)", before, e.l2.Ops())
	}
	s := e.c.Stats()
	if s.Breaker != "open" || s.L2Skipped == 0 {
		t.Fatalf("want open breaker with skips, got %+v", s)
	}
}

func TestGetOrLoadSingleFlight(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int32
	release := make(chan struct{})
	load := func(context.Context) ([]byte, error) {
		calls.Add(1)
		<-release
		return []byte("v"), nil
	}
	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := e.c.GetOrLoad(ctx, "k", time.Minute, load)
			if err == nil && string(v) != "v" {
				err = errors.New("wrong value " + string(v))
			}
			errs <- err
		}()
	}
	waitFor(t, func() bool { return calls.Load() >= 1 })
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if c := calls.Load(); c != 1 {
		t.Fatalf("loader ran %d times, want 1", c)
	}
	if v, err := e.c.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("loaded value not cached: %q %v", v, err)
	}
}

func TestGetOrLoadErrorNotCached(t *testing.T) {
	e := newEnv(t)
	boom := errors.New("boom")
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	v, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("ok"), nil })
	if err != nil || string(v) != "ok" {
		t.Fatalf("got %q %v", v, err)
	}
}

func TestStaleWhileRevalidate(t *testing.T) {
	e := newEnv(t, cachex.WithStaleWindow(time.Minute), cachex.WithL1TTL(time.Hour))
	_, _ = e.c.GetOrLoad(ctx, "k", 10*time.Second, func(context.Context) ([]byte, error) { return []byte("v1"), nil })
	e.clock.Advance(15 * time.Second)

	refreshed := make(chan struct{})
	v, err := e.c.GetOrLoad(ctx, "k", 10*time.Second, func(context.Context) ([]byte, error) {
		defer close(refreshed)
		return []byte("v2"), nil
	})
	if err != nil || string(v) != "v1" {
		t.Fatalf("want stale v1, got %q %v", v, err)
	}
	select {
	case <-refreshed:
	case <-time.After(2 * time.Second):
		t.Fatal("background refresh did not run")
	}
	waitFor(t, func() bool { v, _ := e.c.Get(ctx, "k"); return string(v) == "v2" })
	if e.c.Stats().StaleServed != 1 {
		t.Fatalf("StaleServed = %d", e.c.Stats().StaleServed)
	}
}

func TestExpiredBeyondStaleWindowLoadsSync(t *testing.T) {
	e := newEnv(t, cachex.WithStaleWindow(time.Second), cachex.WithL1TTL(time.Hour))
	_, _ = e.c.GetOrLoad(ctx, "k", 10*time.Second, func(context.Context) ([]byte, error) { return []byte("v1"), nil })
	e.clock.Advance(time.Minute)
	v, err := e.c.GetOrLoad(ctx, "k", 10*time.Second, func(context.Context) ([]byte, error) { return []byte("v2"), nil })
	if err != nil || string(v) != "v2" {
		t.Fatalf("want fresh v2, got %q %v", v, err)
	}
}

func TestXFetchEarlyRefresh(t *testing.T) {
	var r atomic.Value
	r.Store(1.0)
	e := newEnv(t, cachex.WithL1TTL(time.Hour), cachex.WithRand(func() float64 { return r.Load().(float64) }))
	slowClock := e.clock
	_, _ = e.c.GetOrLoad(ctx, "k", 10*time.Second, func(context.Context) ([]byte, error) {
		slowClock.Advance(2 * time.Second) // loader "costs" 2s => delta = 2s
		return []byte("v1"), nil
	})
	// Now 2s after store, expiry at 10s. Gap = 2s * beta * -ln(r).
	e.clock.Advance(5 * time.Second) // at 7s, 3s left
	r.Store(0.5)                     // gap ≈ 1.39s < 3s: no refresh
	_, _ = e.c.GetOrLoad(ctx, "k", 10*time.Second, func(context.Context) ([]byte, error) { return []byte("x"), nil })
	if e.c.Stats().EarlyRefreshes != 0 {
		t.Fatal("refreshed too early")
	}
	r.Store(0.01) // gap ≈ 9.2s >= 3s: refresh
	done := make(chan struct{})
	v, _ := e.c.GetOrLoad(ctx, "k", 10*time.Second, func(context.Context) ([]byte, error) {
		defer close(done)
		return []byte("v2"), nil
	})
	if string(v) != "v1" {
		t.Fatalf("early refresh must still return current value, got %q", v)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("early refresh did not run")
	}
	if e.c.Stats().EarlyRefreshes != 1 {
		t.Fatalf("EarlyRefreshes = %d", e.c.Stats().EarlyRefreshes)
	}
}

func TestInvalidKeyAndClosed(t *testing.T) {
	e := newEnv(t)
	if err := e.c.Set(ctx, "", nil, 0); !errors.Is(err, cachex.ErrInvalidKey) {
		t.Fatalf("want ErrInvalidKey, got %v", err)
	}
	_ = e.c.Close()
	if _, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
	if err := e.c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestOptionValidation(t *testing.T) {
	for name, opt := range map[string]cachex.Option{
		"nil l2":       cachex.WithL2(nil),
		"negative":     cachex.WithL1MaxEntries(-1),
		"zero ttl":     cachex.WithDefaultTTL(0),
		"neg beta":     cachex.WithBeta(-1),
		"neg stale":    cachex.WithStaleWindow(-time.Second),
		"zero version": cachex.WithVersionTTL(0),
	} {
		if _, err := cachex.New(opt); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestL1OnlyAndJanitor(t *testing.T) {
	c, err := cachex.New(cachex.WithSweepInterval(5 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.Set(ctx, "k", []byte("v"), 10*time.Millisecond)
	waitFor(t, func() bool { return c.Stats().Entries == 0 })
}

func TestConcurrentMixedOps(t *testing.T) {
	e := newEnv(t, cachex.WithL1MaxEntries(64))
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				k := string(rune('a' + (g+i)%20))
				switch i % 4 {
				case 0:
					_ = e.c.Set(ctx, k, []byte(k), time.Minute)
				case 1:
					_, _ = e.c.Get(ctx, k)
				case 2:
					_, _ = e.c.GetOrLoad(ctx, k, time.Minute, func(context.Context) ([]byte, error) { return []byte(k), nil })
				case 3:
					_ = e.c.Delete(ctx, k)
				}
			}
		}()
	}
	wg.Wait()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(time.Millisecond)
	}
}

func BenchmarkGetL1Hit(b *testing.B) {
	c, _ := cachex.New(cachex.WithSweepInterval(0))
	defer c.Close()
	_ = c.Set(ctx, "k", []byte("value"), time.Hour)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.Get(ctx, "k")
		}
	})
}

func TestDeleteDuringLoadDoesNotResurrectStaleValue(t *testing.T) {
	e := newEnv(t)
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		v, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
			close(started)
			<-release
			return []byte("old"), nil
		})
		if err != nil || string(v) != "old" {
			t.Errorf("loader caller got %q %v", v, err)
		}
	}()
	<-started
	if err := e.c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	if v, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("stale value cached after Delete: %q %v", v, err)
	}
	if _, err := secondNode(t, e).Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("stale value written to L2 after Delete: %v", err)
	}
}

func TestSetDuringLoadWins(t *testing.T) {
	e := newEnv(t)
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
			close(started)
			<-release
			return []byte("old"), nil
		})
	}()
	<-started
	_ = e.c.Set(ctx, "k", []byte("new"), time.Minute)
	close(release)
	<-done
	if v, err := e.c.Get(ctx, "k"); err != nil || string(v) != "new" {
		t.Fatalf("got %q %v, want new", v, err)
	}
}

func TestLoadAfterDeleteStartsFreshFlight(t *testing.T) {
	e := newEnv(t)
	started, release := make(chan struct{}), make(chan struct{})
	go func() {
		_, _ = e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
			close(started)
			<-release
			return []byte("old"), nil
		})
	}()
	<-started
	defer close(release)
	_ = e.c.Delete(ctx, "k")
	c, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	v, err := e.c.GetOrLoad(c, "k", time.Minute, func(context.Context) ([]byte, error) {
		return []byte("fresh"), nil
	})
	if err != nil || string(v) != "fresh" {
		t.Fatalf("got %q %v, want fresh (joined pre-Delete flight?)", v, err)
	}
}

func TestCallerCancellationDoesNotTripBreaker(t *testing.T) {
	e := newEnv(t)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	for i := 0; i < 100; i++ {
		_, _ = e.c.Get(cctx, "k")
		_ = e.c.Set(cctx, "k", []byte("v"), time.Minute)
	}
	st := e.c.Stats()
	if st.L2Errors != 0 || st.Breaker != "closed" {
		t.Fatalf("cancelled calls counted as L2 failures: errors=%d breaker=%s", st.L2Errors, st.Breaker)
	}
}

func TestCloseDoesNotCountL2Errors(t *testing.T) {
	e := newEnv(t)
	_ = e.l2.Close()
	for i := 0; i < 100; i++ {
		_, _ = e.c.Get(ctx, "k")
	}
	if st := e.c.Stats(); st.L2Errors != 0 {
		t.Fatalf("ErrClosed counted as transport failure: %d", st.L2Errors)
	}
}
