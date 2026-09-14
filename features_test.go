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

func TestReservedKeyPrefixRejected(t *testing.T) {
	e := newEnv(t)
	if err := e.c.Set(ctx, "cachex:lock:x", []byte("v"), time.Minute); !errors.Is(err, cachex.ErrInvalidKey) {
		t.Fatalf("want ErrInvalidKey, got %v", err)
	}
	if _, err := e.c.GetMulti(ctx, []string{"ok", "cachex:ns:a:v"}); !errors.Is(err, cachex.ErrInvalidKey) {
		t.Fatalf("GetMulti: want ErrInvalidKey, got %v", err)
	}
}

func TestNegativeCaching(t *testing.T) {
	e := newEnv(t, cachex.WithNegativeTTL(30*time.Second), cachex.WithL1TTL(time.Hour))
	var calls atomic.Int32
	notFound := func(context.Context) ([]byte, error) { calls.Add(1); return nil, cachex.ErrNotFound }

	for range 3 {
		if _, err := e.c.GetOrLoad(ctx, "ghost", time.Minute, notFound); !errors.Is(err, cachex.ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader ran %d times, want 1", calls.Load())
	}
	if _, err := e.c.Get(ctx, "ghost"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("Get on tombstone: want ErrMiss, got %v", err)
	}
	if s := e.c.Stats(); s.NegativeHits != 2 || s.LoadErrors != 0 {
		t.Fatalf("stats %+v", s)
	}
	e.clock.Advance(31 * time.Second)
	v, err := e.c.GetOrLoad(ctx, "ghost", time.Minute, func(context.Context) ([]byte, error) { return []byte("born"), nil })
	if err != nil || string(v) != "born" {
		t.Fatalf("after negative TTL want reload, got %q %v", v, err)
	}
}

func TestNegativeCachingDisabledByDefault(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int32
	for range 2 {
		_, _ = e.c.GetOrLoad(ctx, "ghost", time.Minute, func(context.Context) ([]byte, error) {
			calls.Add(1)
			return nil, cachex.ErrNotFound
		})
	}
	if calls.Load() != 2 {
		t.Fatalf("without WithNegativeTTL loader should run each time, ran %d", calls.Load())
	}
}

func TestDistributedLockOneLoaderAcrossNodes(t *testing.T) {
	clock := newClock()
	l2 := memstore.NewWithClock(clock.Now)
	var calls atomic.Int32
	release := make(chan struct{})
	load := func(context.Context) ([]byte, error) {
		calls.Add(1)
		<-release
		return []byte("v"), nil
	}
	const nodes = 5
	caches := make([]*cachex.Cache, nodes)
	for i := range caches {
		c, err := cachex.New(cachex.WithClock(clock), cachex.WithL2(l2), cachex.WithSweepInterval(0),
			cachex.WithDistributedLock(5*time.Second, 5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		caches[i] = c
	}
	var wg sync.WaitGroup
	results := make(chan string, nodes)
	for _, c := range caches {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.GetOrLoad(ctx, "hot", time.Minute, load)
			if err != nil {
				results <- "err:" + err.Error()
				return
			}
			results <- string(v)
		}()
	}
	waitFor(t, func() bool {
		var waits uint64
		for _, c := range caches {
			waits += c.Stats().LockWaits
		}
		return calls.Load() == 1 && waits == nodes-1
	})
	close(release)
	wg.Wait()
	close(results)
	for r := range results {
		if r != "v" {
			t.Fatalf("node got %q", r)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader ran %d times across nodes, want 1", calls.Load())
	}
	if _, err := l2.Get(ctx, "cachex:lock:hot"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("lock not released: %v", err)
	}
}

func TestDistributedLockFailsOpenWhenL2Down(t *testing.T) {
	e := newEnv(t, cachex.WithDistributedLock(time.Second, time.Millisecond))
	e.l2.SetDown(true)
	v, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("v"), nil })
	if err != nil || string(v) != "v" {
		t.Fatalf("got %q %v", v, err)
	}
}

func TestDistributedLockWaiterLoadsAfterTimeout(t *testing.T) {
	e := newEnv(t, cachex.WithDistributedLock(20*time.Millisecond, 2*time.Millisecond))
	// A crashed holder left the lock behind.
	_ = e.l2.Add(ctx, "cachex:lock:k", []byte{1}, time.Hour)
	v, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("v"), nil })
	if err != nil || string(v) != "v" {
		t.Fatalf("waiter must load after lock TTL, got %q %v", v, err)
	}
}

func TestGetMulti(t *testing.T) {
	e := newEnv(t)
	other, _ := cachex.New(cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithSweepInterval(0))
	defer other.Close()
	_ = e.c.Set(ctx, "a", []byte("1"), time.Minute)
	_ = e.c.Set(ctx, "b", []byte("2"), time.Minute)
	_, _ = other.Get(ctx, "a") // a in other's L1, b only in L2

	before := e.l2.Ops()
	got, err := other.GetMulti(ctx, []string{"a", "b", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got["a"]) != "1" || string(got["b"]) != "2" {
		t.Fatalf("got %q", got)
	}
	if ops := e.l2.Ops() - before; ops != 1 {
		t.Fatalf("want one L2 round trip, got %d", ops)
	}
	if _, err := other.Get(ctx, "b"); err != nil || other.Stats().L1Hits < 2 {
		t.Fatalf("GetMulti should backfill L1: %v %+v", err, other.Stats())
	}
}

func TestGetMultiL2Down(t *testing.T) {
	e := newEnv(t, cachex.WithL1TTL(time.Hour))
	_ = e.c.Set(ctx, "a", []byte("1"), time.Minute)
	e.l2.SetDown(true)
	got, err := e.c.GetMulti(ctx, []string{"a", "b"})
	if err != nil || len(got) != 1 || string(got["a"]) != "1" {
		t.Fatalf("degraded GetMulti: %q %v", got, err)
	}
}

func TestInvalidatorDropsRemoteL1(t *testing.T) {
	clock := newClock()
	l2 := memstore.NewWithClock(clock.Now)
	bus := memstore.NewBus()
	mk := func() *cachex.Cache {
		c, err := cachex.New(cachex.WithClock(clock), cachex.WithL2(l2), cachex.WithSweepInterval(0),
			cachex.WithL1TTL(time.Hour), cachex.WithVersionTTL(time.Hour), cachex.WithInvalidator(bus))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	a, b := mk(), mk()

	_ = a.Set(ctx, "k", []byte("old"), time.Hour)
	_, _ = b.Get(ctx, "k")
	_ = a.Delete(ctx, "k")
	if _, err := b.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("broadcast Delete must clear remote L1 immediately, got %v", err)
	}

	nsA, _ := a.Namespace("users")
	nsB, _ := b.Namespace("users")
	_ = nsA.Set(ctx, "1", []byte("alice"), time.Hour)
	if v, _ := nsB.Get(ctx, "1"); string(v) != "alice" {
		t.Fatalf("setup: %q", v)
	}
	_ = nsA.Invalidate(ctx)
	if _, err := nsB.Get(ctx, "1"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("broadcast Invalidate must be visible before version TTL, got %v", err)
	}
}

func TestNamespaceSetReturnsErrorWhenVersionUnknown(t *testing.T) {
	e := newEnv(t)
	e.l2.SetDown(true)
	ns, _ := e.c.Namespace("p")
	if err := ns.Set(ctx, "k", []byte("v"), time.Minute); !errors.Is(err, cachex.ErrL2Unavailable) {
		t.Fatalf("want ErrL2Unavailable, got %v", err)
	}
}
