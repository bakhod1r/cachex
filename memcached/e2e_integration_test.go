//go:build integration

package memcached_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memcached"
)

// newPair builds two independent Caches (separate L1) sharing one memcached namespace.
func newPair(t *testing.T) (*cachex.Cache, *cachex.Cache) {
	t.Helper()
	addr := os.Getenv("MEMCACHED_ADDR")
	if addr == "" {
		t.Skip("MEMCACHED_ADDR not set")
	}
	prefix := fmt.Sprintf("e2e:%d:", time.Now().UnixNano())
	mk := func() *cachex.Cache {
		st, err := memcached.New(memcached.Config{Servers: []string{addr}, Timeout: time.Second, KeyPrefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		c, err := cachex.New(cachex.WithL2(st), cachex.WithVersionTTL(200*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close(); _ = st.Close() })
		return c
	}
	return mk(), mk()
}

func TestE2ESetGetAcrossInstances(t *testing.T) {
	a, b := newPair(t)
	ctx := context.Background()
	if err := a.Set(ctx, "user:1", []byte("alice"), time.Minute); err != nil {
		t.Fatal(err)
	}
	v, err := b.Get(ctx, "user:1")
	if err != nil || string(v) != "alice" {
		t.Fatalf("b.Get: %q %v", v, err)
	}
	if err := a.Delete(ctx, "user:1"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, "nope"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("want ErrMiss, got %v", err)
	}
}

func TestE2ENamespaceInvalidateAcrossInstances(t *testing.T) {
	a, b := newPair(t)
	ctx := context.Background()
	na, err := a.Namespace("users")
	if err != nil {
		t.Fatal(err)
	}
	nb, err := b.Namespace("users")
	if err != nil {
		t.Fatal(err)
	}
	if err := na.Set(ctx, "k", []byte("v1"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, err := nb.Get(ctx, "k"); err != nil || string(v) != "v1" {
		t.Fatalf("nb.Get before invalidate: %q %v", v, err)
	}
	if err := na.Invalidate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := na.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("invalidating instance must miss immediately, got %v", err)
	}
	time.Sleep(300 * time.Millisecond) // > version TTL
	if _, err := nb.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("other instance must miss after version TTL, got %v", err)
	}
	if err := nb.Set(ctx, "k", []byte("v2"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, err := na.Get(ctx, "k"); err != nil || string(v) != "v2" {
		t.Fatalf("na.Get after new write: %q %v", v, err)
	}
}

func TestE2EGetOrLoad(t *testing.T) {
	a, b := newPair(t)
	ctx := context.Background()
	var calls atomic.Int32
	load := func(context.Context) ([]byte, error) {
		calls.Add(1)
		return []byte("loaded"), nil
	}
	for _, c := range []*cachex.Cache{a, b, a} {
		v, err := c.GetOrLoad(ctx, "gol", time.Minute, load)
		if err != nil || string(v) != "loaded" {
			t.Fatalf("GetOrLoad: %q %v", v, err)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader calls = %d, want 1 (second instance must hit shared L2)", n)
	}
	boom := errors.New("boom")
	if _, err := b.GetOrLoad(ctx, "fail", time.Minute, func(context.Context) ([]byte, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("want loader error, got %v", err)
	}
	if _, err := a.Get(ctx, "fail"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("failed load must not be cached, got %v", err)
	}
}

func TestE2EGetMultiAndDistributedLock(t *testing.T) {
	addr := os.Getenv("MEMCACHED_ADDR")
	if addr == "" {
		t.Skip("MEMCACHED_ADDR not set")
	}
	prefix := fmt.Sprintf("e2elock%d:", time.Now().UnixNano())
	mk := func() *cachex.Cache {
		st, err := memcached.New(memcached.Config{Servers: []string{addr}, KeyPrefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		c, err := cachex.New(cachex.WithL2(st), cachex.WithDistributedLock(2*time.Second, 10*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	a, b := mk(), mk()
	ctx := context.Background()

	_ = a.Set(ctx, "x", []byte("1"), time.Minute)
	_ = a.Set(ctx, "y", []byte("2"), time.Minute)
	got, err := b.GetMulti(ctx, []string{"x", "y", "nope"})
	if err != nil || len(got) != 2 || string(got["x"]) != "1" || string(got["y"]) != "2" {
		t.Fatalf("GetMulti across instances: %q %v", got, err)
	}

	var calls atomic.Int32
	load := func(context.Context) ([]byte, error) {
		calls.Add(1)
		time.Sleep(150 * time.Millisecond)
		return []byte("hot"), nil
	}
	var wg sync.WaitGroup
	for _, c := range []*cachex.Cache{a, b, a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v, err := c.GetOrLoad(ctx, "hot", time.Minute, load); err != nil || string(v) != "hot" {
				t.Errorf("GetOrLoad: %q %v", v, err)
			}
		}()
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("loader ran %d times across instances with distributed lock, want 1", n)
	}
}

func TestE2ECrossNodeDeleteDuringLoad(t *testing.T) {
	a, b := newPair(t)
	ctx := context.Background()
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.GetOrLoad(ctx, "race", time.Minute, func(context.Context) ([]byte, error) {
			close(started)
			<-release
			return []byte("old"), nil // read from the source before b's Delete
		})
	}()
	<-started
	if err := b.Delete(ctx, "race"); err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	for name, c := range map[string]*cachex.Cache{"a": a, "b": b} {
		if v, err := c.Get(ctx, "race"); !errors.Is(err, cachex.ErrMiss) {
			t.Fatalf("node %s: pre-Delete value survived on memcached: %q %v", name, v, err)
		}
	}
}
