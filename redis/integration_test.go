//go:build integration

package cachexredis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
	"github.com/bakhod1r/cachex/storetest"
	"github.com/redis/go-redis/v9"
)

func client(t *testing.T) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func channelName(t *testing.T) string {
	return fmt.Sprintf("cachex:test:%s:%d", t.Name(), time.Now().UnixNano())
}

func TestIntegrationPubSub(t *testing.T) {
	c := client(t)
	ch := channelName(t)
	a, _ := New(Config{Client: c, Channel: ch})
	b, _ := New(Config{Client: c, Channel: ch})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gotA := make(chan cachex.Invalidation, 4)
	gotB := make(chan cachex.Invalidation, 4)
	if err := a.Subscribe(ctx, func(m cachex.Invalidation) { gotA <- m }); err != nil {
		t.Fatal(err)
	}
	bctx, bcancel := context.WithCancel(ctx)
	if err := b.Subscribe(bctx, func(m cachex.Invalidation) { gotB <- m }); err != nil {
		t.Fatal(err)
	}

	if err := a.Publish(ctx, cachex.Invalidation{Keys: []string{"k1"}, Namespace: "ns"}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-gotB:
		if len(m.Keys) != 1 || m.Keys[0] != "k1" || m.Namespace != "ns" {
			t.Fatalf("bad msg %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("B did not receive within 2s")
	}
	select {
	case m := <-gotA:
		t.Fatalf("A received own message %+v", m)
	case <-time.After(200 * time.Millisecond):
	}

	bcancel()
	time.Sleep(100 * time.Millisecond)
	if err := a.Publish(ctx, cachex.Invalidation{Keys: []string{"k2"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-gotB:
		t.Fatalf("B received after cancel %+v", m)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestIntegrationCacheE2E(t *testing.T) {
	c := client(t)
	ch := channelName(t)
	l2 := memstore.New()
	mk := func() *cachex.Cache {
		inv, err := New(Config{Client: c, Channel: ch})
		if err != nil {
			t.Fatal(err)
		}
		cc, err := cachex.New(cachex.WithL2(l2), cachex.WithInvalidator(inv), cachex.WithL1TTL(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cc.Close() })
		return cc
	}
	a, b := mk(), mk()
	ctx := context.Background()

	if err := a.Set(ctx, "user:1", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if v, err := b.Get(ctx, "user:1"); err != nil || string(v) != "v" {
		t.Fatalf("b.Get = %q, %v", v, err)
	}
	if err := a.Delete(ctx, "user:1"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := b.Get(ctx, "user:1")
		if errors.Is(err, cachex.ErrMiss) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("b still serves stale L1 after 2s: err=%v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestIntegrationStoreConformance(t *testing.T) {
	cl := client(t)
	storetest.Run(t, func(t *testing.T) cachex.Store {
		s, err := NewStore(StoreConfig{Client: cl, KeyPrefix: fmt.Sprintf("cachex:it:%d:", time.Now().UnixNano())})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}, nil)
}

func TestIntegrationRedisAsL2AndInvalidator(t *testing.T) {
	cl := client(t)
	prefix := fmt.Sprintf("cachex:e2e:%d:", time.Now().UnixNano())
	channel := channelName(t)
	mk := func() *cachex.Cache {
		st, err := NewStore(StoreConfig{Client: cl, KeyPrefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		inv, err := New(Config{Client: cl, Channel: channel})
		if err != nil {
			t.Fatal(err)
		}
		c, err := cachex.New(cachex.WithL2(st), cachex.WithInvalidator(inv), cachex.WithL1TTL(time.Hour),
			cachex.WithDistributedLock(2*time.Second, 10*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	a, b := mk(), mk()
	ctx := context.Background()

	if err := a.Set(ctx, "k", []byte("v1"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, err := b.Get(ctx, "k"); err != nil || string(v) != "v1" {
		t.Fatalf("b reads through Redis L2: %q %v", v, err)
	}
	if err := a.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := b.Get(ctx, "k"); errors.Is(err, cachex.ErrMiss) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("b still serves deleted key from L1")
		}
		time.Sleep(10 * time.Millisecond)
	}

	got, err := b.GetMulti(ctx, []string{"k", "none"})
	if err != nil || len(got) != 0 {
		t.Fatalf("GetMulti after delete: %q %v", got, err)
	}
	v, err := a.GetOrLoad(ctx, "loaded", time.Minute, func(context.Context) ([]byte, error) { return []byte("L"), nil })
	if err != nil || string(v) != "L" {
		t.Fatalf("GetOrLoad: %q %v", v, err)
	}
	if v, err := b.Get(ctx, "loaded"); err != nil || string(v) != "L" {
		t.Fatalf("loaded value visible to b: %q %v", v, err)
	}
}
