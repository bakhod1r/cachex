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
