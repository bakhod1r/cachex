//go:build integration

package cachexnats

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
	"github.com/nats-io/nats.go"
)

func conn(t *testing.T) *nats.Conn {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		t.Skip("NATS_URL not set")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func subjectName() string {
	return fmt.Sprintf("cachex.test.%d", time.Now().UnixNano())
}

func TestIntegrationPubSub(t *testing.T) {
	c := conn(t)
	ch := subjectName()
	a, _ := New(Config{Conn: c, Subject: ch})
	b, _ := New(Config{Conn: c, Subject: ch})

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
	c := conn(t)
	ch := subjectName()
	l2 := memstore.New()
	mk := func() *cachex.Cache {
		inv, err := New(Config{Conn: c, Subject: ch})
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
