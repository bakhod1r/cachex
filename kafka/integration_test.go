//go:build integration

package cachexkafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func brokers(t *testing.T) []string {
	t.Helper()
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	return strings.Split(b, ",")
}

// topic creates a unique 3-partition topic and deletes it on cleanup.
func topic(t *testing.T, bs []string) string {
	t.Helper()
	name := fmt.Sprintf("cachex-test-%d", time.Now().UnixNano())
	cl, err := kgo.NewClient(kgo.SeedBrokers(bs...))
	if err != nil {
		t.Fatal(err)
	}
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res, err := adm.CreateTopic(ctx, 3, 1, nil, name)
	if err == nil {
		err = res.Err
	}
	if err != nil {
		cl.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(ctx, name)
		cl.Close()
	})
	return name
}

func newInv(t *testing.T, bs []string, tp string) *Invalidator {
	t.Helper()
	inv, err := New(Config{Brokers: bs, Topic: tp, OnError: func(err error) { t.Log("OnError:", err) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inv.Close() })
	return inv
}

func subscribe(ctx context.Context, t *testing.T, inv *Invalidator) chan cachex.Invalidation {
	t.Helper()
	ch := make(chan cachex.Invalidation, 16)
	if err := inv.Subscribe(ctx, func(m cachex.Invalidation) { ch <- m }); err != nil {
		t.Fatal(err)
	}
	return ch
}

func TestIntegrationPubSub(t *testing.T) {
	bs := brokers(t)
	tp := topic(t, bs)
	a, b := newInv(t, bs, tp), newInv(t, bs, tp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gotA := subscribe(ctx, t, a)
	bctx, bcancel := context.WithCancel(ctx)
	gotB := subscribe(bctx, t, b)

	// Published immediately after Subscribe returned: must not be missed.
	if err := a.Publish(ctx, cachex.Invalidation{Keys: []string{"k1"}, Namespace: "ns"}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-gotB:
		if len(m.Keys) != 1 || m.Keys[0] != "k1" || m.Namespace != "ns" {
			t.Fatalf("bad msg %+v", m)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("B did not receive within 10s")
	}
	select {
	case m := <-gotA:
		t.Fatalf("A received own message %+v", m)
	case <-time.After(500 * time.Millisecond):
	}

	bcancel()
	time.Sleep(200 * time.Millisecond)
	if err := a.Publish(ctx, cachex.Invalidation{Keys: []string{"k2"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-gotB:
		t.Fatalf("B received after cancel %+v", m)
	case <-time.After(time.Second):
	}
}

// TestIntegrationPublishRightAfterSubscribe repeats subscribe-then-publish with fresh subscribers
// on a topic that already holds records, so a lazily resolved "end" offset would drop messages.
func TestIntegrationPublishRightAfterSubscribe(t *testing.T) {
	bs := brokers(t)
	tp := topic(t, bs)
	pub := newInv(t, bs, tp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for n := range 10 {
		sub := newInv(t, bs, tp)
		sctx, scancel := context.WithCancel(ctx)
		got := subscribe(sctx, t, sub)
		key := fmt.Sprintf("k%d", n)
		if err := pub.Publish(ctx, cachex.Invalidation{Keys: []string{key}}); err != nil {
			t.Fatal(err)
		}
		select {
		case m := <-got:
			if len(m.Keys) != 1 || m.Keys[0] != key {
				t.Fatalf("round %d: got %+v want %s (old message replayed?)", n, m, key)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: message published right after Subscribe was missed", n)
		}
		scancel()
	}
}

func TestIntegrationSubscribeMissingTopic(t *testing.T) {
	bs := brokers(t)
	inv := newInv(t, bs, fmt.Sprintf("cachex-missing-%d", time.Now().UnixNano()))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := inv.Subscribe(ctx, func(cachex.Invalidation) {}); err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestIntegrationCacheE2E(t *testing.T) {
	bs := brokers(t)
	tp := topic(t, bs)
	l2 := memstore.New()
	mk := func() *cachex.Cache {
		inv := newInv(t, bs, tp)
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
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := b.Get(ctx, "user:1")
		if errors.Is(err, cachex.ErrMiss) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("b still serves stale L1 after 5s: err=%v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
