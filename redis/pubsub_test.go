package cachexredis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/bakhod1r/cachex"
	"github.com/redis/go-redis/v9"
)

// miniClient returns a go-redis client over an in-process miniredis (supports PUBLISH/SUBSCRIBE).
func miniClient(t *testing.T) redis.UniversalClient {
	t.Helper()
	mr := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func TestPubSubDeliversToPeersNotSelf(t *testing.T) {
	cl := miniClient(t)
	a, _ := New(Config{Client: cl})
	b, _ := New(Config{Client: cl})

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
	case <-time.After(100 * time.Millisecond):
	}

	bcancel()
	time.Sleep(50 * time.Millisecond)
	if err := a.Publish(ctx, cachex.Invalidation{Keys: []string{"k2"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-gotB:
		t.Fatalf("B received after cancel %+v", m)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestPubSubMalformedPayloadReportsError(t *testing.T) {
	cl := miniClient(t)
	errs := make(chan error, 1)
	inv, _ := New(Config{Client: cl, OnError: func(err error) { errs <- err }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := inv.Subscribe(ctx, func(cachex.Invalidation) { t.Error("fn called for garbage") }); err != nil {
		t.Fatal(err)
	}
	if err := cl.Publish(ctx, DefaultChannel, "not json").Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-errs:
	case <-time.After(2 * time.Second):
		t.Fatal("OnError not called")
	}
}

func TestPubSubNilFn(t *testing.T) {
	inv, _ := New(Config{Client: miniClient(t)})
	if err := inv.Subscribe(context.Background(), nil); err == nil {
		t.Fatal("want error for nil fn")
	}
}

// Two caches sharing one Redis as L2 and invalidation bus: Delete and Namespace.Invalidate on A
// must evict B's L1 copy long before the one-hour L1 TTL.
func TestCachesShareRedisStoreAndInvalidator(t *testing.T) {
	cl := miniClient(t)
	mk := func() *cachex.Cache {
		st, err := NewStore(StoreConfig{Client: cl, KeyPrefix: "app:"})
		if err != nil {
			t.Fatal(err)
		}
		inv, err := New(Config{Client: cl})
		if err != nil {
			t.Fatal(err)
		}
		c, err := cachex.New(cachex.WithL2(st), cachex.WithInvalidator(inv), cachex.WithL1TTL(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	a, b := mk(), mk()
	ctx := context.Background()

	eventuallyMiss := func(get func() error) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			if err := get(); errors.Is(err, cachex.ErrMiss) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("B still serves stale L1 after 2s")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	if err := a.Set(ctx, "user:1", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if v, err := b.Get(ctx, "user:1"); err != nil || string(v) != "v" {
		t.Fatalf("b.Get = %q, %v", v, err)
	}
	if err := a.Delete(ctx, "user:1"); err != nil {
		t.Fatal(err)
	}
	eventuallyMiss(func() error { _, err := b.Get(ctx, "user:1"); return err })

	nsA, err := a.Namespace("users")
	if err != nil {
		t.Fatal(err)
	}
	nsB, err := b.Namespace("users")
	if err != nil {
		t.Fatal(err)
	}
	if err := nsA.Set(ctx, "42", []byte("Ada"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if v, err := nsB.Get(ctx, "42"); err != nil || string(v) != "Ada" {
		t.Fatalf("nsB.Get = %q, %v", v, err)
	}
	if err := nsA.Invalidate(ctx); err != nil {
		t.Fatal(err)
	}
	eventuallyMiss(func() error { _, err := nsB.Get(ctx, "42"); return err })
}
