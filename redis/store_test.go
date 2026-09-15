package cachexredis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/storetest"
	"github.com/redis/go-redis/v9"
)

// newMini returns a Store over an in-process miniredis and a function that advances its clock.
func newMini(t *testing.T, prefix string) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s, err := NewStore(StoreConfig{Client: cl, KeyPrefix: prefix, CloseClient: true})
	if err != nil {
		t.Fatal(err)
	}
	return s, mr
}

func TestStoreConformance(t *testing.T) {
	var mr *miniredis.Miniredis
	storetest.Run(t, func(t *testing.T) cachex.Store {
		var s *Store
		s, mr = newMini(t, "app:")
		return s
	}, func(d time.Duration) { mr.FastForward(d) })
}

func TestStoreKeyPrefix(t *testing.T) {
	s, mr := newMini(t, "app:")
	ctx := context.Background()
	if err := s.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if !mr.Exists("app:k") || mr.Exists("k") {
		t.Fatalf("keys stored: %v", mr.Keys())
	}
}

func TestStoreBinaryValuesAndKeys(t *testing.T) {
	s, _ := newMini(t, "")
	ctx := context.Background()
	key := "ключ with spaces\x00and\nnewline"
	val := []byte{0, 1, 2, 255, 254}
	if err := s.Set(ctx, key, val, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, key)
	if err != nil || string(got) != string(val) {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestStoreSubMillisecondTTLStillExpires(t *testing.T) {
	s, mr := newMini(t, "")
	ctx := context.Background()
	if err := s.Set(ctx, "k", []byte("v"), time.Microsecond); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL("k"); ttl <= 0 {
		t.Fatalf("sub-ms TTL must still set an expiry, got %v", ttl)
	}
}

func TestStoreCASDetectsSameValueRewrite(t *testing.T) {
	// A token must reject a CAS after the key was rewritten, even with an identical value (ABA).
	s, _ := newMini(t, "")
	ctx := context.Background()
	_ = s.Set(ctx, "k", []byte("same"), 0)
	_, tok, err := s.Gets(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Set(ctx, "k", []byte("same"), 0)
	if err := s.CompareAndSwap(ctx, "k", []byte("new"), tok, 0); !errors.Is(err, cachex.ErrNotStored) {
		t.Fatalf("want ErrNotStored after same-value rewrite, got %v", err)
	}
}

func TestStoreCASForeignToken(t *testing.T) {
	s, _ := newMini(t, "")
	ctx := context.Background()
	_ = s.Set(ctx, "k", []byte("v"), 0)
	if err := s.CompareAndSwap(ctx, "k", []byte("x"), "not-a-token", 0); err == nil {
		t.Fatal("foreign token accepted")
	}
}

func TestStoreIncrNonNumeric(t *testing.T) {
	s, _ := newMini(t, "")
	ctx := context.Background()
	_ = s.Set(ctx, "k", []byte("abc"), 0)
	if _, err := s.Incr(ctx, "k", 1); err == nil || errors.Is(err, cachex.ErrL2Unavailable) || errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("non-numeric Incr: want plain error, got %v", err)
	}
}

func TestStoreIncrKeepsTTL(t *testing.T) {
	s, mr := newMini(t, "")
	ctx := context.Background()
	_ = s.Set(ctx, "c", []byte("1"), time.Minute)
	if _, err := s.Incr(ctx, "c", 1); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL("c"); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("Incr lost TTL: %v", ttl)
	}
}

func TestStoreForeignValueIsNotServed(t *testing.T) {
	s, mr := newMini(t, "")
	ctx := context.Background()
	_ = mr.Set("k", "short") // written by something other than this Store
	if _, err := s.Get(ctx, "k"); err == nil {
		t.Fatal("value without version header must not be returned as valid")
	}
}

func TestStoreDownIsUnavailable(t *testing.T) {
	s, mr := newMini(t, "")
	mr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := s.Get(ctx, "k"); !errors.Is(err, cachex.ErrL2Unavailable) {
		t.Fatalf("want ErrL2Unavailable, got %v", err)
	}
}

func TestStoreContextCanceledPassesThrough(t *testing.T) {
	s, _ := newMini(t, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Get(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestStoreClose(t *testing.T) {
	s, _ := newMini(t, "")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := s.Get(context.Background(), "k"); !errors.Is(err, cachex.ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestNewStoreValidation(t *testing.T) {
	if _, err := NewStore(StoreConfig{}); err == nil {
		t.Fatal("nil client accepted")
	}
}

func TestStoreAsCacheL2(t *testing.T) {
	mr := miniredis.RunT(t)
	mk := func() *cachex.Cache {
		st, err := NewStore(StoreConfig{Client: redis.NewClient(&redis.Options{Addr: mr.Addr()}), CloseClient: true})
		if err != nil {
			t.Fatal(err)
		}
		c, err := cachex.New(cachex.WithL2(st), cachex.WithSweepInterval(0))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	a, b := mk(), mk()
	ctx := context.Background()
	if err := a.Set(ctx, "user:1", []byte("alice"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, err := b.Get(ctx, "user:1"); err != nil || string(v) != "alice" {
		t.Fatalf("second cache via Redis L2: %q %v", v, err)
	}
	ns, _ := a.Namespace("orders")
	if err := ns.Set(ctx, "1", []byte("o"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := ns.Invalidate(ctx); err != nil {
		t.Fatalf("namespace Invalidate uses Incr: %v", err)
	}
	if _, err := ns.Get(ctx, "1"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("after Invalidate want miss, got %v", err)
	}
}
