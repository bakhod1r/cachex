// Package storetest is a conformance suite for cachex.Store implementations.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

// Run exercises any cachex.Store. newStore returns a fresh empty store.
// advance moves the store's clock forward. Pass nil for stores on real time (memcached):
// TTL subtests then sleep for real and are skipped under -short.
func Run(t *testing.T, newStore func(t *testing.T) cachex.Store, advance func(d time.Duration)) {
	t.Helper()
	realTime := advance == nil
	if realTime {
		advance = time.Sleep
	}
	skipSlow := func(t *testing.T) {
		if realTime && testing.Short() {
			t.Skip("real-time TTL test skipped in -short mode")
		}
	}
	ctx := context.Background()
	fresh := func(t *testing.T) cachex.Store {
		s := newStore(t)
		t.Cleanup(func() { _ = s.Close() })
		return s
	}

	t.Run("GetMissing", func(t *testing.T) {
		s := fresh(t)
		v, err := s.Get(ctx, "missing")
		if !errors.Is(err, cachex.ErrMiss) {
			t.Fatalf("Get missing: err=%v val=%q, want ErrMiss", err, v)
		}
	})

	t.Run("SetGetRoundTrip", func(t *testing.T) {
		s := fresh(t)
		in := []byte("hello")
		if err := s.Set(ctx, "k", in, 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
		in[0] = 'X'
		got, err := s.Get(ctx, "k")
		if err != nil || string(got) != "hello" {
			t.Fatalf("Get = %q, %v; want \"hello\" (input mutation must not leak)", got, err)
		}
		got[0] = 'Y'
		again, err := s.Get(ctx, "k")
		if err != nil || string(again) != "hello" {
			t.Fatalf("Get after mutating returned slice = %q, %v; want \"hello\"", again, err)
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		s := fresh(t)
		mustSet(t, s, "k", "v1", 0)
		mustSet(t, s, "k", "v2", 0)
		expectVal(t, s, "k", "v2")
	})

	t.Run("Add", func(t *testing.T) {
		s := fresh(t)
		if err := s.Add(ctx, "k", []byte("a"), 0); err != nil {
			t.Fatalf("Add absent: %v", err)
		}
		if err := s.Add(ctx, "k", []byte("b"), 0); !errors.Is(err, cachex.ErrNotStored) {
			t.Fatalf("Add present: err=%v, want ErrNotStored", err)
		}
		expectVal(t, s, "k", "a")
	})

	t.Run("Delete", func(t *testing.T) {
		s := fresh(t)
		mustSet(t, s, "k", "v", 0)
		if err := s.Delete(ctx, "k"); err != nil {
			t.Fatalf("Delete present: %v", err)
		}
		if _, err := s.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
			t.Fatalf("Get after Delete: err=%v, want ErrMiss", err)
		}
		if err := s.Delete(ctx, "k"); err != nil {
			t.Fatalf("Delete missing: %v", err)
		}
	})

	t.Run("Incr", func(t *testing.T) {
		s := fresh(t)
		if _, err := s.Incr(ctx, "c", 1); !errors.Is(err, cachex.ErrMiss) {
			t.Fatalf("Incr missing: err=%v, want ErrMiss", err)
		}
		mustSet(t, s, "c", "5", 0)
		n, err := s.Incr(ctx, "c", 3)
		if err != nil || n != 8 {
			t.Fatalf("Incr = %d, %v; want 8", n, err)
		}
		expectVal(t, s, "c", "8")
	})

	t.Run("TTLExpiry", func(t *testing.T) {
		skipSlow(t)
		s := fresh(t)
		mustSet(t, s, "k", "v", 2*time.Second)
		expectVal(t, s, "k", "v")
		advance(3 * time.Second)
		if v, err := s.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
			t.Fatalf("Get after expiry: val=%q err=%v, want ErrMiss", v, err)
		}
	})

	t.Run("NoExpiryForNonPositiveTTL", func(t *testing.T) {
		skipSlow(t)
		s := fresh(t)
		mustSet(t, s, "zero", "v", 0)
		mustSet(t, s, "neg", "v", -time.Second)
		if realTime {
			advance(3 * time.Second)
		} else {
			advance(24 * time.Hour)
		}
		expectVal(t, s, "zero", "v")
		expectVal(t, s, "neg", "v")
	})

	t.Run("GetMulti", func(t *testing.T) {
		s := fresh(t)
		mg, ok := s.(cachex.MultiGetter)
		if !ok {
			t.Skip("store does not implement cachex.MultiGetter")
		}
		mustSet(t, s, "a", "1", 0)
		mustSet(t, s, "b", "2", 0)
		got, err := mg.GetMulti(ctx, []string{"a", "b", "missing"})
		if err != nil {
			t.Fatalf("GetMulti: %v", err)
		}
		if len(got) != 2 || string(got["a"]) != "1" || string(got["b"]) != "2" {
			t.Fatalf("GetMulti = %q; want a=1 b=2 only", got)
		}
	})

	t.Run("CompareAndSwap", func(t *testing.T) {
		s := fresh(t)
		cs, ok := s.(cachex.CASStore)
		if !ok {
			t.Skip("store does not implement cachex.CASStore")
		}
		if _, _, err := cs.Gets(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
			t.Fatalf("Gets missing: %v, want ErrMiss", err)
		}
		mustSet(t, s, "k", "v1", 0)
		v, tok, err := cs.Gets(ctx, "k")
		if err != nil || string(v) != "v1" {
			t.Fatalf("Gets = %q, %v", v, err)
		}
		if err := cs.CompareAndSwap(ctx, "k", []byte("v2"), tok, 0); err != nil {
			t.Fatalf("CAS with fresh token: %v", err)
		}
		expectVal(t, s, "k", "v2")
		if err := cs.CompareAndSwap(ctx, "k", []byte("v3"), tok, 0); !errors.Is(err, cachex.ErrNotStored) {
			t.Fatalf("CAS with used token: %v, want ErrNotStored", err)
		}
		_, tok, _ = cs.Gets(ctx, "k")
		mustSet(t, s, "k", "other", 0)
		if err := cs.CompareAndSwap(ctx, "k", []byte("v4"), tok, 0); !errors.Is(err, cachex.ErrNotStored) {
			t.Fatalf("CAS after concurrent Set: %v, want ErrNotStored", err)
		}
		expectVal(t, s, "k", "other")
		_, tok, _ = cs.Gets(ctx, "k")
		if err := s.Delete(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		if err := cs.CompareAndSwap(ctx, "k", []byte("v5"), tok, 0); !errors.Is(err, cachex.ErrMiss) && !errors.Is(err, cachex.ErrNotStored) {
			t.Fatalf("CAS on deleted key: %v, want ErrMiss or ErrNotStored", err)
		}
		if _, err := s.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
			t.Fatalf("CAS resurrected deleted key: %v", err)
		}
	})

	t.Run("GetsMulti", func(t *testing.T) {
		s := fresh(t)
		mg, ok := s.(cachex.MultiCASGetter)
		cs, _ := s.(cachex.CASStore)
		if !ok || cs == nil {
			t.Skip("store does not implement cachex.MultiCASGetter and cachex.CASStore")
		}
		mustSet(t, s, "a", "1", 0)
		mustSet(t, s, "b", "2", 0)
		vals, toks, err := mg.GetsMulti(ctx, []string{"a", "b", "missing"})
		if err != nil || string(vals["a"]) != "1" || string(vals["b"]) != "2" || len(vals) != 2 || len(toks) != 2 {
			t.Fatalf("GetsMulti = %v %v %v", vals, toks, err)
		}
		mustSet(t, s, "b", "changed", 0)
		if err := cs.CompareAndSwap(ctx, "a", []byte("1x"), toks["a"], 0); err != nil {
			t.Fatalf("CAS with GetsMulti token: %v", err)
		}
		if err := cs.CompareAndSwap(ctx, "b", []byte("2x"), toks["b"], 0); !errors.Is(err, cachex.ErrNotStored) {
			t.Fatalf("CAS after concurrent Set: %v, want ErrNotStored", err)
		}
	})

	t.Run("ConcurrentSetGet", func(t *testing.T) {
		s := fresh(t)
		const workers = 50
		var wg sync.WaitGroup
		errs := make(chan error, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := fmt.Sprintf("k%d", i%5)
				want := fmt.Sprintf("v%d", i)
				if err := s.Set(ctx, key, []byte(want), 0); err != nil {
					errs <- fmt.Errorf("set %s: %w", key, err)
					return
				}
				if _, err := s.Get(ctx, key); err != nil {
					errs <- fmt.Errorf("get %s: %w", key, err)
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})
}

func mustSet(t *testing.T, s cachex.Store, k, v string, ttl time.Duration) {
	t.Helper()
	if err := s.Set(context.Background(), k, []byte(v), ttl); err != nil {
		t.Fatalf("Set %q: %v", k, err)
	}
}

func expectVal(t *testing.T, s cachex.Store, k, want string) {
	t.Helper()
	got, err := s.Get(context.Background(), k)
	if err != nil || string(got) != want {
		t.Fatalf("Get %q = %q, %v; want %q", k, got, err, want)
	}
}
