package memcached

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/storetest"
)

func TestConformanceAgainstFakeServer(t *testing.T) {
	fs := newFakeServer(t)
	storetest.Run(t, func(t *testing.T) cachex.Store {
		s, err := New(Config{Servers: []string{fs.addr()}, KeyPrefix: "t" + time.Now().Format("150405.000000000") + ":"})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}, fs.advance)
}

func TestStoreOpsAgainstFakeServer(t *testing.T) {
	ctx := context.Background()
	fs := newFakeServer(t)
	s, err := New(Config{Servers: []string{fs.addr()}, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(ctx, "k", []byte("x"), 0); !errors.Is(err, cachex.ErrNotStored) {
		t.Fatalf("Add present: %v", err)
	}
	if _, _, err := s.Gets(ctx, "missing"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("Gets missing: %v", err)
	}
	_, tok, err := s.Gets(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSwap(ctx, "other", []byte("v"), tok, 0); err == nil {
		t.Fatal("token for another key accepted")
	}
	if err := s.CompareAndSwap(ctx, "k", []byte("v"), "junk", 0); err == nil {
		t.Fatal("foreign token accepted")
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.CompareAndSwap(cctx, "k", []byte("v"), tok, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("CAS cancelled: %v", err)
	}
	if err := s.Set(ctx, "n", []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Incr(ctx, "n", 1); err == nil || errors.Is(err, cachex.ErrL2Unavailable) {
		t.Fatalf("Incr non-numeric: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestStoreErrorsWhenUnavailable(t *testing.T) {
	ctx := context.Background()
	s, err := New(Config{Servers: []string{"127.0.0.1:1"}, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	unavailable := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, cachex.ErrL2Unavailable) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	_, err = s.GetMulti(ctx, []string{"a"})
	unavailable("GetMulti", err)
	_, _, err = s.GetsMulti(ctx, []string{"a"})
	unavailable("GetsMulti", err)
	_, _, err = s.Gets(ctx, "a")
	unavailable("Gets", err)
	unavailable("Add", s.Add(ctx, "a", nil, 0))
	unavailable("Delete", s.Delete(ctx, "a"))
	_, err = s.Incr(ctx, "a", 1)
	unavailable("Incr", err)

	_ = s.Close()
	closed := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, cachex.ErrClosed) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	_, err = s.GetMulti(ctx, []string{"a"})
	closed("GetMulti", err)
	_, _, err = s.GetsMulti(ctx, []string{"a"})
	closed("GetsMulti", err)
	_, _, err = s.Gets(ctx, "a")
	closed("Gets", err)
	closed("Add", s.Add(ctx, "a", nil, 0))
	_, err = s.Get(ctx, "a")
	closed("Get", err)
}
