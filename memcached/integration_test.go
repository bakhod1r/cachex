//go:build integration

package memcached

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/storetest"
)

var seq atomic.Int64

func TestIntegrationStore(t *testing.T) {
	addr := os.Getenv("MEMCACHED_ADDR")
	if addr == "" {
		t.Skip("MEMCACHED_ADDR not set")
	}
	storetest.Run(t, func(t *testing.T) cachex.Store {
		s, err := New(Config{
			Servers:   []string{addr},
			Timeout:   time.Second,
			KeyPrefix: fmt.Sprintf("it:%d:%d:", time.Now().UnixNano(), seq.Add(1)),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}, nil)
}

func newIntegrationStore(t *testing.T) *Store {
	t.Helper()
	addr := os.Getenv("MEMCACHED_ADDR")
	if addr == "" {
		t.Skip("MEMCACHED_ADDR not set")
	}
	s, err := New(Config{
		Servers:   []string{addr},
		Timeout:   time.Second,
		KeyPrefix: fmt.Sprintf("it:%d:%d:", time.Now().UnixNano(), seq.Add(1)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestIntegrationValueTooLarge(t *testing.T) {
	s := newIntegrationStore(t)
	ctx := context.Background()
	big := make([]byte, 2<<20)
	err := s.Set(ctx, "big", big, time.Minute)
	t.Logf("real reply for 2MB set: %v", err)
	if !errors.Is(err, cachex.ErrValueTooLarge) {
		t.Fatalf("want ErrValueTooLarge, got %v", err)
	}
	// Connection must remain usable after the rejection.
	if err := s.Set(ctx, "small", []byte("ok"), time.Minute); err != nil {
		t.Fatalf("store unusable after too-large: %v", err)
	}
	if v, err := s.Get(ctx, "small"); err != nil || string(v) != "ok" {
		t.Fatalf("get after too-large: %q %v", v, err)
	}
}

func TestIntegrationLongAndNonASCIIKeys(t *testing.T) {
	s := newIntegrationStore(t)
	ctx := context.Background()
	keys := []string{
		strings.Repeat("k", 1000),
		strings.Repeat("k", 1000) + "x",
		"привет мир",
		"日本語キー",
		"with space\nnewline\x00nul",
	}
	for i, k := range keys {
		if err := s.Set(ctx, k, []byte(fmt.Sprint(i)), time.Minute); err != nil {
			t.Fatalf("set %q: %v", k, err)
		}
	}
	for i, k := range keys {
		v, err := s.Get(ctx, k)
		if err != nil || string(v) != fmt.Sprint(i) {
			t.Fatalf("get %q: %q %v", k, v, err)
		}
	}
}

func TestIntegrationTTLExpiry(t *testing.T) {
	s := newIntegrationStore(t)
	ctx := context.Background()
	if err := s.Set(ctx, "ttl", []byte("v"), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "ttl"); err != nil {
		t.Fatalf("immediate get: %v", err)
	}
	time.Sleep(2100 * time.Millisecond)
	if _, err := s.Get(ctx, "ttl"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("want ErrMiss after expiry, got %v", err)
	}
}

func TestIntegrationIncr(t *testing.T) {
	s := newIntegrationStore(t)
	ctx := context.Background()
	if _, err := s.Incr(ctx, "ctr", 1); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("incr absent: want ErrMiss, got %v", err)
	}
	if err := s.Set(ctx, "ctr", []byte("10"), time.Minute); err != nil {
		t.Fatal(err)
	}
	for want := uint64(15); want <= 25; want += 5 {
		v, err := s.Incr(ctx, "ctr", 5)
		if err != nil || v != want {
			t.Fatalf("incr: %d %v want %d", v, err, want)
		}
	}
	if v, err := s.Get(ctx, "ctr"); err != nil || string(v) != "25" {
		t.Fatalf("get ctr: %q %v", v, err)
	}
	if err := s.Set(ctx, "nan", []byte("abc"), time.Minute); err != nil {
		t.Fatal(err)
	}
	_, err := s.Incr(ctx, "nan", 1)
	t.Logf("real reply for incr on non-numeric: %v", err)
	if err == nil || errors.Is(err, cachex.ErrL2Unavailable) || errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("incr non-numeric: want protocol error, got %v", err)
	}
}
