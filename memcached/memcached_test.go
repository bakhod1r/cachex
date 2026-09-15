package memcached

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bradfitz/gomemcache/memcache"
)

func TestExpiration(t *testing.T) {
	cases := []struct {
		ttl  time.Duration
		want int32
	}{
		{-time.Second, 0}, {0, 0}, {time.Nanosecond, 1}, {999 * time.Millisecond, 1},
		{time.Second, 1}, {1001 * time.Millisecond, 2}, {90 * time.Second, 90},
		{30 * 24 * time.Hour, 2592000}, {30*24*time.Hour + time.Millisecond, 2592000},
		{365 * 24 * time.Hour, 2592000}, {time.Duration(1<<63 - 1), 2592000},
	}
	for _, c := range cases {
		if got := expiration(c.ttl); got != c.want {
			t.Errorf("expiration(%v)=%d want %d", c.ttl, got, c.want)
		}
	}
}

func TestNormalizeKey(t *testing.T) {
	if got := normalizeKey("p:", "abc"); got != "p:abc" {
		t.Fatalf("got %q", got)
	}
	long := strings.Repeat("a", 300)
	n := normalizeKey("p:", long)
	if !validKey(n) || !strings.Contains(n, "#") {
		t.Fatalf("invalid %q", n)
	}
	sp := normalizeKey("p:", "has space")
	if !validKey(sp) || !strings.HasPrefix(sp, "p:has_space#") {
		t.Fatalf("got %q", sp)
	}
	if normalizeKey("p:", "has space") == normalizeKey("p:", "has_space") {
		t.Fatal("sanitized key collided with literal key")
	}
	if normalizeKey("", long+"x") == normalizeKey("", long+"y") {
		t.Fatal("collision")
	}
	if normalizeKey("a:", "b c") == normalizeKey("b:", "b c") {
		t.Fatal("prefix ignored")
	}
}

func FuzzNormalizeKey(f *testing.F) {
	f.Add("p:", "k", "k2")
	f.Add("", strings.Repeat("x", 260), strings.Repeat("x", 259)+"y")
	f.Add("pre", "\x00\xff \n", "\x01")
	f.Fuzz(func(t *testing.T, prefix, a, b string) {
		na, nb := normalizeKey(prefix, a), normalizeKey(prefix, b)
		if !validKey(na) || !validKey(nb) {
			t.Fatalf("invalid output %q %q", na, nb)
		}
		if a != b && (na == nb) {
			t.Fatalf("distinct keys %q %q map to %q", a, b, na)
		}
	})
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestMapErr(t *testing.T) {
	var ne net.Error = timeoutErr{}
	cases := []struct {
		in   error
		want error
	}{
		{memcache.ErrCacheMiss, cachex.ErrMiss},
		{memcache.ErrNotStored, cachex.ErrNotStored},
		{memcache.ErrCASConflict, cachex.ErrNotStored},
		{memcache.ErrMalformedKey, cachex.ErrInvalidKey},
		{fmt.Errorf("memcache: unexpected response line from %q: %q", "set", "SERVER_ERROR object too large for cache\r\n"), cachex.ErrValueTooLarge},
		{memcache.ErrNoServers, cachex.ErrL2Unavailable},
		{&memcache.ConnectTimeoutError{Addr: &net.TCPAddr{}}, cachex.ErrL2Unavailable},
		{&net.OpError{Op: "read", Err: ne}, cachex.ErrL2Unavailable},
		{io.EOF, cachex.ErrL2Unavailable},
	}
	for _, c := range cases {
		if got := mapErr(c.in); !errors.Is(got, c.want) {
			t.Errorf("mapErr(%v)=%v want Is %v", c.in, got, c.want)
		}
	}
	if mapErr(nil) != nil {
		t.Fatal("nil")
	}
	if err := mapErr(errors.New("memcache: client error: cannot increment")); errors.Is(err, cachex.ErrL2Unavailable) {
		t.Fatal("client error must not be L2 unavailable")
	}
}

func TestNewAndGuards(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("want error")
	}
	// Unroutable/closed port: must map to ErrL2Unavailable.
	s, err := New(Config{Servers: []string{"127.0.0.1:1"}, Timeout: 50 * time.Millisecond, MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(context.Background(), "k"); !errors.Is(err, cachex.ErrL2Unavailable) {
		t.Fatalf("got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Set(ctx, "k", nil, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	s.sem <- struct{}{}
	if err := s.Delete(context.Background(), "k"); !errors.Is(err, cachex.ErrL2Unavailable) {
		t.Fatalf("got %v", err)
	}
	<-s.sem
	_ = s.Close()
	if _, err := s.Incr(context.Background(), "k", 1); !errors.Is(err, cachex.ErrClosed) {
		t.Fatalf("got %v", err)
	}
}
