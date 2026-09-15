package cachexredis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/bakhod1r/cachex"
	"github.com/redis/go-redis/v9"
)

type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *stepClock) Advance(d time.Duration) { c.mu.Lock(); c.now = c.now.Add(d); c.mu.Unlock() }

// A Redis restart must degrade the cache (L1 still serves, L2 errors are
// ErrL2Unavailable, breaker opens) and recover once Redis is back, without
// recreating the client.
func TestChaosRedisRestart(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	store, err := NewStore(StoreConfig{Client: cl, CloseClient: true})
	if err != nil {
		t.Fatal(err)
	}
	clk := &stepClock{now: time.Now()}
	c, err := cachex.New(cachex.WithL2(store), cachex.WithClock(clk), cachex.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.Set(ctx, "warm", []byte("w"), time.Hour); err != nil {
		t.Fatal(err)
	}

	mr.Close()
	if v, err := c.Get(ctx, "warm"); err != nil || string(v) != "w" {
		t.Fatalf("L1 during outage: %q, %v", v, err)
	}
	for range 30 {
		if err := c.Set(ctx, "during", []byte("d"), time.Hour); err != nil && !errors.Is(err, cachex.ErrL2Unavailable) {
			t.Fatalf("Set during outage: %v", err)
		}
	}
	if s := c.Stats(); s.Breaker != "open" {
		t.Fatalf("breaker %q during outage, want open", s.Breaker)
	}

	if err := mr.Restart(); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Minute)
	for i := 0; i < 10 && c.Stats().Breaker != "closed"; i++ {
		_, _ = c.Get(ctx, "probe")
	}
	if s := c.Stats(); s.Breaker != "closed" {
		t.Fatalf("breaker %q after restart, want closed", s.Breaker)
	}
	if err := c.Set(ctx, "after", []byte("a"), time.Hour); err != nil {
		t.Fatalf("Set after restart: %v", err)
	}
	if !mr.Exists("after") {
		t.Fatalf("write after restart did not reach Redis; keys %v", mr.Keys())
	}
}
