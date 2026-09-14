// Package cachex is a two-tier cache engine: a sharded in-process LRU (L1) in front of
// a shared store such as memcached (L2), with stampede protection, namespaces and stats.
//
// Consistency: L1 is per process. After a Delete on one node, other nodes may serve
// their L1 copy for up to the L1 TTL (WithL1TTL). L2 is the shared source of truth.
package cachex

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bakhod1r/cachex/internal/breaker"
	"github.com/bakhod1r/cachex/internal/envelope"
	"github.com/bakhod1r/cachex/internal/flight"
	"github.com/bakhod1r/cachex/internal/lru"
)

// Loader computes a value on a cache miss.
type Loader func(ctx context.Context) ([]byte, error)

// Cache is safe for concurrent use. Create with New, release with Close.
type Cache struct {
	cfg     config
	l1      *lru.Cache
	l2      Store
	br      *breaker.Breaker
	flights flight.Group
	vers    versions

	refreshSem chan struct{}
	baseCtx    context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	closed     atomic.Bool

	st counters
}

// New builds a Cache.
func New(opts ...Option) (*Cache, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		if err := o(&cfg); err != nil {
			return nil, err
		}
	}
	if cfg.rand == nil {
		cfg.rand = rand.Float64
	}
	c := &Cache{cfg: cfg, l2: cfg.l2, refreshSem: make(chan struct{}, cfg.maxRefreshes)}
	c.l1 = lru.New(lru.Options{
		MaxEntries: cfg.l1MaxEntries,
		MaxBytes:   cfg.l1MaxBytes,
		Shards:     cfg.shards,
		Now:        c.nowNs,
	})
	c.br = breaker.New(breaker.Config{Now: cfg.clock.Now})
	c.baseCtx, c.cancel = context.WithCancel(context.Background())
	if cfg.sweepInterval > 0 {
		c.wg.Add(1)
		go c.janitor()
	}
	return c, nil
}

// Close stops background work and closes the L2 store. Idempotent.
func (c *Cache) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.cancel()
	c.wg.Wait()
	if c.l2 != nil {
		return c.l2.Close()
	}
	return nil
}

// Get returns a copy of the cached value, or ErrMiss. L2 failures degrade to a miss.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	if err := c.check(key); err != nil {
		return nil, err
	}
	e, ok := c.lookup(ctx, key)
	if !ok || e.Flags&envelope.FlagTombstone != 0 || e.Expired(c.nowNs()) {
		return nil, ErrMiss
	}
	return clone(e.Value), nil
}

// Set writes val to L2 then L1. ttl <= 0 uses the default TTL. A failed L2 write is not
// returned as an error (the cache is best effort); L1 then keeps the value only for the
// degraded TTL and Stats.L2Errors grows.
func (c *Cache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if err := c.check(key); err != nil {
		return err
	}
	c.store(ctx, key, envelope.Entry{Value: val}, c.ttl(ttl))
	return nil
}

// Delete removes key from both tiers. Unlike Set, an L2 failure is returned, because a
// value left in L2 would be served to every node.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if err := c.check(key); err != nil {
		return err
	}
	c.l1.Delete(key)
	var err error
	if c.l2 != nil {
		err = c.l2Call(func() error { return c.l2.Delete(ctx, key) })
	}
	c.l1.Delete(key) // closes the race with a concurrent backfill from L2
	return err
}

// GetOrLoad returns the cached value or computes it with load, once per key per process.
// Near expiry it refreshes early (XFetch); within the stale window it serves the old
// value and refreshes in the background. Loader errors are returned and not cached.
func (c *Cache) GetOrLoad(ctx context.Context, key string, ttl time.Duration, load Loader) ([]byte, error) {
	if err := c.check(key); err != nil {
		return nil, err
	}
	if load == nil {
		return nil, errors.New("cachex: nil loader")
	}
	ttl = c.ttl(ttl)
	now := c.nowNs()
	if e, ok := c.lookup(ctx, key); ok && e.Flags&envelope.FlagTombstone == 0 {
		switch {
		case !e.Expired(now):
			if c.earlyRefresh(e, now) {
				c.st.earlyRefreshes.Add(1)
				c.refreshAsync(key, ttl, load)
			}
			return clone(e.Value), nil
		case c.cfg.staleWindow > 0 && now < e.ExpireAt+int64(c.cfg.staleWindow):
			c.st.staleServed.Add(1)
			c.refreshAsync(key, ttl, load)
			return clone(e.Value), nil
		}
	}
	v, shared, err := c.flights.Do(ctx, key, func(fctx context.Context) ([]byte, error) {
		// A flight that finished between our miss and this call may have filled the cache.
		if e, ok := c.lookup(fctx, key); ok && e.Flags&envelope.FlagTombstone == 0 && !e.Expired(c.nowNs()) {
			return e.Value, nil
		}
		return c.load(fctx, key, ttl, load)
	})
	if shared {
		c.st.loadsShared.Add(1)
	}
	if err != nil {
		return nil, err
	}
	return clone(v), nil
}

func (c *Cache) load(ctx context.Context, key string, ttl time.Duration, load Loader) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.loadTimeout)
	defer cancel()
	start := c.cfg.clock.Now()
	v, err := load(ctx)
	c.st.loads.Add(1)
	if err != nil {
		c.st.loadErrors.Add(1)
		return nil, err
	}
	delta := c.cfg.clock.Now().Sub(start)
	c.store(ctx, key, envelope.Entry{Value: v, Delta: int64(delta)}, ttl)
	return v, nil
}

func (c *Cache) refreshAsync(key string, ttl time.Duration, load Loader) {
	select {
	case c.refreshSem <- struct{}{}:
	default:
		return // enough refreshes in flight; caller already has a value
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() { <-c.refreshSem }()
		_, _, _ = c.flights.Do(c.baseCtx, key, func(fctx context.Context) ([]byte, error) {
			return c.load(fctx, key, ttl, load)
		})
	}()
}

// earlyRefresh implements XFetch: refresh when now - delta*beta*ln(rand) >= expiry.
func (c *Cache) earlyRefresh(e envelope.Entry, now int64) bool {
	if c.cfg.beta == 0 || e.Delta <= 0 || e.ExpireAt == 0 {
		return false
	}
	r := c.cfg.rand()
	if r <= 0 {
		r = math.SmallestNonzeroFloat64
	}
	gap := float64(e.Delta) * c.cfg.beta * -math.Log(r)
	return float64(now)+gap >= float64(e.ExpireAt)
}

// lookup reads L1 then L2 and backfills L1. The returned entry may be expired (stale window).
func (c *Cache) lookup(ctx context.Context, key string) (envelope.Entry, bool) {
	if raw, ok := c.l1.Get(key); ok {
		if e, err := envelope.Decode(raw); err == nil {
			c.st.l1Hits.Add(1)
			return e, true
		}
		c.l1.Delete(key)
	}
	c.st.l1Misses.Add(1)
	if c.l2 == nil {
		return envelope.Entry{}, false
	}
	var raw []byte
	err := c.l2Call(func() error {
		var err error
		raw, err = c.l2.Get(ctx, key)
		return err
	})
	if err != nil {
		if errors.Is(err, ErrMiss) {
			c.st.l2Misses.Add(1)
		}
		return envelope.Entry{}, false
	}
	e, err := envelope.Decode(raw)
	if err != nil {
		c.st.decodeErrors.Add(1)
		return envelope.Entry{}, false
	}
	c.st.l2Hits.Add(1)
	now := c.nowNs()
	if hold := c.l1Hold(e, now); hold > 0 {
		c.l1.Set(key, raw, hold)
	}
	return e, true
}

// l1Hold is how long L1 may keep e: its remaining life (plus stale window), capped by L1 TTL.
func (c *Cache) l1Hold(e envelope.Entry, now int64) time.Duration {
	hold := c.cfg.l1TTL
	if e.ExpireAt != 0 {
		left := time.Duration(e.ExpireAt-now) + c.cfg.staleWindow
		hold = min(hold, left)
	}
	return hold
}

func (c *Cache) store(ctx context.Context, key string, e envelope.Entry, ttl time.Duration) {
	now := c.nowNs()
	e.StoredAt = now
	e.ExpireAt = now + int64(ttl)
	raw := envelope.Encode(e)
	hold := c.cfg.l1TTL
	if c.l2 != nil {
		err := c.l2Call(func() error { return c.l2.Set(ctx, key, raw, ttl+c.cfg.staleWindow) })
		if err != nil {
			hold = c.cfg.degradedL1TTL
		}
	}
	c.l1.Set(key, raw, min(hold, ttl+c.cfg.staleWindow))
}

// l2Call runs op through the circuit breaker. Misses and semantic errors don't count as failures.
func (c *Cache) l2Call(op func() error) error {
	if !c.br.Allow() {
		c.st.l2Skipped.Add(1)
		return ErrL2Unavailable
	}
	err := op()
	switch {
	case err == nil, errors.Is(err, ErrMiss), errors.Is(err, ErrNotStored),
		errors.Is(err, ErrInvalidKey), errors.Is(err, ErrValueTooLarge):
		c.br.Success()
	default:
		c.br.Failure()
		c.st.l2Errors.Add(1)
	}
	return err
}

func (c *Cache) janitor() {
	defer c.wg.Done()
	t := time.NewTicker(c.cfg.sweepInterval)
	defer t.Stop()
	for {
		select {
		case <-c.baseCtx.Done():
			return
		case <-t.C:
			c.l1.Sweep(64)
		}
	}
}

func (c *Cache) check(key string) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if key == "" {
		return ErrInvalidKey
	}
	return nil
}

func (c *Cache) ttl(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return c.cfg.defaultTTL
	}
	return ttl
}

func (c *Cache) nowNs() int64 { return c.cfg.clock.Now().UnixNano() }

func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
