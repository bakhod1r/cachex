// Package cachex is a two-tier cache engine: a sharded in-process LRU (L1) in front of
// a shared store such as memcached (L2), with stampede protection, namespaces and stats.
//
// Consistency: L1 is per process. After a Delete on one node, other nodes may serve
// their L1 copy for up to the L1 TTL (WithL1TTL). L2 is the shared source of truth.
package cachex

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bakhod1r/cachex/internal/breaker"
	"github.com/bakhod1r/cachex/internal/envelope"
	"github.com/bakhod1r/cachex/internal/flight"
	"github.com/bakhod1r/cachex/internal/lru"
)

const genStripes = 1024

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

	// gens are striped per-key write generations. Set and Delete bump the stripe; a load
	// that started under an older generation must not publish its value. A stripe collision
	// only skips caching one load, never serves stale data.
	gens    [genStripes]atomic.Uint64
	genSeed maphash.Seed

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
	if cfg.markerTTL == 0 {
		cfg.markerTTL = 2 * cfg.loadTimeout
	}
	if cfg.markerTTL <= cfg.loadTimeout {
		return nil, fmt.Errorf("cachex: delete marker TTL %v must exceed load timeout %v", cfg.markerTTL, cfg.loadTimeout)
	}
	c := &Cache{cfg: cfg, l2: cfg.l2, refreshSem: make(chan struct{}, cfg.maxRefreshes), genSeed: maphash.MakeSeed()}
	c.l1 = lru.New(lru.Options{
		MaxEntries: cfg.l1MaxEntries,
		MaxBytes:   cfg.l1MaxBytes,
		Shards:     cfg.shards,
		Now:        c.nowNs,
	})
	c.br = breaker.New(breaker.Config{Now: cfg.clock.Now})
	c.baseCtx, c.cancel = context.WithCancel(context.Background())
	if cfg.invalidator != nil {
		if err := cfg.invalidator.Subscribe(c.baseCtx, c.onInvalidation); err != nil {
			c.cancel()
			return nil, err
		}
	}
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
	now := c.nowNs()
	e, ok := c.lookupAt(ctx, key, now)
	if !ok || e.Flags&envelope.FlagTombstone != 0 || e.Expired(now) {
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
	c.bump(key)
	c.store(ctx, key, envelope.Entry{Value: val}, c.ttl(ttl))
	return nil
}

// Delete removes key from both tiers. Unlike Set, an L2 failure is returned, because a
// value left in L2 would be served to every node.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if err := c.check(key); err != nil {
		return err
	}
	c.bump(key)
	c.flights.Forget(key) // later loads must not join a flight that read pre-Delete data
	c.l1.Delete(key)
	var err error
	if _, ok := c.l2.(CASStore); ok {
		// Marker instead of delete: an in-flight load's Add/CompareAndSwap then fails.
		now := c.nowNs()
		raw := envelope.Encode(envelope.Entry{Flags: envelope.FlagDeleted, StoredAt: now, ExpireAt: now})
		err = c.l2Call(func() error { return c.l2.Set(ctx, key, raw, c.cfg.markerTTL) })
	} else if c.l2 != nil {
		err = c.l2Call(func() error { return c.l2.Delete(ctx, key) })
	}
	c.l1.Delete(key) // closes the race with a concurrent backfill from L2
	c.publish(ctx, Invalidation{Keys: []string{key}})
	return err
}

// GetMulti returns the cached values that are present; missing keys are simply absent.
// L1 answers first; the rest go to L2 in one round trip when the store implements MultiGetter.
func (c *Cache) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	out, _, err := c.getMulti(ctx, keys)
	return out, err
}

// getMulti is GetMulti that also reports keys holding a live tombstone (cached ErrNotFound).
func (c *Cache) getMulti(ctx context.Context, keys []string) (map[string][]byte, map[string]bool, error) {
	if c.closed.Load() {
		return nil, nil, ErrClosed
	}
	out := make(map[string][]byte, len(keys))
	absent := map[string]bool{}
	var rest []string
	now := c.nowNs()
	for _, k := range keys {
		if k == "" || strings.HasPrefix(k, reservedPrefix) {
			return nil, nil, ErrInvalidKey
		}
		if raw, ok := c.l1.Get(k); ok {
			if e, err := envelope.Decode(raw); err == nil {
				c.st.l1Hits.Add(1)
				switch {
				case e.Expired(now):
				case e.Flags&envelope.FlagTombstone != 0:
					absent[k] = true
				default:
					out[k] = clone(e.Value)
				}
				continue
			}
		}
		c.st.l1Misses.Add(1)
		rest = append(rest, k)
	}
	if len(rest) == 0 || c.l2 == nil {
		return out, absent, nil
	}
	mg, ok := c.l2.(MultiGetter)
	if !ok {
		for _, k := range rest {
			if v, err := c.getL2Only(ctx, k); err == nil {
				out[k] = v
			} else if errors.Is(err, ErrNotFound) {
				absent[k] = true
			}
		}
		return out, absent, nil
	}
	var found map[string][]byte
	if err := c.l2Call(func() error {
		var err error
		found, err = mg.GetMulti(ctx, rest)
		return err
	}); err != nil {
		return out, absent, nil // degraded: L1 answers only
	}
	for _, k := range rest {
		raw, ok := found[k]
		if !ok {
			c.st.l2Misses.Add(1)
			continue
		}
		if v, err := c.accept(k, raw, now); err == nil {
			out[k] = v
		} else if errors.Is(err, ErrNotFound) {
			absent[k] = true
		}
	}
	return out, absent, nil
}

func (c *Cache) getL2Only(ctx context.Context, key string) ([]byte, error) {
	var raw []byte
	if err := c.l2Call(func() error {
		var err error
		raw, err = c.l2.Get(ctx, key)
		return err
	}); err != nil {
		if errors.Is(err, ErrMiss) {
			c.st.l2Misses.Add(1)
		}
		return nil, err
	}
	return c.accept(key, raw, c.nowNs())
}

// accept decodes an L2 value, backfills L1 and returns a live value, ErrNotFound for a
// live tombstone, or ErrMiss.
func (c *Cache) accept(key string, raw []byte, now int64) ([]byte, error) {
	e, err := envelope.Decode(raw)
	if err != nil {
		c.st.decodeErrors.Add(1)
		return nil, ErrMiss
	}
	if e.Flags&envelope.FlagDeleted != 0 {
		c.st.l2Misses.Add(1)
		return nil, ErrMiss
	}
	c.st.l2Hits.Add(1)
	if hold := c.l1Hold(e, now); hold > 0 {
		c.l1.Set(key, raw, hold)
	}
	switch {
	case e.Expired(now):
		return nil, ErrMiss
	case e.Flags&envelope.FlagTombstone != 0:
		return nil, ErrNotFound
	}
	return clone(e.Value), nil
}

// SetMulti writes every entry like Set. Keys are validated first, so an invalid key writes nothing.
func (c *Cache) SetMulti(ctx context.Context, items map[string][]byte, ttl time.Duration) error {
	for k := range items {
		if err := c.check(k); err != nil {
			return err
		}
	}
	ttl = c.ttl(ttl)
	for k, v := range items {
		c.bump(k)
		c.store(ctx, k, envelope.Entry{Value: v}, ttl)
	}
	return nil
}

// MultiLoader loads the given missing keys at once. Keys absent from the result don't exist;
// with WithNegativeTTL that absence is cached.
type MultiLoader func(ctx context.Context, keys []string) (map[string][]byte, error)

// GetOrLoadMulti returns cached values for keys and loads the rest with one loader call.
// Unlike GetOrLoad there is no single-flight across concurrent batches, no stale window and
// no early refresh. Keys the source doesn't have are absent from the result.
func (c *Cache) GetOrLoadMulti(ctx context.Context, keys []string, ttl time.Duration, load MultiLoader) (map[string][]byte, error) {
	if load == nil {
		return nil, errors.New("cachex: nil loader")
	}
	out, absent, err := c.getMulti(ctx, keys)
	if err != nil {
		return nil, err
	}
	var missing []string
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if _, ok := out[k]; ok || absent[k] || seen[k] {
			continue
		}
		seen[k] = true
		missing = append(missing, k)
	}
	c.st.negativeHits.Add(uint64(len(absent)))
	if len(missing) == 0 {
		return out, nil
	}
	ttl = c.ttl(ttl)
	gens := make([]uint64, len(missing))
	for i, k := range missing {
		gens[i] = c.gen(k)
	}
	lctx, cancel := context.WithTimeout(ctx, c.cfg.loadTimeout)
	defer cancel()
	start := c.cfg.clock.Now()
	loaded, err := safeLoad(func() (map[string][]byte, error) { return load(lctx, missing) })
	c.st.loads.Add(1)
	if err != nil {
		c.st.loadErrors.Add(1)
		return nil, err
	}
	delta := int64(c.cfg.clock.Now().Sub(start))
	for i, k := range missing {
		v, ok := loaded[k]
		if ok {
			out[k] = clone(v)
		}
		if c.gen(k) != gens[i] {
			c.st.loadsDiscarded.Add(1)
			continue // Set/Delete raced the loader
		}
		switch {
		case ok:
			c.store(ctx, k, envelope.Entry{Value: v, Delta: delta}, ttl)
		case c.cfg.negativeTTL > 0:
			c.store(ctx, k, envelope.Entry{Flags: envelope.FlagTombstone}, c.cfg.negativeTTL)
		}
	}
	return out, nil
}

// safeLoad runs f, turning a panic into ErrLoaderPanic so one bad loader can't crash the process.
func safeLoad[T any](f func() (T, error)) (v T, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrLoaderPanic, r)
		}
	}()
	return f()
}

func (c *Cache) publish(ctx context.Context, msg Invalidation) {
	if c.cfg.invalidator == nil {
		return
	}
	if err := c.cfg.invalidator.Publish(context.WithoutCancel(ctx), msg); err != nil {
		c.st.publishErrors.Add(1)
	}
}

// onInvalidation applies a message from another process to this process's L1.
func (c *Cache) onInvalidation(msg Invalidation) {
	for _, k := range msg.Keys {
		c.l1.Delete(k)
	}
	if msg.Namespace != "" {
		c.vers.forget(msg.Namespace)
		c.l1.DeletePrefix(msg.Namespace + ":")
	}
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
	if e, ok := c.lookup(ctx, key); ok {
		switch {
		case e.Flags&envelope.FlagTombstone != 0:
			if !e.Expired(now) {
				c.st.negativeHits.Add(1)
				return nil, ErrNotFound
			}
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
		if v, ok, err := c.fresh(fctx, key); ok {
			return v, err
		}
		unlock, cached := c.acquireLoadLock(fctx, key)
		defer unlock()
		if cached {
			if v, ok, err := c.fresh(fctx, key); ok {
				return v, err
			}
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
	gen := c.gen(key)
	ctx, cancel := context.WithTimeout(ctx, c.cfg.loadTimeout)
	defer cancel()
	obs := c.observe(ctx, key) // before the loader reads its source
	start := c.cfg.clock.Now()
	v, err := safeLoad(func() ([]byte, error) { return load(ctx) })
	c.st.loads.Add(1)
	if errors.Is(err, ErrNotFound) && c.cfg.negativeTTL > 0 {
		if c.gen(key) == gen {
			c.commit(ctx, key, envelope.Entry{Flags: envelope.FlagTombstone}, c.cfg.negativeTTL, obs)
		}
		return nil, err
	}
	if err != nil {
		c.st.loadErrors.Add(1)
		return nil, err
	}
	delta := c.cfg.clock.Now().Sub(start)
	if c.gen(key) != gen {
		c.st.loadsDiscarded.Add(1)
		return v, nil // Set/Delete raced the loader: caller gets v, cache keeps the newer write
	}
	c.commit(ctx, key, envelope.Entry{Value: v, Delta: int64(delta)}, ttl, obs)
	if c.gen(key) != gen {
		// Set/Delete landed between the check and our write; undo so ours cannot outlive it.
		c.st.loadsDiscarded.Add(1)
		c.l1.Delete(key)
		if c.l2 != nil && obs.cas == nil { // with CAS the racing write already beat ours in L2
			_ = c.l2Call(func() error { return c.l2.Delete(ctx, key) })
		}
	}
	return v, nil
}

func (c *Cache) gen(key string) uint64 {
	return c.gens[maphash.String(c.genSeed, key)%genStripes].Load()
}

func (c *Cache) bump(key string) {
	c.gens[maphash.String(c.genSeed, key)%genStripes].Add(1)
}

// fresh reports a live cached answer: a value, or ErrNotFound for a live tombstone.
func (c *Cache) fresh(ctx context.Context, key string) ([]byte, bool, error) {
	e, ok := c.lookup(ctx, key)
	if !ok || e.Expired(c.nowNs()) {
		return nil, false, nil
	}
	if e.Flags&envelope.FlagTombstone != 0 {
		c.st.negativeHits.Add(1)
		return nil, true, ErrNotFound
	}
	return e.Value, true, nil
}

// acquireLoadLock takes a cross-process lock in L2 (memcached add) so only one node runs the
// loader. Losers poll the cache until the lock TTL; cached reports that a poll found a value
// or the wait ended, so the caller should re-check before loading. Any L2 problem fails open.
func (c *Cache) acquireLoadLock(ctx context.Context, key string) (unlock func(), cached bool) {
	noop := func() {}
	if c.l2 == nil || c.cfg.lockTTL <= 0 {
		return noop, false
	}
	lk := lockPrefix + key
	err := c.l2Call(func() error { return c.l2.Add(ctx, lk, []byte{1}, c.cfg.lockTTL) })
	if err == nil {
		return func() { _ = c.l2Call(func() error { return c.l2.Delete(context.WithoutCancel(ctx), lk) }) }, false
	}
	if !errors.Is(err, ErrNotStored) {
		return noop, false
	}
	c.st.lockWaits.Add(1)
	deadline := time.NewTimer(c.cfg.lockTTL)
	defer deadline.Stop()
	tick := time.NewTicker(c.cfg.lockPoll)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return noop, true
		case <-deadline.C:
			return noop, true
		case <-tick.C:
			if _, ok, _ := c.fresh(ctx, key); ok {
				return noop, true
			}
		}
	}
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
	return c.lookupAt(ctx, key, c.nowNs())
}

func (c *Cache) lookupAt(ctx context.Context, key string, now int64) (envelope.Entry, bool) {
	if raw, ok := c.l1.GetAt(key, now); ok {
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
	if e.Flags&envelope.FlagDeleted != 0 {
		c.st.l2Misses.Add(1)
		return envelope.Entry{}, false
	}
	c.st.l2Hits.Add(1)
	now = c.nowNs() // L2 round trip took time
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
	c.commit(ctx, key, e, ttl, observation{})
}

// observation is the L2 state a load saw before calling its loader.
type observation struct {
	cas     CASStore // nil: publish with plain Set
	present bool
	token   any
}

// observe snapshots key in a CASStore. Any failure falls back to plain Set, which the
// in-process generation check still guards.
func (c *Cache) observe(ctx context.Context, key string) observation {
	cs, ok := c.l2.(CASStore)
	if !ok {
		return observation{}
	}
	var tok any
	err := c.l2Call(func() error {
		var err error
		_, tok, err = cs.Gets(ctx, key)
		return err
	})
	switch {
	case err == nil:
		return observation{cas: cs, present: true, token: tok}
	case errors.Is(err, ErrMiss):
		return observation{cas: cs}
	}
	return observation{}
}

// commit writes e to L2 then L1. With a CAS observation the L2 write is conditional; if
// another writer (Set, Delete marker, newer load) got there first, nothing is cached.
func (c *Cache) commit(ctx context.Context, key string, e envelope.Entry, ttl time.Duration, obs observation) {
	now := c.nowNs()
	if j := c.cfg.ttlJitter; j > 0 {
		ttl -= time.Duration(float64(ttl) * j * c.cfg.rand())
	}
	e.StoredAt = now
	e.ExpireAt = now + int64(ttl)
	raw := envelope.Encode(e)
	l2ttl := ttl + c.cfg.staleWindow
	hold := c.cfg.l1TTL
	if c.l2 != nil {
		err := c.l2Call(func() error {
			switch {
			case obs.cas == nil:
				return c.l2.Set(ctx, key, raw, l2ttl)
			case obs.present:
				return obs.cas.CompareAndSwap(ctx, key, raw, obs.token, l2ttl)
			default:
				return c.l2.Add(ctx, key, raw, l2ttl)
			}
		})
		if obs.cas != nil && (errors.Is(err, ErrNotStored) || errors.Is(err, ErrMiss)) {
			c.st.loadsDiscarded.Add(1)
			c.l1.Delete(key)
			return
		}
		if err != nil {
			hold = c.cfg.degradedL1TTL
		}
	}
	c.l1.Set(key, raw, min(hold, l2ttl))
}

// l2Call runs op through the circuit breaker. Misses, semantic errors, caller cancellation
// and shutdown don't count as failures.
func (c *Cache) l2Call(op func() error) error {
	if !c.br.Allow() {
		c.st.l2Skipped.Add(1)
		return ErrL2Unavailable
	}
	err := op()
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, ErrClosed):
		// Caller gave up or we are shutting down: no evidence about L2 health either way.
		// DeadlineExceeded still counts, since a slow L2 is exactly what the breaker is for.
		c.br.Cancel()
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
	if key == "" || strings.HasPrefix(key, reservedPrefix) {
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
