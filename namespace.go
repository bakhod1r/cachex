package cachex

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Namespace groups keys so they can be invalidated together with one atomic L2 counter bump.
// Data keys are "<name>:<version>:<key>"; the version lives in L2 at "cachex:ns:<name>:v".
// Other processes see an invalidation within the version TTL (WithVersionTTL).
type Namespace struct {
	c    *Cache
	name string
}

type versionEntry struct {
	v       uint64
	fetched time.Time
}

type versions struct {
	mu sync.Mutex
	m  map[string]versionEntry
}

// Namespace returns a handle. name must be non-empty, at most 64 bytes, printable ASCII
// without ':' or spaces, and must not start with "cachex".
func (c *Cache) Namespace(name string) (*Namespace, error) {
	if name == "" || len(name) > 64 || strings.HasPrefix(name, "cachex") {
		return nil, fmt.Errorf("%w: namespace %q", ErrInvalidKey, name)
	}
	for i := 0; i < len(name); i++ {
		if b := name[i]; b <= 0x20 || b >= 0x7f || b == ':' {
			return nil, fmt.Errorf("%w: namespace %q", ErrInvalidKey, name)
		}
	}
	return &Namespace{c: c, name: name}, nil
}

// Get reads key in the namespace. An unknown version (L2 down, never seen) is a miss.
func (n *Namespace) Get(ctx context.Context, key string) ([]byte, error) {
	k, err := n.key(ctx, key)
	if err != nil {
		return nil, ErrMiss
	}
	return n.c.Get(ctx, k)
}

// Set writes key in the namespace. When the version can't be read (L2 down and never
// fetched) nothing is written and the error is returned.
func (n *Namespace) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	k, err := n.key(ctx, key)
	if err != nil {
		return err
	}
	return n.c.Set(ctx, k, val, ttl)
}

// Delete removes key from the namespace.
func (n *Namespace) Delete(ctx context.Context, key string) error {
	k, err := n.key(ctx, key)
	if err != nil {
		return err
	}
	return n.c.Delete(ctx, k)
}

// GetOrLoad is Cache.GetOrLoad inside the namespace. If the version is unknown the loader
// runs without caching, so callers still get a value.
func (n *Namespace) GetOrLoad(ctx context.Context, key string, ttl time.Duration, load Loader) ([]byte, error) {
	k, err := n.key(ctx, key)
	if err != nil {
		if load == nil {
			return nil, errors.New("cachex: nil loader")
		}
		return load(ctx)
	}
	return n.c.GetOrLoad(ctx, k, ttl, load)
}

// Invalidate makes every key in the namespace unreachable on all nodes by bumping the version.
// Old entries age out of both tiers; this process also frees its L1 copies at once.
func (n *Namespace) Invalidate(ctx context.Context) error {
	c := n.c
	if err := c.check(n.name); err != nil {
		return err
	}
	var next uint64
	if c.l2 == nil {
		next = c.vers.localBump(n.name, c.cfg.clock.Now())
	} else {
		vk := n.versionKey()
		err := c.l2Call(func() error {
			var err error
			next, err = c.l2.Incr(ctx, vk, 1)
			return err
		})
		if errors.Is(err, ErrMiss) {
			next, err = n.seed(ctx)
		}
		if err != nil {
			return err
		}
	}
	c.vers.put(n.name, next, c.cfg.clock.Now())
	c.publish(ctx, Invalidation{Namespace: n.name})
	// Old keys are already unreachable; freeing their memory scans L1, so do it off the caller.
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.l1.DeletePrefix(n.name + ":")
	}()
	return nil
}

func (n *Namespace) key(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", ErrInvalidKey
	}
	v, err := n.version(ctx)
	if err != nil {
		return "", err
	}
	return n.name + ":" + strconv.FormatUint(v, 10) + ":" + key, nil
}

func (n *Namespace) version(ctx context.Context) (uint64, error) {
	c := n.c
	now := c.cfg.clock.Now()
	cached, ok := c.vers.get(n.name)
	if ok && (c.l2 == nil || now.Sub(cached.fetched) < c.cfg.versionTTL) {
		return cached.v, nil
	}
	if c.l2 == nil {
		return c.vers.localBump(n.name, now), nil
	}
	raw, _, err := c.flights.Do(ctx, "\x00ver:"+n.name, func(fctx context.Context) ([]byte, error) {
		v, err := n.fetch(fctx)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatUint(v, 10)), nil
	})
	if err != nil {
		if ok {
			return cached.v, nil // stale version beats no cache while L2 is sick
		}
		return 0, err
	}
	v, _ := strconv.ParseUint(string(raw), 10, 64)
	c.vers.put(n.name, v, c.cfg.clock.Now())
	return v, nil
}

func (n *Namespace) fetch(ctx context.Context) (uint64, error) {
	var raw []byte
	err := n.c.l2Call(func() error {
		var err error
		raw, err = n.c.l2.Get(ctx, n.versionKey())
		return err
	})
	if errors.Is(err, ErrMiss) {
		return n.seed(ctx)
	}
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		// Corrupt counter (foreign writer, bad manual edit): drop it and re-seed. Seeds are
		// wall-clock based, so the new version never collides with an old one.
		n.c.st.decodeErrors.Add(1)
		if err := n.c.l2Call(func() error { return n.c.l2.Delete(ctx, n.versionKey()) }); err != nil {
			return 0, err
		}
		return n.seed(ctx)
	}
	return v, nil
}

// seed creates a missing version counter. It starts at wall-clock milliseconds so a counter
// lost to eviction or restart never reuses an old version and resurrects stale data.
func (n *Namespace) seed(ctx context.Context) (uint64, error) {
	c := n.c
	seed := uint64(c.cfg.clock.Now().UnixMilli())
	if last, ok := c.vers.get(n.name); ok && seed <= last.v {
		seed = last.v + 1
	}
	err := c.l2Call(func() error {
		return c.l2.Add(ctx, n.versionKey(), []byte(strconv.FormatUint(seed, 10)), 0)
	})
	if errors.Is(err, ErrNotStored) {
		return n.fetch(ctx) // another process seeded first
	}
	if err != nil {
		return 0, err
	}
	return seed, nil
}

func (n *Namespace) versionKey() string { return "cachex:ns:" + n.name + ":v" }

func (vs *versions) get(ns string) (versionEntry, bool) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	e, ok := vs.m[ns]
	return e, ok
}

func (vs *versions) forget(ns string) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	delete(vs.m, ns)
}

func (vs *versions) put(ns string, v uint64, at time.Time) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.m == nil {
		vs.m = make(map[string]versionEntry)
	}
	vs.m[ns] = versionEntry{v: v, fetched: at}
}

// localBump serves L1-only caches: first call seeds, later calls come from Invalidate.
func (vs *versions) localBump(ns string, now time.Time) uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.m == nil {
		vs.m = make(map[string]versionEntry)
	}
	e, ok := vs.m[ns]
	if !ok {
		e = versionEntry{v: 1, fetched: now}
		vs.m[ns] = e
		return e.v
	}
	e.v++
	vs.m[ns] = e
	return e.v
}
