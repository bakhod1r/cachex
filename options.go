package cachex

import (
	"fmt"
	"time"
)

type config struct {
	l2            Store
	l1MaxEntries  int
	l1MaxBytes    int64
	shards        int
	l1Frequency   bool
	defaultTTL    time.Duration
	l1TTL         time.Duration
	degradedL1TTL time.Duration
	staleWindow   time.Duration
	beta          float64
	loadTimeout   time.Duration
	versionTTL    time.Duration
	sweepInterval time.Duration
	maxRefreshes  int
	negativeTTL   time.Duration
	ttlJitter     float64
	lockTTL       time.Duration
	lockPoll      time.Duration
	markerTTL     time.Duration // 0 = 2 * loadTimeout
	invalidator   Invalidator
	clock         Clock
	rand          func() float64
}

func defaultConfig() config {
	return config{
		l1MaxEntries:  100_000,
		l1Frequency:   true,
		defaultTTL:    5 * time.Minute,
		l1TTL:         10 * time.Second,
		degradedL1TTL: time.Second,
		beta:          1.0,
		loadTimeout:   5 * time.Second,
		versionTTL:    2 * time.Second,
		sweepInterval: time.Second,
		maxRefreshes:  16,
		clock:         realClock{},
	}
}

// Option configures a Cache.
type Option func(*config) error

// WithL2 sets the shared tier (e.g. memcached.New). Without it the cache is L1-only.
func WithL2(s Store) Option {
	return func(c *config) error {
		if s == nil {
			return fmt.Errorf("cachex: nil L2 store")
		}
		c.l2 = s
		return nil
	}
}

// WithL1MaxEntries bounds the in-process tier by entry count (0 = unlimited). Default 100000.
func WithL1MaxEntries(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("cachex: negative L1 max entries")
		}
		c.l1MaxEntries = n
		return nil
	}
}

// WithL1MaxBytes bounds the in-process tier by approximate bytes (0 = unlimited).
func WithL1MaxBytes(n int64) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("cachex: negative L1 max bytes")
		}
		c.l1MaxBytes = n
		return nil
	}
}

// WithL1Frequency makes L1 eviction frequency-aware (default true): a count-min sketch
// tracks how often keys are read and written, and eviction prefers rarely used entries over
// popular ones. On a Zipf workload it raised hit ratio ~2 points for ~15% slower parallel Get.
// A value just written is never evicted by its own Set either way.
func WithL1Frequency(on bool) Option {
	return func(c *config) error { c.l1Frequency = on; return nil }
}

// WithShards sets the L1 shard count (0 = auto).
func WithShards(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("cachex: negative shards")
		}
		c.shards = n
		return nil
	}
}

// WithDefaultTTL is used when Set/GetOrLoad receive ttl <= 0. Default 5m.
func WithDefaultTTL(d time.Duration) Option {
	return positive("default TTL", d, func(c *config) { c.defaultTTL = d })
}

// WithL1TTL caps how long a per-process copy lives. It is the cross-node staleness
// bound: L1 is eventually consistent with L2 within this window. Default 10s.
func WithL1TTL(d time.Duration) Option { return positive("L1 TTL", d, func(c *config) { c.l1TTL = d }) }

// WithDegradedL1TTL is the L1 TTL used when a write to L2 fails. Default 1s.
func WithDegradedL1TTL(d time.Duration) Option {
	return positive("degraded L1 TTL", d, func(c *config) { c.degradedL1TTL = d })
}

// WithStaleWindow lets GetOrLoad serve an expired value for d while it refreshes in the background.
func WithStaleWindow(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("cachex: negative stale window")
		}
		c.staleWindow = d
		return nil
	}
}

// WithBeta sets XFetch probabilistic early refresh strength (0 disables). Default 1.0.
func WithBeta(b float64) Option {
	return func(c *config) error {
		if b < 0 {
			return fmt.Errorf("cachex: negative beta")
		}
		c.beta = b
		return nil
	}
}

// WithLoadTimeout bounds each loader call. Default 5s.
func WithLoadTimeout(d time.Duration) Option {
	return positive("load timeout", d, func(c *config) { c.loadTimeout = d })
}

// WithDeleteMarkerTTL sets how long Delete leaves a marker in L2 when the store implements
// CASStore. A load that read its source before the Delete fails to publish while the marker
// lives, so the TTL must exceed the load timeout. Default 2 * load timeout.
func WithDeleteMarkerTTL(d time.Duration) Option {
	return positive("delete marker TTL", d, func(c *config) { c.markerTTL = d })
}

// WithVersionTTL sets how long a namespace version is cached locally, which is the
// staleness window after Namespace.Invalidate on other processes. Default 2s.
func WithVersionTTL(d time.Duration) Option {
	return positive("version TTL", d, func(c *config) { c.versionTTL = d })
}

// WithSweepInterval sets the expired-entry janitor period (0 disables; expiry stays lazy). Default 1s.
func WithSweepInterval(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("cachex: negative sweep interval")
		}
		c.sweepInterval = d
		return nil
	}
}

// WithNegativeTTL caches a Loader's ErrNotFound for d, so repeated lookups of absent
// records don't hit the source. 0 (default) disables.
func WithNegativeTTL(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return fmt.Errorf("cachex: negative negative TTL")
		}
		c.negativeTTL = d
		return nil
	}
}

// WithTTLJitter shortens every stored TTL by a random fraction in [0, f), so keys written
// together don't expire together. f must be in [0, 1). 0 (default) disables.
func WithTTLJitter(f float64) Option {
	return func(c *config) error {
		if f < 0 || f >= 1 {
			return fmt.Errorf("cachex: TTL jitter must be in [0, 1)")
		}
		c.ttlJitter = f
		return nil
	}
}

// WithDistributedLock makes GetOrLoad take a lock in L2 before loading, so across all
// processes one loader runs per key. Others poll every poll until the value appears or ttl
// passes, then load themselves. ttl should exceed the slowest load. Needs WithL2.
func WithDistributedLock(ttl, poll time.Duration) Option {
	return func(c *config) error {
		if ttl <= 0 || poll <= 0 {
			return fmt.Errorf("cachex: lock ttl and poll must be positive")
		}
		c.lockTTL, c.lockPoll = ttl, poll
		return nil
	}
}

// WithInvalidator broadcasts Delete and Namespace.Invalidate to other processes.
func WithInvalidator(inv Invalidator) Option {
	return func(c *config) error {
		if inv == nil {
			return fmt.Errorf("cachex: nil invalidator")
		}
		c.invalidator = inv
		return nil
	}
}

// WithClock injects a clock (tests).
func WithClock(cl Clock) Option {
	return func(c *config) error {
		if cl == nil {
			return fmt.Errorf("cachex: nil clock")
		}
		c.clock = cl
		return nil
	}
}

func positive(name string, d time.Duration, set func(*config)) Option {
	return func(c *config) error {
		if d <= 0 {
			return fmt.Errorf("cachex: %s must be positive", name)
		}
		set(c)
		return nil
	}
}
