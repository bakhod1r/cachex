package cachex

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors. A miss is ErrMiss, never (nil, nil).
var (
	ErrMiss          = errors.New("cachex: miss")
	ErrNotStored     = errors.New("cachex: not stored")
	ErrL2Unavailable = errors.New("cachex: l2 unavailable")
	ErrClosed        = errors.New("cachex: closed")
	ErrInvalidKey    = errors.New("cachex: invalid key")
	ErrValueTooLarge = errors.New("cachex: value too large")
	// ErrNotFound is returned by a Loader when the source has no such record. With
	// WithNegativeTTL the absence is cached and GetOrLoad returns ErrNotFound without loading.
	ErrNotFound = errors.New("cachex: not found")
)

// reservedPrefix marks internal keys (namespace versions, load locks); user keys may not use it.
const (
	reservedPrefix = "cachex:"
	lockPrefix     = reservedPrefix + "lock:"
)

// MultiGetter is an optional Store capability used by Cache.GetMulti for one round trip.
// The result holds only keys that were found.
type MultiGetter interface {
	GetMulti(ctx context.Context, keys []string) (map[string][]byte, error)
}

// Invalidation tells other processes to drop L1 copies.
type Invalidation struct {
	Keys      []string // single keys removed with Delete
	Namespace string   // namespace bumped with Invalidate
}

// Invalidator broadcasts invalidations between processes (Redis pub/sub, NATS, ...), making
// cross-node L1 staleness sub-second instead of bounded by the L1 TTL. Delivery is best effort;
// the TTL bounds still hold for lost messages.
type Invalidator interface {
	Publish(ctx context.Context, msg Invalidation) error
	// Subscribe registers fn and returns; the implementation delivers until ctx is done.
	Subscribe(ctx context.Context, fn func(Invalidation)) error
}

// Store is the shared L2 tier (memcached in production, an in-memory fake in tests).
// Values are opaque encoded envelopes. Implementations must be safe for concurrent use.
type Store interface {
	// Get returns ErrMiss when the key is absent.
	Get(ctx context.Context, key string) ([]byte, error)
	// Set stores val. ttl <= 0 means no expiry.
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	// Add stores only if absent, else ErrNotStored.
	Add(ctx context.Context, key string, val []byte, ttl time.Duration) error
	// Delete of a missing key is success.
	Delete(ctx context.Context, key string) error
	// Incr adds delta to a decimal counter; ErrMiss when absent.
	Incr(ctx context.Context, key string, delta uint64) (uint64, error)
	Close() error
}

// Clock is injectable so TTL tests never sleep.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
