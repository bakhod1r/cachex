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
	// ErrLoaderPanic wraps a panic recovered from a Loader. It is returned, never cached.
	ErrLoaderPanic = errors.New("cachex: loader panicked")
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

// CASStore is an optional Store capability. With it, loads publish with compare-and-swap and
// Delete leaves a short-lived marker, so a load that read its source before a Delete on any
// node cannot write the old value back afterwards. Without it that guard is per process only.
type CASStore interface {
	// Gets returns the value and an opaque token for CompareAndSwap; ErrMiss when absent.
	Gets(ctx context.Context, key string) (val []byte, token any, err error)
	// CompareAndSwap stores val only if key is unchanged since Gets returned token.
	// It returns ErrNotStored when the key changed and ErrMiss when it is gone.
	CompareAndSwap(ctx context.Context, key string, val []byte, token any, ttl time.Duration) error
}

// MultiCASGetter is an optional CASStore capability: Gets for many keys in one round trip,
// used by GetOrLoadMulti. Absent keys are missing from both maps.
type MultiCASGetter interface {
	GetsMulti(ctx context.Context, keys []string) (vals map[string][]byte, tokens map[string]any, err error)
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

// Store is the shared L2 tier (memcached or Redis in production, an in-memory fake in tests).
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
