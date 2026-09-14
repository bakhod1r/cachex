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
)

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
