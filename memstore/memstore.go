// Package memstore is an in-memory cachex.Store fake with fault injection for tests.
package memstore

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bakhod1r/cachex"
)

var _ cachex.Store = (*Store)(nil)

type entry struct {
	val    []byte
	expiry time.Time // zero = never
}

// Store is a mutex-protected map with expiry. Safe for concurrent use.
type Store struct {
	now func() time.Time
	ops atomic.Int64

	mu      sync.Mutex
	data    map[string]entry
	failN   int
	failErr error
	down    bool
	closed  bool
}

// New returns a Store using the real clock.
func New() *Store { return NewWithClock(time.Now) }

// NewWithClock returns a Store using now for TTL decisions.
func NewWithClock(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{now: now, data: make(map[string]entry)}
}

// FailNext makes the next n ops return err.
func (s *Store) FailNext(n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failN, s.failErr = n, err
}

// SetDown makes every op return cachex.ErrL2Unavailable while down.
func (s *Store) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

// Ops returns the total number of ops attempted (including failed ones).
func (s *Store) Ops() int64 { return s.ops.Load() }

// begin counts the op, locks, and returns an injected error if any. Caller must Unlock.
func (s *Store) begin(ctx context.Context) error {
	s.ops.Add(1)
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return cachex.ErrClosed
	}
	if s.down {
		return cachex.ErrL2Unavailable
	}
	if s.failN > 0 {
		s.failN--
		return s.failErr
	}
	return nil
}

func (s *Store) live(key string) (entry, bool) {
	e, ok := s.data[key]
	if !ok {
		return entry{}, false
	}
	if !e.expiry.IsZero() && !s.now().Before(e.expiry) {
		delete(s.data, key)
		return entry{}, false
	}
	return e, true
}

func (s *Store) put(key string, val []byte, ttl time.Duration) {
	e := entry{val: append([]byte{}, val...)}
	if ttl > 0 {
		e.expiry = s.now().Add(ttl)
	}
	s.data[key] = e
}

// Get returns a copy of the live value or cachex.ErrMiss.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return nil, err
	}
	e, ok := s.live(key)
	if !ok {
		return nil, cachex.ErrMiss
	}
	return append([]byte{}, e.val...), nil
}

// Set stores a copy of val; ttl <= 0 means no expiry.
func (s *Store) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	s.put(key, val, ttl)
	return nil
}

// Add stores only if key is absent, else cachex.ErrNotStored.
func (s *Store) Add(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	if _, ok := s.live(key); ok {
		return cachex.ErrNotStored
	}
	s.put(key, val, ttl)
	return nil
}

// Delete removes key; a missing key is not an error.
func (s *Store) Delete(ctx context.Context, key string) error {
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	delete(s.data, key)
	return nil
}

// ErrNotNumeric is returned by Incr when the stored value is not a decimal uint64.
var ErrNotNumeric = errors.New("memstore: value is not a decimal counter")

// Incr adds delta to a decimal counter, keeping its TTL.
func (s *Store) Incr(ctx context.Context, key string, delta uint64) (uint64, error) {
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return 0, err
	}
	e, ok := s.live(key)
	if !ok {
		return 0, cachex.ErrMiss
	}
	n, err := strconv.ParseUint(string(e.val), 10, 64)
	if err != nil {
		return 0, ErrNotNumeric
	}
	n += delta // wraps like memcached
	e.val = strconv.AppendUint(nil, n, 10)
	s.data[key] = e // keeps expiry
	return n, nil
}

// Close marks the store closed; subsequent ops return cachex.ErrClosed.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// GetMulti implements cachex.MultiGetter; absent keys are omitted.
func (s *Store) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(keys))
	for _, k := range keys {
		if e, ok := s.live(k); ok {
			out[k] = append([]byte{}, e.val...)
		}
	}
	return out, nil
}

var _ cachex.MultiGetter = (*Store)(nil)
