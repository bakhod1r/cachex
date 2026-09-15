// Package memcached implements cachex.Store over github.com/bradfitz/gomemcache.
package memcached

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bradfitz/gomemcache/memcache"
)

const (
	defaultTimeout = 100 * time.Millisecond
	maxKeyLen      = 250
	maxRelativeTTL = 2592000 // 30 days; larger values are treated as absolute epoch by memcached
	hashLen        = 22
)

// Config configures a Store.
type Config struct {
	Servers        []string
	Timeout        time.Duration // default 100ms; per-op bound, since ctx is checked only before an op starts
	MaxIdleConns   int           // default 2*GOMAXPROCS
	MaxConcurrency int           // 0 = unlimited; when full ops fail fast with cachex.ErrL2Unavailable
	KeyPrefix      string
}

// Store is a memcached-backed cachex.Store.
type Store struct {
	client *memcache.Client
	prefix string
	sem    chan struct{}
	closed atomic.Bool
}

var (
	_ cachex.Store          = (*Store)(nil)
	_ cachex.CASStore       = (*Store)(nil)
	_ cachex.MultiCASGetter = (*Store)(nil)
)

// New builds a Store. It does not dial; connections are lazy.
func New(c Config) (*Store, error) {
	if len(c.Servers) == 0 {
		return nil, errors.New("memcached: no servers configured")
	}
	cl := memcache.New(c.Servers...)
	if cl == nil {
		return nil, errors.New("memcached: invalid server list")
	}
	cl.Timeout = c.Timeout
	if cl.Timeout <= 0 {
		cl.Timeout = defaultTimeout
	}
	cl.MaxIdleConns = c.MaxIdleConns
	if cl.MaxIdleConns <= 0 {
		cl.MaxIdleConns = 2 * runtime.GOMAXPROCS(0)
	}
	s := &Store{client: cl, prefix: c.KeyPrefix}
	if c.MaxConcurrency > 0 {
		s.sem = make(chan struct{}, c.MaxConcurrency)
	}
	return s, nil
}

func (s *Store) begin(ctx context.Context) (func(), error) {
	if s.closed.Load() {
		return nil, cachex.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.sem == nil {
		return func() {}, nil
	}
	select {
	case s.sem <- struct{}{}:
		return func() { <-s.sem }, nil
	default:
		return nil, fmt.Errorf("%w: concurrency limit reached", cachex.ErrL2Unavailable)
	}
}

// Get returns cachex.ErrMiss when absent.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	done, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	it, err := s.client.Get(normalizeKey(s.prefix, key))
	if err != nil {
		return nil, mapErr(err)
	}
	return it.Value, nil
}

// GetMulti implements cachex.MultiGetter with one multi-key get per server.
// Absent keys are omitted.
func (s *Store) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	done, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	wire := make([]string, len(keys))
	back := make(map[string]string, len(keys))
	for i, k := range keys {
		wire[i] = normalizeKey(s.prefix, k)
		back[wire[i]] = k
	}
	items, err := s.client.GetMulti(wire)
	if err != nil {
		return nil, mapErr(err)
	}
	out := make(map[string][]byte, len(items))
	for wk, it := range items {
		out[back[wk]] = it.Value
	}
	return out, nil
}

var _ cachex.MultiGetter = (*Store)(nil)

// Set stores val; ttl <= 0 means no expiry.
func (s *Store) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	return mapErr(s.client.Set(&memcache.Item{Key: normalizeKey(s.prefix, key), Value: val, Expiration: expiration(ttl)}))
}

// Add stores only if absent, else cachex.ErrNotStored.
func (s *Store) Add(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	return mapErr(s.client.Add(&memcache.Item{Key: normalizeKey(s.prefix, key), Value: val, Expiration: expiration(ttl)}))
}

// Gets returns the value and a CAS token (the fetched item).
func (s *Store) Gets(ctx context.Context, key string) ([]byte, any, error) {
	done, err := s.begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer done()
	it, err := s.client.Get(normalizeKey(s.prefix, key))
	if err != nil {
		return nil, nil, mapErr(err)
	}
	return it.Value, it, nil
}

// GetsMulti fetches many keys with CAS tokens in one round trip.
func (s *Store) GetsMulti(ctx context.Context, keys []string) (map[string][]byte, map[string]any, error) {
	done, err := s.begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer done()
	wire := make([]string, len(keys))
	back := make(map[string]string, len(keys))
	for i, k := range keys {
		wire[i] = normalizeKey(s.prefix, k)
		back[wire[i]] = k
	}
	items, err := s.client.GetMulti(wire)
	if err != nil {
		return nil, nil, mapErr(err)
	}
	vals, toks := make(map[string][]byte, len(items)), make(map[string]any, len(items))
	for wk, it := range items {
		vals[back[wk]], toks[back[wk]] = it.Value, it
	}
	return vals, toks, nil
}

// CompareAndSwap stores val only if key is unchanged since Gets; ErrNotStored on conflict.
func (s *Store) CompareAndSwap(ctx context.Context, key string, val []byte, token any, ttl time.Duration) error {
	it, ok := token.(*memcache.Item)
	if !ok || it == nil || it.Key != normalizeKey(s.prefix, key) {
		return fmt.Errorf("memcached: CompareAndSwap token not from Gets for %q", key)
	}
	done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	next := *it // keep the server-assigned CAS id, replace value and TTL
	next.Value, next.Expiration = val, expiration(ttl)
	return mapErr(s.client.CompareAndSwap(&next))
}

// Delete treats a missing key as success.
func (s *Store) Delete(ctx context.Context, key string) error {
	done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	err = s.client.Delete(normalizeKey(s.prefix, key))
	if errors.Is(err, memcache.ErrCacheMiss) {
		return nil
	}
	return mapErr(err)
}

// Incr adds delta to a decimal counter; cachex.ErrMiss when absent.
func (s *Store) Incr(ctx context.Context, key string, delta uint64) (uint64, error) {
	done, err := s.begin(ctx)
	if err != nil {
		return 0, err
	}
	defer done()
	v, err := s.client.Increment(normalizeKey(s.prefix, key), delta)
	if err != nil {
		return 0, mapErr(err)
	}
	return v, nil
}

// Close releases idle connections. Subsequent ops return cachex.ErrClosed.
func (s *Store) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	return s.client.Close()
}

// expiration converts a TTL into a memcached relative expiration in seconds.
// Never returns an absolute epoch: values are clamped to 30 days.
func expiration(ttl time.Duration) int32 {
	if ttl <= 0 {
		return 0
	}
	if ttl < time.Second {
		return 1
	}
	if ttl >= maxRelativeTTL*time.Second {
		return maxRelativeTTL
	}
	secs := int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		secs++
	}
	if secs > maxRelativeTTL {
		secs = maxRelativeTTL
	}
	return int32(secs)
}

func validByte(b byte) bool { return b >= 0x21 && b <= 0x7E }

func validKey(k string) bool {
	if len(k) > maxKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		if !validByte(k[i]) {
			return false
		}
	}
	return true
}

// normalizeKey returns prefix+key when legal, otherwise
// sanitized(prefix+key) head + "#" + 22-char base64url(sha256(prefix+key)).
// Output is always <=250 bytes and within 0x21..0x7E.
func normalizeKey(prefix, key string) string {
	full := prefix + key
	if validKey(full) {
		return full
	}
	sum := sha256.Sum256([]byte(full))
	h := base64.RawURLEncoding.EncodeToString(sum[:])[:hashLen]
	headMax := maxKeyLen - 1 - hashLen
	var b strings.Builder
	b.Grow(maxKeyLen)
	for i := 0; i < len(full) && b.Len() < headMax; i++ {
		c := full[i]
		if !validByte(c) {
			c = '_'
		}
		b.WriteByte(c)
	}
	b.WriteByte('#')
	b.WriteString(h)
	return b.String()
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, memcache.ErrCacheMiss):
		return cachex.ErrMiss
	case errors.Is(err, memcache.ErrNotStored), errors.Is(err, memcache.ErrCASConflict):
		return cachex.ErrNotStored
	case errors.Is(err, memcache.ErrMalformedKey):
		return cachex.ErrInvalidKey
	}
	msg := err.Error()
	if strings.Contains(msg, "object too large") || strings.Contains(msg, "too large") {
		return fmt.Errorf("%w: %v", cachex.ErrValueTooLarge, err)
	}
	if strings.Contains(msg, "client error") {
		// Protocol-level rejection (e.g. incr on non-numeric value): not an availability problem.
		return fmt.Errorf("memcached: %v", err)
	}
	var nerr net.Error
	var cte *memcache.ConnectTimeoutError
	if errors.Is(err, memcache.ErrNoServers) || errors.Is(err, memcache.ErrServerError) ||
		errors.As(err, &nerr) || errors.As(err, &cte) || errors.Is(err, os.ErrDeadlineExceeded) ||
		errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("%w: %v", cachex.ErrL2Unavailable, err)
	}
	// Unknown/unexpected protocol responses or I/O failures: treat as tier unavailable.
	return fmt.Errorf("%w: %v", cachex.ErrL2Unavailable, err)
}
