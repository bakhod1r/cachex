package cachexredis

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/redis/go-redis/v9"
)

// StoreConfig configures a Store.
type StoreConfig struct {
	Client      redis.UniversalClient // required; a Cluster client works (every op touches one key)
	KeyPrefix   string                // prepended to every key
	CloseClient bool                  // Close also closes Client
}

// Store is a cachex.Store (L2) over Redis. It also implements cachex.MultiGetter,
// cachex.CASStore and GetsMulti.
//
// Each value is stored as an 8-byte random version followed by the payload. Every write
// picks a new version, and the version is the compare-and-swap token, so a CAS fails after
// any intervening write, even one that stored identical bytes. Values written to the same keys
// by other clients lack that header and are reported as errors, never served.
//
// Redis keys and values are binary-safe, so keys are used as-is (no hashing, no length limit
// beyond Redis's own).
type Store struct {
	c           redis.UniversalClient
	prefix      string
	closeClient bool
	closed      atomic.Bool
}

var (
	_ cachex.Store       = (*Store)(nil)
	_ cachex.MultiGetter = (*Store)(nil)
	_ cachex.CASStore    = (*Store)(nil)
)

const versionLen = 8

// casToken is the version read by Gets.
type casToken [versionLen]byte

// ErrMalformedValue reports a value that wasn't written by a Store (no version header).
var ErrMalformedValue = errors.New("cachexredis: value has no version header")

// NewStore builds a Store. It does not contact Redis.
func NewStore(c StoreConfig) (*Store, error) {
	if c.Client == nil {
		return nil, errors.New("cachexredis: nil Client")
	}
	return &Store{c: c.Client, prefix: c.KeyPrefix, closeClient: c.CloseClient}, nil
}

// compare-and-swap: KEYS[1]; ARGV[1] expected version, ARGV[2] new stored value, ARGV[3] PX (0 = none).
// Returns 1 stored, 0 version mismatch, -1 missing.
var casScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then return -1 end
if string.sub(cur, 1, 8) ~= ARGV[1] then return 0 end
if tonumber(ARGV[3]) > 0 then
  redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
else
  redis.call('SET', KEYS[1], ARGV[2])
end
return 1
`)

// increment: KEYS[1]; ARGV[1] delta, ARGV[2] new version. Keeps the remaining TTL.
// Returns the new value as a decimal string, or false when missing.
var incrScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if not cur then return false end
local digits = string.sub(cur, 9)
if string.len(cur) < 8 or not string.match(digits, '^%d+$') then
  return redis.error_reply('cachexredis: value is not a decimal counter')
end
local n = tonumber(digits) + tonumber(ARGV[1])
if n >= 9007199254740992 then
  return redis.error_reply('cachexredis: counter exceeds 2^53')
end
local s = string.format('%d', n)
local ttl = redis.call('PTTL', KEYS[1])
if ttl > 0 then
  redis.call('SET', KEYS[1], ARGV[2] .. s, 'PX', ttl)
else
  redis.call('SET', KEYS[1], ARGV[2] .. s)
end
return s
`)

// Get returns cachex.ErrMiss when absent.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	v, _, err := s.Gets(ctx, key)
	return v, err
}

// Gets returns the value and its version as the CAS token.
func (s *Store) Gets(ctx context.Context, key string) ([]byte, any, error) {
	if err := s.begin(ctx); err != nil {
		return nil, nil, err
	}
	raw, err := s.c.Get(ctx, s.prefix+key).Bytes()
	if err != nil {
		return nil, nil, mapErr(err)
	}
	return split(key, raw)
}

// GetMulti fetches keys with one MGET; absent keys are omitted.
func (s *Store) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	vals, _, err := s.GetsMulti(ctx, keys)
	return vals, err
}

// GetsMulti is Gets for many keys in one MGET. Absent keys are missing from both maps.
func (s *Store) GetsMulti(ctx context.Context, keys []string) (map[string][]byte, map[string]any, error) {
	if err := s.begin(ctx); err != nil {
		return nil, nil, err
	}
	vals := make(map[string][]byte, len(keys))
	toks := make(map[string]any, len(keys))
	if len(keys) == 0 {
		return vals, toks, nil
	}
	wire := make([]string, len(keys))
	for i, k := range keys {
		wire[i] = s.prefix + k
	}
	res, err := s.c.MGet(ctx, wire...).Result()
	if err != nil {
		return nil, nil, mapErr(err)
	}
	for i, r := range res {
		str, ok := r.(string)
		if !ok {
			continue // nil: absent
		}
		v, tok, err := split(keys[i], []byte(str))
		if err != nil {
			return nil, nil, err
		}
		vals[keys[i]], toks[keys[i]] = v, tok
	}
	return vals, toks, nil
}

// Set stores val; ttl <= 0 means no expiry.
func (s *Store) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if err := s.begin(ctx); err != nil {
		return err
	}
	return mapErr(s.c.Set(ctx, s.prefix+key, stored(val), expiry(ttl)).Err())
}

// Add stores only if key is absent, else cachex.ErrNotStored.
func (s *Store) Add(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if err := s.begin(ctx); err != nil {
		return err
	}
	ok, err := s.c.SetNX(ctx, s.prefix+key, stored(val), expiry(ttl)).Result()
	if err != nil {
		return mapErr(err)
	}
	if !ok {
		return cachex.ErrNotStored
	}
	return nil
}

// CompareAndSwap stores val only if key still has the version Gets returned. It returns
// cachex.ErrNotStored when the key changed and cachex.ErrMiss when it is gone.
func (s *Store) CompareAndSwap(ctx context.Context, key string, val []byte, token any, ttl time.Duration) error {
	tok, ok := token.(casToken)
	if !ok {
		return fmt.Errorf("cachexredis: CompareAndSwap token for %q not from Gets", key)
	}
	if err := s.begin(ctx); err != nil {
		return err
	}
	px := int64(0)
	if d := expiry(ttl); d > 0 {
		px = d.Milliseconds()
	}
	n, err := casScript.Run(ctx, s.c, []string{s.prefix + key}, string(tok[:]), stored(val), px).Int()
	if err != nil {
		return mapErr(err)
	}
	switch n {
	case 1:
		return nil
	case 0:
		return cachex.ErrNotStored
	default:
		return cachex.ErrMiss
	}
}

// Delete removes key; a missing key is not an error.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := s.begin(ctx); err != nil {
		return err
	}
	return mapErr(s.c.Del(ctx, s.prefix+key).Err())
}

// Incr adds delta to a decimal counter, keeping its TTL; cachex.ErrMiss when absent.
// Counters are limited to 2^53 (Lua numbers are doubles).
func (s *Store) Incr(ctx context.Context, key string, delta uint64) (uint64, error) {
	if err := s.begin(ctx); err != nil {
		return 0, err
	}
	v := newVersion()
	str, err := incrScript.Run(ctx, s.c, []string{s.prefix + key}, strconv.FormatUint(delta, 10), string(v[:])).Text()
	if err != nil {
		return 0, mapErr(err)
	}
	return strconv.ParseUint(str, 10, 64)
}

// Close marks the Store closed; with CloseClient it also closes the client. Idempotent.
func (s *Store) Close() error {
	if s.closed.Swap(true) || !s.closeClient {
		return nil
	}
	return s.c.Close()
}

func (s *Store) begin(ctx context.Context) error {
	if s.closed.Load() {
		return cachex.ErrClosed
	}
	return ctx.Err()
}

func newVersion() casToken {
	var v casToken
	binary.LittleEndian.PutUint64(v[:], rand.Uint64())
	return v
}

func stored(val []byte) []byte {
	v := newVersion()
	out := make([]byte, versionLen+len(val))
	copy(out, v[:])
	copy(out[versionLen:], val)
	return out
}

func split(key string, raw []byte) ([]byte, any, error) {
	if len(raw) < versionLen {
		return nil, nil, fmt.Errorf("%w: key %q", ErrMalformedValue, key)
	}
	var tok casToken
	copy(tok[:], raw[:versionLen])
	return raw[versionLen:], tok, nil
}

// expiry converts a TTL to Redis expiration: 0 for none, and never below 1ms so a tiny
// positive TTL doesn't silently become "no expiry".
func expiry(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	if ttl < time.Millisecond {
		return time.Millisecond
	}
	return ttl.Truncate(time.Millisecond)
}

// mapErr keeps misses, caller cancellation and server replies (bad data) distinct from
// transport failures, which become cachex.ErrL2Unavailable and feed the circuit breaker.
func mapErr(err error) error {
	var rerr redis.Error
	switch {
	case err == nil:
		return nil
	case errors.Is(err, redis.Nil):
		return cachex.ErrMiss
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.As(err, &rerr):
		return fmt.Errorf("cachexredis: %w", err)
	default:
		return fmt.Errorf("%w: %v", cachex.ErrL2Unavailable, err)
	}
}
