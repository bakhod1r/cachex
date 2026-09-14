// Package typed wraps a cachex backend with a Codec so callers work with values, not bytes.
package typed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bakhod1r/cachex"
)

// ErrDecode is wrapped by errors returned when a cached value cannot be unmarshalled.
var ErrDecode = errors.New("typed: decode cached value")

// Codec converts values to and from their cached byte form.
type Codec[V any] interface {
	Marshal(V) ([]byte, error)
	Unmarshal([]byte) (V, error)
}

type jsonCodec[V any] struct{}

func (jsonCodec[V]) Marshal(v V) ([]byte, error) { return json.Marshal(v) }
func (jsonCodec[V]) Unmarshal(b []byte) (V, error) {
	var v V
	err := json.Unmarshal(b, &v)
	return v, err
}

// JSON returns an encoding/json Codec.
func JSON[V any]() Codec[V] { return jsonCodec[V]{} }

type bytesCodec struct{}

func (bytesCodec) Marshal(v []byte) ([]byte, error)   { return v, nil }
func (bytesCodec) Unmarshal(b []byte) ([]byte, error) { return b, nil }

// Bytes returns the identity Codec.
func Bytes() Codec[[]byte] { return bytesCodec{} }

type stringCodec struct{}

func (stringCodec) Marshal(v string) ([]byte, error)   { return []byte(v), nil }
func (stringCodec) Unmarshal(b []byte) (string, error) { return string(b), nil }

// String returns a Codec storing strings as raw bytes.
func String() Codec[string] { return stringCodec{} }

// Backend is the byte-level cache API; *cachex.Cache and *cachex.Namespace satisfy it.
type Backend interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	GetOrLoad(ctx context.Context, key string, ttl time.Duration, load cachex.Loader) ([]byte, error)
}

var (
	_ Backend = (*cachex.Cache)(nil)
	_ Backend = (*cachex.Namespace)(nil)
)

// Cache is a typed view over a Backend. Safe for concurrent use if the Backend and Codec are.
type Cache[V any] struct {
	b Backend
	c Codec[V]
}

// New returns a typed cache. It panics if b or c is nil.
func New[V any](b Backend, c Codec[V]) *Cache[V] {
	if b == nil || c == nil {
		panic("typed: nil backend or codec")
	}
	return &Cache[V]{b: b, c: c}
}

// Get returns the decoded value; a miss yields the zero V and cachex.ErrMiss,
// an undecodable value an error wrapping ErrDecode.
func (t *Cache[V]) Get(ctx context.Context, key string) (V, error) {
	var zero V
	raw, err := t.b.Get(ctx, key)
	if err != nil {
		return zero, err
	}
	return t.decode(key, raw)
}

// Set encodes v and stores it.
func (t *Cache[V]) Set(ctx context.Context, key string, v V, ttl time.Duration) error {
	raw, err := t.c.Marshal(v)
	if err != nil {
		return fmt.Errorf("typed: encode %q: %w", key, err)
	}
	return t.b.Set(ctx, key, raw, ttl)
}

// Delete removes key.
func (t *Cache[V]) Delete(ctx context.Context, key string) error { return t.b.Delete(ctx, key) }

// GetOrLoad returns the cached value or loads, encodes and caches it. A cached value that
// fails to decode is treated as corrupt: the key is deleted and the load retried once.
func (t *Cache[V]) GetOrLoad(ctx context.Context, key string, ttl time.Duration, load func(context.Context) (V, error)) (V, error) {
	var zero V
	if load == nil {
		return zero, errors.New("typed: nil loader")
	}
	loader := func(ctx context.Context) ([]byte, error) {
		v, err := load(ctx)
		if err != nil {
			return nil, err
		}
		raw, err := t.c.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("typed: encode %q: %w", key, err)
		}
		return raw, nil
	}
	for attempt := 0; ; attempt++ {
		raw, err := t.b.GetOrLoad(ctx, key, ttl, loader)
		if err != nil {
			return zero, err
		}
		v, err := t.decode(key, raw)
		if err == nil || attempt > 0 {
			return v, err
		}
		if derr := t.b.Delete(ctx, key); derr != nil {
			return zero, errors.Join(err, derr)
		}
	}
}

func (t *Cache[V]) decode(key string, raw []byte) (V, error) {
	v, err := t.c.Unmarshal(raw)
	if err != nil {
		var zero V
		return zero, fmt.Errorf("%w %q: %w", ErrDecode, key, err)
	}
	return v, nil
}
