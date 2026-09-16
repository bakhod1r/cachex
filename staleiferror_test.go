package cachex_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

var errSource = errors.New("source down")

// Past the stale window, a failing loader must still be answered from the last
// good value while WithStaleIfError has not elapsed.
func TestStaleIfErrorServesLastGoodValue(t *testing.T) {
	e := newEnv(t, cachex.WithStaleIfError(time.Hour))
	ok := func(_ context.Context) ([]byte, error) { return []byte("v1"), nil }
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, ok); err != nil {
		t.Fatal(err)
	}

	e.clock.Advance(90 * time.Second) // expired, no stale window configured
	fail := func(_ context.Context) ([]byte, error) { return nil, errSource }
	v, err := e.c.GetOrLoad(ctx, "k", time.Minute, fail)
	if err != nil || string(v) != "v1" {
		t.Fatalf("GetOrLoad = %q, %v; want v1, nil", v, err)
	}
	if s := e.c.Stats(); s.StaleOnError == 0 {
		t.Fatalf("StaleOnError not counted: %+v", s)
	}

	e.clock.Advance(2 * time.Hour) // past stale-if-error
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, fail); !errors.Is(err, errSource) {
		t.Fatalf("err = %v, want source down", err)
	}
}

// A successful load after a failure must replace the stale value again.
func TestStaleIfErrorRecovers(t *testing.T) {
	e := newEnv(t, cachex.WithStaleIfError(time.Hour))
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
		return []byte("v1"), nil
	}); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(90 * time.Second)
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
		return nil, errSource
	}); err != nil {
		t.Fatal(err)
	}
	v, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
		return []byte("v2"), nil
	})
	if err != nil || string(v) != "v2" {
		t.Fatalf("GetOrLoad = %q, %v; want v2, nil", v, err)
	}
}

// ErrNotFound is an authoritative answer from the source, not a failure: it must
// not be masked by a stale value.
func TestStaleIfErrorDoesNotMaskNotFound(t *testing.T) {
	e := newEnv(t, cachex.WithStaleIfError(time.Hour))
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
		return []byte("v1"), nil
	}); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(90 * time.Second)
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
		return nil, cachex.ErrNotFound
	}); !errors.Is(err, cachex.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// Without the option the old behaviour stands: the loader error reaches the caller.
func TestStaleIfErrorOffReturnsLoaderError(t *testing.T) {
	e := newEnv(t)
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
		return []byte("v1"), nil
	}); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(90 * time.Second)
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
		return nil, errSource
	}); !errors.Is(err, errSource) {
		t.Fatalf("err = %v, want source down", err)
	}
}

func TestStaleIfErrorRejectsNegative(t *testing.T) {
	if _, err := cachex.New(cachex.WithStaleIfError(-time.Second)); err == nil {
		t.Fatal("negative stale-if-error accepted")
	}
}
