package cachex_test

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

func TestLoaderPanicReturnsError(t *testing.T) {
	e := newEnv(t)
	_, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { panic("boom") })
	if !errors.Is(err, cachex.ErrLoaderPanic) {
		t.Fatalf("want ErrLoaderPanic, got %v", err)
	}
	if s := e.c.Stats(); s.LoadErrors != 1 {
		t.Fatalf("want 1 load error, got %+v", s)
	}
	v, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("ok"), nil })
	if err != nil || string(v) != "ok" {
		t.Fatalf("after panic got %q %v", v, err)
	}
}

func TestTTLJitterShortensTTL(t *testing.T) {
	e := newEnv(t, cachex.WithL1TTL(time.Hour), cachex.WithTTLJitter(0.2),
		cachex.WithRand(func() float64 { return 0.5 }))
	_ = e.c.Set(ctx, "k", []byte("v"), 10*time.Second) // effective 10s * (1 - 0.2*0.5) = 9s
	e.clock.Advance(8900 * time.Millisecond)
	if _, err := e.c.Get(ctx, "k"); err != nil {
		t.Fatalf("before jittered expiry: %v", err)
	}
	e.clock.Advance(200 * time.Millisecond)
	if _, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("after jittered expiry want miss, got %v", err)
	}
}

func TestTTLJitterValidation(t *testing.T) {
	for _, f := range []float64{-0.1, 1} {
		if _, err := cachex.New(cachex.WithTTLJitter(f)); err == nil {
			t.Fatalf("jitter %v: want error", f)
		}
	}
}

func TestSetMulti(t *testing.T) {
	e := newEnv(t)
	if err := e.c.SetMulti(ctx, map[string][]byte{"a": []byte("1"), "b": []byte("2")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := e.c.GetMulti(ctx, []string{"a", "b"})
	if err != nil || string(got["a"]) != "1" || string(got["b"]) != "2" {
		t.Fatalf("got %q %v", got, err)
	}
	if err := e.c.SetMulti(ctx, map[string][]byte{"c": nil, "cachex:x": nil}, time.Minute); !errors.Is(err, cachex.ErrInvalidKey) {
		t.Fatalf("want ErrInvalidKey, got %v", err)
	}
	if _, err := e.c.Get(ctx, "c"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatal("invalid batch must write nothing")
	}
}

func TestGetOrLoadMulti(t *testing.T) {
	e := newEnv(t, cachex.WithNegativeTTL(time.Minute))
	_ = e.c.Set(ctx, "a", []byte("cached"), time.Minute)
	var asked []string
	var calls atomic.Int32
	load := func(_ context.Context, missing []string) (map[string][]byte, error) {
		calls.Add(1)
		asked = append([]string(nil), missing...)
		return map[string][]byte{"b": []byte("B")}, nil // "c" does not exist
	}
	got, err := e.c.GetOrLoadMulti(ctx, []string{"a", "b", "c"}, time.Minute, load)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(asked)
	if len(asked) != 2 || asked[0] != "b" || asked[1] != "c" {
		t.Fatalf("loader asked %v", asked)
	}
	if string(got["a"]) != "cached" || string(got["b"]) != "B" || len(got) != 2 {
		t.Fatalf("got %q", got)
	}
	// Second call: b cached, c negatively cached; loader not called.
	got, err = e.c.GetOrLoadMulti(ctx, []string{"a", "b", "c"}, time.Minute, load)
	if err != nil || len(got) != 2 || calls.Load() != 1 {
		t.Fatalf("second call got %q %v calls=%d", got, err, calls.Load())
	}
}

func TestGetOrLoadMultiLoaderError(t *testing.T) {
	e := newEnv(t)
	boom := errors.New("boom")
	_, err := e.c.GetOrLoadMulti(ctx, []string{"x"}, time.Minute, func(context.Context, []string) (map[string][]byte, error) {
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	_, err = e.c.GetOrLoadMulti(ctx, []string{"x"}, time.Minute, func(context.Context, []string) (map[string][]byte, error) {
		panic("bad")
	})
	if !errors.Is(err, cachex.ErrLoaderPanic) {
		t.Fatalf("want ErrLoaderPanic, got %v", err)
	}
}
