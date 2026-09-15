package cachex_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

// blockingLoad returns a loader that signals start and waits for release before returning val.
func blockingLoad(val string) (cachex.Loader, <-chan struct{}, chan<- struct{}) {
	started, release := make(chan struct{}), make(chan struct{})
	return func(context.Context) ([]byte, error) {
		close(started)
		<-release
		return []byte(val), nil
	}, started, release
}

func secondNode(t *testing.T, e env, opts ...cachex.Option) *cachex.Cache {
	t.Helper()
	base := []cachex.Option{cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithSweepInterval(0)}
	c, err := cachex.New(append(base, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func runLoad(c *cachex.Cache, key string, load cachex.Loader) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.GetOrLoad(context.Background(), key, time.Minute, load)
	}()
	return done
}

func TestCrossNodeDeleteDuringColdLoad(t *testing.T) {
	e := newEnv(t)
	b := secondNode(t, e)
	load, started, release := blockingLoad("old")
	done := runLoad(e.c, "k", load)
	<-started
	if err := b.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	for name, c := range map[string]*cachex.Cache{"A": e.c, "B": b} {
		if v, err := c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
			t.Fatalf("node %s serves pre-Delete value: %q %v", name, v, err)
		}
	}
}

func TestCrossNodeDeleteDuringRefresh(t *testing.T) {
	e := newEnv(t, cachex.WithStaleWindow(time.Minute))
	b := secondNode(t, e)
	_ = e.c.Set(ctx, "k", []byte("v1"), time.Second)
	e.clock.Advance(2 * time.Second) // expired, inside stale window: GetOrLoad refreshes in background
	load, started, release := blockingLoad("old")
	if v, _ := e.c.GetOrLoad(ctx, "k", time.Minute, load); string(v) != "v1" {
		t.Fatalf("want stale v1, got %q", v)
	}
	<-started
	if err := b.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitFor(t, func() bool { return e.c.Stats().Loads == 1 })
	if v, err := b.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("refresh overwrote cross-node Delete: %q %v", v, err)
	}
}

func TestDeleteMarkerIsInvisible(t *testing.T) {
	e := newEnv(t, cachex.WithStaleWindow(time.Minute), cachex.WithNegativeTTL(time.Minute))
	b := secondNode(t, e, cachex.WithStaleWindow(time.Minute), cachex.WithNegativeTTL(time.Minute))
	_ = e.c.Set(ctx, "k", []byte("v1"), time.Minute)
	_ = e.c.Delete(ctx, "k")
	if _, err := b.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("Get on deleted key: %v", err)
	}
	if m, _ := b.GetMulti(ctx, []string{"k"}); len(m) != 0 {
		t.Fatalf("GetMulti on deleted key: %v", m)
	}
	v, err := b.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("new"), nil })
	if err != nil || string(v) != "new" {
		t.Fatalf("GetOrLoad after Delete must load fresh (not ErrNotFound, not stale): %q %v", v, err)
	}
	if v, err := e.c.Get(ctx, "k"); err != nil || string(v) != "new" {
		t.Fatalf("load after Delete must be cached: %q %v", v, err)
	}
}

func TestSetOverridesDeleteMarker(t *testing.T) {
	e := newEnv(t)
	_ = e.c.Delete(ctx, "k")
	_ = e.c.Set(ctx, "k", []byte("v"), time.Minute)
	b := secondNode(t, e)
	if v, err := b.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("Set after Delete: %q %v", v, err)
	}
}

func TestDeleteMarkerTTLMustExceedLoadTimeout(t *testing.T) {
	_, err := cachex.New(cachex.WithLoadTimeout(time.Second), cachex.WithDeleteMarkerTTL(time.Second))
	if err == nil {
		t.Fatal("marker TTL <= load timeout must be rejected")
	}
}

// plainStore hides memstore's CASStore methods.
type plainStore struct{ cachex.Store }

func TestDeleteWithoutCASRemovesKey(t *testing.T) {
	e := newEnv(t)
	c, err := cachex.New(cachex.WithClock(e.clock), cachex.WithL2(plainStore{e.l2}), cachex.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.Set(ctx, "k", []byte("v"), time.Minute)
	_ = c.Delete(ctx, "k")
	if _, err := e.l2.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("non-CAS store must get a real delete: %v", err)
	}
}

func TestCrossNodeDeleteDuringColdLoadNegativeCache(t *testing.T) {
	e := newEnv(t, cachex.WithNegativeTTL(time.Minute))
	b := secondNode(t, e, cachex.WithNegativeTTL(time.Minute))
	started, release := make(chan struct{}), make(chan struct{})
	done := runLoad(e.c, "k", func(context.Context) ([]byte, error) {
		close(started)
		<-release
		return nil, cachex.ErrNotFound
	})
	<-started
	_ = b.Delete(ctx, "k") // record created in source, then cache Delete
	close(release)
	<-done
	v, err := b.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("created"), nil })
	if err != nil || string(v) != "created" {
		t.Fatalf("stale negative entry survived Delete: %q %v", v, err)
	}
}

func TestCrossNodeDeleteDuringMultiLoad(t *testing.T) {
	e := newEnv(t)
	b := secondNode(t, e)
	_ = e.c.Set(ctx, "warm", []byte("v1"), time.Second)
	e.clock.Advance(2 * time.Second) // expired: GetOrLoadMulti reloads it (present in L2)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_, _ = e.c.GetOrLoadMulti(context.Background(), []string{"cold", "warm", "kept"}, time.Minute,
			func(context.Context, []string) (map[string][]byte, error) {
				close(started)
				<-release
				return map[string][]byte{"cold": []byte("old"), "warm": []byte("old"), "kept": []byte("k")}, nil
			})
	}()
	<-started
	_ = b.Delete(ctx, "cold")
	_ = b.Delete(ctx, "warm")
	close(release)
	<-done
	for _, k := range []string{"cold", "warm"} {
		if v, err := b.Get(ctx, k); !errors.Is(err, cachex.ErrMiss) {
			t.Fatalf("%s: multi load undid cross-node Delete: %q %v", k, v, err)
		}
	}
	if v, err := b.Get(ctx, "kept"); err != nil || string(v) != "k" {
		t.Fatalf("untouched key must be cached: %q %v", v, err)
	}
}
