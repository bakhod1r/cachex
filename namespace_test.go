package cachex_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

func TestNamespaceName(t *testing.T) {
	e := newEnv(t)
	for _, bad := range []string{"", "a:b", "has space", "cachexinternal", string(make([]byte, 65))} {
		if _, err := e.c.Namespace(bad); !errors.Is(err, cachex.ErrInvalidKey) {
			t.Errorf("%q: want ErrInvalidKey, got %v", bad, err)
		}
	}
}

func TestNamespaceInvalidateAcrossNodes(t *testing.T) {
	e := newEnv(t, cachex.WithVersionTTL(2*time.Second), cachex.WithL1TTL(time.Hour))
	other, _ := cachex.New(cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithSweepInterval(0),
		cachex.WithVersionTTL(2*time.Second), cachex.WithL1TTL(time.Hour))
	defer other.Close()

	nsA, _ := e.c.Namespace("users")
	nsB, _ := other.Namespace("users")
	_ = nsA.Set(ctx, "1", []byte("alice"), time.Hour)
	if v, err := nsB.Get(ctx, "1"); err != nil || string(v) != "alice" {
		t.Fatalf("node B read: %q %v", v, err)
	}

	if err := nsA.Invalidate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := nsA.Get(ctx, "1"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("invalidating node must miss at once, got %v", err)
	}
	if v, _ := nsB.Get(ctx, "1"); string(v) != "alice" {
		t.Fatalf("node B may serve old data inside version TTL, got %q", v)
	}
	e.clock.Advance(3 * time.Second)
	if _, err := nsB.Get(ctx, "1"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("node B must miss after version TTL, got %v", err)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	e := newEnv(t)
	a, _ := e.c.Namespace("a")
	b, _ := e.c.Namespace("b")
	_ = a.Set(ctx, "k", []byte("A"), time.Hour)
	_ = b.Set(ctx, "k", []byte("B"), time.Hour)
	_ = a.Invalidate(ctx)
	if v, err := b.Get(ctx, "k"); err != nil || string(v) != "B" {
		t.Fatalf("invalidating a touched b: %q %v", v, err)
	}
	// Freeing old L1 copies runs in the background.
	waitFor(t, func() bool { return e.c.Stats().Entries == 1 })
}

func TestNamespaceVersionLostNeverResurrects(t *testing.T) {
	e := newEnv(t, cachex.WithVersionTTL(time.Second), cachex.WithL1TTL(time.Millisecond))
	ns, _ := e.c.Namespace("orders")
	_ = ns.Set(ctx, "1", []byte("old"), time.Hour)
	_ = ns.Invalidate(ctx)
	_ = e.l2.Delete(ctx, "cachex:ns:orders:v") // simulate memcached eviction of the counter
	e.clock.Advance(2 * time.Second)
	if _, err := ns.Get(ctx, "1"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("re-seeded version resurrected old data: %v", err)
	}
}

func TestNamespaceGetOrLoadWhenL2Down(t *testing.T) {
	e := newEnv(t)
	e.l2.SetDown(true)
	ns, _ := e.c.Namespace("p")
	v, err := ns.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("fresh"), nil })
	if err != nil || string(v) != "fresh" {
		t.Fatalf("loader must still serve when version unknown: %q %v", v, err)
	}
}

func TestNamespaceL1Only(t *testing.T) {
	c, _ := cachex.New(cachex.WithSweepInterval(0))
	defer c.Close()
	ns, _ := c.Namespace("s")
	_ = ns.Set(ctx, "k", []byte("v"), time.Hour)
	if v, err := ns.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("%q %v", v, err)
	}
	_ = ns.Invalidate(ctx)
	if _, err := ns.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("want miss, got %v", err)
	}
}
