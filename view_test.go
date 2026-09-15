package cachex_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

func TestGetViewL1AndL2(t *testing.T) {
	e := newEnv(t)
	_ = e.c.Set(ctx, "k", []byte("value"), time.Minute)
	var seen string
	if err := e.c.GetView(ctx, "k", func(v []byte) error { seen = string(v); return nil }); err != nil || seen != "value" {
		t.Fatalf("L1 view: %q %v", seen, err)
	}
	other, _ := cachex.New(cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithSweepInterval(0))
	defer other.Close()
	seen = ""
	if err := other.GetView(ctx, "k", func(v []byte) error { seen = string(v); return nil }); err != nil || seen != "value" {
		t.Fatalf("L2 view: %q %v", seen, err)
	}
}

func TestGetViewMissAndErrors(t *testing.T) {
	e := newEnv(t)
	called := false
	if err := e.c.GetView(ctx, "nope", func([]byte) error { called = true; return nil }); !errors.Is(err, cachex.ErrMiss) || called {
		t.Fatalf("miss: err=%v called=%v", err, called)
	}
	_ = e.c.Set(ctx, "k", []byte("v"), time.Minute)
	boom := errors.New("boom")
	if err := e.c.GetView(ctx, "k", func([]byte) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("fn error not returned: %v", err)
	}
	if err := e.c.GetView(ctx, "", func([]byte) error { return nil }); !errors.Is(err, cachex.ErrInvalidKey) {
		t.Fatalf("want ErrInvalidKey, got %v", err)
	}
	if err := e.c.GetView(ctx, "k", nil); err == nil {
		t.Fatal("nil fn: want error")
	}
}

func TestGetViewStableWhileOverwritten(t *testing.T) {
	e := newEnv(t)
	_ = e.c.Set(ctx, "k", []byte("old"), time.Minute)
	err := e.c.GetView(ctx, "k", func(v []byte) error {
		_ = e.c.Set(ctx, "k", []byte("new"), time.Minute) // overwrite during the view
		if string(v) != "old" {
			return fmt.Errorf("view changed under fn: %q", v)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetViewDoesNotAllocateOnL1Hit(t *testing.T) {
	c, _ := cachex.New(cachex.WithSweepInterval(0))
	defer c.Close()
	_ = c.Set(context.Background(), "k", []byte("value"), time.Hour)
	var n int
	fn := func(v []byte) error { n += len(v); return nil }
	if allocs := testing.AllocsPerRun(1000, func() { _ = c.GetView(context.Background(), "k", fn) }); allocs != 0 {
		t.Fatalf("GetView allocated %.1f times per call", allocs)
	}
}

func TestL1FrequencyOption(t *testing.T) {
	// A key read now and then (every 100 writes) in a 64-entry L1 flooded by one-hit keys:
	// CLOCK's single visited bit can't keep it across 100 inserts, frequency can.
	survives := func(freq bool) bool {
		c, err := cachex.New(cachex.WithSweepInterval(0), cachex.WithL1MaxEntries(64), cachex.WithShards(1),
			cachex.WithL1TTL(time.Hour), cachex.WithL1Frequency(freq))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_ = c.Set(ctx, "hot", []byte("h"), time.Hour)
		for i := range 2000 {
			if i%100 == 0 {
				if _, err := c.Get(ctx, "hot"); err != nil {
					_ = c.Set(ctx, "hot", []byte("h"), time.Hour)
				}
			}
			_ = c.Set(ctx, fmt.Sprintf("cold%d", i), []byte("x"), time.Hour)
		}
		_, err = c.Get(ctx, "hot")
		return err == nil
	}
	if !survives(true) {
		t.Fatal("frequency on (default): periodically read key was evicted")
	}
	if survives(false) {
		t.Fatal("frequency off: expected plain CLOCK to evict the key (option not wired?)")
	}
}

func BenchmarkGetView(b *testing.B) {
	c, _ := cachex.New(cachex.WithSweepInterval(0))
	defer c.Close()
	_ = c.Set(ctx, "k", []byte("value"), time.Hour)
	fn := func([]byte) error { return nil }
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = c.GetView(ctx, "k", fn)
		}
	})
}

func BenchmarkGetL1HitFrequencyOff(b *testing.B) {
	c, _ := cachex.New(cachex.WithSweepInterval(0), cachex.WithL1Frequency(false))
	defer c.Close()
	_ = c.Set(ctx, "k", []byte("value"), time.Hour)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = c.Get(ctx, "k")
		}
	})
}
