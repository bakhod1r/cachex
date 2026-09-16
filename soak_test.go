//go:build soak

package cachex_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
)

// TestSoak drives two caches sharing an L2 and an invalidation bus with mixed
// traffic for SOAK_DURATION (default 1m), sampling heap and goroutines after GC.
// It fails if either keeps growing once the key space is warm.
//
//	SOAK_DURATION=30m go test -tags=soak -run TestSoak -timeout=40m -v .
func TestSoak(t *testing.T) {
	dur := time.Minute
	if s := os.Getenv("SOAK_DURATION"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			t.Fatal(err)
		}
		dur = d
	}
	const keys = 50_000

	l2 := memstore.New()
	bus := memstore.NewBus()
	newCache := func() *cachex.Cache {
		c, err := cachex.New(cachex.WithL2(l2), cachex.WithInvalidator(bus),
			cachex.WithL1MaxEntries(10_000), cachex.WithTTLJitter(0.1), cachex.WithStaleWindow(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a, b := newCache(), newCache()
	defer a.Close()
	defer b.Close()
	caches := []*cachex.Cache{a, b}

	ctx := context.Background()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 1))
			zipf := rand.NewZipf(r, 1.1, 1, keys-1)
			val := make([]byte, 256)
			for {
				select {
				case <-stop:
					return
				default:
				}
				c := caches[r.IntN(2)]
				k := fmt.Sprintf("k%d", zipf.Uint64())
				ttl := time.Duration(1+r.IntN(5)) * time.Second
				switch n := r.IntN(100); {
				case n < 60:
					_, _ = c.Get(ctx, k)
				case n < 80:
					_, _ = c.GetOrLoad(ctx, k, ttl, func(context.Context) ([]byte, error) { return val, nil })
				case n < 92:
					_ = c.Set(ctx, k, val, ttl)
				case n < 97:
					_ = c.Delete(ctx, k)
				default:
					_, _ = c.GetMulti(ctx, []string{k, "k1", "k2", "k3"})
				}
			}
		}()
	}

	type sample struct {
		heap uint64
		gor  int
	}
	measure := func() sample {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return sample{m.HeapInuse, runtime.NumGoroutine()}
	}

	interval := max(dur/20, time.Second)
	var samples []sample
	end := time.Now().Add(dur)
	for time.Now().Before(end) {
		time.Sleep(interval)
		s := measure()
		samples = append(samples, s)
		st := a.Stats()
		t.Logf("heap=%.1fMiB goroutines=%d entries=%d hitRatio=%.3f breaker=%s",
			float64(s.heap)/(1<<20), s.gor, st.Entries, st.HitRatio(), st.Breaker)
	}
	close(stop)
	wg.Wait()

	if len(samples) < 4 {
		t.Fatalf("too few samples: %d", len(samples))
	}
	// Compare the first sample after warm-up (a quarter in) with the peak of the last quarter.
	base := samples[len(samples)/4]
	var peak sample
	for _, s := range samples[len(samples)*3/4:] {
		peak.heap = max(peak.heap, s.heap)
		peak.gor = max(peak.gor, s.gor)
	}
	if limit := base.heap + base.heap/2; peak.heap > limit {
		t.Errorf("heap grew: %d after warm-up, %d at end (limit %d)", base.heap, peak.heap, limit)
	}
	if peak.gor > base.gor+8 {
		t.Errorf("goroutines grew: %d after warm-up, %d at end", base.gor, peak.gor)
	}
}
