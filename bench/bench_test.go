// Package bench compares cachex's L1 tier against popular in-process Go caches.
// It is a separate module so the core module stays free of these dependencies.
package bench

import (
	"context"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/dgraph-io/ristretto/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/maypok86/otter/v2"
)

const (
	capacity = 10_000
	keyspace = 100_000
	traceOps = 1_000_000
	zipfS    = 1.01 // skew; math/rand/v2 requires s > 1
	zipfV    = 1.0
)

// cache is the minimal surface every library is adapted to.
type cache interface {
	Get(key string) ([]byte, bool)
	Set(key string, val []byte)
	Close()
}

type cachexC struct{ c *cachex.Cache }

func (a cachexC) Get(k string) ([]byte, bool) {
	v, err := a.c.Get(context.Background(), k)
	return v, err == nil
}
func (a cachexC) Set(k string, v []byte) { _ = a.c.Set(context.Background(), k, v, 0) }
func (a cachexC) Close()                 { _ = a.c.Close() }

type hashiC struct{ c *lru.Cache[string, []byte] }

func (a hashiC) Get(k string) ([]byte, bool) { return a.c.Get(k) }
func (a hashiC) Set(k string, v []byte)      { a.c.Add(k, v) }
func (a hashiC) Close()                      {}

type ristrettoC struct {
	c *ristretto.Cache[string, []byte]
}

func (a ristrettoC) Get(k string) ([]byte, bool) { return a.c.Get(k) }
func (a ristrettoC) Set(k string, v []byte)      { a.c.Set(k, v, 1) }
func (a ristrettoC) Close()                      { a.c.Close() }

type otterC struct{ c *otter.Cache[string, []byte] }

func (a otterC) Get(k string) ([]byte, bool) { return a.c.GetIfPresent(k) }
func (a otterC) Set(k string, v []byte)      { a.c.Set(k, v) }
func (a otterC) Close()                      {}

type lib struct {
	name string
	make func(capacity int) cache
}

var libs = []lib{
	{"cachex", func(n int) cache {
		// L1-only; TTLs raised so the default 10s L1 hold never expires entries mid-run.
		c, err := cachex.New(cachex.WithSweepInterval(0), cachex.WithL1MaxEntries(n),
			cachex.WithL1TTL(time.Hour), cachex.WithDefaultTTL(time.Hour))
		if err != nil {
			panic(err)
		}
		return cachexC{c}
	}},
	{"hashicorp-lru", func(n int) cache {
		c, err := lru.New[string, []byte](n)
		if err != nil {
			panic(err)
		}
		return hashiC{c}
	}},
	{"ristretto", func(n int) cache {
		c, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
			NumCounters: int64(n) * 10, MaxCost: int64(n), BufferItems: 64, IgnoreInternalCost: true,
		})
		if err != nil {
			panic(err)
		}
		return ristrettoC{c}
	}},
	{"otter", func(n int) cache {
		return otterC{otter.Must(&otter.Options[string, []byte]{MaximumSize: n})}
	}},
}

var (
	keys  = makeKeys(keyspace)
	value = make([]byte, 64)
	seed  atomic.Uint64
)

func makeKeys(n int) []string {
	k := make([]string, n)
	for i := range k {
		k[i] = "key:" + strconv.Itoa(i)
	}
	return k
}

func newRand() *rand.Rand {
	s := seed.Add(1)
	return rand.New(rand.NewPCG(s, s*0x9e3779b97f4a7c15))
}

// BenchmarkParallelGetHit: capacity 2x the hot set, all keys prefilled, uniform reads.
func BenchmarkParallelGetHit(b *testing.B) {
	const hot = capacity
	for _, l := range libs {
		b.Run(l.name, func(b *testing.B) {
			c := l.make(hot * 2)
			defer c.Close()
			for _, k := range keys[:hot] {
				c.Set(k, value)
			}
			if r, ok := c.(ristrettoC); ok {
				r.c.Wait()
			}
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := newRand()
				for pb.Next() {
					c.Get(keys[r.IntN(hot)])
				}
			})
		})
	}
}

// BenchmarkParallelMixedZipf: 90% Get / 10% Set, Zipf keys over keyspace, capacity 10k.
func BenchmarkParallelMixedZipf(b *testing.B) {
	for _, l := range libs {
		b.Run(l.name, func(b *testing.B) {
			c := l.make(capacity)
			defer c.Close()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := newRand()
				z := rand.NewZipf(r, zipfS, zipfV, keyspace-1)
				for pb.Next() {
					k := keys[z.Uint64()]
					if r.IntN(10) == 0 {
						c.Set(k, value)
					} else {
						c.Get(k)
					}
				}
			})
		})
	}
}

// BenchmarkHitRatio replays one fixed-seed Zipf trace (1M ops, keyspace 100k,
// capacity 10k) single-threaded: Get, and Set on miss. Run with -benchtime=1x;
// the reported hit-ratio is from the last replay, ns/op is per full trace.
func BenchmarkHitRatio(b *testing.B) {
	r := rand.New(rand.NewPCG(42, 1024))
	z := rand.NewZipf(r, zipfS, zipfV, keyspace-1)
	trace := make([]uint64, traceOps)
	for i := range trace {
		trace[i] = z.Uint64()
	}
	for _, l := range libs {
		b.Run(l.name, func(b *testing.B) {
			var ratio float64
			for range b.N {
				c := l.make(capacity)
				hits := 0
				for _, ki := range trace {
					k := keys[ki]
					if _, ok := c.Get(k); ok {
						hits++
					} else {
						c.Set(k, value)
					}
				}
				c.Close()
				ratio = float64(hits) / float64(len(trace))
			}
			b.ReportMetric(ratio*100, "hit%")
		})
	}
}
