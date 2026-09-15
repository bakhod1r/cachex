package lru

import (
	"math/rand/v2"
	"strconv"
	"testing"
)

// Mirrors bench/bench_test.go BenchmarkHitRatio: fixed-seed Zipf trace, 1M ops,
// keyspace 100k, capacity 10k, s=1.01 v=1.0, PCG(42, 1024); Get, Set on miss.
// Differences: lru.Cache is used directly (no root cachex wrapper, no L2) with
// auto shard count, matching the root's L1 construction via MaxEntries.
const (
	hrOps      = 1_000_000
	hrCapacity = 10_000
	hrKeyspace = 100_000
	hrZipfS    = 1.01
	hrZipfV    = 1.0
)

var modes = []struct {
	name string
	freq bool
}{{"clock", false}, {"freq", true}}

func BenchmarkHitRatio(b *testing.B) {
	r := rand.New(rand.NewPCG(42, 1024))
	z := rand.NewZipf(r, hrZipfS, hrZipfV, hrKeyspace-1)
	trace := make([]uint64, hrOps)
	for i := range trace {
		trace[i] = z.Uint64()
	}
	keys := make([]string, hrKeyspace)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
	}
	value := make([]byte, 64)
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			var ratio float64
			for range b.N {
				c := New(Options{MaxEntries: hrCapacity, Frequency: m.freq})
				hits := 0
				for _, ki := range trace {
					if _, ok := c.Get(keys[ki]); ok {
						hits++
					} else {
						c.Set(keys[ki], value, 0)
					}
				}
				ratio = float64(hits) / float64(len(trace))
			}
			b.ReportMetric(ratio*100, "hit%")
		})
	}
}

func BenchmarkGetParallel(b *testing.B) {
	for _, m := range modes {
		b.Run(m.name, func(b *testing.B) {
			c := New(Options{MaxEntries: 100_000, Frequency: m.freq})
			keys := make([]string, 10_000)
			for i := range keys {
				keys[i] = "key:" + strconv.Itoa(i)
				c.Set(keys[i], []byte("value"), 0)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					c.Get(keys[i%len(keys)])
					i++
				}
			})
		})
	}
}
