package bench

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yiling-J/theine-go"
	"github.com/bakhod1r/cachex"
	"github.com/maypok86/otter/v2"
)

// loadFunc is a read-through: return the value for key, calling loader on miss.
type loadFunc func(key string, loader func() []byte) []byte

type stampedeLib struct {
	name string
	make func() (loadFunc, func())
}

// cacheAside is what callers write for libraries without a loading API: Get, and load+Set on miss.
func cacheAside(c cache) loadFunc {
	return func(k string, loader func() []byte) []byte {
		if v, ok := c.Get(k); ok {
			return v
		}
		v := loader()
		c.Set(k, v)
		return v
	}
}

func stampedeLibs() []stampedeLib {
	out := []stampedeLib{
		{"cachex", func() (loadFunc, func()) {
			c, err := cachex.New(cachex.WithSweepInterval(0), cachex.WithL1MaxEntries(capacity),
				cachex.WithL1TTL(time.Hour), cachex.WithDefaultTTL(time.Hour))
			if err != nil {
				panic(err)
			}
			return func(k string, loader func() []byte) []byte {
				v, _ := c.GetOrLoad(context.Background(), k, 0, func(context.Context) ([]byte, error) { return loader(), nil })
				return v
			}, func() { _ = c.Close() }
		}},
		{"otter", func() (loadFunc, func()) {
			c := otter.Must(&otter.Options[string, []byte]{MaximumSize: capacity})
			return func(k string, loader func() []byte) []byte {
				v, _ := c.Get(context.Background(), k, otter.LoaderFunc[string, []byte](
					func(context.Context, string) ([]byte, error) { return loader(), nil }))
				return v
			}, func() {}
		}},
	}
	// theine's loader is fixed at build time, so the per-call loader goes through a key-indexed map.
	out = append(out, stampedeLib{"theine", func() (loadFunc, func()) {
		var loaders sync.Map
		c, err := theine.NewBuilder[string, []byte](capacity).Loading(
			func(_ context.Context, k string) (theine.Loaded[[]byte], error) {
				f, _ := loaders.Load(k)
				return theine.Loaded[[]byte]{Value: f.(func() []byte)(), Cost: 1}, nil
			}).Build()
		if err != nil {
			panic(err)
		}
		return func(k string, loader func() []byte) []byte {
			loaders.LoadOrStore(k, loader)
			v, _ := c.Get(context.Background(), k)
			return v
		}, c.Close
	}})
	for _, l := range libs {
		if l.name == "cachex" || l.name == "otter" || l.name == "theine" {
			continue
		}
		out = append(out, stampedeLib{l.name + " (cache-aside)", func() (loadFunc, func()) {
			c := l.make(capacity)
			return cacheAside(c), c.Close
		}})
	}
	return out
}

// BenchmarkStampede: 64 goroutines request the same cold key at once; the loader
// takes 1ms. Reports loader calls per key (1 = no stampede). ns/op is per key.
func BenchmarkStampede(b *testing.B) {
	const callers = 64
	for _, l := range stampedeLibs() {
		b.Run(l.name, func(b *testing.B) {
			get, closeFn := l.make()
			defer closeFn()
			var loads atomic.Int64
			loader := func() []byte {
				loads.Add(1)
				time.Sleep(time.Millisecond)
				return value
			}
			b.ResetTimer()
			for i := range b.N {
				k := keys[i%keyspace]
				start := make(chan struct{})
				var wg sync.WaitGroup
				for range callers {
					wg.Add(1)
					go func() {
						defer wg.Done()
						<-start
						get(k, loader)
					}()
				}
				close(start)
				wg.Wait()
			}
			b.ReportMetric(float64(loads.Load())/float64(b.N), "loads/key")
		})
	}
}
