package counter

import (
	"sync"
	"testing"
)

func TestConcurrentAdd(t *testing.T) {
	var c Counter
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				c.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := c.Load(); got != 32000 {
		t.Fatalf("Load = %d, want 32000", got)
	}
}

func TestAddDelta(t *testing.T) {
	var c Counter
	c.Add(5)
	c.Add(7)
	if c.Load() != 12 {
		t.Fatalf("Load = %d", c.Load())
	}
}

func BenchmarkAddParallel(b *testing.B) {
	var c Counter
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Add(1)
		}
	})
}
