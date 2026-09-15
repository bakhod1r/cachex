package sketch

import (
	"hash/maphash"
	"strconv"
	"sync"
	"testing"
)

var seed = maphash.MakeSeed()

func h(i int) uint64 { return maphash.String(seed, "k"+strconv.Itoa(i)) }

func TestEstimateZeroInitially(t *testing.T) {
	s := New(1000)
	for i := range 1000 {
		if e := s.Estimate(h(i)); e != 0 {
			t.Fatalf("key %d estimate %d want 0", i, e)
		}
	}
}

func TestIncrementCountsAndSaturates(t *testing.T) {
	s := New(1000)
	for i := 1; i <= 20; i++ {
		s.Increment(h(7))
		want := uint8(min(i, 15))
		if e := s.Estimate(h(7)); e < want || e > 15 {
			t.Fatalf("after %d increments estimate %d want >= %d and <= 15", i, e, want)
		}
	}
}

func TestNeverUnderestimatesBeforeAging(t *testing.T) {
	s := New(4096)
	counts := map[int]int{}
	for i := range 4000 { // below sampleSize: no halving
		k := i % 500
		counts[k]++
		s.Increment(h(k))
	}
	for k, n := range counts {
		if e := s.Estimate(h(k)); int(e) < min(n, 15) {
			t.Fatalf("key %d estimate %d < true %d", k, e, n)
		}
	}
}

func TestSeparatesHotFromCold(t *testing.T) {
	s := New(1000)
	for range 10 {
		s.Increment(h(1))
	}
	cold := 0
	for i := 2; i < 1002; i++ {
		s.Increment(h(i))
		if s.Estimate(h(i)) >= s.Estimate(h(1)) {
			cold++
		}
	}
	if cold > 10 { // <1% collisions reaching hot count
		t.Fatalf("%d cold keys estimated as hot as the hot key", cold)
	}
}

func TestAgingHalves(t *testing.T) {
	s := New(16)
	for range 15 {
		s.Increment(h(1))
	}
	before := s.Estimate(h(1))
	// Push enough distinct increments to trigger at least one halving.
	for i := 0; i < s.SampleSize(); i++ {
		s.Increment(h(1_000_000 + i))
	}
	after := s.Estimate(h(1))
	if after >= before {
		t.Fatalf("no aging: before %d after %d", before, after)
	}
}

func TestReset(t *testing.T) {
	s := New(100)
	s.Increment(h(1))
	s.Reset()
	if e := s.Estimate(h(1)); e != 0 {
		t.Fatalf("estimate %d after Reset", e)
	}
}

func TestConcurrentIncrement(t *testing.T) {
	s := New(1 << 12)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20_000 {
				s.Increment(h((i * (g + 1)) % 3000))
				_ = s.Estimate(h(i % 3000))
			}
		}()
	}
	wg.Wait()
	for i := range 3000 {
		if e := s.Estimate(h(i)); e > 15 {
			t.Fatalf("counter overflow %d", e)
		}
	}
}

func BenchmarkIncrementParallel(b *testing.B) {
	s := New(1 << 16)
	hs := make([]uint64, 1<<16)
	for i := range hs {
		hs[i] = h(i)
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Increment(hs[i&(len(hs)-1)])
			i++
		}
	})
}

func BenchmarkEstimate(b *testing.B) {
	s := New(1 << 16)
	for i := range b.N {
		_ = s.Estimate(uint64(i) * 0x9e3779b97f4a7c15)
	}
}
