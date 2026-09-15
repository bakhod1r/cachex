package lru

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// A key read often must survive a flood of one-hit wonders. Plain CLOCK loses it
// once its visited bit has been spent and it drifts to the tail unread.
func TestFrequencyKeepsPopularKeyUnderOneHitFlood(t *testing.T) {
	run := func(freq bool) bool {
		c := New(Options{MaxEntries: 8, Shards: 1, Frequency: freq})
		c.Set("pop", []byte("p"), 0)
		for range 10 {
			c.Get("pop")
		}
		for i := range 200 { // pop is never read again during the flood
			c.Set("w"+strconv.Itoa(i), []byte("x"), 0)
		}
		_, ok := c.Get("pop")
		return ok
	}
	if run(false) {
		t.Fatal("precondition: plain CLOCK was expected to evict pop")
	}
	if !run(true) {
		t.Fatal("Frequency=true evicted the popular key")
	}
}

func TestFrequencyReadAfterWriteUnderPressure(t *testing.T) {
	for _, tt := range []struct {
		name string
		o    Options
	}{
		{"entries", Options{MaxEntries: 4, Shards: 1, Frequency: true}},
		{"bytes", Options{MaxBytes: 4 * (entryOverhead + 8), Shards: 1, Frequency: true}},
		{"cap1", Options{MaxEntries: 1, Shards: 1, Frequency: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := New(tt.o)
			for i := range 4 { // make every resident far more frequent than newcomers
				k := "hot" + strconv.Itoa(i)
				c.Set(k, []byte(k), 0)
				for range 20 {
					c.Get(k)
				}
			}
			for i := range 500 {
				k := "new" + strconv.Itoa(i)
				if !c.Set(k, []byte(k), 0) {
					t.Fatalf("Set %s rejected", k)
				}
				if v, ok := c.Get(k); !ok || string(v) != k {
					t.Fatalf("read-after-write failed for %s: %q %v", k, v, ok)
				}
				if s := c.Stats(); (tt.o.MaxEntries > 0 && s.Entries > tt.o.MaxEntries) ||
					(tt.o.MaxBytes > 0 && s.Bytes > tt.o.MaxBytes) {
					t.Fatalf("over capacity: %+v", s)
				}
			}
		})
	}
}

func TestFrequencyEvictsLeastFrequentOfExamined(t *testing.T) {
	r := &evictRec{}
	c := New(Options{MaxEntries: 4, Shards: 1, Frequency: true, OnEvict: r.fn})
	// Insert order a,b,c,d -> tail is a. Frequencies: a=6 b=2 c=6 d=6 (none visited after reset).
	for _, k := range []string{"a", "b", "c", "d"} {
		c.Set(k, []byte(k), 0)
	}
	for _, kv := range []struct {
		k string
		n int
	}{{"a", 5}, {"b", 1}, {"c", 5}, {"d", 5}} {
		for range kv.n {
			c.Get(kv.k)
		}
	}
	// Spend visited bits without evicting: clear them directly (white-box).
	s := c.shards[0]
	for el := s.ll.Front(); el != nil; el = el.Next() {
		el.Value.(*entry).visited.Store(false)
	}
	c.Set("e", []byte("e"), 0)
	if len(r.evs) != 1 || r.evs[0] != "b:0" {
		t.Fatalf("evicted %v, want [b:0]", r.evs)
	}
}

func TestFrequencyUnboundedCacheIgnoresSketch(t *testing.T) {
	c := New(Options{Frequency: true})
	for i := range 1000 {
		c.Set(strconv.Itoa(i), []byte("x"), 0)
	}
	if c.Len() != 1000 {
		t.Fatalf("Len %d", c.Len())
	}
}

func TestFrequencyRaceStress(t *testing.T) {
	c := New(Options{MaxEntries: 256, Shards: 4, Frequency: true, OnEvict: func(string, Reason) {}})
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 20_000 {
				k := "k" + strconv.Itoa((i*(g+1))%2000)
				switch i % 5 {
				case 0:
					if !c.Set(k, []byte(k), 0) {
						t.Error("Set rejected")
					}
				case 1:
					c.Set(k, []byte(k), time.Millisecond)
				case 2:
					if i%1000 == 2 {
						c.Delete(k)
						c.Sweep(8)
					}
				default:
					if v, ok := c.Get(k); ok && string(v) != k {
						t.Errorf("wrong value %q for %q", v, k)
					}
				}
			}
		}()
	}
	wg.Wait()
	if n := c.Len(); n > 256 {
		t.Fatalf("Len %d > cap", n)
	}
}
