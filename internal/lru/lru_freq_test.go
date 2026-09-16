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

// W-TinyLFU: the window tail is admitted into the main space only if the sketch rates it
// above the main victim, so a flood of one-hit wonders cannot flush the working set.
// Some hot keys are still lost — the sketch ages, and keys never read again decay with
// it — so this compares policies rather than demanding perfection.
func TestFrequencySurvivesOneHitFlood(t *testing.T) {
	const hot = 100
	kept := func(freq bool) int {
		c := New(Options{MaxEntries: hot, Shards: 1, Frequency: freq})
		for i := range hot {
			k := "hot" + strconv.Itoa(i)
			c.Set(k, []byte(k), 0)
			for range 5 {
				c.Get(k)
			}
		}
		c.shards[0].drainReads()
		for i := range 10 * hot {
			c.Set("cold"+strconv.Itoa(i), []byte("c"), 0)
		}
		n := 0
		for i := range hot {
			if _, ok := c.Get("hot" + strconv.Itoa(i)); ok {
				n++
			}
		}
		return n
	}
	clock, tinylfu := kept(false), kept(true)
	if tinylfu < 3*clock/2 || tinylfu < hot/2 {
		t.Fatalf("W-TinyLFU kept %d of %d hot keys, CLOCK kept %d", tinylfu, hot, clock)
	}
}

// A newcomer the sketch already rates highly (it was read before, then evicted) must be
// admitted over a resident seen only once.
func TestFrequencyAdmitsFrequentNewcomer(t *testing.T) {
	c := New(Options{MaxEntries: 8, Shards: 1, Frequency: true})
	s := c.shards[0]
	_, h := c.shardFor("comeback")
	for range 20 { // history for a key that is not resident
		s.freq.Increment(h)
	}
	for i := range 8 {
		c.Set("res"+strconv.Itoa(i), []byte("r"), 0) // residents seen once
	}
	c.Set("comeback", []byte("c"), 0)
	c.Set("filler", []byte("f"), 0) // pushes comeback out of the window
	if _, ok := c.Get("comeback"); !ok {
		t.Fatal("frequent newcomer was not admitted")
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

// Reads are buffered; writes drain one stripe each, so a handful of writes is enough to
// fold every buffered read into the sketch.
func TestFrequencyReadsReachSketchOnWrites(t *testing.T) {
	c := New(Options{MaxEntries: 64, Shards: 1, Frequency: true})
	c.Set("hot", []byte("h"), 0)
	s := c.shards[0]
	_, h := c.shardFor("hot")
	before := s.freq.Estimate(h)
	for range 5 {
		c.Get("hot")
	}
	for i := range readStripes { // each write drains one stripe, round-robin
		c.Set("other"+strconv.Itoa(i), []byte("o"), 0)
	}
	if got := s.freq.Estimate(h); got < before+5 {
		t.Fatalf("estimate %d after 5 reads, want at least %d", got, before+5)
	}
}

// A full stripe drains itself, so a hot key reaches saturation without any write.
func TestFrequencyBufferDrainsWhenFull(t *testing.T) {
	c := New(Options{MaxEntries: 64, Shards: 1, Frequency: true})
	c.Set("hot", []byte("h"), 0)
	s := c.shards[0]
	_, h := c.shardFor("hot")
	for range readStripes * readBufSize * 20 {
		c.Get("hot")
	}
	if got := s.freq.Estimate(h); got != 15 {
		t.Fatalf("estimate %d after a flood of reads, want saturated 15", got)
	}
}
