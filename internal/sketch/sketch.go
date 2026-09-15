// Package sketch implements a lock-free count-min sketch with 4-bit saturating
// counters and periodic aging, used to estimate recent access frequency (TinyLFU).
//
// Concurrency: every counter word is an atomic.Uint64 updated with a
// compare-and-swap loop, so Increment and Estimate are safe without locks and can
// run on a cache's read path. A CAS retry only happens when two goroutines touch
// the same 16-counter word at the same instant. Aging halves every word with the
// same CAS loop; increments racing with a halving are never lost to a torn write,
// they only land before or after the halving. The addition count that triggers
// aging is approximate under concurrency (at most one halving runs at a time).
package sketch

import (
	"math/bits"
	"sync/atomic"
)

const (
	depth       = 4                  // rows
	counterMask = 0xF                // 4-bit counter
	halfMask    = 0x7777777777777777 // clears each counter's top bit after >>1
	// widthFactor sets counters per row = nextPow2(capacity*widthFactor).
	widthFactor = 2
	// sampleFactor sets increments between halvings = sampleFactor*capacity.
	sampleFactor = 10
)

// row multipliers: odd 64-bit constants giving independent-ish row indexes.
var rowSeeds = [depth]uint64{0x9e3779b97f4a7c15, 0xc2b2ae3d27d4eb4f, 0x165667b19e3779f9, 0xd6e8feb86659fd93}

// Sketch is safe for concurrent use. Memory: 4 rows * width counters * 4 bits.
type Sketch struct {
	words      []atomic.Uint64 // depth*wordsPerRow words
	wordsRow   uint64
	shift      uint // 64 - log2(width)
	sampleSize int64
	additions  atomic.Int64
	aging      atomic.Bool
	epoch      atomic.Uint64 // advanced before each halving and on Reset
}

// New sizes the sketch for about capacity distinct hot keys. capacity < 1 is treated as 1.
func New(capacity int) *Sketch {
	capacity = max(capacity, 1)
	width := max(nextPow2(capacity*widthFactor), 16) // >= one full word per row
	return &Sketch{
		words:      make([]atomic.Uint64, depth*(width/16)),
		wordsRow:   uint64(width / 16),
		shift:      uint(64 - bits.TrailingZeros(uint(width))),
		sampleSize: int64(sampleFactor * capacity),
	}
}

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// SampleSize returns the number of counted increments between halvings.
func (s *Sketch) SampleSize() int { return int(s.sampleSize) }

// slot returns the word index and bit offset of row r's counter for hash.
func (s *Sketch) slot(hash uint64, r int) (uint64, uint) {
	idx := (hash * rowSeeds[r]) >> s.shift // high bits: depend on every hash bit
	return uint64(r)*s.wordsRow + idx>>4, uint(idx&15) * 4
}

// Estimate returns the minimum counter for hash, 0..15.
func (s *Sketch) Estimate(hash uint64) uint8 {
	m := uint8(counterMask)
	for r := range depth {
		w, off := s.slot(hash, r)
		m = min(m, uint8(s.words[w].Load()>>off&counterMask))
	}
	return m
}

// Epoch changes whenever counters may have decreased (halving or Reset). A caller
// that saw Increment report saturation at epoch e may skip Increment for that hash
// while Epoch still returns e: the call would be a no-op.
func (s *Sketch) Epoch() uint64 { return s.epoch.Load() }

// Increment adds one to hash's counters that are at the current minimum
// (conservative update), saturating at 15. Saturated keys cost no writes, and
// Increment returns true for them.
func (s *Sketch) Increment(hash uint64) bool {
	var ws [depth]uint64
	var offs [depth]uint
	m := uint64(counterMask)
	for r := range depth {
		ws[r], offs[r] = s.slot(hash, r)
		m = min(m, s.words[ws[r]].Load()>>offs[r]&counterMask)
	}
	if m == counterMask {
		return true
	}
	for r := range depth {
		p := &s.words[ws[r]]
		for {
			old := p.Load()
			if old>>offs[r]&counterMask > m {
				break // already above the minimum (or raised concurrently)
			}
			if p.CompareAndSwap(old, old+1<<offs[r]) {
				break
			}
		}
	}
	if s.additions.Add(1) >= s.sampleSize && s.aging.CompareAndSwap(false, true) {
		s.halve()
		s.additions.Store(0)
		s.aging.Store(false)
	}
	return false
}

func (s *Sketch) halve() {
	s.epoch.Add(1)
	for i := range s.words {
		p := &s.words[i]
		for {
			old := p.Load()
			if p.CompareAndSwap(old, old>>1&halfMask) {
				break
			}
		}
	}
}

// Reset zeroes all counters.
func (s *Sketch) Reset() {
	for i := range s.words {
		s.words[i].Store(0)
	}
	s.additions.Store(0)
	s.epoch.Add(1)
}
