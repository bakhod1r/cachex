package lru

import (
	"sync/atomic"

	"github.com/bakhod1r/cachex/internal/fastrand"
)

// Reads record their key's hash in a lossy ring buffer instead of touching the sketch,
// which otherwise costs several atomic read-modify-writes on words shared by every core.
// The buffer is drained into the sketch when it fills, and by the next write to the shard,
// so eviction always decides on counts that include every completed read.
//
// Lossy: a read that arrives while its stripe is full and not yet drained is dropped. The
// sketch is an approximation already, and dropping is what keeps readers wait-free.

const (
	readStripes = 4  // buffers per shard; more stripes, fewer collisions between cores
	readBufSize = 16 // hashes per stripe before it drains
)

type readBuf struct {
	pos    atomic.Uint32
	hashes [readBufSize]atomic.Uint64
	ents   [readBufSize]atomic.Pointer[entry] // entry read, or nil on a miss
	_      [128 - 4]byte                      // keep neighbouring stripes off this cache line
}

// record adds one read to a stripe, draining it when it fills. Safe without a lock.
func (s *shard) record(e *entry, h uint64) {
	b := &s.reads[fastrand.Uint32()&(readStripes-1)]
	p := b.pos.Add(1)
	if p > readBufSize {
		return // full and awaiting its drain: drop this read
	}
	b.ents[p-1].Store(e)
	b.hashes[p-1].Store(h)
	if p == readBufSize {
		b.drain(s)
	}
}

// drain applies the recorded reads to the sketch and reopens the stripe. An entry whose
// counters came back saturated is stamped so later reads of it skip the buffer entirely.
func (b *readBuf) drain(s *shard) { b.apply(s, false) }

// drainPromote is drain from a caller holding the shard lock, so read entries can also
// move between the eviction lists.
func (b *readBuf) drainPromote(s *shard) { b.apply(s, true) }

func (b *readBuf) apply(s *shard, promote bool) {
	ep := s.freq.Epoch() + 1
	for i := range b.hashes {
		h := b.hashes[i].Swap(0)
		e := b.ents[i].Swap(nil)
		if h == 0 {
			continue
		}
		if s.freq.Increment(h) && e != nil {
			e.satEpoch.Store(ep)
		}
		if promote && e != nil {
			s.promote(e)
		}
	}
	b.pos.Store(0)
}

// drainOne applies one stripe, round-robin, so a write pays for a quarter of the buffered
// reads instead of all of them. A stripe also drains itself when it fills, so nothing is
// held back indefinitely.
func (s *shard) drainOne() {
	if s.freq == nil {
		return
	}
	n := s.drainNext.Add(1)
	s.reads[n&(readStripes-1)].drainPromote(s)
}

// drainReads applies every stripe of s.
func (s *shard) drainReads() {
	if s.freq == nil {
		return
	}
	for i := range s.reads {
		s.reads[i].drain(s)
	}
}
