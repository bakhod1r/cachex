package cachex

import "sync/atomic"

type counters struct {
	l1Hits, l1Misses, l2Hits, l2Misses, l2Errors, l2Skipped atomic.Uint64
	loads, loadErrors, loadsShared                          atomic.Uint64
	staleServed, earlyRefreshes, decodeErrors               atomic.Uint64
	negativeHits, lockWaits, publishErrors                  atomic.Uint64
}

// Stats is a point-in-time snapshot. Counters are read independently, so a snapshot
// taken under load is not atomic across fields.
type Stats struct {
	L1Hits, L1Misses       uint64
	L2Hits, L2Misses       uint64
	L2Errors               uint64 // transport failures
	L2Skipped              uint64 // calls short-circuited by the open breaker
	Loads, LoadErrors      uint64
	LoadsShared            uint64 // callers deduplicated by single-flight
	StaleServed            uint64
	EarlyRefreshes         uint64
	DecodeErrors           uint64
	NegativeHits           uint64 // cached ErrNotFound answers
	LockWaits              uint64 // loads that waited on another process's lock
	PublishErrors          uint64 // failed invalidation broadcasts
	Evictions, Expirations uint64
	Entries                int
	Bytes                  int64
	Breaker                string
}

// HitRatio is hits over lookups across both tiers.
func (s Stats) HitRatio() float64 {
	hits := s.L1Hits + s.L2Hits
	total := s.L1Hits + s.L1Misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// Stats returns a snapshot.
func (c *Cache) Stats() Stats {
	l1 := c.l1.Stats()
	return Stats{
		L1Hits: c.st.l1Hits.Load(), L1Misses: c.st.l1Misses.Load(),
		L2Hits: c.st.l2Hits.Load(), L2Misses: c.st.l2Misses.Load(),
		L2Errors: c.st.l2Errors.Load(), L2Skipped: c.st.l2Skipped.Load(),
		Loads: c.st.loads.Load(), LoadErrors: c.st.loadErrors.Load(), LoadsShared: c.st.loadsShared.Load(),
		StaleServed: c.st.staleServed.Load(), EarlyRefreshes: c.st.earlyRefreshes.Load(),
		DecodeErrors: c.st.decodeErrors.Load(), NegativeHits: c.st.negativeHits.Load(),
		LockWaits: c.st.lockWaits.Load(), PublishErrors: c.st.publishErrors.Load(),
		Evictions: l1.Evictions, Expirations: l1.Expirations,
		Entries: l1.Entries, Bytes: l1.Bytes,
		Breaker: c.br.State().String(),
	}
}
