package lru

// W-TinyLFU eviction, used when Options.Frequency is set together with MaxEntries.
//
// A shard holds three LRU lists. Every new key enters the window (win). When the window
// is over its share, its tail becomes a candidate for the main space, where it is admitted
// only if the sketch estimates it at least as frequent as the main victim — the probation
// tail (ll); the loser is evicted. This is what keeps a burst of one-hit wonders from
// flushing the working set while still letting a genuinely hot new key in.
//
// Reads promote: a drain under the shard lock moves a read probation entry to the
// protected list (prot) and moves a read protected entry to its front. Protected is
// capped; its tail demotes to probation when it overflows. Reads that are never followed
// by a write to their shard do not promote, which costs nothing: a shard with no writes
// evicts nothing either.
//
// The plain CLOCK policy (winCap == 0) still serves the Frequency=false case and the
// bytes-only case, where an entry count cannot size the segments.

const (
	// windowPercent is the share of the shard given to the window, as in the
	// W-TinyLFU paper's 1% default for a large cache.
	windowPercent = 1
	// protectedPercent is the share of the main space that is protected.
	protectedPercent = 90
)

// sizeSegments sets the window and protected capacities of s for maxEntries per shard.
// It leaves winCap 0 (plain CLOCK) unless there is room for all three lists.
func (s *shard) sizeSegments(maxEntries int) {
	if maxEntries < 4 {
		return
	}
	s.winCap = max(1, maxEntries*windowPercent/100)
	s.protCap = max(1, (maxEntries-s.winCap)*protectedPercent/100)
}

// admitList is the list a newly written key enters.
func (s *shard) admitList() *entryList {
	if s.winCap > 0 {
		return &s.win
	}
	return &s.ll
}

// over reports whether s exceeds its budget.
func (c *Cache) over(s *shard) bool {
	return (c.maxEntries > 0 && s.count() > c.maxEntries) ||
		(c.maxBytes > 0 && s.bytes > c.maxBytes)
}

// evictTinyLFU brings s back inside its budget; caller holds s.mu. cur, the entry just
// written, is never evicted so that a write is always readable afterwards.
func (c *Cache) evictTinyLFU(s *shard, cur *entry, evs []evicted) []evicted {
	for c.over(s) && s.count() > 1 {
		e := s.pickVictim(cur)
		if e == nil {
			return evs
		}
		s.removeLocked(e)
		evs = append(evs, evicted{e.key, Capacity})
	}
	// The window may be over its share while the shard as a whole is not: move the
	// overflow into the main space so the window keeps admitting new keys.
	for s.win.Len() > s.winCap {
		e := s.win.Back()
		if e == nil || e == cur {
			break
		}
		moveTo(&s.ll, e)
	}
	return evs
}

// pickVictim returns the entry to evict, or nil when only cur is left to choose from.
// A window candidate is weighed against the probation victim by estimated frequency.
func (s *shard) pickVictim(cur *entry) *entry {
	victim := s.mainVictim(cur)
	if s.win.Len() <= s.winCap {
		if victim != nil {
			return victim
		}
		return s.notCur(s.win.Back(), cur)
	}
	cand := s.notCur(s.win.Back(), cur)
	switch {
	case cand == nil:
		return victim
	case victim == nil:
		return cand
	}
	// Strictly greater: on a tie the resident stays. Ties are common once the sketch
	// has aged, and admitting on a tie lets a stream of one-hit wonders in.
	if s.freq.Estimate(cand.hash) > s.freq.Estimate(victim.hash) {
		moveTo(&s.ll, cand) // admitted into the main space
		return victim
	}
	return cand
}

// mainVictim is the probation tail, or the protected tail when probation is empty.
func (s *shard) mainVictim(cur *entry) *entry {
	if e := s.notCur(s.ll.Back(), cur); e != nil {
		return e
	}
	return s.notCur(s.prot.Back(), cur)
}

// notCur returns e unless it is cur or nil, in which case it tries the entry before it.
func (s *shard) notCur(e, cur *entry) *entry {
	if e == cur {
		e = e.Prev()
	}
	return e
}

// moveTo unlinks e from its current list and pushes it to the front of l.
func moveTo(l *entryList, e *entry) {
	if e.list == l {
		l.MoveToFront(e)
		return
	}
	if e.list != nil {
		e.list.Remove(e)
	}
	l.PushFront(e)
}

// promote records a read of e under the shard lock: probation entries move to protected,
// protected and window entries move to the front of their own list.
func (s *shard) promote(e *entry) {
	switch {
	case e.list == nil || s.winCap == 0:
		return
	case e.list == &s.ll:
		moveTo(&s.prot, e)
		for s.prot.Len() > s.protCap {
			d := s.prot.Back()
			if d == nil {
				break
			}
			moveTo(&s.ll, d)
		}
	default:
		e.list.MoveToFront(e)
	}
}
