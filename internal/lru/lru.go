// Package lru implements a sharded, byte-valued LRU cache with per-entry TTL.
//
// Recency uses the CLOCK (second-chance) approximation: Get only sets an atomic
// "visited" bit, so it never moves list nodes. Get takes no lock at all: each shard's
// index is a lock-free-read hash table (table.go) and an entry's value and expiry sit
// behind one atomic pointer. Writes take the shard mutex. At eviction a visited tail entry has its bit cleared and moves to the front
// instead of being evicted. Hit ratio stays close to strict LRU.
//
// With Options.Frequency, Get and Set also record the key in a per-shard
// count-min sketch (TinyLFU-style). When the unvisited tail entry is estimated
// more frequent than the key being written, it gets another pass and up to
// freqCandidates tail entries are examined; the least frequent of them is
// evicted. The entry being written is never evicted, so read-after-write holds.
//
// Expiry is lazy on Get; callers that need bounded memory for expired but
// unread entries drive Sweep from their own janitor.
package lru

import (
	"hash/maphash"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bakhod1r/cachex/internal/counter"
	"github.com/bakhod1r/cachex/internal/sketch"
)

// entryOverhead is the fixed per-entry byte accounting added to len(key)+len(val).
const entryOverhead = 64

// freqCandidates bounds how many unvisited tail entries one eviction examines.
const freqCandidates = 4

// bytesPerEntryGuess sizes the sketch when only MaxBytes bounds the cache.
const bytesPerEntryGuess = 256

// minSketchCap floors per-shard sketch sizing so tiny shards don't age history
// away every few operations (auto sharding already keeps >= 64 entries per shard).
const minSketchCap = 64

// Reason explains why an entry left the cache.
type Reason uint8

// Reasons passed to Options.OnEvict.
const (
	Capacity Reason = iota // evicted to satisfy MaxEntries/MaxBytes
	Expired                // TTL elapsed (Get or Sweep)
	Removed                // Delete, DeletePrefix, Purge, or rejected overwrite
)

// Options configures a Cache.
type Options struct {
	MaxEntries int                             // 0 = unlimited
	MaxBytes   int64                           // 0 = unlimited; size = len(key)+len(val)+64
	Shards     int                             // 0 = auto; always rounded to a power of 2
	Now        func() int64                    // unix nanos; nil = time.Now().UnixNano
	OnEvict    func(key string, reason Reason) // may be nil; called without shard lock held
	// Frequency enables frequency-aware eviction (see package doc). Ignored when
	// neither MaxEntries nor MaxBytes is set, since nothing is ever evicted.
	Frequency bool
}

// Stats is a point-in-time snapshot; counters are summed across shards
// without a global lock, so they are approximately consistent.
type Stats struct {
	Hits, Misses, Evictions, Expirations uint64
	// Entries is the number of stored entries, identical to Len: it includes
	// expired entries not yet removed by Get or Sweep. Use LiveLen for a count
	// of non-expired entries.
	Entries int
	Bytes   int64
}

// entry is one cached key. key and hash never change after the entry is published to
// the shard table; val and expiresAt are replaced together through item.
type entry struct {
	key     string
	hash    uint64 // maphash of key; table probe and sketch lookups
	item    atomic.Pointer[item]
	first   item  // backing storage for the first item: a new key costs one allocation
	size    int64 // guarded by shard.mu
	visited atomic.Bool
	// satEpoch is 1 + the sketch epoch at which Get saw this key's counters saturated
	// (0 = not seen); while it matches, Get skips the sketch.
	satEpoch atomic.Uint64

	prev, next *entry // intrusive list links; owned by the shard's entryList
	list       *entryList
}

type item struct {
	val       []byte
	expiresAt int64 // 0 = never
}

func (it *item) expired(now int64) bool { return it.expiresAt != 0 && now >= it.expiresAt }

type evicted struct {
	key    string
	reason Reason
}

type shard struct {
	mu    sync.Mutex            // guards writes: table mutations, ll, bytes, entry.size
	tab   atomic.Pointer[table] // nil = empty; see table.go
	ll    entryList             // front = MRU
	bytes int64
	freq  *sketch.Sketch // nil unless Options.Frequency with a bound; lock-free

	hits, misses, evictions, expirations counter.Counter
}

// Cache is safe for concurrent use.
type Cache struct {
	shards     []*shard
	mask       uint64
	seed       maphash.Seed
	maxEntries int   // per shard; 0 = unlimited
	maxBytes   int64 // per shard; 0 = unlimited
	now        func() int64
	onEvict    func(string, Reason)
}

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func shardCount(o Options) int {
	if o.Shards > 0 {
		return nextPow2(o.Shards)
	}
	n := min(max(nextPow2(runtime.GOMAXPROCS(0)*4), 16), 256)
	for n > 1 && o.MaxEntries > 0 && o.MaxEntries/n < 64 {
		n >>= 1
	}
	return n
}

// New builds a Cache from o.
func New(o Options) *Cache {
	n := shardCount(o)
	c := &Cache{
		shards:  make([]*shard, n),
		mask:    uint64(n - 1),
		seed:    maphash.MakeSeed(),
		now:     o.Now,
		onEvict: o.OnEvict,
	}
	if c.now == nil {
		c.now = func() int64 { return time.Now().UnixNano() }
	}
	if o.MaxEntries > 0 {
		c.maxEntries = (o.MaxEntries + n - 1) / n
	}
	if o.MaxBytes > 0 {
		c.maxBytes = (o.MaxBytes + int64(n) - 1) / int64(n)
	}
	sketchCap := c.maxEntries
	if sketchCap == 0 && c.maxBytes > 0 {
		sketchCap = int(max(c.maxBytes/bytesPerEntryGuess, 1))
	}
	for i := range c.shards {
		c.shards[i] = &shard{}
		if o.Frequency && sketchCap > 0 {
			c.shards[i].freq = sketch.New(max(sketchCap, minSketchCap))
		}
	}
	return c
}

func (c *Cache) shardFor(key string) (*shard, uint64) {
	h := maphash.String(c.seed, key)
	return c.shards[h&c.mask], h
}

func (c *Cache) notify(evs []evicted) {
	if c.onEvict == nil {
		return
	}
	for _, e := range evs {
		c.onEvict(e.key, e.reason)
	}
}

// removeLocked unlinks el; caller holds s.mu.
func (s *shard) removeLocked(el *entry) *entry {
	e := s.ll.Remove(el)
	s.del(e)
	s.bytes -= e.size
	return e
}

// Get returns the value for key. The returned slice is shared with the cache
// and MUST be treated as read-only.
func (c *Cache) Get(key string) ([]byte, bool) { return c.GetAt(key, c.now()) }

// GetAt is Get with the caller's clock reading, saving a clock call on hot paths.
func (c *Cache) GetAt(key string, now int64) ([]byte, bool) {
	s, h := c.shardFor(key)
	e := s.lookup(key, h)
	if s.freq != nil {
		s.touch(e, h)
	}
	if e == nil {
		s.misses.Add(1)
		return nil, false
	}
	it := e.item.Load()
	if it.expired(now) {
		c.expire(s, e)
		s.misses.Add(1)
		return nil, false
	}
	if !e.visited.Load() { // skip the write when already set: keeps the cache line shared
		e.visited.Store(true)
	}
	s.hits.Add(1)
	return it.val, true
}

// touch records a read of hash h (entry e, nil on a miss) in the sketch, skipping
// the counter reads when e was already saturated in the current sketch epoch.
func (s *shard) touch(e *entry, h uint64) {
	ep := s.freq.Epoch() + 1
	if e != nil && e.satEpoch.Load() == ep {
		return
	}
	if s.freq.Increment(h) && e != nil {
		e.satEpoch.Store(ep)
	}
}

// expire removes e if it is still the stored, expired entry for its key.
func (c *Cache) expire(s *shard, e *entry) {
	s.mu.Lock()
	if s.lookup(e.key, e.hash) != e || !e.item.Load().expired(c.now()) {
		s.mu.Unlock()
		return
	}
	s.removeLocked(e)
	s.mu.Unlock()
	s.expirations.Add(1)
	if c.onEvict != nil {
		c.onEvict(e.key, Expired)
	}
}

// Set stores a copy of val. ttl<=0 means no expiry. It returns false if the
// item alone exceeds the per-shard byte budget; in that case any existing
// value for key is removed so a stale value is never served.
func (c *Cache) Set(key string, val []byte, ttl time.Duration) bool {
	return c.set(key, val, ttl, false)
}

// SetOwned is Set without the copy: the cache keeps val itself, so the caller must not
// modify val afterwards. Use it for buffers built for the cache and never reused.
func (c *Cache) SetOwned(key string, val []byte, ttl time.Duration) bool {
	return c.set(key, val, ttl, true)
}

func (c *Cache) set(key string, val []byte, ttl time.Duration, owned bool) bool {
	size := int64(len(key)+len(val)) + entryOverhead
	s, h := c.shardFor(key)
	var evs []evicted

	if c.maxBytes > 0 && size > c.maxBytes {
		s.mu.Lock()
		if el := s.lookup(key, h); el != nil {
			s.removeLocked(el)
			evs = append(evs, evicted{key, Removed})
		}
		s.mu.Unlock()
		c.notify(evs)
		return false
	}

	var exp int64
	if ttl > 0 {
		exp = c.now() + int64(ttl)
	}
	cp := val
	if !owned {
		cp = make([]byte, len(val))
		copy(cp, val)
	}

	if s.freq != nil {
		s.freq.Increment(h)
	}
	s.mu.Lock()
	cur := s.lookup(key, h)
	if cur != nil {
		s.bytes += size - cur.size
		cur.size = size
		cur.item.Store(&item{val: cp, expiresAt: exp})
		s.ll.MoveToFront(cur)
	} else {
		cur = &entry{key: key, hash: h, size: size, first: item{val: cp, expiresAt: exp}}
		cur.item.Store(&cur.first)
		s.ll.PushFront(cur)
		s.put(cur)
		s.bytes += size
	}
	evs = c.evictLocked(s, cur, h, evs)
	s.mu.Unlock()
	if n := len(evs); n > 0 {
		s.evictions.Add(uint64(n))
	}
	c.notify(evs)
	return true
}

// evictLocked evicts from the tail until s is within budget; caller holds s.mu.
// Second chance: a visited tail entry is cleared and moved to front. With a
// sketch, an unvisited tail entry more frequent than the written key (hash h) is
// also moved to front, up to freqCandidates per eviction; then the least
// frequent examined entry is evicted. cur, the entry just written, is never evicted.
func (c *Cache) evictLocked(s *shard, cur *entry, h uint64, evs []evicted) []evicted {
	var (
		examined  int
		victim    *entry
		victimF   uint8
		incomingF uint8
	)
	if s.freq != nil {
		incomingF = s.freq.Estimate(h)
	}
	for s.ll.Len() > 1 &&
		((c.maxEntries > 0 && s.ll.Len() > c.maxEntries) || (c.maxBytes > 0 && s.bytes > c.maxBytes)) {
		back := s.ll.Back()
		e := back
		if back == cur || e.visited.Load() {
			e.visited.Store(false)
			s.ll.MoveToFront(back)
			continue
		}
		evict := back
		if s.freq != nil {
			f := s.freq.Estimate(e.hash)
			if victim == nil || f < victimF {
				victim, victimF = back, f
			}
			examined++
			if f > incomingF && examined < freqCandidates {
				s.ll.MoveToFront(back)
				continue
			}
			evict = victim
			examined, victim = 0, nil
		}
		e = s.removeLocked(evict)
		evs = append(evs, evicted{e.key, Capacity})
	}
	return evs
}

// Delete removes key and reports whether it was present.
func (c *Cache) Delete(key string) bool {
	s, h := c.shardFor(key)
	s.mu.Lock()
	el := s.lookup(key, h)
	ok := el != nil
	if ok {
		s.removeLocked(el)
	}
	s.mu.Unlock()
	if ok && c.onEvict != nil {
		c.onEvict(key, Removed)
	}
	return ok
}

// DeletePrefix removes every key starting with prefix across all shards.
// Cost is O(total entries).
func (c *Cache) DeletePrefix(prefix string) int {
	total := 0
	for _, s := range c.shards {
		var evs []evicted
		s.mu.Lock()
		s.forEach(func(el *entry) {
			if strings.HasPrefix(el.key, prefix) {
				s.removeLocked(el)
				evs = append(evs, evicted{el.key, Removed})
			}
		})
		s.mu.Unlock()
		total += len(evs)
		c.notify(evs)
	}
	return total
}

// Len returns the number of stored entries, including expired entries that
// have not yet been removed by Get or Sweep. It is O(shards).
func (c *Cache) Len() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		n += s.ll.Len()
		s.mu.Unlock()
	}
	return n
}

// Purge removes all entries. OnEvict is called with Removed for every removed
// entry, after the shard lock is released. No counters are changed.
func (c *Cache) Purge() {
	for _, s := range c.shards {
		var evs []evicted
		s.mu.Lock()
		if c.onEvict != nil && s.ll.Len() > 0 {
			evs = make([]evicted, 0, s.ll.Len())
			for el := s.ll.Front(); el != nil; el = el.Next() {
				evs = append(evs, evicted{el.key, Removed})
			}
		}
		s.resetTable()
		s.ll.Init()
		s.bytes = 0
		s.mu.Unlock()
		c.notify(evs)
	}
}

// LiveLen returns the number of non-expired entries. Unlike Len it scans every
// entry: O(total entries), holding each shard lock for its scan. It does not
// remove expired entries.
func (c *Cache) LiveLen() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		now := c.now()
		for el := s.ll.Front(); el != nil; el = el.Next() {
			if !el.item.Load().expired(now) {
				n++
			}
		}
		s.mu.Unlock()
	}
	return n
}

// Stats returns aggregated counters.
func (c *Cache) Stats() Stats {
	var st Stats
	for _, s := range c.shards {
		st.Hits += s.hits.Load()
		st.Misses += s.misses.Load()
		st.Evictions += s.evictions.Load()
		st.Expirations += s.expirations.Load()
		s.mu.Lock()
		st.Entries += s.ll.Len()
		st.Bytes += s.bytes
		s.mu.Unlock()
	}
	return st
}

// Sweep removes up to maxPerShard expired entries per shard and returns the
// number removed. Each shard scan is O(shard entries) under its lock.
func (c *Cache) Sweep(maxPerShard int) int {
	if maxPerShard <= 0 {
		return 0
	}
	now := c.now()
	total := 0
	for _, s := range c.shards {
		var evs []evicted
		s.mu.Lock()
		for el := s.ll.Back(); el != nil && len(evs) < maxPerShard; {
			prev := el.Prev()
			if el.item.Load().expired(now) {
				s.removeLocked(el)
				evs = append(evs, evicted{el.key, Expired})
			}
			el = prev
		}
		s.mu.Unlock()
		if n := len(evs); n > 0 {
			s.expirations.Add(uint64(n))
			total += n
		}
		c.notify(evs)
	}
	return total
}
