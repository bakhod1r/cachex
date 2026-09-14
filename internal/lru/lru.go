// Package lru implements a sharded, byte-valued LRU cache with per-entry TTL.
//
// Expiry is lazy on Get; callers that need bounded memory for expired but
// unread entries drive Sweep from their own janitor.
package lru

import (
	"container/list"
	"hash/maphash"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// entryOverhead is the fixed per-entry byte accounting added to len(key)+len(val).
const entryOverhead = 64

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

type entry struct {
	key       string
	val       []byte
	expiresAt int64 // 0 = never
	size      int64
}

type evicted struct {
	key    string
	reason Reason
}

type shard struct {
	mu    sync.Mutex
	items map[string]*list.Element
	ll    list.List // front = MRU
	bytes int64

	hits, misses, evictions, expirations atomic.Uint64
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
	for i := range c.shards {
		c.shards[i] = &shard{items: make(map[string]*list.Element)}
	}
	return c
}

func (c *Cache) shardFor(key string) *shard {
	return c.shards[maphash.String(c.seed, key)&c.mask]
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
func (s *shard) removeLocked(el *list.Element) *entry {
	e := s.ll.Remove(el).(*entry)
	delete(s.items, e.key)
	s.bytes -= e.size
	return e
}

// Get returns the value for key. The returned slice is shared with the cache
// and MUST be treated as read-only.
func (c *Cache) Get(key string) ([]byte, bool) {
	s := c.shardFor(key)
	s.mu.Lock()
	el, ok := s.items[key]
	if !ok {
		s.mu.Unlock()
		s.misses.Add(1)
		return nil, false
	}
	e := el.Value.(*entry)
	if e.expiresAt != 0 && c.now() >= e.expiresAt {
		s.removeLocked(el)
		s.mu.Unlock()
		s.expirations.Add(1)
		s.misses.Add(1)
		if c.onEvict != nil {
			c.onEvict(key, Expired)
		}
		return nil, false
	}
	s.ll.MoveToFront(el)
	v := e.val
	s.mu.Unlock()
	s.hits.Add(1)
	return v, true
}

// Set stores a copy of val. ttl<=0 means no expiry. It returns false if the
// item alone exceeds the per-shard byte budget; in that case any existing
// value for key is removed so a stale value is never served.
func (c *Cache) Set(key string, val []byte, ttl time.Duration) bool {
	size := int64(len(key)+len(val)) + entryOverhead
	s := c.shardFor(key)
	var evs []evicted

	if c.maxBytes > 0 && size > c.maxBytes {
		s.mu.Lock()
		if el, ok := s.items[key]; ok {
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
	cp := make([]byte, len(val))
	copy(cp, val)

	s.mu.Lock()
	if el, ok := s.items[key]; ok {
		e := el.Value.(*entry)
		s.bytes += size - e.size
		e.val, e.expiresAt, e.size = cp, exp, size
		s.ll.MoveToFront(el)
	} else {
		s.items[key] = s.ll.PushFront(&entry{key: key, val: cp, expiresAt: exp, size: size})
		s.bytes += size
	}
	// Evict from the tail; the new entry is at the front and fits on its own.
	for s.ll.Len() > 1 &&
		((c.maxEntries > 0 && s.ll.Len() > c.maxEntries) || (c.maxBytes > 0 && s.bytes > c.maxBytes)) {
		e := s.removeLocked(s.ll.Back())
		evs = append(evs, evicted{e.key, Capacity})
	}
	s.mu.Unlock()
	if n := len(evs); n > 0 {
		s.evictions.Add(uint64(n))
	}
	c.notify(evs)
	return true
}

// Delete removes key and reports whether it was present.
func (c *Cache) Delete(key string) bool {
	s := c.shardFor(key)
	s.mu.Lock()
	el, ok := s.items[key]
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
		for k, el := range s.items {
			if strings.HasPrefix(k, prefix) {
				s.removeLocked(el)
				evs = append(evs, evicted{k, Removed})
			}
		}
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
				evs = append(evs, evicted{el.Value.(*entry).key, Removed})
			}
		}
		s.items = make(map[string]*list.Element)
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
			if e := el.Value.(*entry); e.expiresAt == 0 || now < e.expiresAt {
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
			if e := el.Value.(*entry); e.expiresAt != 0 && now >= e.expiresAt {
				s.removeLocked(el)
				evs = append(evs, evicted{e.key, Expired})
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
