package lru

import (
	"math/bits"
	"sync/atomic"
)

// The shard index is an open-addressing hash table (linear probing) whose slots are
// atomic pointers, so lookup runs without the shard lock. Writers (put, del, resetTable,
// and the resize inside put) must hold the shard lock; there is one writer at a time.
//
// Why readers are safe: a slot only ever moves nil -> entry, entry -> tombstone, or
// tombstone -> entry, each as one atomic store, and an entry's key and hash never change
// after it is published. A resize builds a new table and publishes it with one atomic
// store; a reader still probing the old table sees the entries as of that moment. A
// lookup racing a write can therefore return the entry just before or just after the
// write, never a torn or foreign one.

const minTableSlots = 16

// tombstone marks a deleted slot so probes for later keys continue past it.
var tombstone = &entry{}

type table struct {
	slots []atomic.Pointer[entry]
	mask  uint64
	used  int // slots holding an entry or a tombstone
	live  int // slots holding an entry
}

func newTable(n int) *table {
	return &table{slots: make([]atomic.Pointer[entry], n), mask: uint64(n - 1)}
}

// start maps a hash to its first probe slot. The low bits pick the shard, so use the high bits.
func (t *table) start(h uint64) uint64 { return bits.RotateLeft64(h, 32) & t.mask }

// lookup returns the entry for key or nil. Safe without the shard lock.
func (s *shard) lookup(key string, h uint64) *entry {
	t := s.tab.Load()
	if t == nil {
		return nil
	}
	for i, n := t.start(h), uint64(0); n <= t.mask; i, n = (i+1)&t.mask, n+1 {
		e := t.slots[i].Load()
		if e == nil {
			return nil
		}
		if e != tombstone && e.hash == h && e.key == key {
			return e
		}
	}
	return nil
}

// put inserts e, whose key must not be present. Caller holds the shard lock.
func (s *shard) put(e *entry) {
	t := s.tab.Load()
	if t == nil || (t.used+1)*4 > len(t.slots)*3 {
		t = s.rehash(t)
	}
	for i := t.start(e.hash); ; i = (i + 1) & t.mask {
		switch cur := t.slots[i].Load(); cur {
		case nil:
			t.used++
			fallthrough
		case tombstone:
			t.live++
			t.slots[i].Store(e)
			return
		}
	}
}

// rehash publishes a table sized for the live entries plus one, dropping tombstones.
func (s *shard) rehash(old *table) *table {
	live := 0
	if old != nil {
		live = old.live
	}
	t := newTable(max(minTableSlots, nextPow2((live+1)*2)))
	if old != nil {
		for i := range old.slots {
			if e := old.slots[i].Load(); e != nil && e != tombstone {
				for j := t.start(e.hash); ; j = (j + 1) & t.mask {
					if t.slots[j].Load() == nil {
						t.slots[j].Store(e)
						break
					}
				}
			}
		}
		t.used, t.live = live, live
	}
	s.tab.Store(t)
	return t
}

// del removes e if it is in the table. Caller holds the shard lock.
func (s *shard) del(e *entry) {
	t := s.tab.Load()
	if t == nil {
		return
	}
	for i, n := t.start(e.hash), uint64(0); n <= t.mask; i, n = (i+1)&t.mask, n+1 {
		switch t.slots[i].Load() {
		case nil:
			return
		case e:
			t.slots[i].Store(tombstone)
			t.live--
			return
		}
	}
}

// forEach calls fn for every entry; fn may del the entry it is given. Caller holds the lock.
func (s *shard) forEach(fn func(*entry)) {
	t := s.tab.Load()
	if t == nil {
		return
	}
	for i := range t.slots {
		if e := t.slots[i].Load(); e != nil && e != tombstone {
			fn(e)
		}
	}
}

// resetTable drops every entry. Caller holds the shard lock.
func (s *shard) resetTable() { s.tab.Store(nil) }
