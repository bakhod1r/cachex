package lru

import (
	"hash/maphash"
	"testing"
)

func TestSweepNonPositiveLimit(t *testing.T) {
	c := New(Options{MaxEntries: 10})
	if n := c.Sweep(0); n != 0 {
		t.Fatalf("swept %d", n)
	}
}

func TestTableEdgeProbes(t *testing.T) {
	seed := maphash.MakeSeed()
	e := newEntry(seed, "k")

	var empty shard
	empty.del(e) // no table yet

	var full shard
	tab := newTable(minTableSlots)
	for i := range tab.slots {
		tab.slots[i].Store(tombstone)
	}
	full.tab.Store(tab)
	if full.lookup(e.key, e.hash) != nil {
		t.Fatal("lookup in a table of tombstones found an entry")
	}

	var sparse shard
	sparse.put(newEntry(seed, "other"))
	sparse.del(e) // probe reaches an empty slot: not present
	if sparse.lookup("other", maphash.String(seed, "other")) == nil {
		t.Fatal("del removed the wrong entry")
	}
}
