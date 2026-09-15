package lru

import (
	"hash/maphash"
	"strconv"
	"sync"
	"testing"
)

func newEntry(seed maphash.Seed, key string) *entry {
	return &entry{key: key, hash: maphash.String(seed, key)}
}

func TestTablePutGetDel(t *testing.T) {
	var s shard
	seed := maphash.MakeSeed()
	es := make([]*entry, 1000)
	for i := range es {
		es[i] = newEntry(seed, "k"+strconv.Itoa(i))
		s.put(es[i])
	}
	for _, e := range es {
		if got := s.lookup(e.key, e.hash); got != e {
			t.Fatalf("lookup %s = %v", e.key, got)
		}
	}
	for i, e := range es {
		if i%2 == 0 {
			s.del(e)
		}
	}
	for i, e := range es {
		got := s.lookup(e.key, e.hash)
		if (i%2 == 0) != (got == nil) {
			t.Fatalf("after delete, lookup %s = %v", e.key, got)
		}
	}
	n := 0
	s.forEach(func(*entry) { n++ })
	if n != 500 {
		t.Fatalf("forEach saw %d entries, want 500", n)
	}
	s.resetTable()
	if s.lookup(es[1].key, es[1].hash) != nil {
		t.Fatal("entry survived resetTable")
	}
}

func TestTableChurnDoesNotGrowUnbounded(t *testing.T) {
	var s shard
	seed := maphash.MakeSeed()
	for i := range 100_000 {
		e := newEntry(seed, "k"+strconv.Itoa(i))
		s.put(e)
		s.del(e)
	}
	if n := len(s.tab.Load().slots); n > 64 {
		t.Fatalf("table has %d slots after churn with at most 1 live entry", n)
	}
}

// Readers run without the lock while a single writer inserts, deletes and resizes.
func TestTableConcurrentReaders(t *testing.T) {
	var s shard
	var mu sync.Mutex
	seed := maphash.MakeSeed()
	stable := newEntry(seed, "stable")
	s.put(stable)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				if s.lookup(stable.key, stable.hash) != stable {
					t.Error("stable entry not found during concurrent writes")
					return
				}
			}
		})
	}
	for i := range 50_000 {
		mu.Lock()
		e := newEntry(seed, "k"+strconv.Itoa(i))
		s.put(e)
		if i%3 != 0 {
			s.del(e)
		}
		mu.Unlock()
	}
	close(stop)
	wg.Wait()
}
