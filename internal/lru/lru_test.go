package lru

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type clock struct{ t atomic.Int64 }

func (c *clock) Now() int64              { return c.t.Load() }
func (c *clock) Advance(d time.Duration) { c.t.Add(int64(d)) }

func newTest(o Options) (*Cache, *clock) {
	ck := &clock{}
	ck.t.Store(1_000_000)
	o.Now = ck.Now
	if o.Shards == 0 {
		o.Shards = 1
	}
	return New(o), ck
}

type evictRec struct {
	mu  sync.Mutex
	evs []string
}

func (r *evictRec) fn(k string, why Reason) {
	r.mu.Lock()
	r.evs = append(r.evs, fmt.Sprintf("%s:%d", k, why))
	r.mu.Unlock()
}

func TestHitMiss(t *testing.T) {
	c, _ := newTest(Options{})
	if _, ok := c.Get("a"); ok {
		t.Fatal("expected miss")
	}
	if !c.Set("a", []byte("1"), 0) {
		t.Fatal("set rejected")
	}
	v, ok := c.Get("a")
	if !ok || string(v) != "1" {
		t.Fatalf("got %q %v", v, ok)
	}
	s := c.Stats()
	if s.Hits != 1 || s.Misses != 1 || s.Entries != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestSetCopiesValue(t *testing.T) {
	c, _ := newTest(Options{})
	b := []byte("abc")
	c.Set("k", b, 0)
	b[0] = 'z'
	v, _ := c.Get("k")
	if string(v) != "abc" {
		t.Fatalf("aliased: %q", v)
	}
}

func TestOverwriteUpdatesSize(t *testing.T) {
	c, _ := newTest(Options{})
	c.Set("k", make([]byte, 10), 0)
	if got := c.Stats().Bytes; got != int64(1+10+entryOverhead) {
		t.Fatalf("bytes %d", got)
	}
	c.Set("k", make([]byte, 100), 0)
	s := c.Stats()
	if s.Bytes != int64(1+100+entryOverhead) || s.Entries != 1 {
		t.Fatalf("stats %+v", s)
	}
}

func TestLRUOrderEviction(t *testing.T) {
	r := &evictRec{}
	c, _ := newTest(Options{MaxEntries: 3, OnEvict: r.fn})
	for _, k := range []string{"a", "b", "c"} {
		c.Set(k, []byte(k), 0)
	}
	c.Get("a") // order MRU->LRU: a c b
	c.Set("d", []byte("d"), 0)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should be evicted")
	}
	for _, k := range []string{"a", "c", "d"} {
		if _, ok := c.Get(k); !ok {
			t.Fatalf("%s missing", k)
		}
	}
	if c.Stats().Evictions != 1 || len(r.evs) != 1 || r.evs[0] != "b:0" {
		t.Fatalf("evs %v stats %+v", r.evs, c.Stats())
	}
}

func TestTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		advance time.Duration
		hit     bool
	}{
		{"no ttl", 0, time.Hour, true},
		{"negative ttl", -1, time.Hour, true},
		{"before expiry", time.Second, 999 * time.Millisecond, true},
		{"at expiry", time.Second, time.Second, false},
		{"after expiry", time.Second, 2 * time.Second, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &evictRec{}
			c, ck := newTest(Options{OnEvict: r.fn})
			c.Set("k", []byte("v"), tt.ttl)
			ck.Advance(tt.advance)
			_, ok := c.Get("k")
			if ok != tt.hit {
				t.Fatalf("hit=%v want %v", ok, tt.hit)
			}
			s := c.Stats()
			if !tt.hit {
				if s.Expirations != 1 || s.Entries != 0 || s.Misses != 1 || len(r.evs) != 1 || r.evs[0] != "k:1" {
					t.Fatalf("stats %+v evs %v", s, r.evs)
				}
			}
		})
	}
}

func TestByteLimit(t *testing.T) {
	per := int64(1 + 10 + entryOverhead)
	c, _ := newTest(Options{MaxBytes: per * 3})
	for i := 0; i < 5; i++ {
		c.Set(strconv.Itoa(i), make([]byte, 10), 0)
	}
	s := c.Stats()
	if s.Entries != 3 || s.Bytes != per*3 || s.Evictions != 2 {
		t.Fatalf("stats %+v", s)
	}
	if _, ok := c.Get("0"); ok {
		t.Fatal("oldest should be gone")
	}
}

func TestRejectOversize(t *testing.T) {
	c, _ := newTest(Options{MaxBytes: 100})
	c.Set("k", []byte("small"), 0)
	if c.Set("big", make([]byte, 100), 0) {
		t.Fatal("oversize accepted")
	}
	if _, ok := c.Get("k"); !ok {
		t.Fatal("rejection must not evict others")
	}
	// oversize overwrite removes the stale old value
	if c.Set("k", make([]byte, 100), 0) {
		t.Fatal("oversize overwrite accepted")
	}
	if _, ok := c.Get("k"); ok {
		t.Fatal("stale value retained after rejected overwrite")
	}
}

func TestDelete(t *testing.T) {
	r := &evictRec{}
	c, _ := newTest(Options{OnEvict: r.fn})
	c.Set("k", []byte("v"), 0)
	if !c.Delete("k") || c.Delete("k") {
		t.Fatal("delete result wrong")
	}
	if c.Len() != 0 || c.Stats().Bytes != 0 || len(r.evs) != 1 || r.evs[0] != "k:2" {
		t.Fatalf("len %d evs %v", c.Len(), r.evs)
	}
}

func TestDeletePrefix(t *testing.T) {
	c, _ := newTest(Options{Shards: 8})
	for i := 0; i < 50; i++ {
		c.Set("user:"+strconv.Itoa(i), []byte("x"), 0)
		c.Set("order:"+strconv.Itoa(i), []byte("x"), 0)
	}
	if n := c.DeletePrefix("user:"); n != 50 {
		t.Fatalf("removed %d", n)
	}
	if c.Len() != 50 {
		t.Fatalf("len %d", c.Len())
	}
	if n := c.DeletePrefix("nope"); n != 0 {
		t.Fatalf("removed %d", n)
	}
}

func TestSweep(t *testing.T) {
	c, ck := newTest(Options{})
	for i := 0; i < 10; i++ {
		c.Set("e"+strconv.Itoa(i), []byte("x"), time.Second)
	}
	c.Set("live", []byte("x"), 0)
	if n := c.Sweep(100); n != 0 {
		t.Fatalf("swept %d before expiry", n)
	}
	ck.Advance(2 * time.Second)
	if n := c.Sweep(4); n != 4 {
		t.Fatalf("swept %d want 4", n)
	}
	if n := c.Sweep(100); n != 6 {
		t.Fatalf("swept %d want 6", n)
	}
	s := c.Stats()
	if s.Entries != 1 || s.Expirations != 10 {
		t.Fatalf("stats %+v", s)
	}
}

func TestPurge(t *testing.T) {
	c, _ := newTest(Options{Shards: 4})
	for i := 0; i < 20; i++ {
		c.Set(strconv.Itoa(i), []byte("x"), 0)
	}
	c.Purge()
	if s := c.Stats(); s.Entries != 0 || s.Bytes != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestOnEvictWithoutLock(_ *testing.T) {
	var c *Cache
	c, _ = newTest(Options{MaxEntries: 1, OnEvict: func(k string, _ Reason) {
		c.Get(k) // would deadlock if lock held
	}})
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
}

func TestShardSizing(t *testing.T) {
	tests := []struct {
		o    Options
		want int
	}{
		{Options{Shards: 3}, 4},
		{Options{Shards: 16}, 16},
		{Options{MaxEntries: 100}, 1},
		{Options{MaxEntries: 64 * 16}, 16},
	}
	for _, tt := range tests {
		c := New(tt.o)
		if len(c.shards) != tt.want {
			t.Errorf("%+v: shards %d want %d", tt.o, len(c.shards), tt.want)
		}
	}
	if n := len(New(Options{}).shards); n < 16 || n > 256 || n&(n-1) != 0 {
		t.Errorf("auto shards %d", n)
	}
}

func TestConcurrencyStress(t *testing.T) {
	c := New(Options{MaxEntries: 500, MaxBytes: 64 << 10, Shards: 8, OnEvict: func(string, Reason) {}})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				k := "k" + strconv.Itoa((i*g)%1000)
				switch i % 7 {
				case 0:
					c.Delete(k)
				case 1:
					c.Set(k, []byte(k), time.Millisecond)
				case 2:
					c.Sweep(4)
				case 3:
					if i%500 == 3 {
						c.DeletePrefix("k1")
					}
					c.Stats()
				default:
					if v, ok := c.Get(k); ok && string(v) != k {
						t.Errorf("wrong value %q for %q", v, k)
					}
					c.Set(k, []byte(k), 0)
				}
			}
		}(g)
	}
	wg.Wait()
	s := c.Stats()
	if s.Entries > 8*63 || s.Entries != c.Len() {
		t.Fatalf("stats %+v len %d", s, c.Len())
	}
}

func BenchmarkGetParallel(b *testing.B) {
	c := New(Options{MaxEntries: 100_000})
	keys := make([]string, 10_000)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
		c.Set(keys[i], []byte("value"), 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(keys[i%len(keys)])
			i++
		}
	})
}

func TestPurgeCallsOnEvictRemoved(t *testing.T) {
	var r evictRec
	c, _ := newTest(Options{Shards: 4, OnEvict: r.fn})
	for i := 0; i < 20; i++ {
		c.Set(strconv.Itoa(i), []byte("x"), 0)
	}
	before := c.Stats()
	c.Purge()
	if len(r.evs) != 20 {
		t.Fatalf("got %d callbacks, want 20: %v", len(r.evs), r.evs)
	}
	seen := map[string]bool{}
	for _, e := range r.evs {
		seen[e] = true
	}
	for i := 0; i < 20; i++ {
		if k := fmt.Sprintf("%d:%d", i, Removed); !seen[k] {
			t.Fatalf("missing %s", k)
		}
	}
	after := c.Stats()
	if after.Evictions != before.Evictions || after.Expirations != before.Expirations ||
		after.Hits != before.Hits || after.Misses != before.Misses {
		t.Fatalf("Purge changed counters: before %+v after %+v", before, after)
	}
}

func TestPurgeOnEvictWithoutLock(t *testing.T) {
	var c *Cache
	c, _ = newTest(Options{Shards: 2, OnEvict: func(k string, _ Reason) {
		c.Set("re-"+k, []byte("y"), 0) // deadlocks if a shard lock is held
	}})
	c.Set("a", []byte("x"), 0)
	c.Set("b", []byte("x"), 0)
	done := make(chan struct{})
	go func() { c.Purge(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Purge deadlocked calling OnEvict")
	}
}

func TestLiveLen(t *testing.T) {
	c, ck := newTest(Options{Shards: 4})
	c.Set("perm", []byte("x"), 0)
	c.Set("short", []byte("x"), time.Second)
	c.Set("long", []byte("x"), time.Hour)
	if n := c.LiveLen(); n != 3 {
		t.Fatalf("LiveLen=%d want 3", n)
	}
	ck.Advance(time.Second) // expiresAt reached: expired (now >= expiresAt)
	if n := c.LiveLen(); n != 2 {
		t.Fatalf("LiveLen=%d want 2", n)
	}
	if n := c.Len(); n != 3 {
		t.Fatalf("Len=%d want 3 (includes unswept expired)", n)
	}
	if s := c.Stats(); s.Expirations != 0 {
		t.Fatalf("LiveLen must not sweep: %+v", s)
	}
}

func TestSecondChanceKeepsHotKey(t *testing.T) {
	c := New(Options{MaxEntries: 4, Shards: 1})
	c.Set("hot", []byte("h"), 0)
	for i := range 100 {
		if _, ok := c.Get("hot"); !ok {
			t.Fatalf("hot key evicted after %d inserts", i)
		}
		c.Set(string(rune('a'+i%26))+string(rune('0'+i%10)), []byte("x"), 0)
	}
}

func TestNewEntryNotEvictedWhenAllVisited(t *testing.T) {
	c := New(Options{MaxEntries: 3, Shards: 1})
	for _, k := range []string{"a", "b", "c"} {
		c.Set(k, []byte(k), 0)
		c.Get(k)
	}
	c.Set("d", []byte("d"), 0)
	if _, ok := c.Get("d"); !ok {
		t.Fatal("just-written entry was evicted")
	}
	if c.Len() != 3 {
		t.Fatalf("Len = %d, want 3", c.Len())
	}
}
