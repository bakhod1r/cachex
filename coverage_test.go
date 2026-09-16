package cachex_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/internal/envelope"
	"github.com/bakhod1r/cachex/memstore"
)

// hookStore is a memstore whose Get, Set, Add and Delete can be intercepted.
type hookStore struct {
	*memstore.Store
	get func(ctx context.Context, key string) ([]byte, error, bool)
	set func(ctx context.Context, key string) (error, bool)
	add func(ctx context.Context, key string, val []byte, ttl time.Duration) (error, bool)
	del func(ctx context.Context, key string) (error, bool)
}

func (s *hookStore) Get(ctx context.Context, key string) ([]byte, error) {
	if s.get != nil {
		if v, err, ok := s.get(ctx, key); ok {
			return v, err
		}
	}
	return s.Store.Get(ctx, key)
}

func (s *hookStore) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if s.set != nil {
		if err, ok := s.set(ctx, key); ok {
			return err
		}
	}
	return s.Store.Set(ctx, key, val, ttl)
}

func (s *hookStore) Add(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if s.add != nil {
		if err, ok := s.add(ctx, key, val, ttl); ok {
			return err
		}
	}
	return s.Store.Add(ctx, key, val, ttl)
}

func (s *hookStore) Delete(ctx context.Context, key string) error {
	if s.del != nil {
		if err, ok := s.del(ctx, key); ok {
			return err
		}
	}
	return s.Store.Delete(ctx, key)
}

// casOnlyStore is a CASStore without GetsMulti or GetMulti.
type casOnlyStore struct{ *memstore.Store }

func (s casOnlyStore) GetMulti() {} // shadows memstore.GetMulti with a non-matching signature

func (s casOnlyStore) GetsMulti() {}

func newCache(t *testing.T, opts ...cachex.Option) *cachex.Cache {
	t.Helper()
	c, err := cachex.New(append([]cachex.Option{cachex.WithSweepInterval(0)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func constLoad(v string) cachex.Loader {
	return func(context.Context) ([]byte, error) { return []byte(v), nil }
}

func TestOpAndTierStrings(t *testing.T) {
	ops := map[cachex.Op]string{
		cachex.OpGet: "get", cachex.OpGetView: "get_view", cachex.OpSet: "set", cachex.OpDelete: "delete",
		cachex.OpGetMulti: "get_multi", cachex.OpSetMulti: "set_multi", cachex.OpGetOrLoad: "get_or_load",
		cachex.OpGetOrLoadMulti: "get_or_load_multi", cachex.Op(200): "unknown",
	}
	for op, want := range ops {
		if op.String() != want {
			t.Fatalf("%d: %q", op, op.String())
		}
	}
	tiers := map[cachex.Tier]string{
		cachex.TierL1: "l1", cachex.TierL2: "l2", cachex.TierStale: "stale", cachex.TierMiss: "miss",
		cachex.TierLoaded: "loaded", cachex.TierNegative: "negative", cachex.Tier(200): "unknown",
	}
	for tier, want := range tiers {
		if tier.String() != want {
			t.Fatalf("%d: %q", tier, tier.String())
		}
	}
}

func TestMergeEventsAllHooks(t *testing.T) {
	var n atomic.Int32
	e := cachex.Events{
		LoadEnd:       func(context.Context, string, time.Duration, error) { n.Add(1) },
		L2Error:       func(string, error) { n.Add(1) },
		BreakerChange: func(string, string) { n.Add(1) },
		LoaderPanic:   func(string, any) { n.Add(1) },
		PublishError:  func(cachex.Invalidation, error) { n.Add(1) },
	}
	m := cachex.MergeEvents(e, e)
	m.LoadEnd(ctx, "k", 0, nil)
	m.L2Error("get", errDown)
	m.BreakerChange("closed", "open")
	m.LoaderPanic("k", 1)
	m.PublishError(cachex.Invalidation{}, errDown)
	if n.Load() != 10 {
		t.Fatalf("calls %d", n.Load())
	}
	if m := cachex.MergeEvents(cachex.Events{}); m.L2Error != nil || m.BreakerChange != nil || m.LoaderPanic != nil || m.PublishError != nil {
		t.Fatal("unset hooks must stay nil")
	}
}

func TestOptionErrors(t *testing.T) {
	bad := []cachex.Option{
		cachex.WithL1MaxBytes(-1), cachex.WithShards(-1), cachex.WithDefaultTTL(0), cachex.WithBeta(-1),
		cachex.WithSweepInterval(-1), cachex.WithNegativeTTL(-1), cachex.WithDistributedLock(0, time.Second),
		cachex.WithInvalidator(nil), cachex.WithClock(nil),
	}
	for i, o := range bad {
		if _, err := cachex.New(o); err == nil {
			t.Fatalf("option %d accepted", i)
		}
	}
	c := newCache(t, cachex.WithL1MaxBytes(1<<20), cachex.WithShards(4), cachex.WithBeta(0.5))
	if err := c.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
}

type subscribeFail struct{ failInvalidator }

func (subscribeFail) Subscribe(context.Context, func(cachex.Invalidation)) error { return errDown }

func TestNewSubscribeError(t *testing.T) {
	if _, err := cachex.New(cachex.WithInvalidator(subscribeFail{})); !errors.Is(err, errDown) {
		t.Fatalf("want errDown, got %v", err)
	}
}

func TestHitRatio(t *testing.T) {
	if (cachex.Stats{}).HitRatio() != 0 {
		t.Fatal("empty ratio")
	}
	if r := (cachex.Stats{L1Hits: 1, L1Misses: 1, L2Hits: 1}).HitRatio(); r != 1 {
		t.Fatalf("ratio %v", r)
	}
}

func TestHookedOpsErrorsAndMulti(t *testing.T) {
	r := &recorder{}
	e := newEnv(t, cachex.WithEvents(r.events()))
	if err := e.c.Delete(ctx, ""); !errors.Is(err, cachex.ErrInvalidKey) {
		t.Fatal(err)
	}
	if err := e.c.SetMulti(ctx, map[string][]byte{"a": []byte("1")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if info := r.last(t); info.Op != cachex.OpSetMulti || info.Keys != 1 {
		t.Fatalf("%+v", info)
	}
	got, err := e.c.GetOrLoadMulti(ctx, []string{"a"}, time.Minute, func(context.Context, []string) (map[string][]byte, error) {
		return nil, nil
	})
	if err != nil || string(got["a"]) != "1" {
		t.Fatalf("%q %v", got, err)
	}
	if info := r.last(t); info.Op != cachex.OpGetOrLoadMulti || info.Tier != cachex.TierL1 {
		t.Fatalf("%+v", info)
	}
}

func TestArgumentErrors(t *testing.T) {
	e := newEnv(t)
	if _, err := e.c.GetOrLoad(ctx, "", time.Minute, constLoad("v")); !errors.Is(err, cachex.ErrInvalidKey) {
		t.Fatal(err)
	}
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, nil); err == nil {
		t.Fatal("nil loader accepted")
	}
	if _, err := e.c.GetOrLoadMulti(ctx, []string{"k"}, time.Minute, nil); err == nil {
		t.Fatal("nil multi loader accepted")
	}
	noop := func(context.Context, []string) (map[string][]byte, error) { return nil, nil }
	if _, err := e.c.GetOrLoadMulti(ctx, []string{""}, time.Minute, noop); !errors.Is(err, cachex.ErrInvalidKey) {
		t.Fatal(err)
	}
	_ = e.c.Close()
	if _, err := e.c.GetMulti(ctx, []string{"k"}); !errors.Is(err, cachex.ErrClosed) {
		t.Fatal(err)
	}
}

func TestDefaultTTLAndZeroRand(t *testing.T) {
	e := newEnv(t, cachex.WithDefaultTTL(time.Minute), cachex.WithRand(func() float64 { return 0 }))
	var loads atomic.Int32
	load := func(context.Context) ([]byte, error) {
		loads.Add(1)
		e.clock.Advance(time.Millisecond) // nonzero load time enables XFetch
		return []byte("v"), nil
	}
	if _, err := e.c.GetOrLoad(ctx, "k", 0, load); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(59500 * time.Millisecond)
	// rand 0 clamps to the smallest float: -ln(r) ~ 744, so 1ms of load time refreshes 744ms early.
	if _, err := e.c.GetOrLoad(ctx, "k", 0, load); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return loads.Load() == 2 })
}

func TestGetMultiExpiredWithinStaleWindow(t *testing.T) {
	e := newEnv(t, cachex.WithStaleWindow(time.Hour))
	other := newCache(t, cachex.WithClock(e.clock), cachex.WithL2(e.l2))
	_ = e.c.Set(ctx, "k", []byte("v"), time.Second)
	e.clock.Advance(2 * time.Second)
	if got, err := e.c.GetMulti(ctx, []string{"k"}); err != nil || len(got) != 0 {
		t.Fatalf("expired L1 entry served: %q %v", got, err)
	}
	if got, err := other.GetMulti(ctx, []string{"k"}); err != nil || len(got) != 0 {
		t.Fatalf("expired L2 entry served: %q %v", got, err)
	}
}

func TestGetMultiL2Tombstone(t *testing.T) {
	e := newEnv(t, cachex.WithNegativeTTL(time.Minute))
	other := newCache(t, cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithNegativeTTL(time.Minute))
	notFound := func(context.Context) ([]byte, error) { return nil, cachex.ErrNotFound }
	if _, err := e.c.GetOrLoad(ctx, "k", time.Minute, notFound); !errors.Is(err, cachex.ErrNotFound) {
		t.Fatal(err)
	}
	got, err := other.GetOrLoadMulti(ctx, []string{"k"}, time.Minute, func(context.Context, []string) (map[string][]byte, error) {
		t.Fatal("tombstone must not reload")
		return nil, nil
	})
	if err != nil || len(got) != 0 {
		t.Fatalf("%q %v", got, err)
	}
}

func TestGetMultiWithoutMultiGetter(t *testing.T) {
	e := newEnv(t, cachex.WithNegativeTTL(time.Minute))
	_ = e.c.Set(ctx, "hit", []byte("v"), time.Minute)
	_, _ = e.c.GetOrLoad(ctx, "gone", time.Minute, func(context.Context) ([]byte, error) { return nil, cachex.ErrNotFound })
	c := newCache(t, cachex.WithClock(e.clock), cachex.WithL2(plainStore{e.l2}), cachex.WithNegativeTTL(time.Minute))
	got, err := c.GetOrLoadMulti(ctx, []string{"hit", "gone", "miss"}, time.Minute,
		func(_ context.Context, keys []string) (map[string][]byte, error) {
			if len(keys) != 1 || keys[0] != "miss" {
				t.Fatalf("loader keys %v", keys)
			}
			return map[string][]byte{"miss": []byte("m")}, nil
		})
	if err != nil || string(got["hit"]) != "v" || string(got["miss"]) != "m" || len(got) != 2 {
		t.Fatalf("%q %v", got, err)
	}
	c2 := newCache(t, cachex.WithClock(e.clock), cachex.WithL2(plainStore{e.l2}))
	e.l2.FailNext(1, errDown)
	if got, err := c2.GetMulti(ctx, []string{"hit"}); err != nil || len(got) != 0 {
		t.Fatalf("failed L2 get: %q %v", got, err)
	}
}

func TestMultiLoadRacedBySet(t *testing.T) {
	e := newEnv(t)
	got, err := e.c.GetOrLoadMulti(ctx, []string{"k"}, time.Minute, func(context.Context, []string) (map[string][]byte, error) {
		_ = e.c.Set(ctx, "k", []byte("new"), time.Minute)
		return map[string][]byte{"k": []byte("old")}, nil
	})
	if err != nil || string(got["k"]) != "old" {
		t.Fatalf("%q %v", got, err)
	}
	if v, _ := e.c.Get(ctx, "k"); string(v) != "new" {
		t.Fatalf("raced load overwrote Set: %q", v)
	}
}

func TestMultiLoadObserveVariants(t *testing.T) {
	e := newEnv(t)
	load := func(_ context.Context, keys []string) (map[string][]byte, error) {
		out := map[string][]byte{}
		for _, k := range keys {
			out[k] = []byte("v")
		}
		return out, nil
	}
	plain := newCache(t, cachex.WithClock(e.clock), cachex.WithL2(plainStore{e.l2}))
	if got, err := plain.GetOrLoadMulti(ctx, []string{"a"}, time.Minute, load); err != nil || len(got) != 1 {
		t.Fatalf("%q %v", got, err)
	}
	casOnly := newCache(t, cachex.WithClock(e.clock), cachex.WithL2(casOnlyStore{e.l2}))
	if got, err := casOnly.GetOrLoadMulti(ctx, []string{"b"}, time.Minute, load); err != nil || len(got) != 1 {
		t.Fatalf("%q %v", got, err)
	}
	if raw, err := e.l2.Get(ctx, "b"); err != nil || len(raw) == 0 {
		t.Fatalf("CAS-only store not written: %v", err)
	}
	e.l2.FailNext(2, errDown) // get_multi and gets_multi
	if got, err := e.c.GetOrLoadMulti(ctx, []string{"c"}, time.Minute, load); err != nil || len(got) != 1 {
		t.Fatalf("%q %v", got, err)
	}
}

func TestLoadRacedDuringCommitWithoutCAS(t *testing.T) {
	clock := newClock()
	l2 := memstore.NewWithClock(clock.Now)
	var c *cachex.Cache
	var once sync.Once
	hs := &hookStore{Store: l2}
	hs.set = func(ctx context.Context, key string) (error, bool) {
		raced := false
		once.Do(func() { raced = true })
		if raced {
			_ = c.Set(ctx, key, []byte("new"), time.Minute)
		}
		return nil, false
	}
	var err error
	c, err = cachex.New(cachex.WithClock(clock), cachex.WithL2(plainStore{hs}), cachex.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if v, err := c.GetOrLoad(ctx, "k", time.Minute, constLoad("old")); err != nil || string(v) != "old" {
		t.Fatalf("%q %v", v, err)
	}
	if _, err := l2.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("raced load must be undone in L2: %v", err)
	}
	if st := c.Stats(); st.LoadsDiscarded != 1 {
		t.Fatalf("discarded %d", st.LoadsDiscarded)
	}
}

func TestDistributedLockWaiterSeesTombstone(t *testing.T) {
	e := newEnv(t, cachex.WithDistributedLock(time.Hour, time.Millisecond), cachex.WithNegativeTTL(time.Minute))
	_ = e.l2.Add(ctx, "cachex:lock:k", []byte{1}, time.Hour)
	done := make(chan error, 1)
	go func() {
		_, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
			return []byte("loaded"), nil
		})
		done <- err
	}()
	waitFor(t, func() bool { return e.c.Stats().LockWaits == 1 })
	now := e.clock.Now().UnixNano()
	raw := envelope.Encode(envelope.Entry{Flags: envelope.FlagTombstone, StoredAt: now, ExpireAt: now + int64(time.Minute)})
	_ = e.l2.Set(ctx, "k", raw, time.Minute)
	if err := <-done; !errors.Is(err, cachex.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestRefreshLimit(t *testing.T) {
	e := newEnv(t, cachex.WithStaleWindow(time.Hour))
	const n = 17 // one more than the default refresh limit
	keys := make([]string, n)
	for i := range keys {
		keys[i] = string(rune('a' + i))
		_ = e.c.Set(ctx, keys[i], []byte("old"), time.Second)
	}
	e.clock.Advance(2 * time.Second)
	release := make(chan struct{})
	var started atomic.Int32
	load := func(context.Context) ([]byte, error) {
		started.Add(1)
		<-release
		return []byte("new"), nil
	}
	for _, k := range keys {
		if v, err := e.c.GetOrLoad(ctx, k, time.Minute, load); err != nil || string(v) != "old" {
			t.Fatalf("%q %v", v, err)
		}
	}
	waitFor(t, func() bool { return started.Load() == n-1 })
	close(release)
	_ = e.c.Close()
	if started.Load() != n-1 {
		t.Fatalf("refreshes %d", started.Load())
	}
}

func TestExpiredL2EntryNotBackfilled(t *testing.T) {
	e := newEnv(t)
	now := e.clock.Now().UnixNano()
	raw := envelope.Encode(envelope.Entry{Value: []byte("v"), StoredAt: now - 2, ExpireAt: now - 1})
	_ = e.l2.Set(ctx, "k", raw, time.Hour)
	if _, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatal(err)
	}
	if st := e.c.Stats(); st.Entries != 0 {
		t.Fatalf("expired entry backfilled: %d", st.Entries)
	}
}

// shortCodec decompresses to less than an envelope header.
type shortCodec struct{ rle }

func (shortCodec) Decompress(dst, _ []byte) ([]byte, error) { return dst[:0], nil }

func TestDecompressedPayloadTooShort(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 0))
	_ = e.c.Set(ctx, "k", bytesOf('a', 100), time.Minute)
	other := newCache(t, cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithDecompressors(shortCodec{codec}))
	if _, err := other.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatal(err)
	}
	if other.Stats().DecodeErrors != 1 {
		t.Fatal("short payload must count as decode error")
	}
}

func bytesOf(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestNamespaceErrorPaths(t *testing.T) {
	e := newEnv(t)
	ns, _ := e.c.Namespace("n")
	if _, err := ns.Get(ctx, ""); !errors.Is(err, cachex.ErrMiss) {
		t.Fatal(err)
	}
	if err := ns.Delete(ctx, ""); !errors.Is(err, cachex.ErrInvalidKey) {
		t.Fatal(err)
	}
	if _, err := ns.GetOrLoad(ctx, "", time.Minute, nil); err == nil {
		t.Fatal("nil loader accepted")
	}
	if v, err := ns.GetOrLoad(ctx, "k", time.Minute, constLoad("v")); err != nil || string(v) != "v" {
		t.Fatalf("%q %v", v, err)
	}
	_ = ns.Set(ctx, "d", []byte("v"), time.Minute)
	if err := ns.Delete(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	if _, err := ns.Get(ctx, "d"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatal(err)
	}
	e.l2.SetDown(true)
	if err := ns.Invalidate(ctx); !errors.Is(err, cachex.ErrL2Unavailable) {
		t.Fatalf("invalidate with L2 down: %v", err)
	}
	e.l2.SetDown(false)
	_ = e.c.Close()
	if err := ns.Invalidate(ctx); !errors.Is(err, cachex.ErrClosed) {
		t.Fatal(err)
	}
}

func TestNamespaceInvalidateSeedsMissingVersion(t *testing.T) {
	e := newEnv(t)
	ns, _ := e.c.Namespace("n")
	if err := ns.Invalidate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := e.l2.Get(ctx, "cachex:ns:n:v"); err != nil {
		t.Fatalf("version not seeded: %v", err)
	}
}

func TestNamespaceStaleVersionWhileL2Down(t *testing.T) {
	e := newEnv(t, cachex.WithVersionTTL(time.Second))
	ns, _ := e.c.Namespace("n")
	_ = ns.Set(ctx, "k", []byte("v"), time.Hour)
	e.clock.Advance(2 * time.Second)
	e.l2.FailNext(1, errDown) // version fetch
	if v, err := ns.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("stale version must be used: %q %v", v, err)
	}
}

func TestNamespaceSeedNeverReusesVersion(t *testing.T) {
	e := newEnv(t, cachex.WithVersionTTL(time.Nanosecond))
	ns, _ := e.c.Namespace("n")
	_ = ns.Set(ctx, "k", []byte("v"), time.Hour) // version = now in ms
	_ = ns.Invalidate(ctx)                       // version = now in ms + 1
	_ = ns.Set(ctx, "k", []byte("v"), time.Hour)
	_ = e.l2.Delete(ctx, "cachex:ns:n:v")
	e.clock.Advance(time.Millisecond) // a fresh wall-clock seed would equal the cached version
	if _, err := ns.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("re-seeded version must not resurrect old keys: %v", err)
	}
}

func TestNamespaceSeedRaceAndFailures(t *testing.T) {
	clock := newClock()
	l2 := memstore.NewWithClock(clock.Now)
	hs := &hookStore{Store: l2}
	c := newCache(t, cachex.WithClock(clock), cachex.WithL2(hs))

	// Another process seeds between our miss and our add.
	hs.add = func(ctx context.Context, key string, val []byte, ttl time.Duration) (error, bool) {
		if key == "cachex:ns:race:v" {
			_ = l2.Add(ctx, key, []byte("42"), 0)
		}
		return nil, false
	}
	race, _ := c.Namespace("race")
	_ = race.Set(ctx, "k", []byte("v"), time.Minute)
	if v, err := c.Get(ctx, "race:42:k"); err != nil || string(v) != "v" {
		t.Fatalf("must adopt the other seed: %q %v", v, err)
	}

	// Seeding fails.
	hs.add = func(context.Context, string, []byte, time.Duration) (error, bool) { return errDown, true }
	fail, _ := c.Namespace("fail")
	if err := fail.Set(ctx, "k", []byte("v"), time.Minute); !errors.Is(err, errDown) {
		t.Fatalf("want errDown, got %v", err)
	}
	hs.add = nil

	// Corrupt counter that can't be deleted.
	_ = l2.Set(ctx, "cachex:ns:bad:v", []byte("garbage"), 0)
	hs.del = func(context.Context, string) (error, bool) { return errDown, true }
	bad, _ := c.Namespace("bad")
	if err := bad.Set(ctx, "k", []byte("v"), time.Minute); !errors.Is(err, errDown) {
		t.Fatalf("want errDown, got %v", err)
	}
}
