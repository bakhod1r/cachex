package cachex_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

// recorder collects events; safe for the background goroutines the cache starts.
type recorder struct {
	mu      sync.Mutex
	ends    []cachex.OpInfo
	loads   []string
	l2Errs  []string
	changes []string
	panics  []any
	pubErrs int
}

func (r *recorder) events() cachex.Events {
	return cachex.Events{
		OpEnd: func(_ context.Context, info cachex.OpInfo, _ time.Duration) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.ends = append(r.ends, info)
		},
		LoadEnd: func(_ context.Context, key string, _ time.Duration, _ error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.loads = append(r.loads, key)
		},
		L2Error: func(op string, _ error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.l2Errs = append(r.l2Errs, op)
		},
		BreakerChange: func(from, to string) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.changes = append(r.changes, from+"->"+to)
		},
		LoaderPanic: func(key string, recovered any) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.panics = append(r.panics, recovered)
		},
		PublishError: func(cachex.Invalidation, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.pubErrs++
		},
	}
}

func (r *recorder) last(t *testing.T) cachex.OpInfo {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ends) == 0 {
		t.Fatal("no OpEnd")
	}
	return r.ends[len(r.ends)-1]
}

func TestEventsTier(t *testing.T) {
	r := &recorder{}
	e := newEnv(t, cachex.WithEvents(r.events()), cachex.WithStaleWindow(time.Minute), cachex.WithNegativeTTL(time.Minute))
	val := func(context.Context) ([]byte, error) { return []byte("v"), nil }
	check := func(op cachex.Op, tier cachex.Tier) {
		t.Helper()
		got := r.last(t)
		if got.Op != op || got.Tier != tier {
			t.Fatalf("got %v/%v want %v/%v", got.Op, got.Tier, op, tier)
		}
	}

	_, _ = e.c.Get(ctx, "a")
	check(cachex.OpGet, cachex.TierMiss)
	if got := r.last(t); got.Key != "a" || !errors.Is(got.Err, cachex.ErrMiss) {
		t.Fatalf("info %+v", got)
	}
	_ = e.c.Set(ctx, "a", []byte("v"), time.Minute)
	check(cachex.OpSet, cachex.TierMiss)
	_, _ = e.c.Get(ctx, "a")
	check(cachex.OpGet, cachex.TierL1)
	_ = e.c.GetView(ctx, "a", func([]byte) error { return nil })
	check(cachex.OpGetView, cachex.TierL1)

	// Another process sharing L2 answers from L2.
	r2 := &recorder{}
	c2, err := cachex.New(cachex.WithClock(e.clock), cachex.WithL2(e.l2), cachex.WithSweepInterval(0), cachex.WithEvents(r2.events()))
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	_, _ = c2.Get(ctx, "a")
	if got := r2.last(t); got.Tier != cachex.TierL2 {
		t.Fatalf("second process tier %v", got.Tier)
	}

	_, _ = e.c.GetOrLoad(ctx, "b", time.Second, val)
	check(cachex.OpGetOrLoad, cachex.TierLoaded)
	_, _ = e.c.GetOrLoad(ctx, "b", time.Second, val)
	check(cachex.OpGetOrLoad, cachex.TierL1)
	e.clock.Advance(2 * time.Second)
	_, _ = e.c.GetOrLoad(ctx, "b", time.Second, val)
	check(cachex.OpGetOrLoad, cachex.TierStale)

	nf := func(context.Context) ([]byte, error) { return nil, cachex.ErrNotFound }
	_, _ = e.c.GetOrLoad(ctx, "n", time.Minute, nf)
	check(cachex.OpGetOrLoad, cachex.TierLoaded)
	_, _ = e.c.GetOrLoad(ctx, "n", time.Minute, nf)
	check(cachex.OpGetOrLoad, cachex.TierNegative)

	_, _ = e.c.GetMulti(ctx, []string{"a", "zz"})
	if got := r.last(t); got.Op != cachex.OpGetMulti || got.Keys != 2 {
		t.Fatalf("multi info %+v", got)
	}
	_ = e.c.Delete(ctx, "a")
	check(cachex.OpDelete, cachex.TierMiss)

	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.loads) < 2 || r.loads[0] != "b" {
		t.Fatalf("LoadEnd keys %v", r.loads)
	}
}

// failStore fails every Store call.
type failStore struct{ cachex.Store }

var errDown = errors.New("down")

func (failStore) Get(context.Context, string) ([]byte, error) { return nil, errDown }
func (failStore) Close() error                                { return nil }
func (failStore) Set(context.Context, string, []byte, time.Duration) error {
	return errDown
}

func TestEventsL2ErrorAndBreaker(t *testing.T) {
	r := &recorder{}
	clock := newClock()
	c, err := cachex.New(cachex.WithClock(clock), cachex.WithSweepInterval(0), cachex.WithEvents(r.events()),
		cachex.WithL2(failStore{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < 20; i++ {
		_, _ = c.Get(ctx, "k")
	}
	clock.Advance(5 * time.Second)
	_, _ = c.Get(ctx, "k") // half-open probe, fails again
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.l2Errs) != 21 || r.l2Errs[0] != "get" {
		t.Fatalf("L2Error ops %v", r.l2Errs)
	}
	want := []string{"closed->open", "open->half-open", "half-open->open"}
	if len(r.changes) != len(want) {
		t.Fatalf("changes %v", r.changes)
	}
	for i := range want {
		if r.changes[i] != want[i] {
			t.Fatalf("changes %v", r.changes)
		}
	}
}

func TestEventsLoaderPanic(t *testing.T) {
	r := &recorder{}
	e := newEnv(t, cachex.WithEvents(r.events()))
	_, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { panic("boom") })
	if !errors.Is(err, cachex.ErrLoaderPanic) {
		t.Fatalf("err %v", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.panics) != 1 || r.panics[0] != "boom" {
		t.Fatalf("panics %v", r.panics)
	}
}

type failInvalidator struct{}

func (failInvalidator) Publish(context.Context, cachex.Invalidation) error { return errDown }
func (failInvalidator) Subscribe(context.Context, func(cachex.Invalidation)) error {
	return nil
}

func TestEventsPublishError(t *testing.T) {
	r := &recorder{}
	e := newEnv(t, cachex.WithEvents(r.events()), cachex.WithInvalidator(failInvalidator{}))
	_ = e.c.Delete(ctx, "k")
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pubErrs != 1 {
		t.Fatalf("publish errors %d", r.pubErrs)
	}
}

type ctxKey struct{}

// ctxStore records whether the OpStart context value reached the store.
type ctxStore struct {
	cachex.Store
	seen chan any
}

func (s ctxStore) Get(ctx context.Context, key string) ([]byte, error) {
	select {
	case s.seen <- ctx.Value(ctxKey{}):
	default:
	}
	return s.Store.Get(ctx, key)
}

func TestEventsOpStartContextFlows(t *testing.T) {
	clock := newClock()
	e := newEnv(t)
	st := ctxStore{Store: e.l2, seen: make(chan any, 1)}
	c, err := cachex.New(cachex.WithClock(clock), cachex.WithSweepInterval(0), cachex.WithL2(st),
		cachex.WithEvents(cachex.Events{
			OpStart: func(ctx context.Context, _ cachex.Op, _ string) context.Context {
				return context.WithValue(ctx, ctxKey{}, "span")
			},
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var inLoader any
	_, err = c.GetOrLoad(ctx, "k", time.Minute, func(ctx context.Context) ([]byte, error) {
		inLoader = ctx.Value(ctxKey{})
		return []byte("v"), nil
	})
	if err != nil || inLoader != "span" {
		t.Fatalf("loader ctx value %v, err %v", inLoader, err)
	}
	if v := <-st.seen; v != "span" {
		t.Fatalf("store ctx value %v", v)
	}
}

func TestMergeEvents(t *testing.T) {
	var calls []string
	mk := func(name string) cachex.Events {
		return cachex.Events{
			OpStart: func(ctx context.Context, _ cachex.Op, _ string) context.Context {
				calls = append(calls, name+":start")
				return context.WithValue(ctx, ctxKey{}, name)
			},
			OpEnd: func(ctx context.Context, _ cachex.OpInfo, _ time.Duration) {
				calls = append(calls, name+":end:"+ctx.Value(ctxKey{}).(string))
			},
			L2Error: func(string, error) { calls = append(calls, name+":l2") },
		}
	}
	m := cachex.MergeEvents(mk("a"), cachex.Events{}, mk("b"))
	c := m.OpStart(context.Background(), cachex.OpGet, "k")
	m.OpEnd(c, cachex.OpInfo{}, 0)
	m.L2Error("get", errDown)
	if m.LoadEnd != nil || m.LoaderPanic != nil {
		t.Fatal("merged unset hooks should stay nil")
	}
	want := []string{"a:start", "b:start", "a:end:b", "b:end:b", "a:l2", "b:l2"}
	if len(calls) != len(want) {
		t.Fatalf("calls %v", calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls %v", calls)
		}
	}
	if cachex.OpGetOrLoadMulti.String() != "get_or_load_multi" || cachex.TierNegative.String() != "negative" {
		t.Fatalf("names %s %s", cachex.OpGetOrLoadMulti, cachex.TierNegative)
	}
}
