package cachexprom

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bakhod1r/cachex"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeSource struct {
	s     cachex.Stats
	calls atomic.Int64
}

func (f *fakeSource) Stats() cachex.Stats { f.calls.Add(1); return f.s }

func sample() cachex.Stats {
	return cachex.Stats{
		L1Hits: 1, L1Misses: 2, L2Hits: 3, L2Misses: 4, L2Errors: 5, L2Skipped: 6,
		Loads: 7, LoadErrors: 8, LoadsShared: 9, StaleServed: 10, EarlyRefreshes: 11,
		StaleOnError: 20, DecodeErrors: 12, NegativeHits: 17, LockWaits: 18, PublishErrors: 19, Evictions: 13, Expirations: 14, Entries: 15, Bytes: 16, Breaker: "half-open",
	}
}

func TestCollectAll(t *testing.T) {
	src := &fakeSource{s: sample()}
	c := NewCollector(src)
	want := `
# HELP cachex_breaker_state L2 circuit breaker state; 1 for the current state, 0 otherwise.
# TYPE cachex_breaker_state gauge
cachex_breaker_state{state="closed"} 0
cachex_breaker_state{state="half-open"} 1
cachex_breaker_state{state="open"} 0
# HELP cachex_bytes Bytes currently held in L1.
# TYPE cachex_bytes gauge
cachex_bytes 16
# HELP cachex_decode_errors_total Values that failed to decode.
# TYPE cachex_decode_errors_total counter
cachex_decode_errors_total 12
# HELP cachex_early_refreshes_total Early (probabilistic) refreshes triggered.
# TYPE cachex_early_refreshes_total counter
cachex_early_refreshes_total 11
# HELP cachex_entries Entries currently in L1.
# TYPE cachex_entries gauge
cachex_entries 15
# HELP cachex_evictions_total L1 evictions.
# TYPE cachex_evictions_total counter
cachex_evictions_total 13
# HELP cachex_expirations_total L1 expirations.
# TYPE cachex_expirations_total counter
cachex_expirations_total 14
# HELP cachex_hits_total Cache hits by tier.
# TYPE cachex_hits_total counter
cachex_hits_total{tier="l1"} 1
cachex_hits_total{tier="l2"} 3
# HELP cachex_l2_errors_total L2 transport failures.
# TYPE cachex_l2_errors_total counter
cachex_l2_errors_total 5
# HELP cachex_l2_skipped_total L2 calls short-circuited by the open breaker.
# TYPE cachex_l2_skipped_total counter
cachex_l2_skipped_total 6
# HELP cachex_load_errors_total Loader invocations that returned an error.
# TYPE cachex_load_errors_total counter
cachex_load_errors_total 8
# HELP cachex_loads_shared_total Callers deduplicated by single-flight.
# TYPE cachex_loads_shared_total counter
cachex_loads_shared_total 9
# HELP cachex_loads_total Loader invocations.
# TYPE cachex_loads_total counter
cachex_loads_total 7
# HELP cachex_lock_waits_total Loads that waited on another process's distributed lock.
# TYPE cachex_lock_waits_total counter
cachex_lock_waits_total 18
# HELP cachex_misses_total Cache misses by tier.
# TYPE cachex_misses_total counter
cachex_misses_total{tier="l1"} 2
cachex_misses_total{tier="l2"} 4
# HELP cachex_negative_hits_total Lookups answered by a cached not-found.
# TYPE cachex_negative_hits_total counter
cachex_negative_hits_total 17
# HELP cachex_publish_errors_total Failed invalidation broadcasts.
# TYPE cachex_publish_errors_total counter
cachex_publish_errors_total 19
# HELP cachex_stale_served_total Stale values served.
# TYPE cachex_stale_served_total counter
cachex_stale_served_total 10
# HELP cachex_stale_on_error_total Stale values served because the loader failed.
# TYPE cachex_stale_on_error_total counter
cachex_stale_on_error_total 20
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

func TestStatsReadOncePerScrape(t *testing.T) {
	src := &fakeSource{s: sample()}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(NewCollector(src))
	if _, err := reg.Gather(); err != nil {
		t.Fatal(err)
	}
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("Stats() called %d times, want 1", n)
	}
}

func TestNamespaceAndConstLabels(t *testing.T) {
	src := &fakeSource{s: cachex.Stats{Breaker: "open", L1Hits: 42}}
	c := NewCollector(src, WithNamespace("app"), WithConstLabels(prometheus.Labels{"cache": "users"}))
	want := `
# HELP app_breaker_state L2 circuit breaker state; 1 for the current state, 0 otherwise.
# TYPE app_breaker_state gauge
app_breaker_state{cache="users",state="closed"} 0
app_breaker_state{cache="users",state="half-open"} 0
app_breaker_state{cache="users",state="open"} 1
# HELP app_hits_total Cache hits by tier.
# TYPE app_hits_total counter
app_hits_total{cache="users",tier="l1"} 42
app_hits_total{cache="users",tier="l2"} 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "app_breaker_state", "app_hits_total"); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownBreakerStateAllZero(t *testing.T) {
	c := NewCollector(&fakeSource{s: cachex.Stats{Breaker: "unknown"}})
	want := `
# HELP cachex_breaker_state L2 circuit breaker state; 1 for the current state, 0 otherwise.
# TYPE cachex_breaker_state gauge
cachex_breaker_state{state="closed"} 0
cachex_breaker_state{state="half-open"} 0
cachex_breaker_state{state="open"} 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want), "cachex_breaker_state"); err != nil {
		t.Fatal(err)
	}
}

func TestMetricCount(t *testing.T) {
	if n := testutil.CollectAndCount(NewCollector(&fakeSource{})); n != 23 {
		t.Fatalf("got %d series, want 23", n)
	}
}

func TestLint(t *testing.T) {
	problems, err := testutil.CollectAndLint(NewCollector(&fakeSource{s: sample()}))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("lint: %s: %s", p.Metric, p.Text)
	}
}

func TestConcurrentScrapes(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewCollector(&fakeSource{s: sample()}))
	done := make(chan struct{})
	for range 8 {
		go func() {
			defer func() { done <- struct{}{} }()
			for range 50 {
				if _, err := reg.Gather(); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for range 8 {
		<-done
	}
}
