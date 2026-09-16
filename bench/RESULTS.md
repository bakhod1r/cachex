# cachex L1 vs in-process Go caches

`-count=10`, summarized with `benchstat` (median ± 95% confidence interval).

## Machine

- Apple M1, 8 cores, GOMAXPROCS 8, darwin/arm64, go1.25.3
- Laptop on a desktop workload (not isolated, no CPU pinning): high `±` values, notably
  ristretto's, reflect background noise as well as the library.

## Libraries

| Library | Version |
|---|---|
| cachex (this repo, via `replace ../`) | working tree |
| github.com/hashicorp/golang-lru/v2 | v2.0.7 |
| github.com/dgraph-io/ristretto/v2 | v2.4.2 |
| github.com/maypok86/otter/v2 | v2.3.0 |
| github.com/Yiling-J/theine-go | v0.6.2 |
| github.com/allegro/bigcache/v3 | v3.2.0 |
| github.com/coocood/freecache | v1.2.7 |
| github.com/patrickmn/go-cache | v2.1.0 |

## Commands

```
cd bench
go test -run='^$' -bench=. -benchtime=500ms -count=10 . > bench.txt
benchstat bench.txt
```

## Results

Run of 2026-09-16, go1.26.0. Hit ratio and stampede come from a `-count=8` run of the whole
suite; the ns/op columns from a `-count=8` speed-only rerun on a quieter laptop (the first
run's timings had `±` up to 78% and are not reported). Hit ratio replays a fixed trace, so it
is unaffected by machine load.

| Benchmark | cachex | hashicorp-lru | ristretto | otter | theine | bigcache | freecache | go-cache |
|---|---|---|---|---|---|---|---|---|
| ParallelGetHit, ns/op | 51.9 ± 3% | 288.4 ± 5% | 37.2 ± 18% | **22.3** ± 8% | 37.1 ± 17% | 61.0 ± 2% | 105.2 ± 3% | 119.2 ± 2% |
| ParallelGetHit, cachex `GetView` (zero-copy), ns/op | 35.6 ± 7% | | | | | | | |
| ParallelGetHit allocs/op | 1 (0 with `GetView`) | 0 | 0 | 0 | 0 | 2 | 1 | 0 |
| ParallelMixedZipf 90/10, ns/op | 123.6 ± 4% | 393.4 ± 4% | 145.8 ± 8% | **66.5** ± 6% | 177.7 ± 78% | 197.7 ± 25% | 193.6 ± 19% | 429.9 ± 43% |
| Hit ratio (1M-op Zipf, cap 10k, keyspace 100k) | **79.54%** | 74.74% | 76.78% | 78.67% | 79.51% | 71.67% | 74.04% | n/a (unbounded) |
| Stampede: loader calls per cold key (64 callers, 1 ms loader) | **1** | 64 | 64 | **1** | **1** | 64 | 64 | 64 |

L1 changes in this run, each measured on its own before moving on:

| Change | ParallelGetHit | GetHitView | MixedZipf | Hit ratio |
|---|---|---|---|---|
| Before (same laptop, same session) | 108.5 | 83.6 | 323.1 | 75.53% |
| `WithL1Frequency` default on (TinyLFU sketch) | | | | 77.7% |
| Intrusive list (1 alloc per entry, not 2); `lru.SetOwned` skips the second copy of the freshly encoded envelope | ~110 | | ~210 | |
| L1 hit/miss counted once (lru counters) instead of twice | ~82 | ~60 | ~180 | |
| Coarse clock: atomic updated each 1 ms by a ticker that stops when idle, instead of `time.Now` per call | 71.1 | 46.0 | 178.1 | 77.68% |
| Lock-free `Get`: shard index is an open-addressing table with atomic slots, value+expiry behind one atomic pointer; writes keep the shard mutex | 66.4 | 41.9 | 112.7 | 77.68% |
| Buffered reads (lossy per-shard ring, drained into the sketch on write or when full) plus W-TinyLFU eviction: window 1%, protected 90% of the main space, admission by sketch comparison. Hit ratio 77.7% to 79.53%, throughput unchanged (p>0.1) | ~68 | ~41 | ~157 | **79.53%** |
| Hash tags beside the table slots (probe compares tags, dereferences an entry only on a hash match) plus `runtime.cheaprand` and 128-byte padding in the striped counters. Measured A/B, 12 interleaved runs each: GetHit -11.1% (p=0.005), GetView -5.5% (p=0.017), MixedZipf unchanged (p=0.51) | 57.1 | 35.0 | 116.3 | 77.69% |

Where cachex stands in-process: 1st on hit ratio, tied 1st on stampede, 2nd on MixedZipf
throughput, 4th on pure hits (`GetView`; 5th through `Get`, which copies the value so callers
cannot corrupt the cache). otter stays ahead on raw reads: it returns shared slices and
batches its policy work more aggressively.

Tried and dropped, each for want of a significant result: sampling the sketch's aging
counter; parking the decoded envelope beside the bytes in L1 to skip `envelope.Decode` on a
hit (GetView -8% but p=0.13 over 26 paired runs).

- bigcache and freecache are byte-bounded; sized at ~128 B per entry to hold about the same count.
- `BenchmarkStampede` uses the library's loading API where one exists (cachex `GetOrLoad`, otter
  `Get` with loader, theine `LoadingCache`); others use plain cache-aside (Get, load, Set).
- Not measured here, and unique to cachex in this list:
  L2 tier (memcached/Redis), cross-node invalidation, distributed lock, stale serving, negative cache.

Previous run (4 libraries, `-count=10`, go1.25.3):

| Benchmark | cachex | hashicorp-lru | ristretto | otter |
|---|---|---|---|---|
| ParallelGetHit, ns/op | 61.8 ± 2% | 171.5 ± 1% | 19.0 ± 28% | 12.2 ± 9% |
| ParallelMixedZipf 90/10, ns/op | 114.4 ± 3% | 240.9 ± 1% | 167.9 ± 49% | 87.0 ± 3% |
| Hit ratio (1M-op Zipf, cap 10k, keyspace 100k) | 75.53% | 74.74% | 76.86% | 78.65% |
| ParallelGetHit allocs/op | 1 (64 B) | 0 | 0 | 0 |

## Reading the numbers

- cachex's `Get` copies the value before returning it (one allocation) so callers can never
  corrupt the cache; the others return shared references. That copy and the envelope decode
  are the floor for cachex's hit path.
- L1 uses CLOCK (second chance) instead of strict LRU: `Get` takes a read lock and sets an
  atomic bit instead of moving the entry under a write lock. This took the root
  `BenchmarkGetL1Hit` from ~234 to ~116 ns/op and raised hit ratio slightly above strict LRU.
- ristretto and otter use frequency-aware admission (TinyLFU), worth 1-3 points of hit ratio
  on this trace, and lock-free read buffers. cachex keeps exact write-then-read semantics
  (ristretto may drop a `Set`).
- Averages only: Go benchmarks don't report latency percentiles.

## History

| Change | ParallelGetHit | Hit ratio |
|---|---|---|
| Mutex + strict LRU move-to-front (first version, `-count=1`) | 213 ns/op | 74.74% |
| RWMutex + CLOCK, striped counters, one clock read per `Get` | 61.8 ns/op | 75.53% |
