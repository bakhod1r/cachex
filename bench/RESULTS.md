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

Run of 2026-09-15 (after L1 optimizations below), `-benchtime=500ms -count=10`, go1.26.0.
Laptop not isolated; compare ranks within a row, not decimals across runs.

| Benchmark | cachex | hashicorp-lru | ristretto | otter | theine | bigcache | freecache | go-cache |
|---|---|---|---|---|---|---|---|---|
| ParallelGetHit, ns/op | 66.3 ± 6% | 308.2 ± 1% | 36.1 ± 31% | **25.1** ± 11% | 36.6 ± 6% | 67.3 ± 3% | 106.4 ± 3% | 117.5 ± 1% |
| ParallelGetHit, cachex `GetView` (zero-copy), ns/op | 38.1 ± 20% | | | | | | | |
| ParallelGetHit allocs/op | 1 (0 with `GetView`) | 0 | 0 | 0 | 0 | 2 | 1 | 0 |
| ParallelMixedZipf 90/10, ns/op | 119.1 ± 5% | 418.0 ± 2% | 151.7 ± 3% | **78.7** ± 10% | 205.2 ± 52% | 154.3 ± 20% | 166.5 ± 3% | 277.6 ± 2% |
| Hit ratio (1M-op Zipf, cap 10k, keyspace 100k) | 77.69% | 74.74% | 76.61% | 78.66% | **79.53%** | 71.67% | 74.04% | n/a (unbounded) |
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

Where cachex stands in-process: 2nd on MixedZipf throughput (after otter, ahead of ristretto,
bigcache, freecache, theine), 3rd on hit ratio, tied 1st on stampede. On pure hits otter is still
faster: cachex's `Get` copies the value (1 alloc); with `GetView` the gap is ~42 vs ~25 ns, most
of it sketch counter writes on every read (otter batches reads through a lossy buffer instead).
Tried and dropped: sampling the sketch's aging counter (no measurable change).

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
