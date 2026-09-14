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

## Commands

```
cd bench
go test -run='^$' -bench=. -benchtime=500ms -count=10 . > bench.txt
benchstat bench.txt
```

## Results

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
