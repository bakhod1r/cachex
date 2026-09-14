# cachex L1 vs in-process Go caches

Single run, `-count=1`. Treat differences under ~10-15% as noise until repeated
with `-count=10` and compared with `benchstat`.

## Machine

- `go env GOOS GOARCH`: darwin arm64
- `sysctl -n machdep.cpu.brand_string`: Apple M1 (`hw.ncpu` = 8, GOMAXPROCS 8)
- go1.25.3 darwin/arm64, laptop on desktop workload (not isolated, no CPU pinning)

## Libraries

| Library | Version |
|---|---|
| cachex (this repo, via `replace ../`) | working tree |
| github.com/hashicorp/golang-lru/v2 | v2.0.7 |
| github.com/dgraph-io/ristretto/v2 | v2.4.2 |
| github.com/maypok86/otter/v2 | v2.3.0 |

All downloads succeeded; no library skipped.

## Commands

```
cd bench
go test -run='^$' -bench=. -benchtime=1s -count=1
go test -run='^$' -bench=HitRatio -benchtime=1x -count=1
```

## Throughput (8 goroutines, `b.RunParallel`)

| Benchmark | cachex | hashicorp-lru | ristretto | otter |
|---|---|---|---|---|
| ParallelGetHit ns/op | 213.1 | 201.6 | 22.20 | 12.33 |
| ParallelGetHit B/op, allocs/op | 64, 1 | 0, 0 | 0, 0 | 0, 0 |
| ParallelMixedZipf 90/10 ns/op | 161.7 | 275.3 | 105.9 | 52.75 |
| ParallelMixedZipf B/op | 67 | 2 | 17 | 7 |

- GetHit: 10k keys prefilled, capacity 20k, uniform random reads.
- MixedZipf: keyspace 100k, capacity 10k, Zipf s=1.01 v=1, per-goroutine PCG rand.

## Hit ratio (Zipf trace, 1M ops, keyspace 100k, capacity 10k, single goroutine, Get then Set-on-miss)

| Library | hit% (`-benchtime=1x`) | hit% (`-benchtime=1s`, last replay) | ns per 1M-op trace (1x) |
|---|---|---|---|
| cachex | 74.74 | 74.74 | 314,166,416 |
| hashicorp-lru | 74.74 | 74.74 | 121,702,750 |
| ristretto | 76.67 | 76.85 | 106,270,375 |
| otter | 78.79 | 78.81 | 88,672,416 |

## Reading the numbers

- cachex hit ratio matches strict LRU (hashicorp) because L1 is a sharded LRU;
  ristretto (TinyLFU admission) and otter (S3-FIFO) gain +2 to +4 points on Zipf.
- ParallelGetHit: cachex and hashicorp both take a mutex and move-to-front on
  every read, which is the likely cost; ristretto/otter use lock-free or buffered
  reads. Not profiled: no CPU/mutex profile was taken, so this is inference.
- cachex Get allocates 64 B/op: the public `Get` returns a copy of the value
  (`clone` in cache.go) and decodes an envelope. The other libraries return the
  stored slice without copying. This is an API contract difference, not a pure
  data-structure difference.
- cachex beats hashicorp on MixedZipf (sharded locks vs one global lock) but
  its Set path encodes an envelope and copies the value (the 67 B/op).

## Caveats

- One run each; no variance measured. ristretto hit% differed between the two
  runs (76.67 vs 76.85) because its Set is asynchronous and buffers may drop.
- ristretto is not `Wait()`ed during the trace or the mixed benchmark (realistic
  usage, but it can under-report its hit ratio); it is `Wait()`ed after prefill in GetHit.
- cachex is configured with `WithL1TTL(time.Hour)` and `WithDefaultTTL(time.Hour)`
  in addition to `WithSweepInterval(0)` and `WithL1MaxEntries(N)`; otherwise the
  default 10 s L1 hold would expire entries during longer runs.
- cachex shards round capacity up (ceil per shard), so effective capacity can be
  slightly above N; other libraries may also be approximate (ristretto cost-based).
- Values are 64-byte shared slices; string keys pre-built. Latency percentiles
  are not measured here (Go benchmarks report means only) - these are
  throughput numbers, not latency SLO evidence.
- Laptop with other processes; thermal and scheduler noise uncontrolled.
