# cachex

Two-tier cache engine for Go: a sharded in-process LRU (**L1**) in front of a shared
memcached tier (**L2**), with stampede protection, namespace invalidation, stats and a
circuit breaker that degrades to L1-only when L2 is sick.

## Install

```sh
go get github.com/bakhod1r/cachex
```

Requires Go 1.25+.

## Quick start

L1-only:

```go
c, err := cachex.New()
if err != nil { return err }
defer c.Close()

_ = c.Set(ctx, "k", []byte("v"), time.Minute) // ttl <= 0 uses WithDefaultTTL
v, err := c.Get(ctx, "k")                     // errors.Is(err, cachex.ErrMiss) on miss
_ = c.Delete(ctx, "k")
```

With memcached:

```go
import "github.com/bakhod1r/cachex/memcached"

store, err := memcached.New(memcached.Config{
    Servers:   []string{"localhost:11211"},
    Timeout:   100 * time.Millisecond, // default
    KeyPrefix: "app:",
})
if err != nil { return err }

c, err := cachex.New(cachex.WithL2(store), cachex.WithL1TTL(5*time.Second))
if err != nil { return err }
defer c.Close() // also closes the store
```

Reads go L1 -> L2 (backfilling L1) -> miss. `Set` writes L2 then L1. `Delete` removes from
both and returns L2 errors.

## GetOrLoad and stampede protection

```go
v, err := c.GetOrLoad(ctx, "user:42", time.Minute, func(ctx context.Context) ([]byte, error) {
    return db.LoadUser(ctx, 42) // bounded by WithLoadTimeout
})
```

- **Single-flight**: concurrent misses for one key run the loader once per process
  (`Stats.LoadsShared` counts deduplicated callers).
- **XFetch early refresh**: near expiry, a hit may trigger a background refresh with
  probability driven by loader duration and `WithBeta` (0 disables). `Stats.EarlyRefreshes`.
- **Stale window**: with `WithStaleWindow(d)`, an entry expired less than `d` ago is served
  while it refreshes in the background. `Stats.StaleServed`.
- Background refreshes are capped at 16 concurrent; loader errors are returned, never cached.

## Namespaces

```go
users, err := c.Namespace("users") // non-empty, <=64 bytes, no ':'/space, not "cachex*"
_ = users.Set(ctx, "42", data, 0)
v, err := users.GetOrLoad(ctx, "42", time.Minute, load)

err = users.Invalidate(ctx) // drops every key in "users" on all nodes
```

Keys are stored as `<name>:<version>:<key>`; `Invalidate` atomically increments a version
counter in L2 (`cachex:ns:<name>:v`) and frees this process's L1 copies immediately. If the
version is unknown (L2 down, never fetched), `Get` misses, `Set` is skipped and `GetOrLoad`
calls the loader without caching.

## Stats

```go
s := c.Stats()
fmt.Println(s.L1Hits, s.L2Hits, s.Loads, s.Entries, s.Breaker, s.HitRatio())
```

Fields: `L1Hits, L1Misses, L2Hits, L2Misses, L2Errors, L2Skipped, Loads, LoadErrors,
LoadsShared, StaleServed, EarlyRefreshes, DecodeErrors, Evictions, Expirations, Entries,
Bytes, Breaker` (`"closed"`, `"open"`, `"half-open"`). Counters are read independently;
a snapshot is not atomic across fields.

## Degradation (circuit breaker)

All L2 calls go through a breaker (10s window, min 20 requests, opens at 50% failures,
5s cooldown doubling on failed probe). Misses and semantic errors are not failures.

- Breaker open: L2 calls are skipped (`Stats.L2Skipped`), `Get` falls back to L1/miss.
- `Set` never fails because of L2; on L2 write failure L1 keeps the value only for
  `WithDegradedL1TTL` and `Stats.L2Errors` grows.
- `Delete` and `Namespace.Invalidate` return L2 errors (`cachex.ErrL2Unavailable` when open),
  because a value left in L2 would be served to every node.

## Consistency

L2 is the shared source of truth; L1 is per process.

- After a `Delete`/`Set` on one node, other nodes may serve their L1 copy for at most
  **`WithL1TTL`** (default 10s).
- After `Namespace.Invalidate`, other nodes see the new version within
  **`WithVersionTTL`** (default 2s). Old L1 entries need not expire first: the version is
  part of the key, so they simply become unreachable.

## Options

| Option | Default | Meaning |
|---|---|---|
| `WithL2(Store)` | none (L1-only) | Shared tier, e.g. `memcached.New` |
| `WithL1MaxEntries(int)` | `100000` | L1 entry cap (0 = unlimited) |
| `WithL1MaxBytes(int64)` | `0` (unlimited) | L1 approximate byte cap |
| `WithShards(int)` | `0` (auto) | L1 shard count |
| `WithDefaultTTL(d)` | `5m` | TTL when `ttl <= 0` |
| `WithL1TTL(d)` | `10s` | Max L1 lifetime; cross-node staleness bound |
| `WithDegradedL1TTL(d)` | `1s` | L1 TTL after a failed L2 write |
| `WithStaleWindow(d)` | `0` (off) | Serve expired values while refreshing |
| `WithBeta(float64)` | `1.0` | XFetch strength (0 disables) |
| `WithLoadTimeout(d)` | `5s` | Per-loader timeout |
| `WithVersionTTL(d)` | `2s` | Namespace version cache; invalidation visibility bound |
| `WithSweepInterval(d)` | `1s` | Expired-entry janitor (0 = lazy expiry only) |
| `WithClock(Clock)` | wall clock | Injectable clock for tests |

## Example

```sh
go run ./example/
```

## Testing

```sh
go test -race ./...
```

Integration against a real memcached:

```sh
docker run -d --rm -p 11211:11211 memcached:1.6-alpine
MEMCACHED_ADDR=localhost:11211 go test -tags=integration ./memcached/
```
