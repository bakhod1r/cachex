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

**Timeouts:** the memcached client has no context support. A context is checked only before an
operation starts; once it is on the wire it is bounded by `Config.Timeout`, not by your deadline.
Keep `Timeout` well below your request budget, and set `MaxConcurrency` so a slow memcached fails
fast with `ErrL2Unavailable` instead of piling up goroutines.

Reads go L1 -> L2 (backfilling L1) -> miss. `Set` writes L2 then L1. `Delete` removes from
both and returns L2 errors. With a `CASStore` (memcached, memstore) `Delete` writes a short-lived
marker to L2 instead of removing the key; reads treat it as absent.

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
- **Distributed lock**: with `WithDistributedLock(ttl, poll)`, a loader runs once per key
  across *all* processes (memcached `add` lock). Waiters poll until the value appears or
  `ttl` passes, then load themselves; any L2 problem fails open. `Stats.LockWaits`.
- **Negative caching**: return `cachex.ErrNotFound` from the loader and set
  `WithNegativeTTL(d)`; repeated lookups of absent records return `ErrNotFound` without
  loading. `Stats.NegativeHits`.
- Background refreshes are capped at 16 concurrent; other loader errors are returned, never cached.

## Zero-copy reads

```go
err := c.GetView(ctx, "user:42", func(v []byte) error {
    return json.Unmarshal(v, &u) // v is valid only inside fn: don't modify or keep it
})
```

`Get` returns a private copy (one allocation); `GetView` hands the cached bytes to `fn` and
doesn't allocate on an L1 hit. A concurrent `Set` of the same key never changes a slice that
`fn` is already reading.

## GetMulti

```go
vals, err := c.GetMulti(ctx, []string{"a", "b", "c"}) // map holds only found keys
```

L1 answers first; remaining keys go to L2 in one round trip when the store implements
`cachex.MultiGetter` (memcached does). Keys starting with `cachex:` are reserved.

```go
_ = c.SetMulti(ctx, map[string][]byte{"a": a, "b": b}, time.Minute) // invalid key writes nothing

vals, err := c.GetOrLoadMulti(ctx, ids, time.Minute, func(ctx context.Context, missing []string) (map[string][]byte, error) {
    return db.LoadUsers(ctx, missing) // one query for all misses; absent keys = not found
})
```

`GetOrLoadMulti` has no single-flight across concurrent batches, stale window or early refresh.
With `WithNegativeTTL`, keys the loader omits are cached as absent.

## Loader panics and TTL jitter

- A panicking loader returns `cachex.ErrLoaderPanic` (counted in `Stats.LoadErrors`) instead of
  crashing the process; nothing is cached.
- `WithTTLJitter(f)` shortens each stored TTL by a random fraction in `[0, f)` so keys written
  together don't expire together.

## Typed values

```go
import "github.com/bakhod1r/cachex/typed"

users := typed.New(c, typed.JSON[User]())
u, err := users.GetOrLoad(ctx, "42", time.Minute, func(ctx context.Context) (User, error) {
    return db.LoadUser(ctx, 42)
})
```

Codecs: `typed.JSON[V]()`, `typed.String()`, `typed.Bytes()`, or your own `typed.Codec[V]`.
Backends: `*cachex.Cache` or `*cachex.Namespace`. An undecodable cached value is deleted and
reloaded once by `GetOrLoad`; `Get` returns an error wrapping `typed.ErrDecode`.

## Namespaces

```go
users, err := c.Namespace("users") // non-empty, <=64 bytes, no ':'/space, not "cachex*"
_ = users.Set(ctx, "42", data, 0)
v, err := users.GetOrLoad(ctx, "42", time.Minute, load)

err = users.Invalidate(ctx) // drops every key in "users" on all nodes
```

Keys are stored as `<name>:<version>:<key>`; `Invalidate` atomically increments a version
counter in L2 (`cachex:ns:<name>:v`) and frees this process's L1 copies immediately. If the
version is unknown (L2 down, never fetched), `Get` misses, `Set` returns the error and
`GetOrLoad` calls the loader without caching.

## Stats

```go
s := c.Stats()
fmt.Println(s.L1Hits, s.L2Hits, s.Loads, s.Entries, s.Breaker, s.HitRatio())
```

Fields: `L1Hits, L1Misses, L2Hits, L2Misses, L2Errors, L2Skipped, Loads, LoadErrors,
LoadsShared, StaleServed, EarlyRefreshes, DecodeErrors, NegativeHits, LockWaits,
PublishErrors, Evictions, Expirations, Entries, Bytes, Breaker` (`"closed"`, `"open"`, `"half-open"`). Counters are read independently;
a snapshot is not atomic across fields. `HitRatio()` is `(L1Hits+L2Hits) / (L1Hits+L1Misses)`:
every lookup touches L1 once, so L1 hits plus misses is the total lookup count.

### Prometheus

A separate module keeps the core free of Prometheus dependencies:

```go
import cachexprom "github.com/bakhod1r/cachex/prometheus"

prometheus.MustRegister(cachexprom.NewCollector(c, cachexprom.WithNamespace("myapp_cache")))
```

Metrics are read from `Stats()` once per scrape (`*_hits_total{tier}`, `*_misses_total{tier}`,
`*_l2_errors_total`, `*_loads_total`, `*_entries`, `*_breaker_state{state}`, ...).

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
- With `WithInvalidator(inv)` both bounds become "as fast as your message bus": `Delete` and
  `Invalidate` are broadcast and every subscribed process drops its L1 copies immediately.
  Implement `cachex.Invalidator` over Redis pub/sub, NATS, etc. (`memstore.NewBus()` is an
  in-process implementation). Delivery is best effort; the TTL bounds still hold.
- **Loads never undo a `Delete`/`Set`.** A `GetOrLoad` whose loader read the source before a
  `Delete` or `Set` does not write its value back. Within one process this always holds. Across
  processes it needs a store implementing `cachex.CASStore` (memcached and memstore do): the load
  publishes with `add`/`cas` against the L2 state it saw before calling the loader, and `Delete`
  leaves a marker for `WithDeleteMarkerTTL` (default 2 x load timeout). A loader that ignores its
  context and outlives the marker TTL is not covered. `GetOrLoadMulti` has the per-process guard only.
  Without `CASStore`, a load on node A may re-cache a value that node B deleted, until its TTL.

Redis pub/sub adapter (separate module):

```go
import cachexredis "github.com/bakhod1r/cachex/redis"

inv, err := cachexredis.New(cachexredis.Config{Client: redis.NewClient(&redis.Options{Addr: "localhost:6379"})})
c, err := cachex.New(cachex.WithL2(store), cachex.WithInvalidator(inv))
```

Other brokers, each a separate module with the same wire format and origin filtering:

| Module | Transport | Delivery | Notes |
|---|---|---|---|
| `github.com/bakhod1r/cachex/redis` | Redis pub/sub | at-most-once | go-redis reconnects; messages during a disconnect are lost |
| `github.com/bakhod1r/cachex/nats` | core NATS | at-most-once | not JetStream |
| `github.com/bakhod1r/cachex/kafka` | Kafka topic, no consumer group | at-least-once while subscribed | topic must exist before `Subscribe`; each subscriber starts at the current end offsets |
| `github.com/bakhod1r/cachex/rabbitmq` | fanout exchange, exclusive queue per subscriber | at-most-once | amqp091 doesn't reconnect; drops are reported via `OnError` |

```go
import cachexkafka "github.com/bakhod1r/cachex/kafka"

inv, err := cachexkafka.New(cachexkafka.Config{Brokers: []string{"localhost:9092"}})
defer inv.Close()

import cachexrabbitmq "github.com/bakhod1r/cachex/rabbitmq"

conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
inv, err := cachexrabbitmq.New(cachexrabbitmq.Config{Conn: conn})
```

In every case a lost message only delays invalidation: the `WithL1TTL` and `WithVersionTTL`
bounds still apply.

## Options

| Option | Default | Meaning |
|---|---|---|
| `WithL2(Store)` | none (L1-only) | Shared tier, e.g. `memcached.New` |
| `WithL1MaxEntries(int)` | `100000` | L1 entry cap (0 = unlimited) |
| `WithL1MaxBytes(int64)` | `0` (unlimited) | L1 approximate byte cap |
| `WithShards(int)` | `0` (auto) | L1 shard count |
| `WithL1Frequency(bool)` | `true` | Frequency-aware L1 eviction (count-min sketch) |
| `WithDefaultTTL(d)` | `5m` | TTL when `ttl <= 0` |
| `WithL1TTL(d)` | `10s` | Max L1 lifetime; cross-node staleness bound |
| `WithDegradedL1TTL(d)` | `1s` | L1 TTL after a failed L2 write |
| `WithStaleWindow(d)` | `0` (off) | Serve expired values while refreshing |
| `WithBeta(float64)` | `1.0` | XFetch strength (0 disables) |
| `WithLoadTimeout(d)` | `5s` | Per-loader timeout |
| `WithVersionTTL(d)` | `2s` | Namespace version cache; invalidation visibility bound |
| `WithSweepInterval(d)` | `1s` | Expired-entry janitor (0 = lazy expiry only) |
| `WithNegativeTTL(d)` | `0` (off) | Cache a loader's `ErrNotFound` |
| `WithDeleteMarkerTTL(d)` | `2 x load timeout` | L2 Delete marker lifetime (`CASStore` only); must exceed load timeout |
| `WithDistributedLock(ttl, poll)` | off | One loader per key across processes |
| `WithInvalidator(Invalidator)` | none | Broadcast invalidations between processes |
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
MEMCACHED_ADDR=localhost:11211 go test -race -tags=integration ./memcached/
```

Other modules: `cd prometheus && go test -race ./...`; Redis adapter:
`REDIS_ADDR=localhost:6379 go test -race -tags=integration ./...` inside `redis/`; benchmarks against other Go caches
live in `bench/` (see `bench/RESULTS.md`).

Custom `Store` implementations can run the conformance suite: `storetest.Run(t, newStore, advance)`. Implement
`cachex.CASStore` too (the suite then checks it) to get the cross-process Delete guarantee.
