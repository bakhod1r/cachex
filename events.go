package cachex

import (
	"context"
	"time"
)

// Op names a public Cache operation in Events.
type Op uint8

// Operations reported by OpStart and OpEnd.
const (
	OpGet Op = iota
	OpGetView
	OpSet
	OpDelete
	OpGetMulti
	OpSetMulti
	OpGetOrLoad
	OpGetOrLoadMulti
)

func (o Op) String() string {
	switch o {
	case OpGet:
		return "get"
	case OpGetView:
		return "get_view"
	case OpSet:
		return "set"
	case OpDelete:
		return "delete"
	case OpGetMulti:
		return "get_multi"
	case OpSetMulti:
		return "set_multi"
	case OpGetOrLoad:
		return "get_or_load"
	case OpGetOrLoadMulti:
		return "get_or_load_multi"
	}
	return "unknown"
}

// Tier reports who answered an operation.
type Tier uint8

// Tiers reported in OpInfo.
const (
	TierL1       Tier = iota // in-process copy
	TierL2                   // shared store, backfilled into L1
	TierStale                // expired value served within the stale window
	TierMiss                 // nothing cached; also used by writes and failed calls
	TierLoaded               // loader ran (here or in a shared flight)
	TierNegative             // cached ErrNotFound
)

func (t Tier) String() string {
	switch t {
	case TierL1:
		return "l1"
	case TierL2:
		return "l2"
	case TierStale:
		return "stale"
	case TierMiss:
		return "miss"
	case TierLoaded:
		return "loaded"
	case TierNegative:
		return "negative"
	}
	return "unknown"
}

// OpInfo describes a finished operation. Key is empty and Keys is the batch size for
// multi-key operations. For multi-key reads Tier is the farthest tier any key reached
// (L1 < L2 < Loaded); for writes it is TierMiss.
type OpInfo struct {
	Op   Op
	Key  string
	Keys int
	Tier Tier
	Err  error
}

// Events are optional hooks for logging, metrics and tracing. Nil fields are skipped.
// Hooks run synchronously on the calling goroutine (BreakerChange and L2Error may run on a
// background refresh), so they must be fast and must not call back into the Cache.
type Events struct {
	// OpStart runs before a public operation; the returned context (e.g. carrying a span)
	// is used for L2 calls and the loader. Returning nil keeps ctx.
	OpStart func(ctx context.Context, op Op, key string) context.Context
	// OpEnd runs after the operation with the context OpStart returned.
	OpEnd func(ctx context.Context, info OpInfo, dur time.Duration)
	// LoadEnd runs after each single-key loader call, including background refreshes.
	LoadEnd func(ctx context.Context, key string, dur time.Duration, err error)
	// L2Error reports a store call that counted as a breaker failure. op is the Store
	// method, e.g. "get", "set", "add", "delete", "get_multi", "gets", "cas".
	L2Error func(op string, err error)
	// BreakerChange reports a breaker transition ("closed", "open", "half-open").
	BreakerChange func(from, to string)
	// LoaderPanic reports a recovered loader panic. key is empty for GetOrLoadMulti.
	LoaderPanic func(key string, recovered any)
	// PublishError reports a failed Invalidator.Publish.
	PublishError func(msg Invalidation, err error)
}

// MergeEvents combines hooks; each is called in argument order. OpStart contexts chain,
// so later hooks see earlier values. A hook no argument sets stays nil.
func MergeEvents(es ...Events) Events {
	var m Events
	var (
		starts  []func(context.Context, Op, string) context.Context
		ends    []func(context.Context, OpInfo, time.Duration)
		loads   []func(context.Context, string, time.Duration, error)
		l2s     []func(string, error)
		changes []func(string, string)
		panics  []func(string, any)
		pubs    []func(Invalidation, error)
	)
	for _, e := range es {
		if e.OpStart != nil {
			starts = append(starts, e.OpStart)
		}
		if e.OpEnd != nil {
			ends = append(ends, e.OpEnd)
		}
		if e.LoadEnd != nil {
			loads = append(loads, e.LoadEnd)
		}
		if e.L2Error != nil {
			l2s = append(l2s, e.L2Error)
		}
		if e.BreakerChange != nil {
			changes = append(changes, e.BreakerChange)
		}
		if e.LoaderPanic != nil {
			panics = append(panics, e.LoaderPanic)
		}
		if e.PublishError != nil {
			pubs = append(pubs, e.PublishError)
		}
	}
	if len(starts) > 0 {
		m.OpStart = func(ctx context.Context, op Op, key string) context.Context {
			for _, f := range starts {
				if c := f(ctx, op, key); c != nil {
					ctx = c
				}
			}
			return ctx
		}
	}
	if len(ends) > 0 {
		m.OpEnd = func(ctx context.Context, info OpInfo, d time.Duration) {
			for _, f := range ends {
				f(ctx, info, d)
			}
		}
	}
	if len(loads) > 0 {
		m.LoadEnd = func(ctx context.Context, key string, d time.Duration, err error) {
			for _, f := range loads {
				f(ctx, key, d, err)
			}
		}
	}
	if len(l2s) > 0 {
		m.L2Error = func(op string, err error) {
			for _, f := range l2s {
				f(op, err)
			}
		}
	}
	if len(changes) > 0 {
		m.BreakerChange = func(from, to string) {
			for _, f := range changes {
				f(from, to)
			}
		}
	}
	if len(panics) > 0 {
		m.LoaderPanic = func(key string, r any) {
			for _, f := range panics {
				f(key, r)
			}
		}
	}
	if len(pubs) > 0 {
		m.PublishError = func(msg Invalidation, err error) {
			for _, f := range pubs {
				f(msg, err)
			}
		}
	}
	return m
}

// opBegin runs OpStart and returns the context to use and the start time.
// Callers check c.opHooks first so the no-events path skips the clock read.
func (c *Cache) opBegin(ctx context.Context, op Op, key string) (context.Context, time.Time) {
	if f := c.cfg.events.OpStart; f != nil {
		if nc := f(ctx, op, key); nc != nil {
			ctx = nc
		}
	}
	return ctx, c.cfg.clock.Now()
}

func (c *Cache) opFinish(ctx context.Context, start time.Time, info OpInfo) {
	if f := c.cfg.events.OpEnd; f != nil {
		f(ctx, info, c.cfg.clock.Now().Sub(start))
	}
}
