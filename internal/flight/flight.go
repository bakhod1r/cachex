// Package flight provides context-aware singleflight for cache stampede protection.
package flight

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrPanic is wrapped by the error returned when fn panics.
var ErrPanic = errors.New("flight: loader panicked")

type call struct {
	done chan struct{} // closed when val/err are set
	val  []byte
	err  error
	dups int // joiners after the first caller; guarded by Group.mu
}

// Group deduplicates concurrent calls per key. The zero value is usable.
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

// Do runs fn once per key among concurrent callers. The shared call runs with
// context.WithoutCancel(ctx) of the first caller so one caller cancelling does
// not fail the others. Each caller returns early with ctx.Err() if its own ctx
// is done; fn keeps running for the remaining waiters. shared reports whether
// the result was produced by another caller's call. A panic in fn is recovered
// and returned to all waiters as an error wrapping ErrPanic.
//
// fn must itself be bounded (honour ctx deadlines it derives, or its own
// timeout): the detached context carries values but no cancellation.
func (g *Group) Do(ctx context.Context, key string, fn func(ctx context.Context) ([]byte, error)) (val []byte, shared bool, err error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		return wait(ctx, c, true)
	}
	c := &call{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()

	go g.run(context.WithoutCancel(ctx), key, c, fn)
	return wait(ctx, c, false)
}

func wait(ctx context.Context, c *call, shared bool) ([]byte, bool, error) {
	select {
	case <-c.done:
		return c.val, shared, c.err
	case <-ctx.Done():
		return nil, shared, ctx.Err()
	}
}

func (g *Group) run(ctx context.Context, key string, c *call, fn func(context.Context) ([]byte, error)) {
	defer func() {
		if r := recover(); r != nil {
			c.val, c.err = nil, fmt.Errorf("%w: %v", ErrPanic, r)
		}
		g.mu.Lock()
		if g.m[key] == c { // Forget may have installed a newer call
			delete(g.m, key)
		}
		g.mu.Unlock()
		close(c.done)
	}()
	c.val, c.err = fn(ctx)
}

// Forget drops the in-flight entry for key so the next Do starts a new call.
// Current waiters still receive the old call's result.
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}
