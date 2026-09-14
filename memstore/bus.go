package memstore

import (
	"context"
	"sync"

	"github.com/bakhod1r/cachex"
)

// Bus is an in-process cachex.Invalidator: every subscriber receives every published message
// synchronously. Use it in tests or to share invalidations between caches in one process.
type Bus struct {
	mu   sync.RWMutex
	next int
	subs map[int]func(cachex.Invalidation)
}

var _ cachex.Invalidator = (*Bus)(nil)

// NewBus returns an empty bus.
func NewBus() *Bus { return &Bus{subs: make(map[int]func(cachex.Invalidation))} }

// Publish delivers msg to every current subscriber.
func (b *Bus) Publish(_ context.Context, msg cachex.Invalidation) error {
	b.mu.RLock()
	fns := make([]func(cachex.Invalidation), 0, len(b.subs))
	for _, fn := range b.subs {
		fns = append(fns, fn)
	}
	b.mu.RUnlock()
	for _, fn := range fns {
		fn(msg)
	}
	return nil
}

// Subscribe delivers until ctx is done.
func (b *Bus) Subscribe(ctx context.Context, fn func(cachex.Invalidation)) error {
	b.mu.Lock()
	id := b.next
	b.next++
	b.subs[id] = fn
	b.mu.Unlock()
	context.AfterFunc(ctx, func() {
		b.mu.Lock()
		delete(b.subs, id)
		b.mu.Unlock()
	})
	return nil
}
