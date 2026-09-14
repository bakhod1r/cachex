// Package counter provides a striped uint64 counter for hot paths where many goroutines
// increment the same statistic. A single atomic shared by all cores bounces its cache line
// on every Add; spreading increments over padded slots avoids that.
package counter

import (
	"math/rand/v2"
	"sync/atomic"
)

const stripes = 16

type slot struct {
	n atomic.Uint64
	_ [56]byte // pad to a 64-byte cache line
}

// Counter is safe for concurrent use. The zero value is ready.
type Counter struct {
	slots [stripes]slot
}

// Add increments the counter by d. Slot choice uses the runtime's per-P random source,
// so concurrent callers usually land on different slots.
func (c *Counter) Add(d uint64) {
	c.slots[rand.Uint32()&(stripes-1)].n.Add(d)
}

// Load returns the sum of all slots. Concurrent Adds may or may not be included.
func (c *Counter) Load() uint64 {
	var sum uint64
	for i := range c.slots {
		sum += c.slots[i].n.Load()
	}
	return sum
}
