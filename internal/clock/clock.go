// Package clock provides a cheap, coarse wall-clock reading for hot paths.
//
// time.Now reads both the wall and the monotonic clock; on darwin each is a libc call
// costing tens of nanoseconds. NowNano instead returns an atomic updated every tick by a
// background goroutine, the same approach otter and theine use. The goroutine starts on
// first use and exits after idleTicks ticks with no calls, so an idle process pays nothing.
//
// Precision: the value trails the real clock by up to one tick, plus scheduling delay of
// the ticker goroutine on a saturated machine. Fine for cache TTLs; not for measurement.
package clock

import (
	"sync/atomic"
	"time"
)

var (
	tick      = time.Millisecond
	idleTicks = 1000
)

var (
	now     atomic.Int64
	running atomic.Bool
	used    atomic.Bool
)

// NowNano returns the current wall-clock time in unix nanoseconds, coarsened to one tick.
func NowNano() int64 {
	if running.Load() {
		if !used.Load() { // read before write: keeps the cache line shared across cores
			used.Store(true)
		}
		return now.Load()
	}
	t := time.Now().UnixNano()
	if running.CompareAndSwap(false, true) {
		now.Store(t)
		used.Store(true)
		go run(tick, idleTicks)
	}
	return t
}

func run(tick time.Duration, idleTicks int) {
	tk := time.NewTicker(tick)
	defer tk.Stop()
	for n := 1; ; n++ {
		<-tk.C
		now.Store(time.Now().UnixNano())
		if n%idleTicks == 0 && !used.Swap(false) {
			running.Store(false)
			return
		}
	}
}
