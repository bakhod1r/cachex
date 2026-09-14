// Package breaker is a small circuit breaker guarding the L2 cache tier so a
// dead L2 costs zero latency: while Open, Allow returns false immediately.
package breaker

import (
	"sync"
	"time"
)

// State of the breaker.
type State uint8

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	}
	return "unknown"
}

const maxCooldown = 60 * time.Second

// Config tunes the breaker. Zero values take defaults.
type Config struct {
	Window         time.Duration    // failure-ratio window, default 10s
	MinRequests    int              // default 20
	FailureRatio   float64          // default 0.5
	Cooldown       time.Duration    // open duration before half-open, default 5s
	HalfOpenProbes int              // default 1
	Now            func() time.Time // nil = time.Now
}

// Breaker is safe for concurrent use.
type Breaker struct {
	cfg Config

	mu          sync.Mutex
	state       State
	windowStart time.Time
	successes   int
	failures    int
	openedAt    time.Time
	cooldown    time.Duration // current (possibly doubled) cooldown
	inflight    int           // half-open probes outstanding
}

// New returns a Closed breaker.
func New(c Config) *Breaker {
	if c.Window <= 0 {
		c.Window = 10 * time.Second
	}
	if c.MinRequests <= 0 {
		c.MinRequests = 20
	}
	if c.FailureRatio <= 0 {
		c.FailureRatio = 0.5
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 5 * time.Second
	}
	if c.HalfOpenProbes <= 0 {
		c.HalfOpenProbes = 1
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return &Breaker{cfg: c, cooldown: c.Cooldown, windowStart: c.Now()}
}

// advance applies time-driven transitions. Caller holds mu.
func (b *Breaker) advance(now time.Time) {
	switch b.state {
	case Open:
		if now.Sub(b.openedAt) >= b.cooldown {
			b.state = HalfOpen
			b.inflight = 0
		}
	case Closed:
		if now.Sub(b.windowStart) >= b.cfg.Window {
			b.windowStart, b.successes, b.failures = now, 0, 0
		}
	}
}

// Allow reports whether a call may proceed. In HalfOpen it admits at most
// HalfOpenProbes outstanding probes; each admitted call must report Success or Failure.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance(b.cfg.Now())
	switch b.state {
	case Closed:
		return true
	case HalfOpen:
		if b.inflight < b.cfg.HalfOpenProbes {
			b.inflight++
			return true
		}
	}
	return false
}

// Success records a successful call.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	b.advance(now)
	switch b.state {
	case Closed:
		b.successes++
	case HalfOpen:
		b.state = Closed
		b.windowStart, b.successes, b.failures, b.inflight = now, 0, 0, 0
		b.cooldown = b.cfg.Cooldown
	}
}

// Failure records a failed call.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	b.advance(now)
	switch b.state {
	case Closed:
		b.failures++
		total := b.successes + b.failures
		if total >= b.cfg.MinRequests && float64(b.failures)/float64(total) >= b.cfg.FailureRatio {
			b.state, b.openedAt, b.cooldown = Open, now, b.cfg.Cooldown
		}
	case HalfOpen:
		next := b.cooldown * 2
		if limit := max(maxCooldown, b.cfg.Cooldown); next > limit {
			next = limit
		}
		b.state, b.openedAt, b.cooldown, b.inflight = Open, now, next, 0
	}
}

// State returns the current state, applying elapsed-cooldown transitions.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advance(b.cfg.Now())
	return b.state
}
