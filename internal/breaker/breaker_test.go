package breaker

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func newB(c *clock) *Breaker {
	return New(Config{Window: 10 * time.Second, MinRequests: 4, FailureRatio: 0.5, Cooldown: 5 * time.Second, HalfOpenProbes: 1, Now: c.now})
}

func trip(b *Breaker) {
	for i := 0; i < 4; i++ {
		b.Allow()
		b.Failure()
	}
}

func TestStaysClosedUnderThreshold(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newB(c)
	for i := 0; i < 3; i++ {
		b.Failure()
	}
	if b.State() != Closed || !b.Allow() {
		t.Fatal("tripped below MinRequests")
	}
	b2 := newB(c)
	b2.Failure()
	for i := 0; i < 3; i++ {
		b2.Success()
	}
	if b2.State() != Closed {
		t.Fatal("tripped below ratio")
	}
}

func TestTripsAndRejects(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newB(c)
	trip(b)
	if b.State() != Open || b.Allow() {
		t.Fatal("expected open rejecting")
	}
	c.add(4999 * time.Millisecond)
	if b.Allow() {
		t.Fatal("allowed before cooldown")
	}
}

func TestHalfOpenProbeLimitAndSuccessCloses(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newB(c)
	trip(b)
	c.add(5 * time.Second)
	if b.State() != HalfOpen {
		t.Fatalf("state %v", b.State())
	}
	if !b.Allow() {
		t.Fatal("probe rejected")
	}
	if b.Allow() {
		t.Fatal("probe limit exceeded")
	}
	b.Success()
	if b.State() != Closed || !b.Allow() {
		t.Fatal("success did not close")
	}
	// counts reset: 3 failures should not trip
	for i := 0; i < 3; i++ {
		b.Failure()
	}
	if b.State() != Closed {
		t.Fatal("counts not reset")
	}
}

func TestFailureReopensDoublingWithCapAndReset(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newB(c)
	trip(b)
	want := []time.Duration{10, 20, 40, 60, 60}
	c.add(5 * time.Second)
	for _, w := range want {
		if !b.Allow() {
			t.Fatal("probe rejected")
		}
		b.Failure()
		if b.State() != Open {
			t.Fatal("not reopened")
		}
		c.add(w*time.Second - time.Millisecond)
		if b.Allow() {
			t.Fatalf("allowed before %ds", w)
		}
		c.add(time.Millisecond)
		if b.State() != HalfOpen {
			t.Fatalf("not half-open after %ds", w)
		}
	}
	b.Allow()
	b.Success()
	trip(b)
	c.add(5 * time.Second)
	if b.State() != HalfOpen {
		t.Fatal("cooldown not reset to base on close")
	}
}

func TestWindowReset(t *testing.T) {
	c := &clock{t: time.Unix(0, 0)}
	b := newB(c)
	for i := 0; i < 3; i++ {
		b.Failure()
	}
	c.add(10 * time.Second)
	b.Failure()
	if b.State() != Closed {
		t.Fatal("window did not reset")
	}
}

func TestDefaultsAndString(t *testing.T) {
	b := New(Config{})
	if b.cfg.Window != 10*time.Second || b.cfg.MinRequests != 20 || b.cfg.FailureRatio != 0.5 || b.cfg.Cooldown != 5*time.Second || b.cfg.HalfOpenProbes != 1 || b.cfg.Now == nil {
		t.Fatalf("defaults %+v", b.cfg)
	}
	if Closed.String() != "closed" || Open.String() != "open" || HalfOpen.String() != "half-open" || State(9).String() != "unknown" {
		t.Fatal("String")
	}
}

func TestConcurrent(t *testing.T) {
	b := New(Config{MinRequests: 5})
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(g int) {
			for i := 0; i < 1000; i++ {
				if b.Allow() {
					if (i+g)%2 == 0 {
						b.Failure()
					} else {
						b.Success()
					}
				}
				_ = b.State()
			}
			done <- struct{}{}
		}(g)
	}
	for g := 0; g < 8; g++ {
		<-done
	}
}
