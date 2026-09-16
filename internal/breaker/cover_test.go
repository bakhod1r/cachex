package breaker

import (
	"testing"
	"time"
)

func TestSetSameStateIsNoop(t *testing.T) {
	var calls int
	b := New(Config{Now: time.Now, OnChange: func(State, State) { calls++ }})
	b.mu.Lock()
	b.set(Closed)
	b.unlock()
	if calls != 0 || b.State() != Closed {
		t.Fatalf("calls %d state %v", calls, b.State())
	}
}
