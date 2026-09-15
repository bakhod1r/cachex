package clock

import (
	"sync"
	"testing"
	"time"
)

// tolerance allows for ticker lag under -race and a loaded test machine.
const tolerance = 50 * time.Millisecond

func abs(d int64) int64 {
	if d < 0 {
		return -d
	}
	return d
}

func TestNowTracksWallClock(t *testing.T) {
	for range 1000 {
		want := time.Now().UnixNano()
		if d := abs(NowNano() - want); d > int64(tolerance) {
			t.Fatalf("NowNano off by %v", time.Duration(d))
		}
	}
}

func TestStopsWhenIdleAndRestarts(t *testing.T) {
	oldTick, oldIdle := tick, idleTicks
	tick, idleTicks = time.Millisecond, 5
	defer func() { tick, idleTicks = oldTick, oldIdle }()
	waitStopped(t)
	NowNano()
	if !running.Load() {
		t.Fatal("ticker not started by NowNano")
	}
	waitStopped(t)
	if d := abs(NowNano() - time.Now().UnixNano()); d > int64(tolerance) {
		t.Fatalf("NowNano off by %v after restart", time.Duration(d))
	}
}

func waitStopped(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for running.Load() {
		if time.Now().After(deadline) {
			t.Fatal("ticker did not stop while idle")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNowConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10000 {
				if d := abs(NowNano() - time.Now().UnixNano()); d > int64(tolerance) {
					t.Errorf("NowNano off by %v", time.Duration(d))
					return
				}
			}
		})
	}
	wg.Wait()
}

func BenchmarkNowNano(b *testing.B) {
	for range b.N {
		_ = NowNano()
	}
}

func BenchmarkTimeNow(b *testing.B) {
	for range b.N {
		_ = time.Now().UnixNano()
	}
}
