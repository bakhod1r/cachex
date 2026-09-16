package clock

import "testing"

func TestNowNanoMarksUsedWhileRunning(t *testing.T) {
	for range 100 {
		NowNano() // ensure the ticker goroutine is running
		used.Store(false)
		if !running.Load() {
			continue // goroutine exited between the calls; retry
		}
		NowNano()
		if running.Load() && used.Load() {
			return
		}
	}
	t.Fatal("NowNano never marked the clock as used")
}
