package cachex

import (
	"testing"
	"time"
)

// A version fetch that read v=5 before an Invalidate bumped it to 6 must not
// overwrite 6 when it finishes late: that would serve invalidated data from
// the old keys for up to versionTTL.
func TestVersionsNeverGoBackwards(t *testing.T) {
	var vs versions
	now := time.Now()
	vs.put("users", 6, now)
	vs.put("users", 5, now.Add(time.Millisecond)) // the slow fetch lands
	if e, _ := vs.get("users"); e.v != 6 {
		t.Fatalf("version = %d, want 6", e.v)
	}
}

// A remote invalidation forces a refetch, but a stale fetch landing after it
// must still not take the version below what this process already saw.
func TestForgetKeepsTheFloor(t *testing.T) {
	var vs versions
	now := time.Now()
	vs.put("users", 6, now)
	vs.forget("users")
	if e, ok := vs.get("users"); ok && !e.fetched.IsZero() {
		t.Fatalf("forget left a fresh entry: %+v", e)
	}
	vs.put("users", 5, now)
	if e, _ := vs.get("users"); e.v < 6 {
		t.Fatalf("version = %d after forget and a stale put, want >= 6", e.v)
	}
}
