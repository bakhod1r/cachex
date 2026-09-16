package cachex

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Corrupt L1 bytes can't be produced through the public API; plant them directly.
func TestCorruptL1EntryIsDropped(t *testing.T) {
	c, err := New(WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.l1.Set("k", []byte("junk"), time.Minute)
	if _, err := c.Get(context.Background(), "k"); !errors.Is(err, ErrMiss) {
		t.Fatal(err)
	}
	c.l1.Set("k", []byte("junk"), time.Minute)
	if got, err := c.GetMulti(context.Background(), []string{"k"}); err != nil || len(got) != 0 {
		t.Fatalf("%q %v", got, err)
	}
}

// Loads run on a detached context, so only a direct call can reach the cancelled-wait branch.
func TestLoadLockWaitHonoursContext(t *testing.T) {
	c, err := New(WithSweepInterval(0), WithL2(&lockedStore{}), WithDistributedLock(time.Hour, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	unlock, cached := c.acquireLoadLock(cctx, "k")
	unlock()
	if !cached {
		t.Fatal("cancelled wait must report cached")
	}
}

// lockedStore reports every Add as already taken.
type lockedStore struct{ Store }

func (*lockedStore) Add(context.Context, string, []byte, time.Duration) error { return ErrNotStored }
func (*lockedStore) Close() error                                             { return nil }
