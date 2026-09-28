package cachex_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

// now + ttl overflowed int64 for a "forever" TTL, so the entry was stored
// already expired.
func TestHugeTTLDoesNotExpireImmediately(t *testing.T) {
	c, err := cachex.New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if err := c.Set(ctx, "k", []byte("v"), time.Duration(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "k")
	if err != nil || string(got) != "v" {
		t.Fatalf("Get = %q, %v; want v", got, err)
	}
}
