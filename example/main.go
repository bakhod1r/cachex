// Command example shows L1-only cachex usage: GetOrLoad, a Namespace, Invalidate and Stats.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/bakhod1r/cachex"
)

func main() {
	ctx := context.Background()

	c, err := cachex.New(
		cachex.WithL1MaxEntries(10_000),
		cachex.WithDefaultTTL(time.Minute),
		cachex.WithStaleWindow(5*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	}()

	calls := 0
	loadUser := func(_ context.Context) ([]byte, error) {
		calls++
		return []byte(`{"id":42,"name":"Ada"}`), nil
	}

	for i := 0; i < 3; i++ {
		v, err := c.GetOrLoad(ctx, "user:42", time.Minute, loadUser)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("GetOrLoad #%d: %s\n", i+1, v)
	}
	fmt.Printf("loader calls: %d\n", calls)

	users, err := c.Namespace("users")
	if err != nil {
		log.Fatal(err)
	}
	if err := users.Set(ctx, "42", []byte("Ada"), 0); err != nil {
		log.Fatal(err)
	}
	v, err := users.Get(ctx, "42")
	fmt.Printf("namespace get before invalidate: %q err=%v\n", v, err)

	if err := users.Invalidate(ctx); err != nil {
		log.Fatal(err)
	}
	_, err = users.Get(ctx, "42")
	fmt.Printf("namespace get after invalidate: miss=%v\n", errors.Is(err, cachex.ErrMiss))

	s := c.Stats()
	fmt.Printf("stats: L1Hits=%d L1Misses=%d Loads=%d Entries=%d Breaker=%s HitRatio=%.2f\n",
		s.L1Hits, s.L1Misses, s.Loads, s.Entries, s.Breaker, s.HitRatio())
}
