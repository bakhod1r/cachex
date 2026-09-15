// Command example runs two cachex instances ("processes") that share one Redis as L2 and as the
// pub/sub invalidation bus. Delete and Namespace.Invalidate on A evict B's L1 copy immediately.
//
//	REDIS_ADDR=localhost:6379 go run ./example
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/bakhod1r/cachex"
	cachexredis "github.com/bakhod1r/cachex/redis"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis %s: %v", addr, err)
	}

	a, b := newCache(rdb), newCache(rdb)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	if err := a.Set(ctx, "user:42", []byte("Ada"), time.Hour); err != nil {
		log.Fatal(err)
	}
	v, err := b.Get(ctx, "user:42")
	fmt.Printf("B get (L2 then L1): %q err=%v\n", v, err)

	if err := a.Delete(ctx, "user:42"); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("B miss after A.Delete: %v\n", waitMiss(func() error { _, err := b.Get(ctx, "user:42"); return err }))

	usersA, _ := a.Namespace("users")
	usersB, _ := b.Namespace("users")
	if err := usersA.Set(ctx, "7", []byte("Grace"), time.Hour); err != nil {
		log.Fatal(err)
	}
	v, err = usersB.Get(ctx, "7")
	fmt.Printf("B namespace get: %q err=%v\n", v, err)
	if err := usersA.Invalidate(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("B namespace miss after A.Invalidate: %v\n", waitMiss(func() error { _, err := usersB.Get(ctx, "7"); return err }))
}

func newCache(rdb redis.UniversalClient) *cachex.Cache {
	store, err := cachexredis.NewStore(cachexredis.StoreConfig{Client: rdb, KeyPrefix: "example:"})
	if err != nil {
		log.Fatal(err)
	}
	inv, err := cachexredis.New(cachexredis.Config{
		Client:  rdb,
		OnError: func(err error) { log.Printf("invalidation: %v", err) },
	})
	if err != nil {
		log.Fatal(err)
	}
	c, err := cachex.New(
		cachex.WithL2(store),
		cachex.WithInvalidator(inv),
		cachex.WithL1TTL(time.Hour), // long on purpose: only pub/sub makes B see changes quickly
	)
	if err != nil {
		log.Fatal(err)
	}
	return c
}

// waitMiss polls for up to 1s; pub/sub delivery is asynchronous.
func waitMiss(get func() error) bool {
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if errors.Is(get(), cachex.ErrMiss) {
			return true
		}
	}
	return false
}
