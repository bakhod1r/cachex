package cachexredis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/bakhod1r/cachex"
	"github.com/redis/go-redis/v9"
)

func TestStoreClosedRejectsEveryOp(t *testing.T) {
	s, _ := newMini(t, "")
	_ = s.Close()
	ctx := context.Background()
	check := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, cachex.ErrClosed) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	_, _, err := s.GetsMulti(ctx, []string{"a"})
	check("GetsMulti", err)
	check("Set", s.Set(ctx, "a", nil, 0))
	check("Add", s.Add(ctx, "a", nil, 0))
	check("CompareAndSwap", s.CompareAndSwap(ctx, "a", nil, casToken{}, 0))
	check("Delete", s.Delete(ctx, "a"))
	_, err = s.Incr(ctx, "a", 1)
	check("Incr", err)
}

func TestStoreUnavailableOps(t *testing.T) {
	mr := miniredis.RunT(t)
	s, err := NewStore(StoreConfig{Client: redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1}), CloseClient: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	_ = s.Set(ctx, "a", []byte("v"), 0)
	_, tok, err := s.Gets(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	mr.Close()
	check := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, cachex.ErrL2Unavailable) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	_, _, err = s.GetsMulti(ctx, []string{"a"})
	check("GetsMulti", err)
	check("Add", s.Add(ctx, "a", nil, 0))
	check("CompareAndSwap", s.CompareAndSwap(ctx, "a", nil, tok, time.Minute))
}

func TestStoreGetsMultiEmptyAndForeignValue(t *testing.T) {
	s, mr := newMini(t, "")
	ctx := context.Background()
	vals, toks, err := s.GetsMulti(ctx, nil)
	if err != nil || len(vals) != 0 || len(toks) != 0 {
		t.Fatalf("%v %v %v", vals, toks, err)
	}
	_ = mr.Set("bad", "x")
	if _, _, err := s.GetsMulti(ctx, []string{"bad"}); !errors.Is(err, ErrMalformedValue) {
		t.Fatalf("want ErrMalformedValue, got %v", err)
	}
}

func TestMapErrDeadlinePassesThrough(t *testing.T) {
	if err := mapErr(context.DeadlineExceeded); err != context.DeadlineExceeded {
		t.Fatalf("got %v", err)
	}
}

func TestInvalidatorRedisDown(t *testing.T) {
	mr := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = cl.Close() })
	inv, _ := New(Config{Client: cl})
	mr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := inv.Publish(ctx, cachex.Invalidation{Keys: []string{"k"}}); err == nil {
		t.Fatal("publish to a closed server succeeded")
	}
	if err := inv.Subscribe(ctx, func(cachex.Invalidation) {}); err == nil {
		t.Fatal("subscribe to a closed server succeeded")
	}
}

func TestInvalidatorDefaultOnErrorAndClosedChannel(t *testing.T) {
	mr := miniredis.RunT(t)
	cl := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	inv, _ := New(Config{Client: cl}) // no OnError: decode errors are dropped
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := inv.Subscribe(ctx, func(cachex.Invalidation) {}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Publish(ctx, DefaultChannel, "not json").Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	_ = cl.Close()
	time.Sleep(50 * time.Millisecond)
}
