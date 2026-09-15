package memstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
)

func TestBusDeliversToSubscribersUntilCancel(t *testing.T) {
	b := memstore.NewBus()
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	var got1, got2 []cachex.Invalidation
	if err := b.Subscribe(ctx1, func(m cachex.Invalidation) { got1 = append(got1, m) }); err != nil {
		t.Fatal(err)
	}
	if err := b.Subscribe(ctx2, func(m cachex.Invalidation) { got2 = append(got2, m) }); err != nil {
		t.Fatal(err)
	}

	msg := cachex.Invalidation{}
	if err := b.Publish(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(got1) != 1 || len(got2) != 1 {
		t.Fatalf("deliveries = %d, %d; want 1, 1", len(got1), len(got2))
	}

	cancel1()
	deadline := time.Now().Add(time.Second)
	for {
		_ = b.Publish(context.Background(), msg)
		if len(got1) == 1 || time.Now().After(deadline) {
			break
		}
		got1 = got1[:1] // AfterFunc runs asynchronously; retry until unsubscribed
		time.Sleep(time.Millisecond)
	}
	if len(got1) != 1 {
		t.Fatalf("cancelled subscriber still receives: %d", len(got1))
	}
	if len(got2) < 2 {
		t.Fatalf("live subscriber lost messages: %d", len(got2))
	}
}

func TestEveryOpHonoursInjectedFailure(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	ops := map[string]func(s *memstore.Store) error{
		"Get":       func(s *memstore.Store) error { _, err := s.Get(ctx, "k"); return err },
		"GetMulti":  func(s *memstore.Store) error { _, err := s.GetMulti(ctx, []string{"k"}); return err },
		"Set":       func(s *memstore.Store) error { return s.Set(ctx, "k", []byte("v"), 0) },
		"Add":       func(s *memstore.Store) error { return s.Add(ctx, "k", []byte("v"), 0) },
		"Delete":    func(s *memstore.Store) error { return s.Delete(ctx, "k") },
		"Gets":      func(s *memstore.Store) error { _, _, err := s.Gets(ctx, "k"); return err },
		"GetsMulti": func(s *memstore.Store) error { _, _, err := s.GetsMulti(ctx, []string{"k"}); return err },
		"CAS":       func(s *memstore.Store) error { return s.CompareAndSwap(ctx, "k", nil, []byte("v"), 0) },
		"Incr":      func(s *memstore.Store) error { _, err := s.Incr(ctx, "k", 1); return err },
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			s := memstore.New()
			s.FailNext(1, boom)
			if err := op(s); !errors.Is(err, boom) {
				t.Fatalf("err = %v, want boom", err)
			}
		})
	}
}

func TestNewWithNilClockUsesRealClock(t *testing.T) {
	ctx := context.Background()
	s := memstore.NewWithClock(nil)
	if err := s.Set(ctx, "k", []byte("v"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("Get = %q, %v", v, err)
	}
}
