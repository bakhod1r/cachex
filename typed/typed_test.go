package typed_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/typed"
)

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func newCache(t testing.TB) *cachex.Cache {
	t.Helper()
	c, err := cachex.New(cachex.WithSweepInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

var ctx = context.Background()

func TestJSONRoundTrip(t *testing.T) {
	tc := typed.New[user](newCache(t), typed.JSON[user]())
	want := user{ID: 7, Name: "ann"}
	if err := tc.Set(ctx, "u", want, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := tc.Get(ctx, "u")
	if err != nil || got != want {
		t.Fatalf("got %+v, %v", got, err)
	}
	if err := tc.Delete(ctx, "u"); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.Get(ctx, "u"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("after delete: %v", err)
	}
}

func TestStringAndBytes(t *testing.T) {
	c := newCache(t)
	s := typed.New(c, typed.String())
	if err := s.Set(ctx, "s", "hello", time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, err := s.Get(ctx, "s"); err != nil || v != "hello" {
		t.Fatalf("%q %v", v, err)
	}
	b := typed.New(c, typed.Bytes())
	if v, err := b.Get(ctx, "s"); err != nil || string(v) != "hello" {
		t.Fatalf("%q %v", v, err)
	}
}

func TestMissReturnsZero(t *testing.T) {
	tc := typed.New(newCache(t), typed.JSON[user]())
	v, err := tc.Get(ctx, "nope")
	if !errors.Is(err, cachex.ErrMiss) || v != (user{}) {
		t.Fatalf("%+v %v", v, err)
	}
}

func TestGetOrLoadLoadsOnce(t *testing.T) {
	tc := typed.New(newCache(t), typed.JSON[user]())
	calls := 0
	load := func(context.Context) (user, error) { calls++; return user{ID: 1, Name: "x"}, nil }
	for i := 0; i < 3; i++ {
		v, err := tc.GetOrLoad(ctx, "k", time.Minute, load)
		if err != nil || v.ID != 1 {
			t.Fatalf("%+v %v", v, err)
		}
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestGetOrLoadLoaderError(t *testing.T) {
	tc := typed.New(newCache(t), typed.JSON[user]())
	boom := errors.New("boom")
	v, err := tc.GetOrLoad(ctx, "k", time.Minute, func(context.Context) (user, error) { return user{ID: 9}, boom })
	if !errors.Is(err, boom) || v != (user{}) {
		t.Fatalf("%+v %v", v, err)
	}
	if _, err := tc.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("error must not be cached: %v", err)
	}
}

func TestCorruptValue(t *testing.T) {
	c := newCache(t)
	tc := typed.New(c, typed.JSON[user]())
	if err := c.Set(ctx, "k", []byte("{not json"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.Get(ctx, "k"); !errors.Is(err, typed.ErrDecode) {
		t.Fatalf("Get: want ErrDecode, got %v", err)
	}
	calls := 0
	v, err := tc.GetOrLoad(ctx, "k", time.Minute, func(context.Context) (user, error) {
		calls++
		return user{ID: 2}, nil
	})
	if err != nil || v.ID != 2 || calls != 1 {
		t.Fatalf("%+v %v calls=%d", v, err, calls)
	}
	if v, err := tc.Get(ctx, "k"); err != nil || v.ID != 2 {
		t.Fatalf("repaired: %+v %v", v, err)
	}
}

func TestNamespaceBackend(t *testing.T) {
	ns, err := newCache(t).Namespace("users")
	if err != nil {
		t.Fatal(err)
	}
	tc := typed.New[user](ns, typed.JSON[user]())
	if err := tc.Set(ctx, "a", user{ID: 3}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, err := tc.Get(ctx, "a"); err != nil || v.ID != 3 {
		t.Fatalf("%+v %v", v, err)
	}
	if err := ns.Invalidate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.Get(ctx, "a"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("after invalidate: %v", err)
	}
}

func Example() {
	c, _ := cachex.New(cachex.WithSweepInterval(0))
	defer c.Close()
	users := typed.New[user](c, typed.JSON[user]())
	u, _ := users.GetOrLoad(context.Background(), "user:1", time.Minute,
		func(context.Context) (user, error) { return user{ID: 1, Name: "ann"}, nil })
	fmt.Println(u.Name)
	// Output: ann
}
