package typed_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/typed"
)

var errBoom = errors.New("boom")

// badCodec fails to marshal.
type badCodec struct{}

func (badCodec) Marshal(int) ([]byte, error)   { return nil, errBoom }
func (badCodec) Unmarshal([]byte) (int, error) { return 0, nil }

// noDelete is a Backend whose Delete fails.
type noDelete struct{ *cachex.Cache }

func (noDelete) Delete(context.Context, string) error { return errBoom }

func TestNewPanicsOnNil(t *testing.T) {
	for _, f := range []func(){
		func() { typed.New[int](nil, typed.JSON[int]()) },
		func() { typed.New[int](newCache(t), nil) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic")
				}
			}()
			f()
		}()
	}
}

func TestEncodeErrors(t *testing.T) {
	tc := typed.New[int](newCache(t), badCodec{})
	if err := tc.Set(ctx, "k", 1, time.Minute); !errors.Is(err, errBoom) {
		t.Fatalf("Set: %v", err)
	}
	if _, err := tc.GetOrLoad(ctx, "k", time.Minute, func(context.Context) (int, error) { return 1, nil }); !errors.Is(err, errBoom) {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if _, err := tc.GetOrLoad(ctx, "k", time.Minute, nil); err == nil {
		t.Fatal("nil loader accepted")
	}
}

func TestBytesMarshalAndDeleteFailureOnCorrupt(t *testing.T) {
	c := newCache(t)
	bc := typed.New(c, typed.Bytes())
	if err := bc.Set(ctx, "b", []byte("raw"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if v, err := bc.Get(ctx, "b"); err != nil || string(v) != "raw" {
		t.Fatalf("%q %v", v, err)
	}
	_ = c.Set(ctx, "k", []byte("{not json"), time.Minute)
	tc := typed.New[user](noDelete{c}, typed.JSON[user]())
	_, err := tc.GetOrLoad(ctx, "k", time.Minute, func(context.Context) (user, error) { return user{}, nil })
	if !errors.Is(err, typed.ErrDecode) || !errors.Is(err, errBoom) {
		t.Fatalf("want decode and delete errors, got %v", err)
	}
}
