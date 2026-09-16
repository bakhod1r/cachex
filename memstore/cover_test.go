package memstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bakhod1r/cachex/memstore"
)

func TestCancelledContextAndNonNumericIncr(t *testing.T) {
	s := memstore.New()
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Get(cctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get cancelled: %v", err)
	}
	ctx := context.Background()
	_ = s.Set(ctx, "k", []byte("abc"), 0)
	if _, err := s.Incr(ctx, "k", 1); !errors.Is(err, memstore.ErrNotNumeric) {
		t.Fatalf("Incr non-numeric: %v", err)
	}
}
