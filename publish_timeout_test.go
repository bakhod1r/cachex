package cachex_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

// hangInvalidator models a broker client that retries until its context ends,
// as franz-go's ProduceSync does while Kafka is unreachable.
type hangInvalidator struct{}

func (hangInvalidator) Publish(ctx context.Context, _ cachex.Invalidation) error {
	<-ctx.Done()
	return ctx.Err()
}
func (hangInvalidator) Subscribe(context.Context, func(cachex.Invalidation)) error { return nil }

func TestDeleteDoesNotHangOnStuckBroker(t *testing.T) {
	var pubErr error
	c, err := cachex.New(
		cachex.WithInvalidator(hangInvalidator{}),
		cachex.WithPublishTimeout(50*time.Millisecond),
		cachex.WithEvents(cachex.Events{PublishError: func(_ cachex.Invalidation, err error) { pubErr = err }}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	done := make(chan struct{})
	go func() {
		_ = c.Delete(context.Background(), "k")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Delete blocked on a broker that never answers")
	}
	if !errors.Is(pubErr, context.DeadlineExceeded) {
		t.Fatalf("PublishError got %v, want deadline exceeded", pubErr)
	}
}

func TestDefaultPublishTimeoutIsBounded(t *testing.T) {
	c, err := cachex.New(cachex.WithInvalidator(hangInvalidator{}), cachex.WithLoadTimeout(30*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan struct{})
	go func() { _ = c.Delete(context.Background(), "k"); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Delete blocked with the default publish timeout")
	}
}

func TestWithPublishTimeoutRejectsNonPositive(t *testing.T) {
	if _, err := cachex.New(cachex.WithPublishTimeout(0)); err == nil {
		t.Fatal("want error for zero publish timeout")
	}
}
