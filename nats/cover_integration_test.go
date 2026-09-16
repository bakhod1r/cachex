//go:build integration

package cachexnats

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/nats-io/nats.go"
)

func TestIntegrationArgumentAndClosedConnErrors(t *testing.T) {
	c := conn(t)
	inv, _ := New(Config{Conn: c, Subject: subjectName()})
	if err := inv.Subscribe(context.Background(), nil); err == nil {
		t.Fatal("nil fn accepted")
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := inv.Publish(cctx, cachex.Invalidation{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish cancelled: %v", err)
	}
	c.Close()
	if err := inv.Publish(context.Background(), cachex.Invalidation{}); err == nil {
		t.Fatal("Publish on closed conn succeeded")
	}
	if err := inv.Subscribe(context.Background(), func(cachex.Invalidation) {}); err == nil {
		t.Fatal("Subscribe on closed conn succeeded")
	}
}

func TestIntegrationSubscribeWithDeadline(t *testing.T) {
	c := conn(t)
	inv, _ := New(Config{Conn: c, Subject: subjectName()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := inv.Subscribe(ctx, func(cachex.Invalidation) {}); err != nil {
		t.Fatal(err)
	}
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	if err := inv.Subscribe(expired, func(cachex.Invalidation) {}); err == nil {
		t.Fatal("flush with an expired deadline succeeded")
	}
}

func TestIntegrationMalformedPayloadDefaultOnError(t *testing.T) {
	c := conn(t)
	subject := subjectName()
	errs := make(chan error, 1)
	withHook, _ := New(Config{Conn: c, Subject: subject, OnError: func(err error) { errs <- err }})
	silent, _ := New(Config{Conn: c, Subject: subject}) // default OnError drops the error
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, inv := range []*Invalidator{withHook, silent} {
		if err := inv.Subscribe(ctx, func(cachex.Invalidation) { t.Error("fn called for garbage") }); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Publish(subject, []byte("not json")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-errs:
	case <-time.After(2 * time.Second):
		t.Fatal("OnError not called")
	}
	_ = c.Flush()
}

// Cancelling while the connection drains makes Unsubscribe fail with ErrConnectionDraining,
// which is reported through OnError.
func TestIntegrationUnsubscribeErrorReported(t *testing.T) {
	c := conn(t)
	subject := subjectName()
	errs := make(chan error, 4)
	inv, _ := New(Config{Conn: c, Subject: subject, OnError: func(err error) { errs <- err }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	if err := inv.Subscribe(ctx, func(cachex.Invalidation) {
		close(entered)
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	pub := conn(t)
	other, _ := New(Config{Conn: pub, Subject: subject})
	if err := other.Publish(context.Background(), cachex.Invalidation{Keys: []string{"k"}}); err != nil {
		t.Fatal(err)
	}
	_ = pub.Flush()
	<-entered // the handler holds the drain open
	if err := c.Drain(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-errs:
		if !errors.Is(err, nats.ErrConnectionDraining) || !strings.Contains(err.Error(), "unsubscribe") {
			t.Fatalf("got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unsubscribe error not reported")
	}
	close(release)
}
