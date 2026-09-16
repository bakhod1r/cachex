package cachexrabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	amqp "github.com/rabbitmq/amqp091-go"
)

var errFake = errors.New("fake")

// fakeChannel is a channel whose calls fail when the matching field says so.
type fakeChannel struct {
	declareErr, queueErr, bindErr, consumeErr, publishErr, cancelErr, closeErr error

	deliveries chan amqp.Delivery
	closeOnce  sync.Once
	closed     chan *amqp.Error // what NotifyClose delivers once deliveries end
}

func newFake() *fakeChannel {
	return &fakeChannel{deliveries: make(chan amqp.Delivery, 4), closed: make(chan *amqp.Error, 1)}
}

func (f *fakeChannel) ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error {
	return f.declareErr
}

func (f *fakeChannel) QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: "q"}, f.queueErr
}

func (f *fakeChannel) QueueBind(string, string, string, bool, amqp.Table) error { return f.bindErr }

func (f *fakeChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	return f.deliveries, f.consumeErr
}

func (f *fakeChannel) NotifyClose(chan *amqp.Error) chan *amqp.Error { return f.closed }

func (f *fakeChannel) PublishWithContext(context.Context, string, string, bool, bool, amqp.Publishing) error {
	return f.publishErr
}

func (f *fakeChannel) Cancel(string, bool) error { return f.cancelErr }

func (f *fakeChannel) Close() error {
	f.closeOnce.Do(func() { close(f.deliveries) })
	return f.closeErr
}

func fakeInvalidator(t *testing.T, ch *fakeChannel, openErr error) (*Invalidator, chan error) {
	t.Helper()
	errs := make(chan error, 8)
	i, err := newInvalidator(Config{Conn: &amqp.Connection{}, OnError: func(err error) { errs <- err }})
	if err != nil {
		t.Fatal(err)
	}
	i.openChannel = func() (channel, error) {
		if openErr != nil {
			return nil, openErr
		}
		return ch, nil
	}
	return i, errs
}

func wantErr(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), substr) {
		t.Fatalf("want error containing %q, got %v", substr, err)
	}
}

func TestDeclareErrors(t *testing.T) {
	i, _ := fakeInvalidator(t, nil, errFake)
	wantErr(t, i.declare(), "open channel")
	i, _ = fakeInvalidator(t, &fakeChannel{declareErr: errFake, deliveries: make(chan amqp.Delivery)}, nil)
	wantErr(t, i.declare(), "declare exchange")
}

func TestCloseAndPublishErrors(t *testing.T) {
	ch := newFake()
	i, _ := fakeInvalidator(t, ch, nil)
	if err := i.declare(); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := i.Publish(cctx, cachex.Invalidation{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	ch.publishErr = errFake
	wantErr(t, i.Publish(context.Background(), cachex.Invalidation{}), "publish")
	ch.closeErr = amqp.ErrClosed
	if err := i.Close(); err != nil {
		t.Fatalf("ErrClosed must be ignored: %v", err)
	}
	ch.closeErr = errFake
	wantErr(t, i.Close(), "close")
}

func TestSubscribeSetupErrors(t *testing.T) {
	i, _ := fakeInvalidator(t, newFake(), nil)
	if err := i.Subscribe(context.Background(), nil); err == nil {
		t.Fatal("nil fn accepted")
	}
	fn := func(cachex.Invalidation) {}
	i, _ = fakeInvalidator(t, nil, errFake)
	wantErr(t, i.Subscribe(context.Background(), fn), "open channel")
	for substr, ch := range map[string]*fakeChannel{
		"declare queue": {queueErr: errFake},
		"bind queue":    {bindErr: errFake},
		"consume":       {consumeErr: errFake},
	} {
		ch.deliveries = make(chan amqp.Delivery)
		i, _ = fakeInvalidator(t, ch, nil)
		wantErr(t, i.Subscribe(context.Background(), fn), substr)
	}
}

func TestSubscribeDeliveryAndTeardownErrors(t *testing.T) {
	ch := newFake()
	ch.cancelErr, ch.closeErr = errFake, errFake
	i, errs := fakeInvalidator(t, ch, nil)
	got := make(chan cachex.Invalidation, 1)
	ctx, cancel := context.WithCancel(context.Background())
	if err := i.Subscribe(ctx, func(m cachex.Invalidation) { got <- m }); err != nil {
		t.Fatal(err)
	}
	body, _ := encode(cachex.Invalidation{Keys: []string{"k"}}, "peer")
	ch.deliveries <- amqp.Delivery{Body: []byte("not json")}
	ch.deliveries <- amqp.Delivery{Body: body}
	wantErr(t, recv(t, errs), "decode")
	if m := <-got; len(m.Keys) != 1 || m.Keys[0] != "k" {
		t.Fatalf("got %+v", m)
	}
	ch.closed <- &amqp.Error{Code: 320, Reason: "forced"}
	cancel()
	seen := map[string]bool{}
	for range 3 {
		err := recv(t, errs)
		for _, s := range []string{"cancel consumer", "close channel", "subscription channel closed"} {
			if strings.Contains(err.Error(), s) {
				seen[s] = true
			}
		}
	}
	if len(seen) != 3 {
		t.Fatalf("teardown errors seen: %v", seen)
	}
}

func TestSubscribeTeardownIgnoresClosed(t *testing.T) {
	ch := newFake()
	ch.cancelErr, ch.closeErr = amqp.ErrClosed, amqp.ErrClosed
	i, errs := fakeInvalidator(t, ch, nil)
	i.onError = func(error) {} // replaced after construction; default hook behaviour
	ctx, cancel := context.WithCancel(context.Background())
	if err := i.Subscribe(ctx, func(cachex.Invalidation) {}); err != nil {
		t.Fatal(err)
	}
	close(ch.closed) // clean close: nothing to report
	cancel()
	select {
	case err := <-errs:
		t.Fatalf("unexpected %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDefaultOnError(t *testing.T) {
	i, err := newInvalidator(Config{Conn: &amqp.Connection{}})
	if err != nil {
		t.Fatal(err)
	}
	i.onError(errFake) // no-op default must not panic
}

func recv(t *testing.T, errs chan error) error {
	t.Helper()
	select {
	case err := <-errs:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("no error reported")
		return nil
	}
}
