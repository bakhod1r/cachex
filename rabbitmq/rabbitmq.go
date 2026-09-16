// Package cachexrabbitmq is a cachex.Invalidator over a RabbitMQ fanout exchange.
//
// Delivery is at-most-once and best effort: messages are published transient and consumed with
// auto-ack into per-subscriber exclusive, auto-delete, non-durable queues. amqp091-go does not
// reconnect; if a subscriber's connection or channel drops, its queue and any messages routed to
// it are lost and delivery stops (the close is reported via Config.OnError). The cache's L1 TTL
// still bounds staleness for lost messages. Duplicate or reordered messages are harmless because
// an invalidation only drops L1 copies.
package cachexrabbitmq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/bakhod1r/cachex"
	amqp "github.com/rabbitmq/amqp091-go"
)

// DefaultExchange is used when Config.Exchange is empty.
const DefaultExchange = "cachex.invalidate"

// Config configures an Invalidator.
type Config struct {
	Conn     *amqp.Connection // required
	Exchange string           // default "cachex.invalidate"
	OnError  func(error)      // optional: decode, cancel and channel/connection close errors
}

// Invalidator publishes and receives cachex invalidations on a RabbitMQ fanout exchange.
type Invalidator struct {
	openChannel func() (channel, error) // conn.Channel; replaced by fakes in tests
	exchange    string
	onError     func(error)
	origin      string

	mu    sync.Mutex // guards pubCh; amqp channels are not goroutine-safe
	pubCh channel
}

// channel is the subset of *amqp.Channel the Invalidator uses.
type channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Cancel(consumer string, noWait bool) error
	Close() error
}

var _ cachex.Invalidator = (*Invalidator)(nil)

// newInvalidator validates c and fills defaults without any network I/O.
func newInvalidator(c Config) (*Invalidator, error) {
	if c.Conn == nil {
		return nil, errors.New("cachexrabbitmq: nil conn")
	}
	if c.Exchange == "" {
		c.Exchange = DefaultExchange
	}
	if c.OnError == nil {
		c.OnError = func(error) {}
	}
	var b [16]byte
	_, _ = rand.Read(b[:]) // never fails since Go 1.24
	conn := c.Conn
	return &Invalidator{
		openChannel: func() (channel, error) { return conn.Channel() },
		exchange:    c.Exchange,
		onError:     c.OnError,
		origin:      hex.EncodeToString(b[:]),
	}, nil
}

// New validates c, opens a publish channel and declares a durable fanout exchange. The returned
// Invalidator has a random origin id. Call Close to release the publish channel.
func New(c Config) (*Invalidator, error) {
	i, err := newInvalidator(c)
	if err != nil {
		return nil, err
	}
	if err := i.declare(); err != nil {
		return nil, err
	}
	return i, nil
}

// declare opens the publish channel and declares the exchange.
func (i *Invalidator) declare() error {
	ch, err := i.openChannel()
	if err != nil {
		return fmt.Errorf("cachexrabbitmq: open channel: %w", err)
	}
	if err := ch.ExchangeDeclare(i.exchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		return fmt.Errorf("cachexrabbitmq: declare exchange: %w", err)
	}
	i.pubCh = ch
	return nil
}

// Close closes the publish channel. Subscriptions are stopped by cancelling their contexts.
func (i *Invalidator) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.pubCh.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("cachexrabbitmq: close: %w", err)
	}
	return nil
}

type wireMsg struct {
	Keys      []string `json:"k,omitempty"`
	Namespace string   `json:"ns,omitempty"`
	Origin    string   `json:"o"`
}

func encode(msg cachex.Invalidation, origin string) ([]byte, error) {
	return json.Marshal(wireMsg{Keys: msg.Keys, Namespace: msg.Namespace, Origin: origin})
}

// decode parses payload; ok is false for messages from self (already applied locally).
func decode(payload []byte, self string) (msg cachex.Invalidation, ok bool, err error) {
	var w wireMsg
	if err := json.Unmarshal(payload, &w); err != nil {
		return cachex.Invalidation{}, false, fmt.Errorf("cachexrabbitmq: decode: %w", err)
	}
	if w.Origin == self {
		return cachex.Invalidation{}, false, nil
	}
	return cachex.Invalidation{Keys: w.Keys, Namespace: w.Namespace}, true, nil
}

// Publish sends msg to the exchange as a transient application/json message (at-most-once, no
// publisher confirms, no retry). ctx bounds the publish.
func (i *Invalidator) Publish(ctx context.Context, msg cachex.Invalidation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b, _ := encode(msg, i.origin) // marshalling strings cannot fail
	i.mu.Lock()
	defer i.mu.Unlock()
	err := i.pubCh.PublishWithContext(ctx, i.exchange, "", false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Transient,
		Body:         b,
	})
	if err != nil {
		return fmt.Errorf("cachexrabbitmq: publish: %w", err)
	}
	return nil
}

// Subscribe opens its own channel, declares a server-named exclusive auto-delete queue, binds it
// to the exchange and starts consuming before returning, so messages published after Subscribe
// returns are delivered. fn is called from one delivery goroutine per Subscribe call until ctx is
// done; then the consumer is cancelled and the channel closed. Multiple calls are allowed.
func (i *Invalidator) Subscribe(ctx context.Context, fn func(cachex.Invalidation)) error {
	if fn == nil {
		return errors.New("cachexrabbitmq: nil fn")
	}
	ch, err := i.openChannel()
	if err != nil {
		return fmt.Errorf("cachexrabbitmq: open channel: %w", err)
	}
	fail := func(op string, err error) error {
		_ = ch.Close()
		return fmt.Errorf("cachexrabbitmq: %s: %w", op, err)
	}
	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return fail("declare queue", err)
	}
	if err := ch.QueueBind(q.Name, "", i.exchange, false, nil); err != nil {
		return fail("bind queue", err)
	}
	var tb [8]byte
	_, _ = rand.Read(tb[:])
	tag := "cachex-" + hex.EncodeToString(tb[:])
	deliveries, err := ch.Consume(q.Name, tag, true, true, false, false, nil)
	if err != nil {
		return fail("consume", err)
	}
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))

	go func() {
		<-ctx.Done()
		if err := ch.Cancel(tag, false); err != nil && !errors.Is(err, amqp.ErrClosed) {
			i.onError(fmt.Errorf("cachexrabbitmq: cancel consumer: %w", err))
		}
		if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			i.onError(fmt.Errorf("cachexrabbitmq: close channel: %w", err))
		}
	}()
	go func() {
		for d := range deliveries {
			msg, ok, err := decode(d.Body, i.origin)
			if err != nil {
				i.onError(err)
				continue
			}
			if ok && ctx.Err() == nil {
				fn(msg)
			}
		}
		// A nil value (closed notify channel) means a graceful Close by us.
		if e, ok := <-closed; ok && e != nil {
			i.onError(fmt.Errorf("cachexrabbitmq: subscription channel closed: %w", e))
		}
	}()
	return nil
}
