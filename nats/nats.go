// Package cachexnats is a cachex.Invalidator over core NATS publish/subscribe.
//
// Delivery is at-most-once and best effort (core NATS, not JetStream). While the connection is
// reconnecting, nats.go buffers outgoing publishes (up to its reconnect buffer size), but messages
// published while a subscriber is disconnected are lost; the cache's L1 TTL still bounds staleness
// for those. Duplicate or reordered messages are harmless because an invalidation only drops L1
// copies.
package cachexnats

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bakhod1r/cachex"
	"github.com/nats-io/nats.go"
)

// DefaultSubject is used when Config.Subject is empty.
const DefaultSubject = "cachex.invalidate"

// Config configures an Invalidator.
type Config struct {
	Conn    *nats.Conn  // required
	Subject string      // default "cachex.invalidate"
	OnError func(error) // optional: decode/subscription errors
}

// Invalidator publishes and receives cachex invalidations on a NATS subject.
type Invalidator struct {
	conn    *nats.Conn
	subject string
	onError func(error)
	origin  string
}

var _ cachex.Invalidator = (*Invalidator)(nil)

// New validates c and returns an Invalidator with a random origin id.
func New(c Config) (*Invalidator, error) {
	if c.Conn == nil {
		return nil, errors.New("cachexnats: nil conn")
	}
	if c.Subject == "" {
		c.Subject = DefaultSubject
	}
	if c.OnError == nil {
		c.OnError = func(error) {}
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("cachexnats: origin id: %w", err)
	}
	return &Invalidator{conn: c.Conn, subject: c.Subject, onError: c.OnError, origin: hex.EncodeToString(b[:])}, nil
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
		return cachex.Invalidation{}, false, fmt.Errorf("cachexnats: decode: %w", err)
	}
	if w.Origin == self {
		return cachex.Invalidation{}, false, nil
	}
	return cachex.Invalidation{Keys: w.Keys, Namespace: w.Namespace}, true, nil
}

// Publish sends msg on the subject with core NATS (at-most-once). conn.Publish only enqueues into
// the client's write buffer and does not block on the network, so ctx is only checked up front.
// No retry is done here.
func (i *Invalidator) Publish(ctx context.Context, msg cachex.Invalidation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b, err := encode(msg, i.origin)
	if err != nil {
		return err
	}
	if err := i.conn.Publish(i.subject, b); err != nil {
		return fmt.Errorf("cachexnats: publish: %w", err)
	}
	return nil
}

// Subscribe subscribes and flushes the connection before returning, so the subscription is
// registered server-side and messages published after Subscribe returns are delivered. If ctx has
// a deadline it bounds the flush. fn is called from the subscription's delivery goroutine (one per
// Subscribe call) until ctx is done, then the subscription is removed. Multiple calls are allowed.
func (i *Invalidator) Subscribe(ctx context.Context, fn func(cachex.Invalidation)) error {
	if fn == nil {
		return errors.New("cachexnats: nil fn")
	}
	sub, err := i.conn.Subscribe(i.subject, func(m *nats.Msg) {
		msg, ok, err := decode(m.Data, i.origin)
		if err != nil {
			i.onError(err)
			return
		}
		if ok && ctx.Err() == nil {
			fn(msg)
		}
	})
	if err != nil {
		return fmt.Errorf("cachexnats: subscribe: %w", err)
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		err = i.conn.FlushWithContext(ctx)
	} else {
		err = i.conn.Flush()
	}
	if err != nil {
		_ = sub.Unsubscribe()
		return fmt.Errorf("cachexnats: subscribe flush: %w", err)
	}
	go func() {
		<-ctx.Done()
		if err := sub.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) && !errors.Is(err, nats.ErrBadSubscription) {
			i.onError(fmt.Errorf("cachexnats: unsubscribe: %w", err))
		}
	}()
	return nil
}
