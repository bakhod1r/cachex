// Package cachexredis is a cachex.Invalidator over Redis pub/sub.
//
// Delivery is at-most-once and best effort. go-redis PubSub reconnects and resubscribes
// automatically, but messages published while a subscriber is disconnected are lost; the cache's
// L1 TTL still bounds staleness for those. Duplicate or reordered messages are harmless because
// an invalidation only drops L1 copies.
package cachexredis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/bakhod1r/cachex"
	"github.com/redis/go-redis/v9"
)

// DefaultChannel is used when Config.Channel is empty.
const DefaultChannel = "cachex:invalidate"

// Config configures an Invalidator.
type Config struct {
	Client  redis.UniversalClient // required
	Channel string                // default "cachex:invalidate"
	OnError func(error)           // optional: decode/subscription errors
}

// Invalidator publishes and receives cachex invalidations on a Redis channel.
type Invalidator struct {
	client  redis.UniversalClient
	channel string
	onError func(error)
	origin  string
}

var _ cachex.Invalidator = (*Invalidator)(nil)

// New validates c and returns an Invalidator with a random origin id.
func New(c Config) (*Invalidator, error) {
	if c.Client == nil {
		return nil, errors.New("cachexredis: nil client")
	}
	if c.Channel == "" {
		c.Channel = DefaultChannel
	}
	if c.OnError == nil {
		c.OnError = func(error) {}
	}
	var b [16]byte
	_, _ = rand.Read(b[:]) // never fails since Go 1.24
	return &Invalidator{client: c.Client, channel: c.Channel, onError: c.OnError, origin: hex.EncodeToString(b[:])}, nil
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
func decode(payload, self string) (msg cachex.Invalidation, ok bool, err error) {
	var w wireMsg
	if err := json.Unmarshal([]byte(payload), &w); err != nil {
		return cachex.Invalidation{}, false, fmt.Errorf("cachexredis: decode: %w", err)
	}
	if w.Origin == self {
		return cachex.Invalidation{}, false, nil
	}
	return cachex.Invalidation{Keys: w.Keys, Namespace: w.Namespace}, true, nil
}

// Publish PUBLISHes msg to the channel. Bound it with ctx; no retry is done here.
func (i *Invalidator) Publish(ctx context.Context, msg cachex.Invalidation) error {
	b, _ := encode(msg, i.origin) // marshalling strings cannot fail
	if err := i.client.Publish(ctx, i.channel, b).Err(); err != nil {
		return fmt.Errorf("cachexredis: publish: %w", err)
	}
	return nil
}

// Subscribe SUBSCRIBEs and waits for the confirmation before returning, so messages published
// after Subscribe returns are delivered. fn is called from one goroutine per Subscribe call until
// ctx is done, then the PubSub is closed. Multiple calls are allowed.
func (i *Invalidator) Subscribe(ctx context.Context, fn func(cachex.Invalidation)) error {
	if fn == nil {
		return errors.New("cachexredis: nil fn")
	}
	ps := i.client.Subscribe(ctx, i.channel)
	if _, err := ps.Receive(ctx); err != nil {
		_ = ps.Close()
		return fmt.Errorf("cachexredis: subscribe: %w", err)
	}
	ch := ps.Channel()
	go func() {
		defer func() { _ = ps.Close() }()
		for {
			select {
			case <-ctx.Done():
				return
			case m, open := <-ch:
				if !open {
					return
				}
				msg, ok, err := decode(m.Payload, i.origin)
				if err != nil {
					i.onError(err)
					continue
				}
				if ok && ctx.Err() == nil {
					fn(msg)
				}
			}
		}
	}()
	return nil
}
