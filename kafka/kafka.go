// Package cachexkafka is a cachex.Invalidator over a Kafka topic (franz-go).
//
// Invalidation is a broadcast: every process must see every message. Subscribe therefore does not
// join a consumer group (a group would split partitions between processes and each message would
// reach only one of them). Each Subscribe call runs its own group-less consumer that starts at the
// end of every partition; older invalidations are irrelevant to a fresh L1.
//
// Delivery is at-least-once while a subscriber is running: producer retries can duplicate a
// record, and duplicate or reordered messages are harmless because an invalidation only drops L1
// copies. Messages published while a process is not subscribed (down, restarting) are not
// replayed to it; the cache's L1 TTL still bounds staleness for those. Records are keyed by the
// publisher's origin id, so ordering holds per publisher within its partition; there is no
// ordering across publishers. Partitions added to the topic after Subscribe returns are not
// consumed by that subscription.
package cachexkafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/bakhod1r/cachex"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultTopic is used when Config.Topic is empty.
const DefaultTopic = "cachex-invalidate"

// Config configures an Invalidator.
type Config struct {
	Brokers       []string    // required
	Topic         string      // default "cachex-invalidate"; must exist before Subscribe
	OnError       func(error) // optional: decode/fetch errors
	ClientOptions []kgo.Opt   // extra client options (TLS, SASL, ...), applied to every client
}

// Invalidator publishes and receives cachex invalidations on a Kafka topic.
type Invalidator struct {
	brokers []string
	topic   string
	onError func(error)
	opts    []kgo.Opt
	origin  string

	mu     sync.Mutex
	closed bool
	prod   *kgo.Client
	subs   map[*subscription]struct{}
}

// subscription is owned by its poll goroutine, which is the only caller of client.Close:
// concurrent kgo.Client.Close calls race inside franz-go (observed with -race on v1.21.7).
type subscription struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var _ cachex.Invalidator = (*Invalidator)(nil)

// New validates c, creates the producer client and returns an Invalidator with a random origin
// id. Client creation does not contact the brokers.
func New(c Config) (*Invalidator, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("cachexkafka: no brokers")
	}
	if c.Topic == "" {
		c.Topic = DefaultTopic
	}
	if c.OnError == nil {
		c.OnError = func(error) {}
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("cachexkafka: origin id: %w", err)
	}
	i := &Invalidator{
		brokers: c.Brokers,
		topic:   c.Topic,
		onError: c.OnError,
		opts:    c.ClientOptions,
		origin:  hex.EncodeToString(b[:]),
		subs:    map[*subscription]struct{}{},
	}
	prod, err := i.newClient(kgo.DefaultProduceTopic(c.Topic))
	if err != nil {
		return nil, err
	}
	i.prod = prod
	return i, nil
}

func (i *Invalidator) newClient(extra ...kgo.Opt) (*kgo.Client, error) {
	opts := append([]kgo.Opt{kgo.SeedBrokers(i.brokers...)}, i.opts...)
	cl, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("cachexkafka: client: %w", err)
	}
	return cl, nil
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
		return cachex.Invalidation{}, false, fmt.Errorf("cachexkafka: decode: %w", err)
	}
	if w.Origin == self {
		return cachex.Invalidation{}, false, nil
	}
	return cachex.Invalidation{Keys: w.Keys, Namespace: w.Namespace}, true, nil
}

// Publish produces msg synchronously (key = origin id, value = JSON) and returns once the broker
// acknowledged it or ctx is done. The client's internal retries may produce duplicates, which are
// harmless. No retry is done beyond the client's.
func (i *Invalidator) Publish(ctx context.Context, msg cachex.Invalidation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b, err := encode(msg, i.origin)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: i.topic, Key: []byte(i.origin), Value: b}
	if err := i.prod.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("cachexkafka: publish: %w", err)
	}
	return nil
}

// Subscribe starts a group-less consumer and returns once it is positioned.
//
// Positioning: Subscribe lists the current end offset of every partition (ListOffsets, bounded by
// ctx) and assigns the consumer to exactly those offsets with kgo.ConsumePartitions. Any record
// acknowledged after Subscribe returns has an offset >= the listed end, so it is delivered. This is
// used instead of kgo.ConsumeTopics with a reset-to-end offset, because that resolves "end" lazily
// at the first fetch, after Subscribe would have returned, and could skip records published in
// between. The topic must exist; otherwise Subscribe returns an error.
//
// fn is called from the subscription's poll goroutine (one per Subscribe call) until ctx is done
// or the Invalidator is closed, then the consumer client is closed. Multiple calls are allowed.
func (i *Invalidator) Subscribe(ctx context.Context, fn func(cachex.Invalidation)) error {
	if fn == nil {
		return errors.New("cachexkafka: nil fn")
	}
	ends, err := kadm.NewClient(i.prod).ListEndOffsets(ctx, i.topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return fmt.Errorf("cachexkafka: list end offsets: %w", err)
	}
	parts := map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) {
		parts[o.Partition] = kgo.NewOffset().At(o.Offset)
	})
	if len(parts) == 0 {
		return fmt.Errorf("cachexkafka: topic %q has no partitions", i.topic)
	}
	cl, err := i.newClient(kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{i.topic: parts}))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	sub := &subscription{cancel: cancel, done: make(chan struct{})}
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		cancel()
		cl.Close()
		return errors.New("cachexkafka: closed")
	}
	i.subs[sub] = struct{}{}
	i.mu.Unlock()

	go i.consume(ctx, sub, cl, fn)
	return nil
}

func (i *Invalidator) consume(ctx context.Context, sub *subscription, cl *kgo.Client, fn func(cachex.Invalidation)) {
	defer func() {
		i.mu.Lock()
		delete(i.subs, sub)
		i.mu.Unlock()
		cl.Close()
		sub.cancel()
		close(sub.done)
	}()
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				i.onError(fmt.Errorf("cachexkafka: fetch: %w", err))
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			msg, ok, err := decode(r.Value, i.origin)
			if err != nil {
				i.onError(err)
				return
			}
			if ok && ctx.Err() == nil {
				fn(msg)
			}
		})
	}
}

// Close stops all running subscriptions, waits for their poll goroutines to exit (fn is not
// called after Close returns), then closes the producer. It is idempotent.
func (i *Invalidator) Close() error {
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return nil
	}
	i.closed = true
	subs := make([]*subscription, 0, len(i.subs))
	for s := range i.subs {
		subs = append(subs, s)
	}
	i.mu.Unlock()
	for _, s := range subs {
		s.cancel()
	}
	for _, s := range subs {
		<-s.done
	}
	i.prod.Close()
	return nil
}
