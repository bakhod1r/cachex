package cachexkafka

import (
	"context"
	"strings"
	"testing"

	"github.com/bakhod1r/cachex"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func unreachable(t *testing.T, opts ...kgo.Opt) *Invalidator {
	t.Helper()
	inv, err := New(Config{Brokers: []string{"127.0.0.1:1"}, Topic: "t", ClientOptions: opts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inv.Close() })
	return inv
}

func onePartition(context.Context, ...string) (kadm.ListedOffsets, error) {
	return kadm.ListedOffsets{"t": {0: {Topic: "t", Partition: 0, Offset: 5}}}, nil
}

func TestNewInvalidClientOptions(t *testing.T) {
	_, err := New(Config{Brokers: []string{"127.0.0.1:1"}, ClientOptions: []kgo.Opt{kgo.TransactionalID("")}})
	if err == nil || !strings.Contains(err.Error(), "cachexkafka: client") {
		t.Fatalf("want client error, got %v", err)
	}
}

func TestPublishCancelledAndClosed(t *testing.T) {
	inv := unreachable(t)
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := inv.Publish(cctx, cachex.Invalidation{}); err != context.Canceled {
		t.Fatalf("got %v", err)
	}
	_ = inv.Close()
	if err := inv.Publish(context.Background(), cachex.Invalidation{}); err == nil {
		t.Fatal("publish on closed client succeeded")
	}
}

func TestSubscribeNoPartitions(t *testing.T) {
	inv := unreachable(t)
	inv.listEnds = func(context.Context, ...string) (kadm.ListedOffsets, error) { return kadm.ListedOffsets{}, nil }
	if err := inv.Subscribe(context.Background(), func(cachex.Invalidation) {}); err == nil || !strings.Contains(err.Error(), "no partitions") {
		t.Fatalf("got %v", err)
	}
}

func TestSubscribeConsumerClientError(t *testing.T) {
	// Direct partition consuming is invalid for a group client, so only the consumer fails.
	inv := unreachable(t, kgo.ConsumerGroup("g"))
	inv.listEnds = onePartition
	if err := inv.Subscribe(context.Background(), func(cachex.Invalidation) {}); err == nil || !strings.Contains(err.Error(), "cachexkafka: client") {
		t.Fatalf("got %v", err)
	}
}

func TestSubscribeAfterClose(t *testing.T) {
	inv := unreachable(t)
	inv.listEnds = onePartition
	_ = inv.Close()
	if err := inv.Subscribe(context.Background(), func(cachex.Invalidation) {}); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("got %v", err)
	}
}
