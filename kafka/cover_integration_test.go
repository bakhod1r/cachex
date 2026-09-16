//go:build integration

package cachexkafka

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestIntegrationMalformedRecordAndFetchErrors(t *testing.T) {
	bs := brokers(t)
	tp := topic(t, bs)
	var mu sync.Mutex
	var decodeErrs, fetchErrs int
	inv, err := New(Config{Brokers: bs, Topic: tp, OnError: func(err error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.Contains(err.Error(), "decode"):
			decodeErrs++
		case strings.Contains(err.Error(), "fetch"):
			fetchErrs++
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer inv.Close()
	silent, err := New(Config{Brokers: bs, Topic: tp}) // default OnError drops errors
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, i := range []*Invalidator{inv, silent} {
		if err := i.Subscribe(ctx, func(cachex.Invalidation) { t.Error("fn called for garbage") }); err != nil {
			t.Fatal(err)
		}
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(bs...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
	defer pcancel()
	if err := cl.ProduceSync(pctx, &kgo.Record{Topic: tp, Value: []byte("not json")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { mu.Lock(); defer mu.Unlock(); return decodeErrs > 0 })

	// Deleting the topic under the consumers makes their fetches fail.
	if _, err := kadm.NewClient(cl).DeleteTopics(pctx, tp); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, func() bool { mu.Lock(); defer mu.Unlock(); return fetchErrs > 0 })
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 30s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
