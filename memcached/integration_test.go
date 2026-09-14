//go:build integration

package memcached

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/storetest"
)

var seq atomic.Int64

func TestIntegrationStore(t *testing.T) {
	addr := os.Getenv("MEMCACHED_ADDR")
	if addr == "" {
		t.Skip("MEMCACHED_ADDR not set")
	}
	storetest.Run(t, func(t *testing.T) cachex.Store {
		s, err := New(Config{
			Servers:   []string{addr},
			Timeout:   time.Second,
			KeyPrefix: fmt.Sprintf("it:%d:%d:", time.Now().UnixNano(), seq.Add(1)),
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}, nil)
}
