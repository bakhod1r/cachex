package cachexkafka

import (
	"context"
	"reflect"
	"testing"

	"github.com/bakhod1r/cachex"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := cachex.Invalidation{Keys: []string{"a", "b"}, Namespace: "users"}
	b, err := encode(in, "origin-a")
	if err != nil {
		t.Fatal(err)
	}
	out, ok, err := decode(b, "origin-b")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestWireFormat(t *testing.T) {
	b, _ := encode(cachex.Invalidation{Keys: []string{"a"}, Namespace: "n"}, "o1")
	if want := `{"k":["a"],"ns":"n","o":"o1"}`; string(b) != want {
		t.Fatalf("got %s want %s", b, want)
	}
}

func TestDecodeFiltersOwnOrigin(t *testing.T) {
	b, _ := encode(cachex.Invalidation{Keys: []string{"a"}}, "self")
	if _, ok, err := decode(b, "self"); ok || err != nil {
		t.Fatalf("own message: ok=%v err=%v", ok, err)
	}
}

func TestDecodeInvalid(t *testing.T) {
	if _, ok, err := decode([]byte("{not json"), "x"); ok || err == nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for no brokers")
	}
	// kgo.NewClient does not dial, so an unreachable address is fine here.
	a, err := New(Config{Brokers: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	b, _ := New(Config{Brokers: []string{"127.0.0.1:1"}})
	defer func() { _ = b.Close() }()
	if a.topic != DefaultTopic || a.origin == "" || a.origin == b.origin {
		t.Fatalf("topic=%q origins %q %q", a.topic, a.origin, b.origin)
	}
	if err := a.Subscribe(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil fn")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal("Close not idempotent:", err)
	}
}
