package cachexredis

import (
	"reflect"
	"testing"

	"github.com/bakhod1r/cachex"
	"github.com/redis/go-redis/v9"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := cachex.Invalidation{Keys: []string{"a", "b"}, Namespace: "users"}
	b, err := encode(in, "origin-a")
	if err != nil {
		t.Fatal(err)
	}
	out, ok, err := decode(string(b), "origin-b")
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestDecodeFiltersOwnOrigin(t *testing.T) {
	b, _ := encode(cachex.Invalidation{Keys: []string{"a"}}, "self")
	if _, ok, err := decode(string(b), "self"); ok || err != nil {
		t.Fatalf("own message: ok=%v err=%v", ok, err)
	}
}

func TestDecodeInvalid(t *testing.T) {
	if _, ok, err := decode("{not json", "x"); ok || err == nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("expected error for nil client")
	}
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer func() { _ = c.Close() }()
	a, err := New(Config{Client: c})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := New(Config{Client: c})
	if a.channel != DefaultChannel || a.origin == "" || a.origin == b.origin {
		t.Fatalf("channel=%q origins %q %q", a.channel, a.origin, b.origin)
	}
}
