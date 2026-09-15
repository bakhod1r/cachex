package cachexnats

import (
	"reflect"
	"testing"

	"github.com/bakhod1r/cachex"
	"github.com/nats-io/nats.go"
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
		t.Fatal("expected error for nil conn")
	}
	c := &nats.Conn{} // never used for I/O in this test
	a, err := New(Config{Conn: c})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := New(Config{Conn: c})
	if a.subject != DefaultSubject || a.origin == "" || a.origin == b.origin {
		t.Fatalf("subject=%q origins %q %q", a.subject, a.origin, b.origin)
	}
}
