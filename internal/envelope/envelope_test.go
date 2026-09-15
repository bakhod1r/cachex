package envelope

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	cases := []Entry{
		{},
		{Value: []byte("hello"), ExpireAt: 100, StoredAt: 50, Delta: 7},
		{Flags: FlagTombstone, StoredAt: -1, Delta: -5},
		{Value: bytes.Repeat([]byte{0xff}, 4096), Flags: 0xff, ExpireAt: 1<<63 - 1, StoredAt: -1 << 63},
		{Value: []byte("zz"), Codec: 7, ExpireAt: 9},
	}
	for i, c := range cases {
		b := Encode(c)
		if len(b) != HeaderSize+len(c.Value) {
			t.Fatalf("%d: len %d", i, len(b))
		}
		got, err := Decode(b)
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if !bytes.Equal(got.Value, c.Value) || got.Flags != c.Flags || got.Codec != c.Codec || got.ExpireAt != c.ExpireAt ||
			got.StoredAt != c.StoredAt || got.Delta != c.Delta {
			t.Fatalf("%d: got %+v want %+v", i, got, c)
		}
	}
}

func TestDecodeNoCopy(t *testing.T) {
	b := Encode(Entry{Value: []byte("abc")})
	e, _ := Decode(b)
	b[HeaderSize] = 'z'
	if e.Value[0] != 'z' {
		t.Fatal("Value must alias input")
	}
}

func TestDecodeErrors(t *testing.T) {
	good := Encode(Entry{Value: []byte("payload")})
	mut := func(f func([]byte) []byte) []byte {
		c := append([]byte(nil), good...)
		return f(c)
	}
	cases := []struct {
		name string
		in   []byte
		want error
	}{
		{"nil", nil, ErrCorrupt},
		{"truncated header", good[:HeaderSize-1], ErrCorrupt},
		{"truncated payload", good[:len(good)-1], ErrCorrupt},
		{"trailing bytes", append(append([]byte(nil), good...), 0), ErrCorrupt},
		{"bad magic", mut(func(b []byte) []byte { b[0] = 0; return b }), ErrCorrupt},
		{"bad version", mut(func(b []byte) []byte { b[1] = 2; return b }), ErrVersion},
		{"len too big", mut(func(b []byte) []byte { b[4] = 0xff; return b }), ErrCorrupt},
	}
	for _, c := range cases {
		if _, err := Decode(c.in); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v want %v", c.name, err, c.want)
		}
	}
}

func TestExpiredRemaining(t *testing.T) {
	never := Entry{}
	if never.Expired(1 << 62) {
		t.Fatal("never expires")
	}
	if never.Remaining(5) != -1 {
		t.Fatal("never remaining must be -1")
	}
	e := Entry{ExpireAt: 1000}
	if e.Expired(999) || !e.Expired(1000) || !e.Expired(1001) {
		t.Fatal("expired boundary")
	}
	if e.Remaining(400) != 600*time.Nanosecond {
		t.Fatal("remaining")
	}
	if e.Remaining(1000) != 0 || e.Remaining(2000) != 0 {
		t.Fatal("expired remaining must be 0")
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(Encode(Entry{Value: []byte("x"), ExpireAt: 1, StoredAt: 2, Delta: 3}))
	f.Add(Encode(Entry{Flags: FlagTombstone}))
	f.Add([]byte{0xCA, 1, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		e, err := Decode(b)
		if err != nil {
			return
		}
		if !bytes.Equal(Encode(e), b) {
			t.Fatalf("re-encode mismatch")
		}
	})
}

func TestCodecByteIsHeaderByte3(t *testing.T) {
	// Byte 3 was "reserved 0" in the original format: raw payloads keep it zero, so old
	// entries decode as Codec 0, and old readers reject compressed entries as corrupt.
	if b := Encode(Entry{Value: []byte("v")}); b[3] != 0 {
		t.Fatalf("raw entry byte3 = %d", b[3])
	}
	b := Encode(Entry{Value: []byte("v"), Codec: 0x42})
	if b[3] != 0x42 {
		t.Fatalf("byte3 = %#x", b[3])
	}
	e, err := Decode(b)
	if err != nil || e.Codec != 0x42 {
		t.Fatalf("got %+v %v", e, err)
	}
}

func TestPutHeader(t *testing.T) {
	want := Entry{Value: []byte("payload"), Flags: FlagTombstone, Codec: 3, ExpireAt: 5, StoredAt: 6, Delta: 7}
	b := make([]byte, HeaderSize, HeaderSize+len(want.Value))
	b = append(b, want.Value...)
	PutHeader(b, want)
	if !bytes.Equal(b, Encode(want)) {
		t.Fatal("PutHeader differs from Encode")
	}
}
