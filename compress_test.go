package cachex_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/internal/envelope"
)

// rle is a dependency-free test codec: (count, byte) pairs. Repetitive data shrinks,
// random data doubles, which exercises the "store raw when not smaller" rule.
type rle struct{ id byte }

func (r rle) ID() byte { return r.id }

func (rle) Compress(dst, src []byte) ([]byte, error) {
	for i := 0; i < len(src); {
		j := i
		for j < len(src) && src[j] == src[i] && j-i < 255 {
			j++
		}
		dst = append(dst, byte(j-i), src[i])
		i = j
	}
	return dst, nil
}

func (rle) Decompress(dst, src []byte) ([]byte, error) {
	if len(src)%2 != 0 {
		return nil, errors.New("rle: odd length")
	}
	for i := 0; i < len(src); i += 2 {
		if src[i] == 0 {
			return nil, errors.New("rle: zero run")
		}
		for range src[i] {
			dst = append(dst, src[i+1])
		}
	}
	return dst, nil
}

var codec = rle{id: 0x7F}

func rawL2(t *testing.T, e env, key string) envelope.Entry {
	t.Helper()
	raw, err := e.l2.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	ent, err := envelope.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return ent
}

func randomBytes(n int) []byte {
	r := rand.New(rand.NewPCG(1, 2))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func TestCompressionOptionValidation(t *testing.T) {
	cases := map[string]cachex.Option{
		"nil codec":            cachex.WithCompression(nil, 0),
		"zero id":              cachex.WithCompression(rle{id: 0}, 0),
		"negative min":         cachex.WithCompression(codec, -1),
		"nil decompressor":     cachex.WithDecompressors(nil),
		"zero id decompressor": cachex.WithDecompressors(rle{id: 0}),
	}
	for name, opt := range cases {
		if _, err := cachex.New(opt); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestCompressionRoundTripThroughL2(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 16))
	val := bytes.Repeat([]byte("a"), 1000)
	if err := e.c.Set(ctx, "k", val, time.Minute); err != nil {
		t.Fatal(err)
	}
	ent := rawL2(t, e, "k")
	if ent.Codec != codec.ID() || len(ent.Value) >= len(val) {
		t.Fatalf("L2 not compressed: codec %d len %d", ent.Codec, len(ent.Value))
	}
	// Writer's L1 holds the uncompressed value.
	if err := e.c.GetView(ctx, "k", func(v []byte) error {
		if !bytes.Equal(v, val) {
			t.Fatal("L1 view differs")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	other := secondNode(t, e, cachex.WithCompression(codec, 16))
	for range 2 { // L2 read, then L1 backfill
		if v, err := other.Get(ctx, "k"); err != nil || !bytes.Equal(v, val) {
			t.Fatalf("got len %d %v", len(v), err)
		}
	}
	if s := other.Stats(); s.L2Hits != 1 || s.L1Hits != 1 || s.DecodeErrors != 0 {
		t.Fatalf("stats %+v", s)
	}
}

func TestCompressionMultiAndLoadPaths(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 16))
	big := func(c byte) []byte { return bytes.Repeat([]byte{c}, 500) }
	if err := e.c.SetMulti(ctx, map[string][]byte{"a": big('a'), "b": big('b')}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := e.c.GetOrLoad(ctx, "c", time.Minute, func(context.Context) ([]byte, error) { return big('c'), nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := e.c.GetOrLoadMulti(ctx, []string{"d"}, time.Minute, func(_ context.Context, keys []string) (map[string][]byte, error) {
		return map[string][]byte{"d": big('d')}, nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b", "c", "d"} {
		if ent := rawL2(t, e, k); ent.Codec != codec.ID() {
			t.Fatalf("%s stored raw", k)
		}
	}
	other := secondNode(t, e, cachex.WithCompression(codec, 16))
	got, err := other.GetMulti(ctx, []string{"a", "b", "c", "d"})
	if err != nil || len(got) != 4 {
		t.Fatalf("GetMulti %d %v", len(got), err)
	}
	for k, v := range got {
		if !bytes.Equal(v, big(k[0])) {
			t.Fatalf("%s wrong value", k)
		}
	}
	// GetOrLoad on another node must read the compressed value, not reload.
	third := secondNode(t, e, cachex.WithCompression(codec, 16))
	v, err := third.GetOrLoad(ctx, "c", time.Minute, func(context.Context) ([]byte, error) {
		t.Fatal("loader ran")
		return nil, nil
	})
	if err != nil || !bytes.Equal(v, big('c')) {
		t.Fatalf("GetOrLoad %v", err)
	}
}

func TestCompressionBelowMinSizeStoredRaw(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 100))
	val := bytes.Repeat([]byte("a"), 99)
	_ = e.c.Set(ctx, "k", val, time.Minute)
	if ent := rawL2(t, e, "k"); ent.Codec != 0 || !bytes.Equal(ent.Value, val) {
		t.Fatalf("want raw, codec %d", ent.Codec)
	}
}

func TestCompressionIncompressibleStoredRaw(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 0))
	val := randomBytes(1000)
	_ = e.c.Set(ctx, "k", val, time.Minute)
	if ent := rawL2(t, e, "k"); ent.Codec != 0 || !bytes.Equal(ent.Value, val) {
		t.Fatalf("want raw, codec %d", ent.Codec)
	}
	if v, err := secondNode(t, e).Get(ctx, "k"); err != nil || !bytes.Equal(v, val) {
		t.Fatalf("read %v", err)
	}
}

func TestCompressingNodeReadsOldRawPayload(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 0))
	now := e.clock.Now().UnixNano()
	val := bytes.Repeat([]byte("x"), 1000)
	raw := envelope.Encode(envelope.Entry{Value: val, StoredAt: now, ExpireAt: now + int64(time.Minute)})
	_ = e.l2.Set(ctx, "k", raw, time.Minute)
	if v, err := e.c.Get(ctx, "k"); err != nil || !bytes.Equal(v, val) {
		t.Fatalf("got %v", err)
	}
}

func TestDecodeOnlyNodeReadsCompressedPayload(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 0))
	val := bytes.Repeat([]byte("z"), 1000)
	_ = e.c.Set(ctx, "k", val, time.Minute)
	reader := secondNode(t, e, cachex.WithDecompressors(codec))
	if v, err := reader.Get(ctx, "k"); err != nil || !bytes.Equal(v, val) {
		t.Fatalf("got %v", err)
	}
	// Its own writes stay raw.
	_ = reader.Set(ctx, "k2", val, time.Minute)
	if ent := rawL2(t, e, "k2"); ent.Codec != 0 {
		t.Fatal("decode-only node compressed a write")
	}
}

func TestUnknownCodecIsMissAndDecodeError(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 0))
	val := bytes.Repeat([]byte("z"), 1000)
	_ = e.c.Set(ctx, "k", val, time.Minute)
	reader := secondNode(t, e) // no codec
	if _, err := reader.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("Get want miss, got %v", err)
	}
	if got, err := reader.GetMulti(ctx, []string{"k"}); err != nil || len(got) != 0 {
		t.Fatalf("GetMulti %v %v", got, err)
	}
	if s := reader.Stats(); s.DecodeErrors != 2 || s.L2Hits != 0 {
		t.Fatalf("stats %+v", s)
	}
	// A loader repopulates the key (miss semantics).
	v, err := reader.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("new"), nil })
	if err != nil || string(v) != "new" {
		t.Fatalf("GetOrLoad %q %v", v, err)
	}
}

func TestCorruptCompressedPayloadIsMiss(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 0))
	now := e.clock.Now().UnixNano()
	raw := envelope.Encode(envelope.Entry{Value: []byte{0, 'x', 1}, Codec: codec.ID(), StoredAt: now, ExpireAt: now + int64(time.Minute)})
	_ = e.l2.Set(ctx, "k", raw, time.Minute)
	if _, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("want miss, got %v", err)
	}
	if got, _ := e.c.GetMulti(ctx, []string{"k"}); len(got) != 0 {
		t.Fatal("GetMulti returned corrupt key")
	}
	if s := e.c.Stats(); s.DecodeErrors != 2 {
		t.Fatalf("DecodeErrors %d", s.DecodeErrors)
	}
}

func TestCompressionNeverTouchesMarkersOrTombstones(t *testing.T) {
	e := newEnv(t, cachex.WithCompression(codec, 0), cachex.WithNegativeTTL(time.Minute))
	_ = e.c.Set(ctx, "k", bytes.Repeat([]byte("a"), 1000), time.Minute)
	if err := e.c.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if ent := rawL2(t, e, "k"); ent.Codec != 0 || ent.Flags&envelope.FlagDeleted == 0 {
		t.Fatalf("marker %+v", ent)
	}
	_, _ = e.c.GetOrLoad(ctx, "gone", time.Minute, func(context.Context) ([]byte, error) { return nil, cachex.ErrNotFound })
	if ent := rawL2(t, e, "gone"); ent.Codec != 0 || ent.Flags&envelope.FlagTombstone == 0 {
		t.Fatalf("tombstone %+v", ent)
	}
	ns, err := e.c.Namespace("users")
	if err != nil {
		t.Fatal(err)
	}
	_ = ns.Set(ctx, "u", bytes.Repeat([]byte("b"), 1000), time.Minute)
	if err := ns.Invalidate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ns.Get(ctx, "u"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("namespace invalidate: %v", err)
	}
}
