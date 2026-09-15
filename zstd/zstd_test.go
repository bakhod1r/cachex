package cachexzstd_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
	cachexzstd "github.com/bakhod1r/cachex/zstd"
	"github.com/klauspost/compress/zstd"
)

func newCodec(t *testing.T, opts ...cachexzstd.Option) *cachexzstd.Codec {
	t.Helper()
	z, err := cachexzstd.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = z.Close() })
	return z
}

func TestRoundTripAppends(t *testing.T) {
	z := newCodec(t)
	if z.ID() != 1 {
		t.Fatalf("ID %d", z.ID())
	}
	src := bytes.Repeat([]byte("hello zstd "), 500)
	comp, err := z.Compress([]byte("HDR"), src)
	if err != nil || !bytes.HasPrefix(comp, []byte("HDR")) || len(comp) >= len(src) {
		t.Fatalf("compress len %d err %v", len(comp), err)
	}
	out, err := z.Decompress([]byte("hd"), comp[3:])
	if err != nil || !bytes.Equal(out[2:], src) || string(out[:2]) != "hd" {
		t.Fatalf("decompress %v", err)
	}
}

func TestCorruptInputErrors(t *testing.T) {
	z := newCodec(t)
	if _, err := z.Decompress(nil, []byte("not zstd at all")); err == nil {
		t.Fatal("want error")
	}
}

func TestMaxDecodedSize(t *testing.T) {
	big := bytes.Repeat([]byte{0}, 1<<20)
	comp, _ := newCodec(t).Compress(nil, big)
	small := newCodec(t, cachexzstd.WithMaxDecodedSize(1<<10))
	if _, err := small.Decompress(nil, comp); err == nil {
		t.Fatal("want size error")
	}
}

func TestOptionValidation(t *testing.T) {
	for i, o := range []cachexzstd.Option{cachexzstd.WithMaxDecodedSize(0), cachexzstd.WithLevel(zstd.EncoderLevel(99))} {
		if _, err := cachexzstd.New(o); err == nil {
			t.Errorf("%d: want error", i)
		}
	}
	newCodec(t, cachexzstd.WithLevel(zstd.SpeedBestCompression))
}

func TestConcurrentUse(t *testing.T) {
	z := newCodec(t)
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for g := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				src := bytes.Repeat([]byte(fmt.Sprintf("g%d-i%d;", g, i)), 50+i)
				comp, err := z.Compress(nil, src)
				if err != nil {
					errs <- err
					return
				}
				out, err := z.Decompress(nil, comp)
				if err != nil || !bytes.Equal(out, src) {
					errs <- fmt.Errorf("mismatch g%d i%d: %v", g, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestWithCacheSharedL2(t *testing.T) {
	ctx := context.Background()
	l2 := memstore.New()
	z := newCodec(t)
	writer, err := cachex.New(cachex.WithL2(l2), cachex.WithSweepInterval(0), cachex.WithCompression(z, 64))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	reader, err := cachex.New(cachex.WithL2(l2), cachex.WithSweepInterval(0), cachex.WithDecompressors(z))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	plain, _ := cachex.New(cachex.WithL2(l2), cachex.WithSweepInterval(0))
	defer plain.Close()

	val := bytes.Repeat([]byte(`{"name":"cachex","ok":true}`), 100)
	if err := writer.Set(ctx, "k", val, time.Minute); err != nil {
		t.Fatal(err)
	}
	if raw, _ := l2.Get(ctx, "k"); len(raw) >= len(val) {
		t.Fatalf("L2 entry not compressed: %d bytes", len(raw))
	}
	if v, err := reader.Get(ctx, "k"); err != nil || !bytes.Equal(v, val) {
		t.Fatalf("reader %v", err)
	}
	if _, err := plain.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) || plain.Stats().DecodeErrors != 1 {
		t.Fatalf("plain node: %v %+v", err, plain.Stats())
	}
}
