// Package cachexzstd is a zstd Compressor for cachex L2 values. It lives in its own module
// so the core library stays dependency-free.
//
//	z, err := cachexzstd.New()
//	c, err := cachex.New(cachex.WithL2(store), cachex.WithCompression(z, 512))
package cachexzstd

import (
	"errors"
	"runtime"

	"github.com/bakhod1r/cachex"
	"github.com/klauspost/compress/zstd"
)

// ID is the codec id written into compressed cachex entries. It never changes.
const ID byte = 1

// DefaultMaxDecodedSize bounds how much memory decoding one value may use (256 MiB).
const DefaultMaxDecodedSize uint64 = 256 << 20

type settings struct {
	level      zstd.EncoderLevel
	maxDecoded uint64
}

// Option configures New.
type Option func(*settings) error

// WithLevel sets the encoder level. Default zstd.SpeedDefault. Decoding doesn't depend on it.
func WithLevel(l zstd.EncoderLevel) Option {
	return func(s *settings) error {
		if l < zstd.SpeedFastest || l > zstd.SpeedBestCompression {
			return errors.New("cachexzstd: invalid level")
		}
		s.level = l
		return nil
	}
}

// WithMaxDecodedSize caps the decompressed size of one value; larger (or hostile) entries
// fail to decode and the cache treats them as a miss. Default DefaultMaxDecodedSize.
func WithMaxDecodedSize(n uint64) Option {
	return func(s *settings) error {
		if n == 0 {
			return errors.New("cachexzstd: max decoded size must be positive")
		}
		s.maxDecoded = n
		return nil
	}
}

// Codec is safe for concurrent use: one encoder and one decoder are shared by all goroutines.
type Codec struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

var _ cachex.Compressor = (*Codec)(nil)

// New builds a zstd codec with reusable encoder and decoder.
func New(opts ...Option) (*Codec, error) {
	s := settings{level: zstd.SpeedDefault, maxDecoded: DefaultMaxDecodedSize}
	for _, o := range opts {
		if err := o(&s); err != nil {
			return nil, err
		}
	}
	n := runtime.GOMAXPROCS(0)
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(s.level), zstd.WithEncoderConcurrency(n))
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(n),
		zstd.WithDecoderMaxMemory(s.maxDecoded), zstd.WithDecoderMaxWindow(min(s.maxDecoded, zstd.MaxWindowSize)))
	if err != nil {
		_ = enc.Close()
		return nil, err
	}
	return &Codec{enc: enc, dec: dec}, nil
}

// ID implements cachex.Compressor.
func (*Codec) ID() byte { return ID }

// Compress implements cachex.Compressor.
func (z *Codec) Compress(dst, src []byte) ([]byte, error) { return z.enc.EncodeAll(src, dst), nil }

// Decompress implements cachex.Compressor.
func (z *Codec) Decompress(dst, src []byte) ([]byte, error) { return z.dec.DecodeAll(src, dst) }

// Close releases the encoder and decoder. The codec must not be used afterwards.
func (z *Codec) Close() error {
	z.dec.Close()
	return z.enc.Close()
}
