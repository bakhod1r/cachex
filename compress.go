package cachex

import (
	"errors"
	"fmt"

	"github.com/bakhod1r/cachex/internal/envelope"
)

// Compressor compresses L2 values. Implementations must be safe for concurrent use.
//
// The ID is written into every compressed L2 entry and selects the decoder on read, so it
// must be stable across versions and unique among the codecs a deployment uses. 0 is
// reserved for "raw".
type Compressor interface {
	ID() byte
	// Compress appends the compressed form of src to dst and returns the extended slice.
	Compress(dst, src []byte) ([]byte, error)
	// Decompress appends the decompressed form of src to dst and returns the extended slice.
	// It must return an error, not panic, on malformed input.
	Decompress(dst, src []byte) ([]byte, error)
}

var errUnknownCodec = errors.New("cachex: unknown compression codec")

// WithCompression compresses values of at least minSize bytes before writing them to L2
// (Set, SetMulti, GetOrLoad, GetOrLoadMulti). A value is stored raw when compressing
// fails or doesn't make it smaller. L1 always holds the uncompressed value, so L1 reads and
// GetView are unaffected. c is also registered as a decoder (see WithDecompressors).
//
// Roll out in two deploys: first ship a version where every node can decode the codec
// (WithDecompressors, or WithCompression with a huge minSize), then enable compression.
// Nodes that can't decode an entry treat it as a miss and count Stats.DecodeErrors.
func WithCompression(c Compressor, minSize int) Option {
	return func(cfg *config) error {
		if minSize < 0 {
			return fmt.Errorf("cachex: negative compression min size")
		}
		if err := cfg.addDecompressor(c); err != nil {
			return err
		}
		cfg.compressor, cfg.compressMin = c, minSize
		return nil
	}
}

// WithDecompressors lets the cache read L2 entries compressed with these codecs without
// compressing its own writes. Uncompressed entries are always readable.
func WithDecompressors(cs ...Compressor) Option {
	return func(cfg *config) error {
		for _, c := range cs {
			if err := cfg.addDecompressor(c); err != nil {
				return err
			}
		}
		return nil
	}
}

func (cfg *config) addDecompressor(c Compressor) error {
	if c == nil {
		return fmt.Errorf("cachex: nil compressor")
	}
	id := c.ID()
	if id == 0 {
		return fmt.Errorf("cachex: compressor ID 0 is reserved")
	}
	if cfg.decompressors == nil {
		cfg.decompressors = map[byte]Compressor{}
	}
	cfg.decompressors[id] = c
	return nil
}

// compressL2 returns the L2 form of an entry whose uncompressed encoding is raw. Markers,
// tombstones and small values are never compressed.
func (c *Cache) compressL2(e envelope.Entry, raw []byte) []byte {
	cp := c.cfg.compressor
	if cp == nil || e.Flags != 0 || len(e.Value) == 0 || len(e.Value) < c.cfg.compressMin {
		return raw
	}
	out, err := cp.Compress(make([]byte, envelope.HeaderSize, envelope.HeaderSize+len(e.Value)), e.Value)
	if err != nil || len(out)-envelope.HeaderSize >= len(e.Value) {
		return raw
	}
	e.Codec = cp.ID()
	envelope.PutHeader(out, e)
	return out
}

// decodeL2 decodes an L2 entry, decompressing it if needed. l1raw is the uncompressed
// encoding to backfill L1 with; owned reports that it is a fresh buffer.
func (c *Cache) decodeL2(raw []byte) (e envelope.Entry, l1raw []byte, owned bool, err error) {
	e, err = envelope.Decode(raw)
	if err != nil || e.Codec == 0 {
		return e, raw, false, err
	}
	d := c.cfg.decompressors[e.Codec]
	if d == nil {
		return envelope.Entry{}, nil, false, errUnknownCodec
	}
	out, err := d.Decompress(make([]byte, envelope.HeaderSize, envelope.HeaderSize+2*len(e.Value)), e.Value)
	if err != nil {
		return envelope.Entry{}, nil, false, err
	}
	if len(out) < envelope.HeaderSize || uint64(len(out)-envelope.HeaderSize) > 0xFFFFFFFF {
		return envelope.Entry{}, nil, false, envelope.ErrCorrupt
	}
	e.Codec = 0
	envelope.PutHeader(out, e)
	e.Value = out[envelope.HeaderSize:len(out):len(out)]
	return e, out, true, nil
}
