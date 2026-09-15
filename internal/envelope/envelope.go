// Package envelope is the binary wrapper stored in L2. It carries the absolute
// expiry (so L1 backfill knows remaining TTL) and loader compute duration
// (for XFetch stampede protection).
//
// Layout (big-endian, 32-byte header):
//
//	0  1 magic 0xCA
//	1  1 version 0x01
//	2  1 flags (bit0 Tombstone)
//	3  1 codec id (0 = raw payload; else payload is compressed by that codec)
//	4  4 payloadLen uint32
//	8  8 expireAt int64 unix nanos (0 = never)
//	16 8 storedAt int64 unix nanos
//	24 8 deltaNs int64
//	32 N payload
//
// Byte 3 was "reserved, must be 0" before compression existed, so raw entries written by
// older versions decode unchanged (codec 0) and older readers reject compressed entries
// as corrupt (a miss) instead of returning compressed bytes as the value.
package envelope

import (
	"encoding/binary"
	"errors"
	"time"
)

// Header size and flag bits.
const (
	HeaderSize          = 32
	FlagTombstone uint8 = 1 // cached absence (negative cache)
	FlagDeleted   uint8 = 2 // Delete marker: reads treat the key as absent

	magic   = 0xCA
	version = 0x01
)

// Decode errors.
var (
	ErrCorrupt = errors.New("envelope: corrupt")
	ErrVersion = errors.New("envelope: unsupported version")
)

// Entry is a decoded envelope.
type Entry struct {
	Value                     []byte
	Flags                     uint8
	Codec                     uint8 // 0 = raw; else Value is compressed with this codec
	ExpireAt, StoredAt, Delta int64
}

// Encode serialises e. Panics only if len(e.Value) exceeds uint32.
func Encode(e Entry) []byte {
	if uint64(len(e.Value)) > 0xFFFFFFFF {
		panic("envelope: payload exceeds 4GiB")
	}
	b := make([]byte, HeaderSize+len(e.Value))
	copy(b[HeaderSize:], e.Value)
	PutHeader(b, e)
	return b
}

// PutHeader writes e's header into b[:HeaderSize]; the payload b[HeaderSize:] must already
// be in place (e.Value is ignored, the payload length is len(b)-HeaderSize).
func PutHeader(b []byte, e Entry) {
	if uint64(len(b)-HeaderSize) > 0xFFFFFFFF {
		panic("envelope: payload exceeds 4GiB")
	}
	b[0], b[1], b[2], b[3] = magic, version, e.Flags, e.Codec
	binary.BigEndian.PutUint32(b[4:], uint32(len(b)-HeaderSize))
	binary.BigEndian.PutUint64(b[8:], uint64(e.ExpireAt))
	binary.BigEndian.PutUint64(b[16:], uint64(e.StoredAt))
	binary.BigEndian.PutUint64(b[24:], uint64(e.Delta))
}

// Decode parses b. Value aliases b (no copy). The buffer length must equal
// HeaderSize+payloadLen exactly. A non-zero Codec means Value is still compressed.
func Decode(b []byte) (Entry, error) {
	if len(b) < HeaderSize || b[0] != magic {
		return Entry{}, ErrCorrupt
	}
	if b[1] != version {
		return Entry{}, ErrVersion
	}
	n := uint64(binary.BigEndian.Uint32(b[4:]))
	if uint64(len(b)-HeaderSize) != n {
		return Entry{}, ErrCorrupt
	}
	return Entry{
		Value:    b[HeaderSize:len(b):len(b)],
		Flags:    b[2],
		Codec:    b[3],
		ExpireAt: int64(binary.BigEndian.Uint64(b[8:])),
		StoredAt: int64(binary.BigEndian.Uint64(b[16:])),
		Delta:    int64(binary.BigEndian.Uint64(b[24:])),
	}, nil
}

// Expired reports ExpireAt != 0 && nowNs >= ExpireAt.
func (e Entry) Expired(nowNs int64) bool {
	return e.ExpireAt != 0 && nowNs >= e.ExpireAt
}

// Remaining returns time left; 0 if expired, -1 if the entry never expires.
func (e Entry) Remaining(nowNs int64) time.Duration {
	if e.ExpireAt == 0 {
		return -1
	}
	if e.Expired(nowNs) {
		return 0
	}
	return time.Duration(e.ExpireAt - nowNs)
}
