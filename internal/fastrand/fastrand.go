// Package fastrand exposes the runtime's cheap per-thread random source for hot paths
// that only need to spread work over stripes, not statistical quality.
package fastrand

import _ "unsafe" // for go:linkname

// Uint32 returns a pseudo-random uint32 from the runtime's per-thread wyrand state: a
// couple of instructions, where math/rand/v2 runs a ChaCha8 step. Not for anything that
// needs unpredictability. The runtime marks cheaprand linkname-accessible.
//
//go:linkname Uint32 runtime.cheaprand
func Uint32() uint32
