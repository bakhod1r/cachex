//go:build unix

package envelope

import (
	"syscall"
	"testing"
)

// oversized maps (but never touches) more than 4GiB of address space: the panicking checks
// run before any byte is read or written, so no physical memory is used.
func oversized(t *testing.T) []byte {
	t.Helper()
	b, err := syscall.Mmap(-1, 0, HeaderSize+1<<32, syscall.PROT_NONE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		t.Skipf("mmap: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Munmap(b) })
	return b
}

func mustPanic(t *testing.T, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	f()
}

func TestOversizedPayloadPanics(t *testing.T) {
	b := oversized(t)
	mustPanic(t, func() { Encode(Entry{Value: b}) })
	mustPanic(t, func() { PutHeader(b, Entry{}) })
}
