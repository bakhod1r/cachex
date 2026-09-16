package fastrand

import "testing"

func TestUint32SpreadsOverStripes(t *testing.T) {
	const stripes = 16
	var seen [stripes]int
	for range 100_000 {
		seen[Uint32()&(stripes-1)]++
	}
	for i, n := range seen {
		if n < 100_000/stripes/2 {
			t.Fatalf("stripe %d got %d of 100000, want a roughly even share", i, n)
		}
	}
}

func BenchmarkUint32(b *testing.B) {
	for range b.N {
		_ = Uint32()
	}
}
