package lru

import (
	"flag"
	"fmt"
	"os"
	"testing"
)

// testFrequency is applied by newCache; TestMain runs every test once with
// Frequency=false and once with Frequency=true.
var testFrequency bool

func newCache(o Options) *Cache {
	o.Frequency = o.Frequency || testFrequency
	return New(o)
}

func TestMain(m *testing.M) {
	flag.Parse()
	if code := m.Run(); code != 0 {
		os.Exit(code)
	}
	testFrequency = true
	_ = flag.Set("test.bench", "") // benchmarks cover both modes explicitly; run them once
	fmt.Println("=== second pass: Options.Frequency=true")
	os.Exit(m.Run())
}
