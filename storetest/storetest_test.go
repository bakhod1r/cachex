package storetest_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
	"github.com/bakhod1r/cachex/storetest"
)

func TestMemstorePasses(t *testing.T) {
	var mu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	storetest.Run(t, func(*testing.T) cachex.Store { return memstore.NewWithClock(clock) },
		func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() })
}

// plain hides the optional interfaces, so their subtests skip.
type plain struct{ cachex.Store }

func TestOptionalInterfacesSkip(t *testing.T) {
	var mu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	storetest.Run(t, func(*testing.T) cachex.Store { return plain{memstore.NewWithClock(clock)} },
		func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() })
}

func TestRealTimeTTL(t *testing.T) {
	short := testing.Short()
	setShort(t, true)
	storetest.Run(t, func(*testing.T) cachex.Store { return memstore.New() }, nil) // TTL subtests skip
	if short {
		return
	}
	setShort(t, false)
	storetest.Run(t, func(*testing.T) cachex.Store { return memstore.New() }, nil) // sleeps ~6s
}

func setShort(t *testing.T, v bool) {
	t.Helper()
	f := flag.Lookup("test.short")
	old := f.Value.String()
	if err := f.Value.Set(strconv.FormatBool(v)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Value.Set(old) })
}

// TestRunReportsBrokenStores feeds the suite stores with one bug each and checks that the
// suite fails. The suite runs in a nested test runner so its failures don't fail this test.
func TestRunReportsBrokenStores(t *testing.T) {
	bugs := []string{
		"missNil", "setErr", "setAlias", "getAlias", "noOverwrite", "addErr", "addOverwrites",
		"delErr", "delNoop", "delMissErr", "incrNoMiss", "incrWrong", "noTTL", "zeroTTLExpires",
		"getMultiErr", "getMultiExtra", "getsMissNil", "getsErr", "casErr", "casIgnoreToken",
		"setNoBump", "casResurrect", "getsMultiErr", "getErr",
	}
	for _, bug := range bugs {
		if runNested(t, bug) {
			t.Errorf("suite passed a store with bug %q", bug)
		}
	}
}

func runNested(t *testing.T, bug string) (ok bool) {
	t.Helper()
	name := t.Name()
	test := testing.InternalTest{Name: name, F: func(t *testing.T) {
		var now time.Time
		var mu sync.Mutex
		storetest.Run(t, func(*testing.T) cachex.Store {
			return &buggy{bug: bug, data: map[string]item{}, now: func() time.Time { mu.Lock(); defer mu.Unlock(); return now }}
		}, func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() })
	}}
	stdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	drained := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, r); close(drained) }()
	os.Stdout = w
	defer func() {
		os.Stdout = stdout
		_ = w.Close()
		<-drained
	}()
	return testing.RunTests(func(pat, str string) (bool, error) { return regexp.MatchString(pat, str) }, []testing.InternalTest{test})
}

var errBug = errors.New("bug")

type item struct {
	val    []byte
	expiry time.Time
	ver    uint64
}

// buggy is a Store, MultiGetter, CASStore and MultiCASGetter that misbehaves as bug says.
type buggy struct {
	bug string
	now func() time.Time

	mu   sync.Mutex
	data map[string]item
	ver  uint64
}

func (b *buggy) live(k string) (item, bool) {
	it, ok := b.data[k]
	if ok && !it.expiry.IsZero() && !b.now().Before(it.expiry) {
		delete(b.data, k)
		return item{}, false
	}
	return it, ok
}

func (b *buggy) put(k string, v []byte, ttl time.Duration) {
	it := item{val: v}
	if b.bug != "setAlias" {
		it.val = bytes.Clone(v)
	}
	switch {
	case b.bug == "zeroTTLExpires" && ttl <= 0:
		it.expiry = b.now()
	case ttl > 0 && b.bug != "noTTL":
		it.expiry = b.now().Add(ttl)
	}
	b.ver++
	it.ver = b.ver
	b.data[k] = it
}

func (b *buggy) Get(_ context.Context, k string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	it, ok := b.live(k)
	switch {
	case !ok && b.bug == "missNil":
		return nil, nil
	case !ok:
		return nil, cachex.ErrMiss
	case b.bug == "getErr":
		return nil, errBug
	case b.bug == "getAlias":
		return it.val, nil
	}
	return bytes.Clone(it.val), nil
}

func (b *buggy) Set(_ context.Context, k string, v []byte, ttl time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur, ok := b.live(k)
	switch {
	case b.bug == "setErr":
		return errBug
	case b.bug == "noOverwrite" && ok:
		return nil
	case b.bug == "setNoBump" && ok:
		cur.val = bytes.Clone(v)
		b.data[k] = cur
		return nil
	}
	b.put(k, v, ttl)
	return nil
}

func (b *buggy) Add(_ context.Context, k string, v []byte, ttl time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.live(k)
	switch {
	case b.bug == "addErr":
		return errBug
	case ok && b.bug != "addOverwrites":
		return cachex.ErrNotStored
	}
	b.put(k, v, ttl)
	return nil
}

func (b *buggy) Delete(_ context.Context, k string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.live(k)
	switch {
	case b.bug == "delErr":
		return errBug
	case b.bug == "delNoop":
		return nil
	case !ok && b.bug == "delMissErr":
		return cachex.ErrMiss
	}
	delete(b.data, k)
	return nil
}

func (b *buggy) Incr(_ context.Context, k string, delta uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	it, ok := b.live(k)
	if !ok {
		if b.bug == "incrNoMiss" {
			return 0, nil
		}
		return 0, cachex.ErrMiss
	}
	n, err := strconv.ParseUint(string(it.val), 10, 64)
	if err != nil {
		return 0, err
	}
	n += delta
	if b.bug == "incrWrong" {
		n++
	}
	it.val = []byte(strconv.FormatUint(n, 10))
	b.data[k] = it
	return n, nil
}

func (b *buggy) Close() error { return nil }

func (b *buggy) GetMulti(_ context.Context, keys []string) (map[string][]byte, error) {
	if b.bug == "getMultiErr" {
		return nil, errBug
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := map[string][]byte{}
	for _, k := range keys {
		if it, ok := b.live(k); ok || b.bug == "getMultiExtra" {
			out[k] = bytes.Clone(it.val)
		}
	}
	return out, nil
}

func (b *buggy) Gets(_ context.Context, k string) ([]byte, any, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	it, ok := b.live(k)
	switch {
	case !ok && b.bug == "getsMissNil":
		return nil, nil, nil
	case !ok:
		return nil, nil, cachex.ErrMiss
	case b.bug == "getsErr":
		return nil, nil, errBug
	}
	return bytes.Clone(it.val), it.ver, nil
}

func (b *buggy) GetsMulti(_ context.Context, keys []string) (map[string][]byte, map[string]any, error) {
	if b.bug == "getsMultiErr" {
		return nil, nil, errBug
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	vals, toks := map[string][]byte{}, map[string]any{}
	for _, k := range keys {
		if it, ok := b.live(k); ok {
			vals[k], toks[k] = bytes.Clone(it.val), it.ver
		}
	}
	return vals, toks, nil
}

func (b *buggy) CompareAndSwap(_ context.Context, k string, v []byte, tok any, ttl time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	it, ok := b.live(k)
	switch {
	case b.bug == "casErr":
		return errBug
	case !ok && b.bug == "casResurrect":
		b.put(k, v, ttl)
		return cachex.ErrMiss
	case !ok:
		return cachex.ErrMiss
	case b.bug != "casIgnoreToken" && tok != it.ver:
		return cachex.ErrNotStored
	}
	b.put(k, v, ttl)
	return nil
}
