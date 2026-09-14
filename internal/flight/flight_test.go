package flight

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitDups blocks until n callers have joined the in-flight call for key.
// It yields instead of sleeping; the deadline only guards against hangs.
func waitDups(t *testing.T, g *Group, key string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		g.mu.Lock()
		c := g.m[key]
		d := 0
		if c != nil {
			d = c.dups
		}
		g.mu.Unlock()
		if d >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %d dups on %q, have %d", n, key, d)
		}
		runtime.Gosched()
	}
}

func waitInFlight(t *testing.T, g *Group, key string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		g.mu.Lock()
		_, ok := g.m[key]
		g.mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for in-flight %q", key)
		}
		runtime.Gosched()
	}
}

func TestDo_ConcurrentCallersShareOneCall(t *testing.T) {
	var g Group
	var calls atomic.Int32
	release := make(chan struct{})
	fn := func(ctx context.Context) ([]byte, error) {
		calls.Add(1)
		<-release
		return []byte("v"), nil
	}
	const n = 100
	var wg sync.WaitGroup
	var sharedCount atomic.Int32
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, shared, err := g.Do(context.Background(), "k", fn)
			if err != nil || !bytes.Equal(v, []byte("v")) {
				errs <- errors.New("bad result")
				return
			}
			if shared {
				sharedCount.Add(1)
			}
		}()
	}
	waitInFlight(t, &g, "k")
	waitDups(t, &g, "k", n-1)
	close(release)
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("fn calls = %d, want 1", got)
	}
	if got := sharedCount.Load(); got != n-1 {
		t.Fatalf("shared = %d, want %d", got, n-1)
	}
}

func TestDo_ErrorPropagatesToAll(t *testing.T) {
	var g Group
	boom := errors.New("boom")
	release := make(chan struct{})
	fn := func(ctx context.Context) ([]byte, error) { <-release; return nil, boom }
	const n = 10
	var wg sync.WaitGroup
	res := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, err := g.Do(context.Background(), "k", fn); res <- err }()
	}
	waitInFlight(t, &g, "k")
	waitDups(t, &g, "k", n-1)
	close(release)
	wg.Wait()
	close(res)
	for err := range res {
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
	}
}

func TestDo_JoinerCancelReturnsCtxErr_OthersGetValue(t *testing.T) {
	var g Group
	release := make(chan struct{})
	fn := func(ctx context.Context) ([]byte, error) { <-release; return []byte("v"), nil }

	firstDone := make(chan error, 1)
	go func() {
		v, _, err := g.Do(context.Background(), "k", fn)
		if err == nil && string(v) != "v" {
			err = errors.New("bad value")
		}
		firstDone <- err
	}()
	waitInFlight(t, &g, "k")

	ctx, cancel := context.WithCancel(context.Background())
	joinerDone := make(chan error, 1)
	go func() { _, _, err := g.Do(ctx, "k", fn); joinerDone <- err }()
	waitDups(t, &g, "k", 1)
	cancel()
	if err := <-joinerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("joiner err = %v, want Canceled", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first err = %v", err)
	}
}

func TestDo_FirstCallerCancelDoesNotFailOthers(t *testing.T) {
	var g Group
	release := make(chan struct{})
	ctxErrInFn := make(chan error, 1)
	fn := func(ctx context.Context) ([]byte, error) {
		<-release
		ctxErrInFn <- ctx.Err()
		return []byte("v"), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { _, _, err := g.Do(ctx, "k", fn); firstDone <- err }()
	waitInFlight(t, &g, "k")

	joinerDone := make(chan error, 1)
	go func() {
		v, shared, err := g.Do(context.Background(), "k", fn)
		if err == nil && (string(v) != "v" || !shared) {
			err = errors.New("bad value or shared flag")
		}
		joinerDone <- err
	}()
	waitDups(t, &g, "k", 1)
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first err = %v, want Canceled", err)
	}
	close(release)
	if err := <-joinerDone; err != nil {
		t.Fatalf("joiner err = %v", err)
	}
	if err := <-ctxErrInFn; err != nil {
		t.Fatalf("fn ctx was cancelled: %v", err)
	}
}

func TestDo_AlreadyCancelledCtx(t *testing.T) {
	var g Group
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := g.Do(ctx, "k", func(context.Context) ([]byte, error) { return []byte("v"), nil })
	// Either the value (fn raced to finish) or Canceled is acceptable; never another error.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestDo_PanicRecovered(t *testing.T) {
	var g Group
	release := make(chan struct{})
	fn := func(context.Context) ([]byte, error) { <-release; panic("kaboom") }
	const n = 5
	var wg sync.WaitGroup
	res := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _, err := g.Do(context.Background(), "k", fn); res <- err }()
	}
	waitInFlight(t, &g, "k")
	waitDups(t, &g, "k", n-1)
	close(release)
	wg.Wait()
	close(res)
	for err := range res {
		if !errors.Is(err, ErrPanic) {
			t.Fatalf("err = %v, want ErrPanic", err)
		}
	}
	// key freed after panic
	v, _, err := g.Do(context.Background(), "k", func(context.Context) ([]byte, error) { return []byte("ok"), nil })
	if err != nil || string(v) != "ok" {
		t.Fatalf("after panic: v=%q err=%v", v, err)
	}
}

func TestDo_KeyFreedAfterCompletion(t *testing.T) {
	var g Group
	var calls atomic.Int32
	fn := func(context.Context) ([]byte, error) { calls.Add(1); return []byte("v"), nil }
	for i := 0; i < 3; i++ {
		_, shared, err := g.Do(context.Background(), "k", fn)
		if err != nil || shared {
			t.Fatalf("iter %d: shared=%v err=%v", i, shared, err)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
	g.mu.Lock()
	l := len(g.m)
	g.mu.Unlock()
	if l != 0 {
		t.Fatalf("map len = %d, want 0", l)
	}
}

func TestForget_NextDoStartsNewCall(t *testing.T) {
	var g Group
	release1 := make(chan struct{})
	first := make(chan string, 1)
	go func() {
		v, _, _ := g.Do(context.Background(), "k", func(context.Context) ([]byte, error) { <-release1; return []byte("old"), nil })
		first <- string(v)
	}()
	waitInFlight(t, &g, "k")
	g.Forget("k")

	release2 := make(chan struct{})
	second := make(chan string, 1)
	go func() {
		v, shared, _ := g.Do(context.Background(), "k", func(context.Context) ([]byte, error) { <-release2; return []byte("new"), nil })
		if shared {
			v = []byte("shared?!")
		}
		second <- string(v)
	}()
	waitInFlight(t, &g, "k")
	// Old call completing must not remove the new in-flight entry.
	close(release1)
	if got := <-first; got != "old" {
		t.Fatalf("first = %q", got)
	}
	waitInFlight(t, &g, "k")
	close(release2)
	if got := <-second; got != "new" {
		t.Fatalf("second = %q", got)
	}
	g.Forget("absent") // no-op
}
