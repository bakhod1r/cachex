package cachex_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
)

func newNS(t *testing.T, opts ...cachex.Option) (env, *cachex.Namespace) {
	t.Helper()
	e := newEnv(t, opts...)
	ns, err := e.c.Namespace("app")
	if err != nil {
		t.Fatal(err)
	}
	return e, ns
}

func TestNamespaceSetMultiGetMulti(t *testing.T) {
	_, ns := newNS(t)
	if err := ns.SetMulti(ctx, map[string][]byte{"a": []byte("1"), "b": []byte("2")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := ns.GetMulti(ctx, []string{"a", "b", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got["a"]) != "1" || string(got["b"]) != "2" {
		t.Fatalf("GetMulti = %v", got)
	}
	if _, ok := got["missing"]; ok {
		t.Fatal("absent key present in result")
	}
}

// Keys must come back under the caller's names, not the versioned internal ones,
// and Invalidate must hide them all.
func TestNamespaceMultiIsolatedAndInvalidated(t *testing.T) {
	e, ns := newNS(t)
	other, err := e.c.Namespace("other")
	if err != nil {
		t.Fatal(err)
	}
	if err := ns.SetMulti(ctx, map[string][]byte{"a": []byte("mine")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := other.SetMulti(ctx, map[string][]byte{"a": []byte("theirs")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	got, err := other.GetMulti(ctx, []string{"a"})
	if err != nil || string(got["a"]) != "theirs" {
		t.Fatalf("other namespace GetMulti = %v, %v", got, err)
	}
	if err := ns.Invalidate(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := ns.GetMulti(ctx, []string{"a"}); err != nil || len(got) != 0 {
		t.Fatalf("after Invalidate: %v, %v", got, err)
	}
	if got, err := other.GetMulti(ctx, []string{"a"}); err != nil || string(got["a"]) != "theirs" {
		t.Fatalf("other namespace lost data: %v, %v", got, err)
	}
}

func TestNamespaceGetOrLoadMulti(t *testing.T) {
	_, ns := newNS(t)
	var asked []string
	load := func(_ context.Context, keys []string) (map[string][]byte, error) {
		asked = append(asked, keys...)
		out := make(map[string][]byte, len(keys))
		for _, k := range keys {
			out[k] = []byte("v" + k)
		}
		return out, nil
	}
	got, err := ns.GetOrLoadMulti(ctx, []string{"a", "b"}, time.Minute, load)
	if err != nil {
		t.Fatal(err)
	}
	if string(got["a"]) != "va" || string(got["b"]) != "vb" {
		t.Fatalf("GetOrLoadMulti = %v", got)
	}
	for _, k := range asked {
		if k != "a" && k != "b" {
			t.Fatalf("loader saw internal key %q, want caller keys", k)
		}
	}
	asked = nil
	if _, err := ns.GetOrLoadMulti(ctx, []string{"a", "b"}, time.Minute, load); err != nil {
		t.Fatal(err)
	}
	if len(asked) != 0 {
		t.Fatalf("second call loaded again: %v", asked)
	}
}

func TestNamespaceGetView(t *testing.T) {
	_, ns := newNS(t)
	if err := ns.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	var seen string
	if err := ns.GetView(ctx, "k", func(val []byte) error { seen = string(val); return nil }); err != nil {
		t.Fatal(err)
	}
	if seen != "v" {
		t.Fatalf("GetView saw %q", seen)
	}
	if err := ns.GetView(ctx, "absent", func([]byte) error { return nil }); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("absent GetView err = %v, want ErrMiss", err)
	}
}

// With L2 down and no version ever fetched, reads miss and writes report the error.
func TestNamespaceMultiWithUnknownVersion(t *testing.T) {
	e, ns := newNS(t)
	e.l2.SetDown(true)
	if got, err := ns.GetMulti(ctx, []string{"a"}); err != nil || len(got) != 0 {
		t.Fatalf("GetMulti = %v, %v; want empty, nil", got, err)
	}
	if err := ns.SetMulti(ctx, map[string][]byte{"a": []byte("1")}, time.Minute); err == nil {
		t.Fatal("SetMulti with unknown version must fail")
	}
	if err := ns.GetView(ctx, "a", func([]byte) error { return nil }); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("GetView err = %v, want ErrMiss", err)
	}
	loaded := false
	got, err := ns.GetOrLoadMulti(ctx, []string{"a"}, time.Minute, func(_ context.Context, keys []string) (map[string][]byte, error) {
		loaded = true
		return map[string][]byte{"a": []byte("fresh")}, nil
	})
	if err != nil || string(got["a"]) != "fresh" || !loaded {
		t.Fatalf("GetOrLoadMulti = %v, %v (loaded=%v); want the loader's value", got, err, loaded)
	}
}
