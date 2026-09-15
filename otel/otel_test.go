package cachexotel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bakhod1r/cachex"
	"github.com/bakhod1r/cachex/memstore"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type env struct {
	c     *cachex.Cache
	store *memstore.Store
	spans *tracetest.SpanRecorder
	rd    *sdkmetric.ManualReader
}

func newEnv(t *testing.T, opts ...Option) *env {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	rd := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rd))
	ev, err := Events(append([]Option{WithTracerProvider(tp), WithMeterProvider(mp)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	st := memstore.New()
	c, err := cachex.New(cachex.WithL2(st), cachex.WithEvents(ev))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &env{c: c, store: st, spans: sr, rd: rd}
}

func attrs(kv []attribute.KeyValue) map[attribute.Key]attribute.Value {
	m := map[attribute.Key]attribute.Value{}
	for _, a := range kv {
		m[a.Key] = a.Value
	}
	return m
}

func (e *env) metrics(t *testing.T) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := e.rd.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	m := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, x := range sm.Metrics {
			m[x.Name] = x
		}
	}
	return m
}

func TestSpansTierAndMiss(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.c.Get(ctx, "k"); !errors.Is(err, cachex.ErrMiss) {
		t.Fatalf("err %v", err)
	}
	if err := e.c.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := e.c.Get(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	ended := e.spans.Ended()
	if len(ended) != 3 {
		t.Fatalf("spans %d", len(ended))
	}
	wantName := []string{"cachex.get", "cachex.set", "cachex.get"}
	wantTier := []string{"miss", "miss", "l1"}
	for i, s := range ended {
		if s.Name() != wantName[i] {
			t.Fatalf("span %d name %q", i, s.Name())
		}
		a := attrs(s.Attributes())
		if a["cachex.tier"].AsString() != wantTier[i] {
			t.Fatalf("span %d tier %v", i, a["cachex.tier"])
		}
		if _, ok := a["cachex.key"]; ok {
			t.Fatalf("key attr present by default")
		}
		if s.Status().Code == codes.Error {
			t.Fatalf("span %d error status (miss is not an error)", i)
		}
	}
}

func TestKeyAttributeAndErrorStatus(t *testing.T) {
	e := newEnv(t, WithKeyAttribute(true))
	ctx := context.Background()
	_, err := e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) {
		return nil, errors.New("db down")
	})
	if err == nil {
		t.Fatal("want error")
	}
	s := e.spans.Ended()[0]
	if s.Name() != "cachex.get_or_load" || attrs(s.Attributes())["cachex.key"].AsString() != "k" {
		t.Fatalf("span %q attrs %v", s.Name(), s.Attributes())
	}
	if s.Status().Code != codes.Error {
		t.Fatalf("status %v", s.Status())
	}
	var sawLoad bool
	for _, ev := range s.Events() {
		sawLoad = sawLoad || ev.Name == "cachex.load"
	}
	if !sawLoad {
		t.Fatalf("no cachex.load event: %v", s.Events())
	}
}

func TestMultiKeysAttr(t *testing.T) {
	e := newEnv(t)
	_, _ = e.c.GetMulti(context.Background(), []string{"a", "b"})
	a := attrs(e.spans.Ended()[0].Attributes())
	if a["cachex.keys"].AsInt64() != 2 {
		t.Fatalf("attrs %v", a)
	}
}

func TestMetrics(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	_, _ = e.c.Get(ctx, "k")
	_, _ = e.c.GetOrLoad(ctx, "k", time.Minute, func(context.Context) ([]byte, error) { return []byte("v"), nil })
	m := e.metrics(t)
	op, ok := m["cachex.op.duration"].Data.(metricdata.Histogram[float64])
	if !ok || len(op.DataPoints) == 0 || m["cachex.op.duration"].Unit != "s" {
		t.Fatalf("op.duration %+v", m["cachex.op.duration"])
	}
	for _, dp := range op.DataPoints {
		if _, ok := dp.Attributes.Value("op"); !ok {
			t.Fatal("missing op attr")
		}
		if v, _ := dp.Attributes.Value("error"); v.AsBool() {
			t.Fatalf("unexpected error=true %v", dp.Attributes)
		}
	}
	load, ok := m["cachex.load.duration"].Data.(metricdata.Histogram[float64])
	if !ok || len(load.DataPoints) != 1 || load.DataPoints[0].Count != 1 {
		t.Fatalf("load.duration %+v", m["cachex.load.duration"])
	}
}

func sum(t *testing.T, m metricdata.Metrics) (int64, []metricdata.DataPoint[int64]) {
	t.Helper()
	s, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s not a sum: %T", m.Name, m.Data)
	}
	var n int64
	for _, dp := range s.DataPoints {
		n += dp.Value
	}
	return n, s.DataPoints
}

func TestL2ErrorsAndBreaker(t *testing.T) {
	e := newEnv(t)
	e.store.SetDown(true)
	for i := 0; i < 25; i++ {
		_, _ = e.c.Get(context.Background(), "k")
	}
	m := e.metrics(t)
	if n, _ := sum(t, m["cachex.l2.errors"]); n < 20 {
		t.Fatalf("l2 errors %d", n)
	}
	n, dps := sum(t, m["cachex.breaker.transitions"])
	if n != 1 {
		t.Fatalf("transitions %d", n)
	}
	from, _ := dps[0].Attributes.Value("from")
	to, _ := dps[0].Attributes.Value("to")
	if from.AsString() != "closed" || to.AsString() != "open" {
		t.Fatalf("attrs %v", dps[0].Attributes)
	}
}

func TestLoaderPanicCounter(t *testing.T) {
	e := newEnv(t)
	_, _ = e.c.GetOrLoad(context.Background(), "k", time.Minute, func(context.Context) ([]byte, error) { panic("boom") })
	if n, _ := sum(t, e.metrics(t)["cachex.loader.panics"]); n != 1 {
		t.Fatalf("panics %d", n)
	}
}

func TestPublishErrorCounter(t *testing.T) {
	rd := sdkmetric.NewManualReader()
	ev, err := Events(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(rd))))
	if err != nil {
		t.Fatal(err)
	}
	ev.PublishError(cachex.Invalidation{}, errors.New("x"))
	e := &env{rd: rd}
	if n, _ := sum(t, e.metrics(t)["cachex.publish.errors"]); n != 1 {
		t.Fatalf("publish errors %d", n)
	}
}

func TestOpEndWithoutSpan(t *testing.T) {
	rd := sdkmetric.NewManualReader()
	ev, err := Events(WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(rd))))
	if err != nil {
		t.Fatal(err)
	}
	ev.OpEnd(context.Background(), cachex.OpInfo{Op: cachex.OpGet, Tier: cachex.TierL1}, time.Millisecond)
	ev.LoadEnd(context.Background(), "k", time.Millisecond, nil)
	m := (&env{rd: rd}).metrics(t)
	if _, ok := m["cachex.op.duration"]; !ok {
		t.Fatal("no op metric without span")
	}
}
