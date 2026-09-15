// Package cachexotel reports cachex.Events as OpenTelemetry spans and metrics.
package cachexotel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bakhod1r/cachex"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const scope = "github.com/bakhod1r/cachex/otel"

type config struct {
	tp      trace.TracerProvider
	mp      metric.MeterProvider
	keyAttr bool
}

// Option configures Events.
type Option func(*config)

// WithTracerProvider sets the tracer provider; default otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option { return func(c *config) { c.tp = tp } }

// WithMeterProvider sets the meter provider; default otel.GetMeterProvider().
func WithMeterProvider(mp metric.MeterProvider) Option { return func(c *config) { c.mp = mp } }

// WithKeyAttribute adds the cache key as span attribute cachex.key. Off by default: keys may
// carry personal data and are unbounded. Keys are never metric attributes.
func WithKeyAttribute(on bool) Option { return func(c *config) { c.keyAttr = on } }

// Events returns hooks that trace each operation as span "cachex.<op>" and record metrics:
// cachex.op.duration and cachex.load.duration histograms (seconds), and cachex.l2.errors,
// cachex.loader.panics, cachex.publish.errors and cachex.breaker.transitions counters.
// ErrMiss and ErrNotFound do not mark a span or metric as an error.
func Events(opts ...Option) (cachex.Events, error) {
	cfg := config{}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.tp == nil {
		cfg.tp = otel.GetTracerProvider()
	}
	if cfg.mp == nil {
		cfg.mp = otel.GetMeterProvider()
	}
	tracer := cfg.tp.Tracer(scope)
	meter := cfg.mp.Meter(scope)

	opDur, err1 := meter.Float64Histogram("cachex.op.duration", metric.WithUnit("s"),
		metric.WithDescription("Duration of cache operations."))
	loadDur, err2 := meter.Float64Histogram("cachex.load.duration", metric.WithUnit("s"),
		metric.WithDescription("Duration of loader calls."))
	l2Errs, err3 := meter.Int64Counter("cachex.l2.errors", metric.WithDescription("L2 store failures."))
	panics, err4 := meter.Int64Counter("cachex.loader.panics", metric.WithDescription("Recovered loader panics."))
	pubErrs, err5 := meter.Int64Counter("cachex.publish.errors", metric.WithDescription("Failed invalidation publishes."))
	trans, err6 := meter.Int64Counter("cachex.breaker.transitions", metric.WithDescription("L2 circuit breaker transitions."))
	if err := errors.Join(err1, err2, err3, err4, err5, err6); err != nil {
		return cachex.Events{}, fmt.Errorf("cachexotel: create instruments: %w", err)
	}

	bg := context.Background()
	return cachex.Events{
		OpStart: func(ctx context.Context, op cachex.Op, _ string) context.Context {
			ctx, _ = tracer.Start(ctx, "cachex."+op.String())
			return ctx
		},
		OpEnd: func(ctx context.Context, info cachex.OpInfo, d time.Duration) {
			failed := isFailure(info.Err)
			opDur.Record(ctx, d.Seconds(), metric.WithAttributes(
				attribute.String("op", info.Op.String()),
				attribute.String("tier", info.Tier.String()),
				attribute.Bool("error", failed),
			))
			span := trace.SpanFromContext(ctx)
			if !span.IsRecording() {
				return
			}
			span.SetAttributes(attribute.String("cachex.tier", info.Tier.String()))
			if info.Keys > 0 {
				span.SetAttributes(attribute.Int("cachex.keys", info.Keys))
			}
			if cfg.keyAttr && info.Key != "" {
				span.SetAttributes(attribute.String("cachex.key", info.Key))
			}
			if failed {
				span.RecordError(info.Err)
				span.SetStatus(codes.Error, info.Err.Error())
			}
			span.End()
		},
		LoadEnd: func(ctx context.Context, _ string, d time.Duration, err error) {
			failed := isFailure(err)
			loadDur.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.Bool("error", failed)))
			if span := trace.SpanFromContext(ctx); span.IsRecording() {
				span.AddEvent("cachex.load", trace.WithAttributes(
					attribute.Float64("duration_s", d.Seconds()),
					attribute.Bool("error", failed),
				))
			}
		},
		L2Error: func(op string, _ error) {
			l2Errs.Add(bg, 1, metric.WithAttributes(attribute.String("op", op)))
		},
		BreakerChange: func(from, to string) {
			trans.Add(bg, 1, metric.WithAttributes(attribute.String("from", from), attribute.String("to", to)))
		},
		LoaderPanic: func(string, any) { panics.Add(bg, 1) },
		PublishError: func(cachex.Invalidation, error) {
			pubErrs.Add(bg, 1)
		},
	}, nil
}

// isFailure reports whether err is a real failure; misses and not-found are outcomes.
func isFailure(err error) bool {
	return err != nil && !errors.Is(err, cachex.ErrMiss) && !errors.Is(err, cachex.ErrNotFound)
}
