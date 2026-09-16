package cachexotel

import (
	"errors"
	"testing"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestDefaultProviders(t *testing.T) {
	ev, err := Events()
	if err != nil || ev.OpStart == nil {
		t.Fatalf("%v", err)
	}
}

var errInstrument = errors.New("instrument")

type failingProvider struct{ noop.MeterProvider }

func (failingProvider) Meter(string, ...metric.MeterOption) metric.Meter { return failingMeter{} }

type failingMeter struct{ noop.Meter }

func (failingMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return noop.Int64Counter{}, errInstrument
}

func TestInstrumentCreationError(t *testing.T) {
	if _, err := Events(WithMeterProvider(failingProvider{})); !errors.Is(err, errInstrument) {
		t.Fatalf("want errInstrument, got %v", err)
	}
}
