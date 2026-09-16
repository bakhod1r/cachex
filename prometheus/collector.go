// Package cachexprom exports cachex.Stats as Prometheus metrics. It lives in a
// separate module so the core cachex module carries no Prometheus dependency.
package cachexprom

import (
	"github.com/bakhod1r/cachex"
	"github.com/prometheus/client_golang/prometheus"
)

// StatsSource is anything that yields a cachex.Stats snapshot (e.g. *cachex.Cache).
type StatsSource interface{ Stats() cachex.Stats }

// Option configures the collector.
type Option func(*config)

type config struct {
	namespace   string
	constLabels prometheus.Labels
}

// WithNamespace sets the metric prefix. Default "cachex".
func WithNamespace(ns string) Option { return func(c *config) { c.namespace = ns } }

// WithConstLabels attaches constant labels to every metric.
func WithConstLabels(l prometheus.Labels) Option {
	return func(c *config) { c.constLabels = l }
}

// breakerStates are the states exported by breaker_state; the current one is 1.
var breakerStates = []string{"closed", "open", "half-open"}

// metric is one row of the export table. Adding a Stats field = adding one row.
type metric struct {
	name, help string
	typ        prometheus.ValueType
	labels     []string // variable label names
	values     []string // variable label values for this row
	get        func(cachex.Stats) float64
}

var metrics = []metric{
	{"hits_total", "Cache hits by tier.", prometheus.CounterValue, []string{"tier"}, []string{"l1"}, func(s cachex.Stats) float64 { return float64(s.L1Hits) }},
	{"hits_total", "Cache hits by tier.", prometheus.CounterValue, []string{"tier"}, []string{"l2"}, func(s cachex.Stats) float64 { return float64(s.L2Hits) }},
	{"misses_total", "Cache misses by tier.", prometheus.CounterValue, []string{"tier"}, []string{"l1"}, func(s cachex.Stats) float64 { return float64(s.L1Misses) }},
	{"misses_total", "Cache misses by tier.", prometheus.CounterValue, []string{"tier"}, []string{"l2"}, func(s cachex.Stats) float64 { return float64(s.L2Misses) }},
	{"l2_errors_total", "L2 transport failures.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.L2Errors) }},
	{"l2_skipped_total", "L2 calls short-circuited by the open breaker.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.L2Skipped) }},
	{"loads_total", "Loader invocations.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.Loads) }},
	{"load_errors_total", "Loader invocations that returned an error.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.LoadErrors) }},
	{"loads_shared_total", "Callers deduplicated by single-flight.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.LoadsShared) }},
	{"stale_served_total", "Stale values served.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.StaleServed) }},
	{"stale_on_error_total", "Stale values served because the loader failed.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.StaleOnError) }},
	{"early_refreshes_total", "Early (probabilistic) refreshes triggered.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.EarlyRefreshes) }},
	{"decode_errors_total", "Values that failed to decode.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.DecodeErrors) }},
	{"negative_hits_total", "Lookups answered by a cached not-found.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.NegativeHits) }},
	{"lock_waits_total", "Loads that waited on another process's distributed lock.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.LockWaits) }},
	{"publish_errors_total", "Failed invalidation broadcasts.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.PublishErrors) }},
	{"evictions_total", "L1 evictions.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.Evictions) }},
	{"expirations_total", "L1 expirations.", prometheus.CounterValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.Expirations) }},
	{"entries", "Entries currently in L1.", prometheus.GaugeValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.Entries) }},
	{"bytes", "Bytes currently held in L1.", prometheus.GaugeValue, nil, nil, func(s cachex.Stats) float64 { return float64(s.Bytes) }},
}

type collector struct {
	src     StatsSource
	descs   []*prometheus.Desc // parallel to metrics
	breaker *prometheus.Desc
	unique  []*prometheus.Desc
}

// NewCollector returns a prometheus.Collector reading src.Stats() once per scrape.
func NewCollector(src StatsSource, opts ...Option) prometheus.Collector {
	cfg := config{namespace: "cachex"}
	for _, o := range opts {
		o(&cfg)
	}
	c := &collector{src: src}
	byName := map[string]*prometheus.Desc{}
	for _, m := range metrics {
		d, ok := byName[m.name]
		if !ok {
			d = prometheus.NewDesc(prometheus.BuildFQName(cfg.namespace, "", m.name), m.help, m.labels, cfg.constLabels)
			byName[m.name] = d
			c.unique = append(c.unique, d)
		}
		c.descs = append(c.descs, d)
	}
	c.breaker = prometheus.NewDesc(prometheus.BuildFQName(cfg.namespace, "", "breaker_state"),
		"L2 circuit breaker state; 1 for the current state, 0 otherwise.", []string{"state"}, cfg.constLabels)
	c.unique = append(c.unique, c.breaker)
	return c
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.unique {
		ch <- d
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	s := c.src.Stats()
	for i, m := range metrics {
		ch <- prometheus.MustNewConstMetric(c.descs[i], m.typ, m.get(s), m.values...)
	}
	for _, st := range breakerStates {
		v := 0.0
		if s.Breaker == st {
			v = 1
		}
		ch <- prometheus.MustNewConstMetric(c.breaker, prometheus.GaugeValue, v, st)
	}
}
