package cachex

// WithRand fixes the XFetch random source in tests.
func WithRand(f func() float64) Option {
	return func(c *config) error { c.rand = f; return nil }
}
