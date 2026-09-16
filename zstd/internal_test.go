package cachexzstd

import "testing"

// Options can't produce an invalid encoder level from outside the package; force one to
// exercise the encoder construction error.
func TestNewEncoderError(t *testing.T) {
	if _, err := New(func(s *settings) error { s.level = 0; return nil }); err == nil {
		t.Fatal("want encoder error")
	}
}

// The decoder window must be at least 1KiB, so a smaller max decoded size fails in New.
func TestNewDecoderError(t *testing.T) {
	if _, err := New(WithMaxDecodedSize(1)); err == nil {
		t.Fatal("want decoder error")
	}
}
