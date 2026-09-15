package cachex_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/bakhod1r/cachex"
)

func TestSlogEvents(t *testing.T) {
	var buf bytes.Buffer
	ev := cachex.SlogEvents(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if ev.OpStart != nil || ev.OpEnd != nil || ev.LoadEnd != nil {
		t.Fatal("per-operation hooks must stay nil")
	}
	ev.L2Error("get", errors.New("down"))
	ev.BreakerChange("closed", "open")
	ev.BreakerChange("open", "half-open")
	ev.LoaderPanic("k", "boom")
	ev.PublishError(cachex.Invalidation{Keys: []string{"k"}}, errors.New("nats"))

	want := []struct{ level, field, value string }{
		{"WARN", "op", "get"},
		{"WARN", "to", "open"},
		{"INFO", "to", "half-open"},
		{"ERROR", "key", "k"},
		{"WARN", "error", "nats"},
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != len(want) {
		t.Fatalf("got %d lines:\n%s", len(lines), buf.String())
	}
	for i, w := range want {
		var rec map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &rec); err != nil {
			t.Fatal(err)
		}
		if rec["level"] != w.level || rec[w.field] != w.value {
			t.Fatalf("line %d: %s", i, lines[i])
		}
	}
}
