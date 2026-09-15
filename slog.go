package cachex

import (
	"context"
	"fmt"
	"log/slog"
)

// SlogEvents logs rare, actionable events to l: L2 errors and publish errors at Warn, breaker
// opening at Warn (other transitions at Info), loader panics at Error. Per-operation hooks
// stay nil, so the hot path is unaffected. Combine with other hooks via MergeEvents.
func SlogEvents(l *slog.Logger) Events {
	return Events{
		L2Error: func(op string, err error) {
			l.Warn("cachex: L2 error", "op", op, "error", err.Error())
		},
		BreakerChange: func(from, to string) {
			level := slog.LevelInfo
			if to == "open" {
				level = slog.LevelWarn
			}
			l.Log(context.Background(), level, "cachex: breaker state changed", "from", from, "to", to)
		},
		LoaderPanic: func(key string, recovered any) {
			l.Error("cachex: loader panicked", "key", key, "panic", fmt.Sprint(recovered))
		},
		PublishError: func(msg Invalidation, err error) {
			l.Warn("cachex: invalidation publish failed", "keys", msg.Keys, "namespace", msg.Namespace, "error", err.Error())
		},
	}
}
