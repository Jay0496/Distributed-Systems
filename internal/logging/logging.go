// Package logging builds the process-wide structured logger. Container
// processes use JSON (LOG_FORMAT=json) so logs can be parsed by a collector;
// local runs use a readable text handler.
package logging

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"distributed-kv-store/internal/rpc"
)

// FormatJSON and FormatText are the supported LOG_FORMAT values.
const (
	FormatJSON = "json"
	FormatText = "text"
)

// New returns a structured logger with the service identity attached. Unknown
// formats fall back to text so a typo never disables logging.
func New(w io.Writer, format, service string, attrs ...any) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}

	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case FormatJSON:
		handler = slog.NewJSONHandler(w, opts)
	default:
		handler = slog.NewTextHandler(w, opts)
	}

	base := []any{"service", service}
	base = append(base, attrs...)
	return slog.New(handler).With(base...)
}

// RequestAttrs returns the correlation fields for one request. It returns no
// fields when the context has no request ID, so background workers stay clean.
func RequestAttrs(ctx context.Context) []any {
	id := rpc.RequestID(ctx)
	if id == "" {
		return nil
	}
	return []any{"request_id", id}
}
