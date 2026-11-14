package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"distributed-kv-store/internal/logging"
	"distributed-kv-store/internal/rpc"
)

func TestJSONLoggerEmitsStructuredLifecycleFields(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.New(&buf, logging.FormatJSON, "server", "node_id", "node1")

	ctx := rpc.WithRequestID(context.Background(), "request-42")
	logger.Info("write committed",
		append(logging.RequestAttrs(ctx),
			"key", "alpha", "stripe_id", "stripe-1", "mode", "rs32", "version", 1)...)

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("log is not JSON: %v (%q)", err, buf.String())
	}
	want := map[string]any{
		"msg":        "write committed",
		"service":    "server",
		"node_id":    "node1",
		"key":        "alpha",
		"stripe_id":  "stripe-1",
		"mode":       "rs32",
		"request_id": "request-42",
	}
	for field, expected := range want {
		if got := record[field]; got != expected {
			t.Fatalf("field %q = %v, want %v", field, got, expected)
		}
	}

	for _, forbidden := range []string{"secret-token", "payload-bytes"} {
		if strings.Contains(buf.String(), forbidden) {
			t.Fatalf("log contains forbidden value %q: %s", forbidden, buf.String())
		}
	}
}

func TestTextLoggerAddsServiceIdentity(t *testing.T) {
	var buf bytes.Buffer
	logger := logging.New(&buf, logging.FormatText, "gateway")
	logger.Warn("retry", "attempt", 2)

	out := buf.String()
	if !strings.Contains(out, "service=gateway") || !strings.Contains(out, "msg=retry") {
		t.Fatalf("text log = %q", out)
	}
}
