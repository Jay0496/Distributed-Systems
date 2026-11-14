package rpc_test

import (
	"context"
	"testing"

	"distributed-kv-store/internal/rpc"

	"google.golang.org/grpc/metadata"
)

func TestNewRequestIDIsUniqueAndNonEmpty(t *testing.T) {
	seen := make(map[string]struct{}, 64)
	for i := 0; i < 64; i++ {
		id := rpc.NewRequestID()
		if id == "" {
			t.Fatal("NewRequestID returned an empty ID")
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate request ID %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestRequestIDIncomingAndOutgoing(t *testing.T) {
	incoming := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		rpc.RequestIDMetadataKey, "request-123",
	))
	ctx, id := rpc.EnsureRequestID(incoming)
	if id != "request-123" {
		t.Fatalf("request ID = %q, want request-123", id)
	}
	if rpc.RequestID(ctx) != "request-123" {
		t.Fatalf("context request ID = %q", rpc.RequestID(ctx))
	}

	outgoing := rpc.AppendRequestID(ctx)
	md, ok := metadata.FromOutgoingContext(outgoing)
	if !ok {
		t.Fatal("outgoing request ID metadata is absent")
	}
	if got := md.Get(rpc.RequestIDMetadataKey); len(got) != 1 || got[0] != "request-123" {
		t.Fatalf("outgoing request ID = %v", got)
	}
}

func TestEnsureRequestIDGeneratesWhenAbsent(t *testing.T) {
	ctx, id := rpc.EnsureRequestID(context.Background())
	if id == "" {
		t.Fatal("EnsureRequestID did not generate an ID")
	}
	if rpc.RequestID(ctx) != id {
		t.Fatalf("context request ID = %q, want %q", rpc.RequestID(ctx), id)
	}
}
