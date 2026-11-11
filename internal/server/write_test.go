package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"
)

// metadataApply is a Raft Apply fake backed by a testStore. It handles the two
// metadata commands the write coordinator reads before it stores fragments.
func metadataApply(store *testStore) applyFunc {
	return func(data []byte, _ time.Duration) (interface{}, error) {
		var cmd consensus.Command
		if err := json.Unmarshal(data, &cmd); err != nil {
			return nil, err
		}
		switch cmd.Op {
		case consensus.OpGetMeta:
			meta, err := store.GetKey(cmd.Key)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					return &consensus.Result{Found: false}, nil
				}
				return &consensus.Result{Err: consensus.ResultErrInternal}, nil
			}
			return &consensus.Result{Meta: &meta, Found: true}, nil
		case consensus.OpPutMeta:
			old, err := store.ApplyPutMeta(1, *cmd.Meta)
			if err != nil {
				if errors.Is(err, storage.ErrConflict) {
					return &consensus.Result{Err: consensus.ResultErrConflict}, nil
				}
				return &consensus.Result{Err: consensus.ResultErrInternal}, nil
			}
			result := &consensus.Result{Meta: cmd.Meta, Found: true}
			if old.Key != "" && old.StripeID != cmd.Meta.StripeID {
				result.OldMeta = &old
			}
			return result, nil
		default:
			return nil, fmt.Errorf("unexpected command %q", cmd.Op)
		}
	}
}

func newWriteTestServer(store *testStore) *Server {
	srv := &Server{store: store, cfg: config.Config{NodeID: "node1"}.WithDefaults()}
	srv.apply = metadataApply(store)
	return srv
}

func TestValidatePut(t *testing.T) {
	const maxValue = 16

	tests := []struct {
		name    string
		key     string
		value   []byte
		mode    string
		version int64
		wantErr bool
	}{
		{name: "valid", key: "k", value: []byte("v"), mode: "rs32"},
		{name: "empty key", key: "", value: []byte("v"), wantErr: true},
		{name: "long key", key: strings.Repeat("k", 513), value: []byte("v"), wantErr: true},
		{name: "control key", key: "bad\x00key", value: []byte("v"), wantErr: true},
		{name: "empty value", key: "k", value: nil, wantErr: true},
		{name: "oversized value", key: "k", value: make([]byte, 17), wantErr: true},
		{name: "invalid mode", key: "k", value: []byte("v"), mode: "triple", wantErr: true},
		{name: "empty mode resolves later", key: "k", value: []byte("v"), mode: ""},
		{name: "negative expected version", key: "k", value: []byte("v"), version: -1, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePut(tc.key, tc.value, tc.mode, tc.version, maxValue)
			if tc.wantErr && err == nil {
				t.Fatal("expected a validation error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}

func TestEffectiveModeResolution(t *testing.T) {
	store := newTestStore()
	srv := newWriteTestServer(store)

	t.Run("defaults to rs32", func(t *testing.T) {
		mode, err := srv.effectiveMode("")
		if err != nil {
			t.Fatalf("effectiveMode: %v", err)
		}
		if mode != redundancy.ModeRS32 {
			t.Fatalf("mode = %q, want rs32", mode)
		}
	})

	t.Run("cluster config wins over the default", func(t *testing.T) {
		if err := store.ApplySetConfig(1, storage.ClusterConfig{DefaultMode: string(redundancy.ModeReplication)}); err != nil {
			t.Fatalf("set config: %v", err)
		}
		mode, err := srv.effectiveMode("")
		if err != nil {
			t.Fatalf("effectiveMode: %v", err)
		}
		if mode != redundancy.ModeReplication {
			t.Fatalf("mode = %q, want replication", mode)
		}
	})

	t.Run("request overrides cluster config", func(t *testing.T) {
		mode, err := srv.effectiveMode(string(redundancy.ModeRS32))
		if err != nil {
			t.Fatalf("effectiveMode: %v", err)
		}
		if mode != redundancy.ModeRS32 {
			t.Fatalf("mode = %q, want rs32", mode)
		}
	})

	t.Run("rejects unknown mode", func(t *testing.T) {
		if _, err := srv.effectiveMode("triple"); err == nil {
			t.Fatal("expected an invalid-mode error")
		}
	})
}

func TestNewWriteIntentVersioning(t *testing.T) {
	store := newTestStore()
	srv := newWriteTestServer(store)
	ctx := context.Background()

	t.Run("new key starts at version 1", func(t *testing.T) {
		intent, err := srv.newWriteIntent(ctx, &pb.PutRequest{Key: "new", Value: []byte("v")})
		if err != nil {
			t.Fatalf("newWriteIntent: %v", err)
		}
		if intent.version != 1 {
			t.Fatalf("version = %d, want 1", intent.version)
		}
		if len(intent.stripeID) != 64 {
			t.Fatalf("stripe ID = %q, want a 64-character hex digest", intent.stripeID)
		}
		if intent.stripeID == intent.key {
			t.Fatal("stripe ID must not be the key")
		}
	})

	t.Run("existing key increments the version", func(t *testing.T) {
		meta := storage.KeyMeta{
			Key:      "existing",
			StripeID: "stripe-a",
			Mode:     string(redundancy.ModeRS32),
			Size:     1,
			Checksum: redundancy.Checksum([]byte("v")),
			Version:  2,
		}
		if _, err := store.ApplyPutMeta(2, meta); err != nil {
			t.Fatalf("seed metadata: %v", err)
		}

		intent, err := srv.newWriteIntent(ctx, &pb.PutRequest{Key: "existing", Value: []byte("v")})
		if err != nil {
			t.Fatalf("newWriteIntent: %v", err)
		}
		if intent.version != 3 {
			t.Fatalf("version = %d, want 3", intent.version)
		}
	})

	t.Run("expected version must match", func(t *testing.T) {
		_, err := srv.newWriteIntent(ctx, &pb.PutRequest{
			Key:             "existing",
			Value:           []byte("v"),
			ExpectedVersion: 1,
		})
		if !errors.Is(err, errVersionConflict) {
			t.Fatalf("expected errVersionConflict, got %v", err)
		}

		intent, err := srv.newWriteIntent(ctx, &pb.PutRequest{
			Key:             "existing",
			Value:           []byte("v"),
			ExpectedVersion: 2,
		})
		if err != nil {
			t.Fatalf("matching expected_version: %v", err)
		}
		if intent.version != 3 {
			t.Fatalf("version = %d, want 3", intent.version)
		}
	})

	t.Run("expected version on a missing key conflicts", func(t *testing.T) {
		_, err := srv.newWriteIntent(ctx, &pb.PutRequest{
			Key:             "absent",
			Value:           []byte("v"),
			ExpectedVersion: 1,
		})
		if !errors.Is(err, errVersionConflict) {
			t.Fatalf("expected errVersionConflict, got %v", err)
		}
	})

	t.Run("stripe IDs are unique for the same key and version", func(t *testing.T) {
		first, err := newStripeID("same", 1)
		if err != nil {
			t.Fatalf("newStripeID: %v", err)
		}
		second, err := newStripeID("same", 1)
		if err != nil {
			t.Fatalf("newStripeID: %v", err)
		}
		if first == second {
			t.Fatal("two stripe IDs for the same key/version collided")
		}
	})
}
