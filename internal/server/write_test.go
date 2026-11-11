package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
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

// applyRecorder wraps a Raft apply function and retains every command for
// assertions about payload exclusion and command shape.
type applyRecorder struct {
	mu    sync.Mutex
	inner applyFunc
	raw   [][]byte
}

func (r *applyRecorder) apply(data []byte, timeout time.Duration) (interface{}, error) {
	r.mu.Lock()
	r.raw = append(r.raw, append([]byte(nil), data...))
	r.mu.Unlock()
	return r.inner(data, timeout)
}

func (r *applyRecorder) commands(op string) [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	var out [][]byte
	for _, data := range r.raw {
		var cmd consensus.Command
		if err := json.Unmarshal(data, &cmd); err != nil {
			continue
		}
		if cmd.Op == op {
			out = append(out, data)
		}
	}
	return out
}

func nonLeaderIDs(c *testCluster, leader *testNode) []string {
	var ids []string
	for _, tn := range c.nodes {
		if tn.id != leader.id {
			ids = append(ids, tn.id)
		}
	}
	return ids
}

func putRPC(t *testing.T, client pb.StorageServiceClient, req *pb.PutRequest) *pb.PutResponse {
	t.Helper()
	ctx, cancel := ctxWithTimeout(t, 20*time.Second)
	defer cancel()
	resp, err := client.Put(ctx, req)
	if err != nil {
		t.Fatalf("Put(%q) RPC failed: %v", req.GetKey(), err)
	}
	return resp
}

func waitForFragmentCount(t *testing.T, c *testCluster, stripeID string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int
	for time.Now().Before(deadline) {
		last = 0
		for _, tn := range c.nodes {
			refs, err := tn.store.ListFragments(stripeID)
			if err != nil {
				t.Fatalf("%s list fragments: %v", tn.id, err)
			}
			last += len(refs)
		}
		if last == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("stripe %q has %d fragments, want %d", stripeID, last, want)
}

func waitForDegraded(t *testing.T, c *testCluster, key string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, tn := range c.nodes {
			meta, err := tn.store.GetKey(key)
			if err != nil || !meta.Degraded {
				ok = false
				continue
			}
		}
		if ok {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("metadata for %q never reported degraded on every node", key)
}

func TestWritePathCommitsMetadataWithoutPayload(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)

	recorder := &applyRecorder{inner: leader.srv.apply}
	leader.srv.apply = recorder.apply

	value := bytes.Repeat([]byte("payload-value-"), 1024) // ~14 KiB
	resp := putRPC(t, leader.client, &pb.PutRequest{Key: "meta-only", Value: value, Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	if resp.GetStripeId() == "" || resp.GetMode() != "rs32" || resp.GetVersion() != 1 {
		t.Fatalf("PutResponse = %+v", resp)
	}
	if resp.GetShardsStored() != 5 || resp.GetDegraded() {
		t.Fatalf("healthy write reported stored=%d degraded=%v", resp.GetShardsStored(), resp.GetDegraded())
	}
	if resp.GetChecksum() != redundancy.Checksum(value) {
		t.Fatalf("response checksum = %q", resp.GetChecksum())
	}

	commands := recorder.commands(consensus.OpPutMeta)
	if len(commands) != 1 {
		t.Fatalf("recorded %d put_meta commands, want 1", len(commands))
	}
	command := commands[0]
	if len(command) > 64*1024 {
		t.Fatalf("put_meta command is %d bytes", len(command))
	}
	if bytes.Contains(command, []byte("payload-value-")) {
		t.Fatal("put_meta command contains payload bytes")
	}

	meta, err := leader.store.GetKey("meta-only")
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if meta.StripeID != resp.GetStripeId() || meta.Mode != "rs32" || meta.Version != 1 || meta.Size != int64(len(value)) {
		t.Fatalf("metadata = %+v", meta)
	}
	if len(meta.Placement) != redundancy.TotalShards {
		t.Fatalf("metadata has %d placements", len(meta.Placement))
	}
}

func TestWriteDegradedWithTwoUnreachableOwners(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	down := nonLeaderIDs(c, leader)
	down = down[:2]

	original := leader.srv.dial
	leader.srv.dial = func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		for _, id := range down {
			if target == id {
				return nil, fmt.Errorf("simulated unreachable owner %s", id)
			}
		}
		return original(ctx, target)
	}

	resp := putRPC(t, leader.client, &pb.PutRequest{Key: "degraded", Value: []byte("survives"), Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("degraded write rejected: %s", resp.GetMessage())
	}
	if !resp.GetDegraded() || resp.GetShardsStored() != writeQuorum {
		t.Fatalf("response stored=%d degraded=%v, want %d and true", resp.GetShardsStored(), resp.GetDegraded(), writeQuorum)
	}
	waitForDegraded(t, c, "degraded")
	waitForFragmentCount(t, c, resp.GetStripeId(), writeQuorum, 5*time.Second)
}

func TestWriteFailsBelowQuorum(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)
	down := nonLeaderIDs(c, leader)
	down = down[:3]

	original := leader.srv.dial
	leader.srv.dial = func(ctx context.Context, target string) (*grpc.ClientConn, error) {
		for _, id := range down {
			if target == id {
				return nil, fmt.Errorf("simulated unreachable owner %s", id)
			}
		}
		return original(ctx, target)
	}

	resp := putRPC(t, leader.client, &pb.PutRequest{Key: "no-quorum", Value: []byte("lost"), Mode: "rs32"})
	if resp.GetSuccess() {
		t.Fatalf("write below quorum succeeded: %+v", resp)
	}
	if resp.GetShardsStored() != 0 {
		t.Fatalf("failed response reported %d stored shards", resp.GetShardsStored())
	}

	if _, err := leader.store.GetKey("no-quorum"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("failed write left metadata: %v", err)
	}
	for _, tn := range c.nodes {
		refs, err := tn.store.ListFragments("")
		if err != nil {
			t.Fatalf("%s list fragments: %v", tn.id, err)
		}
		if len(refs) != 0 {
			t.Fatalf("%s retains %d fragments after a failed write", tn.id, len(refs))
		}
	}
}

func TestWriteDefiniteCommitRejectionCleansFragments(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)

	original := leader.srv.apply
	leader.srv.apply = func(data []byte, timeout time.Duration) (interface{}, error) {
		var cmd consensus.Command
		if err := json.Unmarshal(data, &cmd); err == nil && cmd.Op == consensus.OpPutMeta {
			return &consensus.Result{Err: consensus.ResultErrInvalid}, nil
		}
		return original(data, timeout)
	}

	resp := putRPC(t, leader.client, &pb.PutRequest{Key: "rejected", Value: []byte("gone"), Mode: "rs32"})
	if resp.GetSuccess() {
		t.Fatalf("rejected commit reported success: %+v", resp)
	}
	if _, err := leader.store.GetKey("rejected"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("rejected write left metadata: %v", err)
	}
	waitForFragmentCount(t, c, "", 0, 5*time.Second)
}

func TestWriteAmbiguousCommitKeepsFragments(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)

	original := leader.srv.apply
	leader.srv.apply = func(data []byte, timeout time.Duration) (interface{}, error) {
		var cmd consensus.Command
		if err := json.Unmarshal(data, &cmd); err == nil && cmd.Op == consensus.OpPutMeta {
			return nil, errors.New("simulated apply timeout")
		}
		return original(data, timeout)
	}

	resp := putRPC(t, leader.client, &pb.PutRequest{Key: "ambiguous", Value: []byte("maybe"), Mode: "rs32"})
	if resp.GetSuccess() {
		t.Fatalf("ambiguous commit reported success: %+v", resp)
	}
	if _, err := leader.store.GetKey("ambiguous"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("ambiguous write left metadata: %v", err)
	}
	waitForFragmentCount(t, c, "", redundancy.TotalShards, 5*time.Second)
}

func TestWriteOverwriteGarbageCollectsOldStripe(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)

	first := putRPC(t, leader.client, &pb.PutRequest{Key: "overwrite", Value: []byte("first"), Mode: "rs32"})
	if !first.GetSuccess() {
		t.Fatalf("first Put rejected: %s", first.GetMessage())
	}
	waitForMetaVersion(t, c, "overwrite", 1)

	second := putRPC(t, leader.client, &pb.PutRequest{Key: "overwrite", Value: []byte("second"), Mode: "rs32"})
	if !second.GetSuccess() {
		t.Fatalf("overwrite rejected: %s", second.GetMessage())
	}
	if second.GetVersion() != 2 {
		t.Fatalf("overwrite version = %d, want 2", second.GetVersion())
	}
	if second.GetStripeId() == first.GetStripeId() {
		t.Fatal("overwrite reused the old stripe ID")
	}

	waitForMetaVersion(t, c, "overwrite", 2)
	waitForFragmentCount(t, c, first.GetStripeId(), 0, 10*time.Second)
	waitForFragmentCount(t, c, second.GetStripeId(), redundancy.TotalShards, 5*time.Second)
}

func TestWriteIdempotentRetryKeepsCurrentStripe(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)

	original := leader.srv.apply
	leader.srv.apply = func(data []byte, timeout time.Duration) (interface{}, error) {
		var cmd consensus.Command
		if err := json.Unmarshal(data, &cmd); err == nil && cmd.Op == consensus.OpPutMeta && cmd.Meta != nil {
			response, err := original(data, timeout)
			if err != nil {
				return response, err
			}
			if result, ok := response.(*consensus.Result); ok && result.OldMeta == nil {
				// Simulate an idempotent replay reporting the same stripe as
				// the previous metadata. The GC guard must ignore it.
				result.OldMeta = cmd.Meta
			}
			return response, nil
		}
		return original(data, timeout)
	}

	resp := putRPC(t, leader.client, &pb.PutRequest{Key: "idempotent", Value: []byte("keep"), Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	// Give a buggy asynchronous GC time to delete the current stripe.
	time.Sleep(200 * time.Millisecond)
	waitForFragmentCount(t, c, resp.GetStripeId(), redundancy.TotalShards, 5*time.Second)
}

func TestWriteExpectedVersionConflict(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitLeader(t)

	first := putRPC(t, leader.client, &pb.PutRequest{Key: "cas", Value: []byte("v1")})
	if !first.GetSuccess() {
		t.Fatalf("first Put rejected: %s", first.GetMessage())
	}
	waitForMetaVersion(t, c, "cas", 1)

	stale := putRPC(t, leader.client, &pb.PutRequest{Key: "cas", Value: []byte("v2"), ExpectedVersion: 99})
	if stale.GetSuccess() {
		t.Fatalf("stale expected_version was accepted: %+v", stale)
	}

	matched := putRPC(t, leader.client, &pb.PutRequest{Key: "cas", Value: []byte("v2"), ExpectedVersion: 1})
	if !matched.GetSuccess() || matched.GetVersion() != 2 {
		t.Fatalf("matching expected_version failed: %+v", matched)
	}
}
