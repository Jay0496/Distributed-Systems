package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetConfig returns the cluster-wide default redundancy mode. The leader
// performs a get_config apply through Raft so the value is linearizable;
// followers forward to the leader.
func (s *Server) GetConfig(ctx context.Context, req *pb.GetConfigRequest) (*pb.ConfigResponse, error) {
	if s.node == nil || s.node.Raft == nil {
		return nil, status.Error(codes.Internal, "raft node is not configured")
	}
	if s.node.Raft.State() != raft.Leader {
		return s.forwardGetConfig(ctx, req)
	}
	return s.getConfig()
}

// SetConfig validates and commits a new cluster-wide default mode. Only the
// leader mutates configuration; followers forward using the admin forwarding
// path.
func (s *Server) SetConfig(ctx context.Context, req *pb.SetConfigRequest) (*pb.ConfigResponse, error) {
	mode := req.GetDefaultMode()
	if mode == "" {
		return &pb.ConfigResponse{Success: false, Message: "default_mode is required"}, nil
	}
	if err := validateMode(mode); err != nil {
		return &pb.ConfigResponse{Success: false, Message: err.Error()}, nil
	}

	if s.node == nil || s.node.Raft == nil {
		return nil, status.Error(codes.Internal, "raft node is not configured")
	}
	if s.node.Raft.State() != raft.Leader {
		return s.forwardSetConfig(ctx, req)
	}
	return s.setConfig(mode)
}

// Delete removes a key's metadata through Raft and then garbage-collects the
// stripe's fragments on their owners. Metadata always commits first. A missing
// key is idempotent and reports found=false. Followers forward to the leader.
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := validateKey(req.GetKey()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetExpectedVersion() < 0 {
		return nil, status.Error(codes.InvalidArgument, "expected_version must not be negative")
	}
	if s.node == nil || s.node.Raft == nil {
		return nil, status.Error(codes.Internal, "raft node is not configured")
	}
	if s.node.Raft.State() != raft.Leader {
		return s.forwardDelete(ctx, req)
	}
	return s.deleteKey(req)
}

// deleteKey applies delete_meta on the leader and only then removes fragments.
// The FSM returns the full old metadata atomically with the version check, so
// a concurrent write cannot slip between the check and the delete.
func (s *Server) deleteKey(req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	data, err := marshalCommand(consensus.Command{
		Op:      consensus.OpDeleteMeta,
		Key:     req.GetKey(),
		Version: req.GetExpectedVersion(),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	response, err := s.apply(data, s.cfg.WriteTimeout)
	if err != nil {
		return nil, adminStatusError("delete metadata", err)
	}
	result, ok := response.(*consensus.Result)
	if !ok || result == nil {
		return nil, status.Errorf(codes.Internal, "unexpected delete_meta response %T", response)
	}
	if result.Err != "" {
		if result.Err == consensus.ResultErrConflict {
			return nil, status.Errorf(codes.Aborted, "key %q version does not match expected_version %d", req.GetKey(), req.GetExpectedVersion())
		}
		return nil, adminResultStatus("delete metadata", result.Err)
	}
	if !result.Found || result.OldMeta == nil {
		return &pb.DeleteResponse{
			Success: true,
			Message: fmt.Sprintf("key %q was already absent", req.GetKey()),
			Found:   false,
		}, nil
	}

	old := *result.OldMeta
	// The metadata commit already removed keys, stripes, and placements on
	// every node. Fragment deletion is therefore a fragment-only cleanup and
	// runs after the commit, never before it. A partial failure leaves the
	// remainder to task 09's orphan sweep.
	gcCtx, cancel := context.WithTimeout(context.Background(), gcTimeout)
	s.deleteStripeFragments(gcCtx, old.StripeID, old.Placement)
	cancel()

	return &pb.DeleteResponse{
		Success:     true,
		Message:     fmt.Sprintf("key %q deleted", req.GetKey()),
		OldStripeId: old.StripeID,
		Found:       true,
	}, nil
}

// forwardDelete forwards a delete to the leader, preserving application
// statuses such as Aborted from the leader.
func (s *Server) forwardDelete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	var resp *pb.DeleteResponse
	err := s.forwardToLeader(ctx, func(fctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.Delete(fctx, req)
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// getConfig applies a get_config command on the leader. The FSM reports the
// rs32 default when no configuration has been committed yet.
func (s *Server) getConfig() (*pb.ConfigResponse, error) {
	data, err := marshalCommand(consensus.Command{Op: consensus.OpGetConfig})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	response, err := s.apply(data, s.cfg.WriteTimeout)
	if err != nil {
		return nil, adminStatusError("read config", err)
	}
	result, ok := response.(*consensus.Result)
	if !ok || result == nil {
		return nil, status.Errorf(codes.Internal, "unexpected get_config response %T", response)
	}
	if result.Err != "" {
		return nil, adminResultStatus("read config", result.Err)
	}
	if result.Config == nil {
		return nil, status.Error(codes.Internal, "get_config returned no configuration")
	}
	return &pb.ConfigResponse{
		DefaultMode: result.Config.DefaultMode,
		Success:     true,
		Message:     "cluster configuration",
		LeaderId:    s.cfg.NodeID,
	}, nil
}

// setConfig applies a set_config command on the leader and waits for commit.
func (s *Server) setConfig(mode string) (*pb.ConfigResponse, error) {
	cfg := storage.ClusterConfig{DefaultMode: mode}
	data, err := marshalCommand(consensus.Command{Op: consensus.OpSetConfig, Config: &cfg})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	response, err := s.apply(data, s.cfg.WriteTimeout)
	if err != nil {
		return nil, adminStatusError("commit config", err)
	}
	result, ok := response.(*consensus.Result)
	if !ok || result == nil {
		return nil, status.Errorf(codes.Internal, "unexpected set_config response %T", response)
	}
	if result.Err != "" {
		return &pb.ConfigResponse{
			Success:  false,
			Message:  fmt.Sprintf("set_config rejected: %s", result.Err),
			LeaderId: s.cfg.NodeID,
		}, nil
	}

	committed := mode
	if result.Config != nil {
		committed = result.Config.DefaultMode
	}
	return &pb.ConfigResponse{
		DefaultMode: committed,
		Success:     true,
		Message:     "cluster configuration committed",
		LeaderId:    s.cfg.NodeID,
	}, nil
}

func (s *Server) forwardGetConfig(ctx context.Context, req *pb.GetConfigRequest) (*pb.ConfigResponse, error) {
	var resp *pb.ConfigResponse
	err := s.forwardToLeader(ctx, func(fctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.GetConfig(fctx, req)
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *Server) forwardSetConfig(ctx context.Context, req *pb.SetConfigRequest) (*pb.ConfigResponse, error) {
	var resp *pb.ConfigResponse
	err := s.forwardToLeader(ctx, func(fctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.SetConfig(fctx, req)
		if err != nil {
			return err
		}
		resp = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// adminStatusError maps a failed Raft apply to a gRPC status. Admin mutations
// are not idempotent responses, so an ambiguous failure is never reported as
// success.
func adminStatusError(action string, err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%s timed out: %v", action, err)
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "%s cancelled: %v", action, err)
	default:
		return status.Errorf(codes.Internal, "%s: %v", action, err)
	}
}

// adminResultStatus maps a typed FSM error code to a gRPC status.
func adminResultStatus(action, code string) error {
	switch code {
	case consensus.ResultErrConflict:
		return status.Errorf(codes.Aborted, "%s conflict", action)
	case consensus.ResultErrInvalid:
		return status.Errorf(codes.InvalidArgument, "%s rejected as invalid", action)
	case consensus.ResultErrNotFound:
		return status.Errorf(codes.NotFound, "%s: not found", action)
	default:
		return status.Errorf(codes.Internal, "%s failed", action)
	}
}

// defaultModeForStats reads the locally applied cluster configuration without
// another Raft round trip. Stats is deliberately local-only.
func (s *Server) defaultModeForStats() string {
	cfg, err := s.store.GetConfig()
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return string(redundancy.ModeRS32)
		}
		return string(redundancy.ModeRS32)
	}
	if _, err := redundancy.ParseMode(cfg.DefaultMode); err != nil {
		return string(redundancy.ModeRS32)
	}
	return cfg.DefaultMode
}

// uptimeSeconds reports whole seconds since the server was constructed. A
// zero construction time (as in some tests) reports zero.
func (s *Server) uptimeSeconds() int64 {
	if s.startedAt.IsZero() {
		return 0
	}
	seconds := int64(time.Since(s.startedAt) / time.Second)
	if seconds < 0 {
		return 0
	}
	return seconds
}
