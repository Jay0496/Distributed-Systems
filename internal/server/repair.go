package server

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"distributed-kv-store/internal/cluster"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Repair validates an on-demand repair request and enqueues it on the leader.
// Followers forward, like every other metadata operation. The RPC returns as
// soon as the work is accepted; it never waits for the full reconstruction.
func (s *Server) Repair(ctx context.Context, req *pb.RepairRequest) (*pb.RepairResponse, error) {
	if strings.TrimSpace(req.GetKey()) == "" {
		return &pb.RepairResponse{Success: false, Message: "key is required"}, nil
	}
	if err := validateKey(req.GetKey()); err != nil {
		return &pb.RepairResponse{Success: false, Message: err.Error()}, nil
	}
	if s.node == nil || s.node.Raft == nil {
		return nil, status.Error(codes.Internal, "raft node is not configured")
	}

	// Only the leader owns metadata reads and repair scheduling.
	if s.node.Raft.State() != raft.Leader {
		return s.forwardRepair(ctx, req)
	}
	return s.repair(ctx, req)
}

// repair resolves the key's current stripe and enqueues one task per placement.
// stripe_id is optional precision and must match the committed stripe. force
// re-encodes and replaces present shards, which is only safe because the
// repairer reads a verified value first.
func (s *Server) repair(ctx context.Context, req *pb.RepairRequest) (*pb.RepairResponse, error) {
	meta, found, err := s.readMeta(ctx, req.GetKey())
	if err != nil {
		return nil, adminStatusError("read metadata for repair", err)
	}
	if !found {
		return &pb.RepairResponse{
			Success: false,
			Message: fmt.Sprintf("key %q not found", req.GetKey()),
		}, nil
	}
	if stripeID := req.GetStripeId(); stripeID != "" && stripeID != meta.StripeID {
		return &pb.RepairResponse{
			Success: false,
			Message: fmt.Sprintf("stripe_id %q does not match the current stripe %q", stripeID, meta.StripeID),
		}, nil
	}
	if s.repairQueue == nil {
		return &pb.RepairResponse{Success: false, Message: "repair queue is not configured"}, nil
	}

	priority := cluster.PriorityDegraded
	if req.GetForce() {
		priority = cluster.PriorityReadRepair
	}

	accepted := 0
	for _, placement := range meta.Placement {
		task := cluster.Task{
			Key:        meta.Key,
			StripeID:   meta.StripeID,
			Version:    meta.Version,
			ShardIndex: placement.ShardIndex,
			Owner:      placement.NodeID,
			Replace:    req.GetForce(),
			Priority:   priority,
		}
		if err := s.repairQueue.Enqueue(task); err != nil {
			slog.Warn("repair enqueue rejected",
				"key", meta.Key, "stripe_id", meta.StripeID,
				"shard_index", placement.ShardIndex, "error", err)
			continue
		}
		accepted++
	}
	if accepted == 0 {
		return &pb.RepairResponse{Success: false, Message: "repair queue rejected every task"}, nil
	}

	message := fmt.Sprintf("repair accepted for %d shards of key %q", accepted, meta.Key)
	if req.GetForce() {
		message = fmt.Sprintf("force repair accepted for %d shards of key %q", accepted, meta.Key)
	}
	return &pb.RepairResponse{Success: true, Message: message}, nil
}

// forwardRepair forwards a repair request to the current leader and preserves
// the leader's application response.
func (s *Server) forwardRepair(ctx context.Context, req *pb.RepairRequest) (*pb.RepairResponse, error) {
	var resp *pb.RepairResponse
	err := s.forwardToLeader(ctx, func(fctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.Repair(fctx, req)
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
