package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

// Stats reports this node's local storage and repair counters. It never
// forwards: every node can answer from its own SQLite file.
func (s *Server) Stats(ctx context.Context, _ *pb.StatsRequest) (*pb.StatsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	stats, err := s.store.Stats()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read local stats: %v", err)
	}
	queue := s.localQueueStats()

	leader := s.node != nil && s.node.Raft != nil && s.node.Raft.State() == raft.Leader
	return &pb.StatsResponse{
		NodeId:           s.cfg.NodeID,
		Mode:             s.defaultModeForStats(),
		Leader:           leader,
		Keys:             nonNegative(stats.KeyCount),
		Stripes:          nonNegative(stats.StripeCount),
		Fragments:        nonNegative(stats.FragmentCount),
		StorageBytes:     nonNegative(stats.Bytes),
		RepairQueueDepth: uint64(queue.Depth),
		RepairsCompleted: queue.Completed,
		RepairsFailed:    queue.Failed,
		UptimeSeconds:    s.uptimeSeconds(),
	}, nil
}

// localQueueStats reads the shared queue counters when the configured queue
// exposes them. A test double that only implements Enqueue/Close reports zero.
func (s *Server) localQueueStats() cluster.QueueStats {
	if provider, ok := s.repairQueue.(interface{ Stats() cluster.QueueStats }); ok {
		return provider.Stats()
	}
	return cluster.QueueStats{}
}

// ClusterHealth reports the leader, global mode, and an on-demand probe of
// every Raft voter. Followers forward because only the leader has the complete
// Raft configuration.
func (s *Server) ClusterHealth(ctx context.Context, req *pb.ClusterHealthRequest) (*pb.ClusterHealthResponse, error) {
	if s.node == nil || s.node.Raft == nil {
		return nil, status.Error(codes.Internal, "raft node is not configured")
	}
	if s.node.Raft.State() != raft.Leader {
		return s.forwardClusterHealth(ctx, req)
	}
	return s.clusterHealth(ctx)
}

// clusterHealth reports the cached detector view when available and falls back
// to an on-demand probe for nodes the detector has not seen. The detector is
// maintained by the leader, so followers still forward here.
func (s *Server) clusterHealth(ctx context.Context) (*pb.ClusterHealthResponse, error) {
	configuration, err := s.raftConfiguration(s.cfg.ClusterProbeTimeout)
	if err != nil {
		return &pb.ClusterHealthResponse{
			Success: false,
			Mode:    s.defaultModeForStats(),
			Message: fmt.Sprintf("read raft configuration: %v", err),
		}, nil
	}

	servers := append([]raft.Server(nil), configuration.Servers...)
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].ID < servers[j].ID
	})

	_, leaderID := s.node.Raft.LeaderWithID()

	keys, err := s.store.ListKeys()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "count degraded stripes: %v", err)
	}
	degradedStripes := uint64(0)
	for _, meta := range keys {
		if meta.Degraded {
			degradedStripes++
		}
	}

	var cached map[string]cluster.NodeState
	if s.detector != nil {
		cached = s.detector.Snapshot()
	}

	nodes := make([]*pb.NodeHealth, len(servers))
	var wg sync.WaitGroup
	for i, server := range servers {
		if state, ok := cached[string(server.ID)]; ok {
			nodes[i] = healthFromState(state)
			continue
		}
		wg.Add(1)
		go func(i int, server raft.Server) {
			defer wg.Done()
			nodes[i] = s.probeNode(ctx, string(server.ID))
		}(i, server)
	}
	wg.Wait()

	healthy := uint32(0)
	for _, node := range nodes {
		if node.GetHealthy() {
			healthy++
		}
	}
	total := uint32(len(nodes))
	degraded := healthy != total || degradedStripes > 0
	queue := s.localQueueStats()

	message := fmt.Sprintf("%d/%d nodes healthy, %d degraded stripes", healthy, total, degradedStripes)
	if leaderID == "" {
		message = "no leader elected; " + message
	}

	return &pb.ClusterHealthResponse{
		Success:          true,
		LeaderId:         string(leaderID),
		Mode:             s.defaultModeForStats(),
		Degraded:         degraded,
		HealthyNodes:     healthy,
		TotalNodes:       total,
		Nodes:            nodes,
		DegradedStripes:  degradedStripes,
		RepairQueueDepth: uint64(queue.Depth),
		Message:          message,
	}, nil
}

// healthFromState converts a cached detector entry to the wire shape.
func healthFromState(state cluster.NodeState) *pb.NodeHealth {
	health := &pb.NodeHealth{
		NodeId:    state.NodeID,
		GrpcAddr:  state.GRPCAddr,
		Healthy:   state.Healthy,
		Leader:    state.Leader,
		Error:     state.LastError,
		RaftState: "",
	}
	if !state.LastSeen.IsZero() {
		health.LastSeenUnix = state.LastSeen.Unix()
	}
	if state.Leader {
		health.RaftState = raft.Leader.String()
	}
	return health
}

// probeNode asks one voter for NodeStatus and Stats. A node is healthy only
// when both queries succeed. The local node is probed in-process; remote nodes
// use the injected dialer, which carries AUTH_TOKEN in production.
func (s *Server) probeNode(ctx context.Context, nodeID string) *pb.NodeHealth {
	health := &pb.NodeHealth{
		NodeId:   nodeID,
		GrpcAddr: s.grpcAddrFor(nodeID),
	}

	probeCtx, cancel := context.WithTimeout(ctx, s.cfg.ClusterProbeTimeout)
	defer cancel()

	var (
		status *pb.NodeStatusResponse
		stats  *pb.StatsResponse
		err    error
	)
	if nodeID == s.cfg.NodeID {
		status, err = s.NodeStatus(probeCtx, &pb.NodeStatusRequest{})
	} else {
		status, err = s.remoteNodeStatus(probeCtx, nodeID)
	}
	if err != nil {
		health.Error = "node_status: " + err.Error()
		return health
	}

	health.RaftState = status.GetState()
	health.Leader = status.GetState() == raft.Leader.String()

	if nodeID == s.cfg.NodeID {
		stats, err = s.Stats(probeCtx, &pb.StatsRequest{})
	} else {
		stats, err = s.remoteStats(probeCtx, nodeID)
	}
	if err != nil {
		health.Error = "stats: " + err.Error()
		return health
	}

	health.Healthy = true
	health.LastSeenUnix = time.Now().Unix()
	health.Fragments = stats.GetFragments()
	health.StorageBytes = stats.GetStorageBytes()
	return health
}

func (s *Server) remoteNodeStatus(ctx context.Context, nodeID string) (*pb.NodeStatusResponse, error) {
	client, closer, err := s.peerClient(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	defer closer()
	return client.NodeStatus(ctx, &pb.NodeStatusRequest{})
}

// ProbePeer performs one detector probe: the standard gRPC health service plus
// NodeStatus. Both must succeed. The health service stays unauthenticated; the
// NodeStatus call rides the shared cached dialer, which carries AUTH_TOKEN in
// production. This method is wired as cluster.DetectorOptions.Probe.
func (s *Server) ProbePeer(ctx context.Context, nodeID, grpcAddr string) (cluster.NodeState, error) {
	state := cluster.NodeState{NodeID: nodeID, GRPCAddr: grpcAddr}
	probeCtx, cancel := context.WithTimeout(ctx, s.cfg.ClusterProbeTimeout)
	defer cancel()

	if nodeID == s.cfg.NodeID {
		status, err := s.NodeStatus(probeCtx, &pb.NodeStatusRequest{})
		if err != nil {
			return state, err
		}
		state.LastSeen = time.Now()
		state.Leader = status.GetState() == raft.Leader.String()
		return state, nil
	}

	target := grpcAddr
	if target == "" {
		target = s.grpcAddrFor(nodeID)
	}
	if target == "" {
		return state, fmt.Errorf("no gRPC address for node %q", nodeID)
	}
	conn, err := s.getConn(probeCtx, target)
	if err != nil {
		return state, fmt.Errorf("dial %q: %w", nodeID, err)
	}

	health, err := healthpb.NewHealthClient(conn).Check(probeCtx, &healthpb.HealthCheckRequest{
		Service: "storage.StorageService",
	})
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			s.dropConn(target)
		}
		return state, fmt.Errorf("health check %q: %w", nodeID, err)
	}
	if health.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return state, fmt.Errorf("node %q health is %s", nodeID, health.GetStatus())
	}

	statusResp, err := pb.NewStorageServiceClient(conn).NodeStatus(probeCtx, &pb.NodeStatusRequest{})
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			s.dropConn(target)
		}
		return state, fmt.Errorf("node status %q: %w", nodeID, err)
	}

	state.LastSeen = time.Now()
	state.Leader = statusResp.GetState() == raft.Leader.String()
	return state, nil
}

func (s *Server) remoteStats(ctx context.Context, nodeID string) (*pb.StatsResponse, error) {
	client, closer, err := s.peerClient(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	defer closer()
	return client.Stats(ctx, &pb.StatsRequest{})
}

// peerClient dials a peer through the cached connection pool. It returns a
// no-op closer because the pool owns connection lifetime; dropConn is used on
// transport failures elsewhere.
func (s *Server) peerClient(ctx context.Context, nodeID string) (pb.StorageServiceClient, func(), error) {
	target := s.grpcAddrFor(nodeID)
	if target == "" {
		return nil, nil, fmt.Errorf("no gRPC address for node %q", nodeID)
	}
	conn, err := s.getConn(ctx, target)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %q: %w", nodeID, err)
	}
	return pb.NewStorageServiceClient(conn), func() {}, nil
}

// grpcAddrFor resolves a node ID to its gRPC address. The local node falls
// back to its advertised PEERS entry, then to NODE_ID:PORT.
func (s *Server) grpcAddrFor(nodeID string) string {
	if addr, ok := s.cfg.Peers[nodeID]; ok && addr != "" {
		return addr
	}
	if nodeID == s.cfg.NodeID && s.cfg.GRPCPort != "" {
		return fmt.Sprintf("%s:%s", s.cfg.NodeID, s.cfg.GRPCPort)
	}
	return ""
}

// raftConfiguration reads the current Raft configuration with a bounded wait.
func (s *Server) raftConfiguration(timeout time.Duration) (raft.Configuration, error) {
	if timeout <= 0 {
		timeout = s.cfg.ClusterProbeTimeout
	}
	future := s.node.Raft.GetConfiguration()

	done := make(chan error, 1)
	go func() {
		done <- future.Error()
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-done:
		if err != nil {
			return raft.Configuration{}, err
		}
		return future.Configuration(), nil
	case <-timer.C:
		return raft.Configuration{}, context.DeadlineExceeded
	}
}

func (s *Server) forwardClusterHealth(ctx context.Context, req *pb.ClusterHealthRequest) (*pb.ClusterHealthResponse, error) {
	var resp *pb.ClusterHealthResponse
	err := s.forwardToLeader(ctx, func(fctx context.Context, client pb.StorageServiceClient) error {
		out, err := client.ClusterHealth(fctx, req)
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

// nonNegative clamps a store counter to uint64 for the proto field.
func nonNegative(value int64) uint64 {
	if value < 0 {
		return 0
	}
	return uint64(value)
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
