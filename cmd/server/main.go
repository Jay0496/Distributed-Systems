// Command server runs one storage node. It loads configuration, starts the Raft
// group and the gRPC API, and shuts down cleanly on SIGINT/SIGTERM.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"syscall"
	"time"

	"distributed-kv-store/internal/cluster"
	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
	"distributed-kv-store/internal/logging"
	"distributed-kv-store/internal/rpc"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/storage"
	"distributed-kv-store/internal/version"

	pb "distributed-kv-store/proto"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	cfg := config.Load()
	logger := logging.New(os.Stdout, cfg.LogFormat, "server",
		"node_id", cfg.NodeID,
		"version", version.Value(),
	)
	slog.SetDefault(logger)

	// Keep one concise human-readable line for the demo; structured lifecycle
	// logs carry the machine-readable detail.
	fmt.Printf("Starting %s %s (gRPC on %s, Raft on %s, data dir %s)...\n",
		cfg.NodeID, version.Value(), cfg.GRPCPort, cfg.RaftPort, cfg.DataDir)
	logger.Info("starting node",
		"grpc_port", cfg.GRPCPort,
		"raft_port", cfg.RaftPort,
		"data_dir", cfg.DataDir,
		"log_format", cfg.LogFormat,
	)

	// Open the SQLite store before Raft so a broken database fails fast.
	store, err := storage.Open(storage.StoreConfig{
		Path:        cfg.StorePath,
		BusyTimeout: cfg.StoreBusyTimeout,
	})
	if err != nil {
		fatalf(logger, "open store", "path", cfg.StorePath, "error", err)
	}

	// Resolve the advertised Raft address. In Docker Compose this resolves
	// to the container name (e.g. node1:60051).
	advertiseAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", cfg.NodeID, cfg.RaftPort))
	if err != nil {
		fatalf(logger, "resolve raft advertise address", "error", err)
	}

	transport, err := raft.NewTCPTransport(fmt.Sprintf("0.0.0.0:%s", cfg.RaftPort), advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		fatalf(logger, "create raft transport", "error", err)
	}

	bootstrapConfig := raft.Configuration{}
	if cfg.Bootstrap {
		ids := make([]string, 0, len(cfg.BootstrapServers))
		for id := range cfg.BootstrapServers {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		servers := make([]raft.Server, 0, len(ids))
		for _, id := range ids {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(id),
				Address: raft.ServerAddress(cfg.BootstrapServers[id]),
			})
		}
		bootstrapConfig = raft.Configuration{Servers: servers}
	}

	node, err := consensus.StartRaft(cfg.NodeID, store, transport, cfg.DataDir, bootstrapConfig)
	if err != nil {
		fatalf(logger, "start raft", "error", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		fatalf(logger, "listen on gRPC port", "error", err)
	}

	var grpcOpts []grpc.ServerOption
	if cfg.TLSEnabled {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			fatalf(logger, "load TLS credentials", "error", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}

	// Bound every gRPC message. Fragment payloads are split into
	// UPLOAD_CHUNK_SIZE pieces, so the lower message limit never caps a
	// complete fragment; it only rejects a single oversized message.
	grpcOpts = append(grpcOpts,
		grpc.MaxRecvMsgSize(cfg.MaxMessageSize),
		grpc.MaxSendMsgSize(cfg.MaxMessageSize),
	)
	// Every request gets a correlation ID that survives forwarding and fragment
	// calls, so logs from different nodes can be joined by one ID.
	grpcOpts = append(grpcOpts, rpc.RequestIDServerOptions()...)

	// AUTH_TOKEN is enforced by interceptors at the process boundary. Health
	// checks stay exempt so Compose and the detector keep working.
	if cfg.AuthToken != "" {
		grpcOpts = append(grpcOpts, server.AuthServerOptions(cfg.AuthToken)...)
	}

	grpcServer := grpc.NewServer(grpcOpts...)
	storageServer := server.New(cfg, store, node, nil)
	if cfg.AuthToken != "" {
		storageServer.EnableAuth()
		logger.Info("authentication enabled")
	} else {
		logger.Warn("AUTH_TOKEN is not set; gRPC authentication is disabled")
	}
	if err := storageServer.CheckAuth(); err != nil {
		fatalf(logger, "invalid authorization configuration", "error", err)
	}
	pb.RegisterStorageServiceServer(grpcServer, storageServer)

	// "" is liveness and storage.StorageService is readiness. Both start
	// NOT_SERVING until the store pings and Raft knows a leader.
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthServer.SetServingStatus(server.HealthServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	storageServer.SetHealthServer(healthServer)

	// Optionally join an existing cluster through a live leader.
	joinCluster(cfg, string(transport.LocalAddr()), logger)

	// Start leader-gated failure detection and repair. Every node runs the
	// detector supervisor, but only the leader probes peers and reconstructs
	// shards; the local orphan sweep runs everywhere.
	failureCtx, stopFailure := context.WithCancel(context.Background())
	storageServer.StartHealthMonitor(failureCtx)
	detector := cluster.NewDetector(cluster.DetectorOptions{
		Interval:  cfg.DetectInterval,
		Timeout:   cfg.DetectTimeout,
		Failures:  cfg.DetectFailures,
		Successes: cfg.DetectSuccesses,
		Nodes:     detectorNodes(cfg),
		SelfNode:  cfg.NodeID,
		Probe:     storageServer.ProbePeer,
		IsLeader:  func() bool { return node.Raft.State() == raft.Leader },
		LeaderCh:  node.Raft.LeaderCh(),
	})
	if repairer := storageServer.Repairer(); repairer != nil {
		repairer.SetOrphanSweeper(cluster.NewOrphanSweeper(cluster.OrphanSweeperOptions{
			Store:       store,
			SelfNode:    cfg.NodeID,
			GracePeriod: cfg.OrphanGracePeriod,
			Guard:       orphanGuard(node, cfg.OrphanMaxIndexLag),
		}))
	}
	storageServer.AttachFailureManagement(failureCtx, detector)

	shutdown := func(trigger string) {
		logger.Info("shutting down", "trigger", trigger)
		if err := server.Shutdown(server.ShutdownOptions{
			Server:        storageServer,
			GRPC:          grpcServer,
			Store:         store,
			Raft:          node,
			Transport:     transport,
			CancelProcess: stopFailure,
			Timeout:       cfg.ShutdownTimeout,
			Logger:        logger,
		}); err != nil {
			logger.Error("shutdown failed", "error", err)
		}
	}

	// Shut down cleanly on SIGINT/SIGTERM so the BoltDB file lock is released
	// (important for fast container restarts) and in-flight RPCs can drain.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		shutdown("signal")
	}()

	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	logger.Info("gRPC server listening", "port", cfg.GRPCPort)
	if err := grpcServer.Serve(lis); err != nil {
		logger.Warn("gRPC server stopped", "error", err)
	}

	// If Serve returns without a signal, still release Raft and SQLite. The
	// store and Raft closers are idempotent, so a repeated signal is safe.
	shutdown("serve_return")
}

// fatalf logs a structured startup failure and exits.
func fatalf(logger *slog.Logger, msg string, attrs ...any) {
	logger.Error(msg, attrs...)
	os.Exit(1)
}

// detectorNodes builds the detector's node-to-gRPC-address map, including the
// local node when PEERS omits it.
func detectorNodes(cfg config.Config) map[string]string {
	nodes := make(map[string]string, len(cfg.Peers)+1)
	for id, addr := range cfg.Peers {
		nodes[id] = addr
	}
	if _, ok := nodes[cfg.NodeID]; !ok {
		nodes[cfg.NodeID] = fmt.Sprintf("%s:%s", cfg.NodeID, cfg.GRPCPort)
	}
	return nodes
}

// orphanGuard prevents a stale node from deleting fragments for metadata it has
// not applied yet. A pass requires a known leader and an applied index within
// ORPHAN_MAX_INDEX_LAG of the leader-reported commit index.
func orphanGuard(node *consensus.Node, maxLag uint64) func(context.Context) (bool, error) {
	return func(context.Context) (bool, error) {
		if node == nil || node.Raft == nil {
			return false, nil
		}
		_, leaderID := node.Raft.LeaderWithID()
		if leaderID == "" {
			return false, nil
		}
		stats := node.Raft.Stats()
		commit, _ := strconv.ParseUint(stats["commit_index"], 10, 64)
		applied := node.Raft.AppliedIndex()
		if commit > 0 && applied+maxLag < commit {
			return false, nil
		}
		return true, nil
	}
}

// joinCluster asks the node configured in JOIN_ADDR to add this node as a
// voter. It retries so that startup ordering between containers does not
// matter.
func joinCluster(cfg config.Config, raftAddr string, logger *slog.Logger) {
	if cfg.JoinAddr == "" {
		return
	}

	go func() {
		opts, err := rpc.DialOptions(cfg.AuthToken, cfg.MaxMessageSize)
		if err != nil {
			logger.Warn("join disabled", "error", err)
			return
		}
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

		for attempt := 1; attempt <= 10; attempt++ {
			conn, err := grpc.Dial(cfg.JoinAddr, opts...)
			if err == nil {
				client := pb.NewStorageServiceClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				resp, err := client.Join(ctx, &pb.JoinRequest{
					NodeId:   cfg.NodeID,
					RaftAddr: raftAddr,
					GrpcAddr: fmt.Sprintf("%s:%s", cfg.NodeID, cfg.GRPCPort),
				})
				cancel()
				conn.Close()

				if err == nil && resp.GetSuccess() {
					logger.Info("joined cluster", "leader_addr", cfg.JoinAddr)
					return
				}
				if err != nil {
					logger.Warn("join attempt failed",
						"attempt", attempt, "leader_addr", cfg.JoinAddr, "error", err)
				} else {
					logger.Warn("join attempt rejected",
						"attempt", attempt, "leader_addr", cfg.JoinAddr, "message", resp.GetMessage())
				}
			} else {
				logger.Warn("join dial failed",
					"attempt", attempt, "leader_addr", cfg.JoinAddr, "error", err)
			}
			time.Sleep(2 * time.Second)
		}
		logger.Warn("gave up joining cluster", "leader_addr", cfg.JoinAddr, "attempts", 10)
	}()
}
