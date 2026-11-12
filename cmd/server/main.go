// Command server runs one storage node. It loads configuration, starts the Raft
// group and the gRPC API, and shuts down cleanly on SIGINT/SIGTERM.
package main

import (
	"context"
	"fmt"
	"log"
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
	"distributed-kv-store/internal/rpc"
	"distributed-kv-store/internal/server"
	"distributed-kv-store/internal/storage"

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

	fmt.Printf("Starting %s (gRPC on %s, Raft on %s, data dir %s)...\n", cfg.NodeID, cfg.GRPCPort, cfg.RaftPort, cfg.DataDir)

	// Open the SQLite store before Raft so a broken database fails fast.
	store, err := storage.Open(storage.StoreConfig{
		Path:        cfg.StorePath,
		BusyTimeout: cfg.StoreBusyTimeout,
	})
	if err != nil {
		log.Fatalf("Failed to open store at %s: %v", cfg.StorePath, err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("Failed to close store: %v", err)
		}
	}()

	// Resolve the advertised Raft address. In Docker Compose this resolves
	// to the container name (e.g. node1:60051).
	advertiseAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%s", cfg.NodeID, cfg.RaftPort))
	if err != nil {
		log.Fatalf("Failed to resolve raft advertise address: %v", err)
	}

	transport, err := raft.NewTCPTransport(fmt.Sprintf("0.0.0.0:%s", cfg.RaftPort), advertiseAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Fatalf("Failed to create raft transport: %v", err)
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
		log.Fatalf("Failed to start Raft: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	var grpcOpts []grpc.ServerOption
	if cfg.TLSEnabled {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			log.Fatalf("Failed to load TLS credentials: %v", err)
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

	// AUTH_TOKEN is enforced by interceptors at the process boundary. Health
	// checks stay exempt so Compose and the detector keep working.
	if cfg.AuthToken != "" {
		grpcOpts = append(grpcOpts, server.AuthServerOptions(cfg.AuthToken)...)
	}

	grpcServer := grpc.NewServer(grpcOpts...)
	storageServer := server.New(cfg, store, node, nil)
	if cfg.AuthToken != "" {
		storageServer.EnableAuth()
	} else {
		log.Printf("WARNING: AUTH_TOKEN is not set; gRPC authentication is disabled")
	}
	if err := storageServer.CheckAuth(); err != nil {
		log.Fatalf("Invalid authorization configuration: %v", err)
	}
	pb.RegisterStorageServiceServer(grpcServer, storageServer)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("storage.StorageService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	// Optionally join an existing cluster through a live leader.
	joinCluster(cfg, string(transport.LocalAddr()))

	// Start leader-gated failure detection and repair. Every node runs the
	// detector supervisor, but only the leader probes peers and reconstructs
	// shards; the local orphan sweep runs everywhere.
	failureCtx, stopFailure := context.WithCancel(context.Background())
	defer stopFailure()
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

	// Shut down cleanly on SIGINT/SIGTERM so the BoltDB file lock is released
	// (important for fast container restarts) and in-flight RPCs can drain.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("received shutdown signal, stopping %s", cfg.NodeID)
		stopFailure()
		_ = storageServer.Close()
		grpcServer.GracefulStop()
		_ = node.Close()
		_ = transport.Close()
		_ = store.Close()
	}()

	fmt.Printf("gRPC server listening on port %s\n", cfg.GRPCPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Printf("gRPC server stopped: %v", err)
	}

	// If Serve returns without a signal, still release Raft and BoltDB.
	stopFailure()
	_ = storageServer.Close()
	_ = node.Close()
	_ = transport.Close()
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
func joinCluster(cfg config.Config, raftAddr string) {
	if cfg.JoinAddr == "" {
		return
	}

	go func() {
		opts, err := rpc.DialOptions(cfg.AuthToken, cfg.MaxMessageSize)
		if err != nil {
			log.Printf("join disabled: %v", err)
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
					log.Printf("joined cluster via %s", cfg.JoinAddr)
					return
				}
				if err != nil {
					log.Printf("join attempt %d via %s failed: %v", attempt, cfg.JoinAddr, err)
				} else {
					log.Printf("join attempt %d via %s rejected: %s", attempt, cfg.JoinAddr, resp.GetMessage())
				}
			} else {
				log.Printf("join attempt %d via %s dial failed: %v", attempt, cfg.JoinAddr, err)
			}
			time.Sleep(2 * time.Second)
		}
		log.Printf("gave up joining cluster via %s after 10 attempts", cfg.JoinAddr)
	}()
}
