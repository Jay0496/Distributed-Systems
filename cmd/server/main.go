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
	"syscall"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/consensus"
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

	grpcServer := grpc.NewServer(grpcOpts...)
	pb.RegisterStorageServiceServer(grpcServer, server.New(cfg, store, node, nil))

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("storage.StorageService", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	// Optionally join an existing cluster through a live leader.
	joinCluster(cfg, string(transport.LocalAddr()))

	// Shut down cleanly on SIGINT/SIGTERM so the BoltDB file lock is released
	// (important for fast container restarts) and in-flight RPCs can drain.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("received shutdown signal, stopping %s", cfg.NodeID)
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
	_ = node.Close()
	_ = transport.Close()
}

// joinCluster asks the node configured in JOIN_ADDR to add this node as a
// voter. It retries so that startup ordering between containers does not
// matter.
func joinCluster(cfg config.Config, raftAddr string) {
	if cfg.JoinAddr == "" {
		return
	}

	go func() {
		for attempt := 1; attempt <= 10; attempt++ {
			conn, err := grpc.Dial(cfg.JoinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
