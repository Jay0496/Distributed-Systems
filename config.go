package main

import (
	"fmt"
	"os"
	"strings"
)

// Config holds the runtime configuration for a single storage node.
type Config struct {
	NodeID     string
	GRPCPort   string
	RaftPort   string
	DataDir    string
	Bootstrap  bool
	JoinAddr   string
	TLSEnabled bool
	TLSCert    string
	TLSKey     string
	TLSCA      string

	// BootstrapServers maps Raft server ID -> Raft address.
	BootstrapServers map[string]string
	// Peers maps Raft server ID -> gRPC address.
	Peers map[string]string
}

// loadConfig reads the node configuration from environment variables.
func loadConfig() Config {
	cfg := Config{
		NodeID:           envOr("NODE_ID", "node1"),
		GRPCPort:         envOr("PORT", "50051"),
		RaftPort:         envOr("RAFT_PORT", "60051"),
		DataDir:          envOr("DATA_DIR", "/data"),
		Bootstrap:        os.Getenv("BOOTSTRAP") == "true",
		JoinAddr:         os.Getenv("JOIN_ADDR"),
		TLSEnabled:       os.Getenv("TLS_ENABLED") == "true",
		TLSCert:          os.Getenv("TLS_CERT_FILE"),
		TLSKey:           os.Getenv("TLS_KEY_FILE"),
		TLSCA:            os.Getenv("TLS_CA_FILE"),
		BootstrapServers: parsePeerMap(os.Getenv("BOOTSTRAP_SERVERS")),
		Peers:            parsePeerMap(os.Getenv("PEERS")),
	}
	return cfg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parsePeerMap parses "id1=host:port,id2=host:port" into a map.
func parsePeerMap(raw string) map[string]string {
	out := make(map[string]string)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			fmt.Fprintf(os.Stderr, "ignoring malformed peer entry %q\n", entry)
			continue
		}
		out[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return out
}
