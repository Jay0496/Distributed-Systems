package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"distributed-kv-store/internal/gateway"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const rpcTimeout = 5 * time.Second

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Fprintln(os.Stderr, usage())
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("a command is required")
	}

	switch args[0] {
	case "put":
		if len(args) != 3 {
			return errors.New("put requires a key and value")
		}
		node, err := entryNode(args[1])
		if err != nil {
			return err
		}
		return executePut(node, args[1], args[2], "")
	case "get":
		if len(args) != 2 {
			return errors.New("get requires a key")
		}
		node, err := entryNode(args[1])
		if err != nil {
			return err
		}
		return executeGet(node, args[1])
	case "mode":
		if len(args) != 2 {
			return errors.New("mode requires replication or rs32")
		}
		if args[1] != "replication" && args[1] != "rs32" {
			return fmt.Errorf("invalid mode %q: expected replication or rs32", args[1])
		}
		return executePut("localhost:50051", "", "", args[1])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func usage() string {
	return "Usage:\n  go run ./cmd/gateway put <key> <val>\n  go run ./cmd/gateway get <key>\n  go run ./cmd/gateway mode <replication|rs32>"
}

// entryNode is the transitional entry-node lookup for commit 1. Commit 2
// replaces it with the env-configured gateway client.
func entryNode(key string) (string, error) {
	router := gateway.NewRouter([]string{
		"localhost:50051",
		"localhost:50052",
		"localhost:50053",
		"localhost:50054",
		"localhost:50055",
	}, 100)
	node, ok := router.GetNode(key)
	if !ok {
		return "", errors.New("no gateway nodes configured")
	}
	return node, nil
}

func executePut(addr, key, value, mode string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := pb.NewStorageServiceClient(conn).Put(ctx, &pb.PutRequest{
		Key:   key,
		Value: []byte(value),
		Mode:  mode,
	})
	if err != nil {
		return fmt.Errorf("put RPC to %s: %w", addr, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("put rejected by %s: %s", addr, resp.GetMessage())
	}

	fmt.Printf("Put succeeded on %s: %s\n", addr, resp.GetMessage())
	return nil
}

func executeGet(addr, key string) error {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := pb.NewStorageServiceClient(conn).Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return fmt.Errorf("get RPC to %s: %w", addr, err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("get rejected by %s", addr)
	}

	fmt.Printf("Get succeeded on %s: %s\n", addr, string(resp.GetValue()))
	return nil
}
