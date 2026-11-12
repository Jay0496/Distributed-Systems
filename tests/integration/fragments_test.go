package integration

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"distributed-kv-store/internal/redundancy"
	"distributed-kv-store/tests/support"

	pb "distributed-kv-store/proto"
)

// uploadFragment sends data in chunks through the production fragment stream.
func uploadFragment(t *testing.T, client pb.StorageServiceClient, stripeID string, shardIndex int, data []byte, chunkSize int, replace bool) *pb.FragmentResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stream, err := client.UploadFragment(ctx)
	if err != nil {
		t.Fatalf("UploadFragment: %v", err)
	}
	if chunkSize <= 0 {
		chunkSize = len(data)
	}
	if chunkSize == 0 {
		chunkSize = 1
	}
	for offset := 0; offset < len(data) || offset == 0; offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := &pb.FragmentChunk{
			StripeId:   stripeID,
			ShardIndex: int32(shardIndex),
			Checksum:   redundancy.Checksum(data),
			Offset:     int64(offset),
			TotalSize:  int64(len(data)),
			Data:       data[offset:end],
			Last:       end == len(data),
			Replace:    replace,
		}
		if err := stream.Send(chunk); err != nil {
			t.Fatalf("Send chunk at %d: %v", offset, err)
		}
		if end == len(data) {
			break
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	return resp
}

func downloadFragment(t *testing.T, client pb.StorageServiceClient, stripeID string, shardIndex int) ([]byte, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	stream, err := client.DownloadFragment(ctx, &pb.FragmentRequest{
		StripeId: stripeID, ShardIndex: int32(shardIndex),
	})
	if err != nil {
		t.Fatalf("DownloadFragment: %v", err)
	}
	var data []byte
	var checksum string
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		checksum = chunk.GetChecksum()
		data = append(data, chunk.GetData()...)
		if chunk.GetLast() {
			break
		}
	}
	return data, checksum
}

func TestFragmentStreamRoundTrip(t *testing.T) {
	c := support.StartCluster(t, 5)
	node := c.Nodes[0]
	data := bytes.Repeat([]byte("fragment-stream-"), 4096)

	resp := uploadFragment(t, node.Client, "stripe-stream", 1, data, 1024, false)
	if !resp.GetSuccess() {
		t.Fatalf("upload rejected: %s", resp.GetMessage())
	}

	got, checksum := downloadFragment(t, node.Client, "stripe-stream", 1)
	if !bytes.Equal(got, data) {
		t.Fatalf("download = %d bytes, want %d", len(got), len(data))
	}
	if checksum != redundancy.Checksum(data) {
		t.Fatalf("download checksum = %q, want %q", checksum, redundancy.Checksum(data))
	}

	stat, err := node.Client.StatFragment(context.Background(), &pb.DeleteFragmentRequest{
		StripeId: "stripe-stream", ShardIndex: 1,
	})
	if err != nil {
		t.Fatalf("StatFragment: %v", err)
	}
	if stat.GetSize() != int64(len(data)) || stat.GetChecksum() != redundancy.Checksum(data) {
		t.Fatalf("stat = %+v", stat)
	}

	stream, err := node.Client.ListFragments(context.Background(), &pb.ListFragmentsRequest{StripeId: "stripe-stream"})
	if err != nil {
		t.Fatalf("ListFragments: %v", err)
	}
	listed := 0
	for {
		info, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("list recv: %v", err)
		}
		if info.GetStripeId() == "stripe-stream" && info.GetShardIndex() == 1 {
			listed++
		}
	}
	if listed != 1 {
		t.Fatalf("ListFragments returned %d matching entries, want 1", listed)
	}

	deleted, err := node.Client.DeleteFragment(context.Background(), &pb.DeleteFragmentRequest{
		StripeId: "stripe-stream", ShardIndex: 1,
	})
	if err != nil || !deleted.GetSuccess() {
		t.Fatalf("DeleteFragment = %+v, %v", deleted, err)
	}
	if _, err := node.Store.GetFragment("stripe-stream", 1); err == nil {
		t.Fatal("fragment still present after DeleteFragment")
	}
}

func TestFragmentUploadRejectsChecksumMismatch(t *testing.T) {
	c := support.StartCluster(t, 5)
	node := c.Nodes[0]
	data := []byte("checksum-mismatch")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := node.Client.UploadFragment(ctx)
	if err != nil {
		t.Fatalf("UploadFragment: %v", err)
	}
	if err := stream.Send(&pb.FragmentChunk{
		StripeId:   "stripe-bad",
		ShardIndex: 0,
		Checksum:   redundancy.Checksum([]byte("different")),
		Offset:     0,
		TotalSize:  int64(len(data)),
		Data:       data,
		Last:       true,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err == nil {
		t.Fatal("upload with a mismatched checksum succeeded")
	}
	if _, err := node.Store.GetFragment("stripe-bad", 0); err == nil {
		t.Fatal("rejected fragment was still stored")
	}
}

func TestDeleteStripeFragmentsRemovesEveryShard(t *testing.T) {
	c := support.StartCluster(t, 5)

	resp := c.Put(t, &pb.PutRequest{Key: "stripe-delete", Value: []byte("payload"), Mode: "rs32"})
	if !resp.GetSuccess() {
		t.Fatalf("Put rejected: %s", resp.GetMessage())
	}
	meta := waitWriteMeta(t, c, "stripe-delete", 1)
	waitFragmentTotal(t, c, meta.StripeID, redundancy.TotalShards)

	for _, placement := range meta.Placement {
		node := c.Node(t, placement.NodeID)
		op, err := node.Client.DeleteStripeFragments(context.Background(), &pb.DeleteStripeFragmentsRequest{
			StripeId: meta.StripeID,
		})
		if err != nil || !op.GetSuccess() {
			t.Fatalf("DeleteStripeFragments on %s = %+v, %v", placement.NodeID, op, err)
		}
	}
	waitFragmentTotal(t, c, meta.StripeID, 0)
}
