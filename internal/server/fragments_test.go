package server

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"distributed-kv-store/internal/config"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const fragmentTestBufSize = 8 << 20

// newFragmentTestServer starts a gRPC server that serves only the fragment
// transport. The Raft node is nil because the fragment handlers do not consult
// it, which keeps these tests independent of consensus.
func newFragmentTestServer(t *testing.T, cfg config.Config, store storage.Store, opts ...grpc.ServerOption) (pb.StorageServiceClient, *Server) {
	t.Helper()

	lis := bufconn.Listen(fragmentTestBufSize)
	srv := New(cfg, store, nil, nil)

	grpcSrv := grpc.NewServer(opts...)
	pb.RegisterStorageServiceServer(grpcSrv, srv)
	go func() {
		_ = grpcSrv.Serve(lis)
	}()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		grpcSrv.Stop()
		_ = lis.Close()
		t.Fatalf("dial fragment test server: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		grpcSrv.Stop()
		_ = lis.Close()
	})

	return pb.NewStorageServiceClient(conn), srv
}

func openUpload(t *testing.T, client pb.StorageServiceClient) pb.StorageService_UploadFragmentClient {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	stream, err := client.UploadFragment(ctx)
	if err != nil {
		t.Fatalf("UploadFragment: %v", err)
	}
	return stream
}

func sendChunk(t *testing.T, stream pb.StorageService_UploadFragmentClient, chunk *pb.FragmentChunk) {
	t.Helper()
	if err := stream.Send(chunk); err != nil {
		t.Fatalf("stream.Send: %v", err)
	}
}

func chunkFor(stripeID string, shardIndex int32, isParity, replace bool, checksum string, totalSize, offset int64, data []byte, last bool) *pb.FragmentChunk {
	return &pb.FragmentChunk{
		StripeId:   stripeID,
		ShardIndex: shardIndex,
		IsParity:   isParity,
		Replace:    replace,
		Checksum:   checksum,
		TotalSize:  totalSize,
		Offset:     offset,
		Data:       data,
		Last:       last,
	}
}

// uploadFragment sends data in chunks of at most chunkSize and returns the
// final response. An empty payload is sent as one zero-length chunk.
func uploadFragment(t *testing.T, client pb.StorageServiceClient, stripeID string, shardIndex int32, isParity bool, data []byte, chunkSize int, replace bool) (*pb.FragmentResponse, error) {
	t.Helper()

	stream := openUpload(t, client)
	checksum := fragmentChecksum(data)
	total := int64(len(data))

	if len(data) == 0 {
		sendChunk(t, stream, chunkFor(stripeID, shardIndex, isParity, replace, checksum, 0, 0, nil, true))
		return stream.CloseAndRecv()
	}

	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		sendChunk(t, stream, chunkFor(
			stripeID, shardIndex, isParity, replace, checksum, total,
			int64(offset), data[offset:end], end == len(data),
		))
	}
	return stream.CloseAndRecv()
}

func requireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("expected %s, got %v", want, err)
	}
}

func TestUploadSingleChunk(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	data := []byte("single-chunk fragment")
	resp, err := uploadFragment(t, client, "stripe-1", 0, false, data, len(data), false)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("upload rejected: %s", resp.GetMessage())
	}

	frag, err := store.GetFragment("stripe-1", 0)
	if err != nil {
		t.Fatalf("GetFragment: %v", err)
	}
	if string(frag.Data) != string(data) {
		t.Fatalf("stored data = %q, want %q", frag.Data, data)
	}
	if frag.Checksum != fragmentChecksum(data) {
		t.Fatalf("stored checksum = %q, want %q", frag.Checksum, fragmentChecksum(data))
	}
	if frag.IsParity {
		t.Fatal("data shard stored as parity")
	}
}

func TestUploadMultiChunkReassembles(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{UploadChunkSize: 4, MaxMessageSize: 1024}, store)

	data := []byte("a multi-chunk fragment payload that spans several chunks")
	resp, err := uploadFragment(t, client, "stripe-2", 3, true, data, 7, false)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("upload rejected: %s", resp.GetMessage())
	}

	frag, err := store.GetFragment("stripe-2", 3)
	if err != nil {
		t.Fatalf("GetFragment: %v", err)
	}
	if string(frag.Data) != string(data) {
		t.Fatalf("stored data = %q, want %q", frag.Data, data)
	}
	if !frag.IsParity {
		t.Fatal("parity shard stored as data")
	}
}

func TestUploadRejectsBadOffsets(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	data := []byte("0123456789")
	checksum := fragmentChecksum(data)
	total := int64(len(data))

	cases := []struct {
		name   string
		chunks []*pb.FragmentChunk
	}{
		{
			name: "gap",
			chunks: []*pb.FragmentChunk{
				chunkFor("stripe-3", 0, false, false, checksum, total, 0, data[:3], false),
				chunkFor("stripe-3", 0, false, false, checksum, total, 5, data[5:], true),
			},
		},
		{
			name: "overlap",
			chunks: []*pb.FragmentChunk{
				chunkFor("stripe-3", 0, false, false, checksum, total, 0, data[:6], false),
				chunkFor("stripe-3", 0, false, false, checksum, total, 4, data[4:], true),
			},
		},
		{
			name: "duplicate",
			chunks: []*pb.FragmentChunk{
				chunkFor("stripe-3", 0, false, false, checksum, total, 0, data[:5], false),
				chunkFor("stripe-3", 0, false, false, checksum, total, 0, data[:5], false),
			},
		},
		{
			name: "exceeds total size",
			chunks: []*pb.FragmentChunk{
				chunkFor("stripe-3", 0, false, false, checksum, total-1, 0, data, true),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := openUpload(t, client)
			for _, chunk := range tc.chunks {
				sendChunk(t, stream, chunk)
			}
			_, err := stream.CloseAndRecv()
			requireCode(t, err, codes.InvalidArgument)
		})
	}

	if _, err := store.GetFragment("stripe-3", 0); err == nil {
		t.Fatal("a rejected upload left a committed fragment")
	}
}

func TestUploadRejectsInconsistentStream(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	data := []byte("abcdef")
	checksum := fragmentChecksum(data)
	total := int64(len(data))

	cases := []struct {
		name   string
		second *pb.FragmentChunk
	}{
		{
			name:   "stripe changed",
			second: chunkFor("other-stripe", 0, false, false, checksum, total, 3, data[3:], true),
		},
		{
			name:   "shard changed",
			second: chunkFor("stripe-4", 1, false, false, checksum, total, 3, data[3:], true),
		},
		{
			name:   "total size changed",
			second: chunkFor("stripe-4", 0, false, false, checksum, total+1, 3, data[3:], true),
		},
		{
			name:   "checksum changed",
			second: chunkFor("stripe-4", 0, false, false, fragmentChecksum([]byte("different")), total, 3, data[3:], true),
		},
		{
			name:   "replace changed",
			second: chunkFor("stripe-4", 0, false, true, checksum, total, 3, data[3:], true),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := openUpload(t, client)
			sendChunk(t, stream, chunkFor("stripe-4", 0, false, false, checksum, total, 0, data[:3], false))
			sendChunk(t, stream, tc.second)
			_, err := stream.CloseAndRecv()
			requireCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestUploadIdempotentRetry(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	data := []byte("retry me")
	first, err := uploadFragment(t, client, "stripe-5", 1, false, data, len(data), false)
	if err != nil || !first.GetSuccess() {
		t.Fatalf("first upload: resp=%+v err=%v", first, err)
	}

	second, err := uploadFragment(t, client, "stripe-5", 1, false, data, len(data), false)
	if err != nil {
		t.Fatalf("second upload: %v", err)
	}
	if !second.GetSuccess() {
		t.Fatalf("idempotent retry rejected: %s", second.GetMessage())
	}
	if !strings.Contains(second.GetMessage(), "already stored") {
		t.Fatalf("retry message = %q, want it to report an existing fragment", second.GetMessage())
	}
}

func TestUploadExistingDifferentChecksum(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	if _, err := uploadFragment(t, client, "stripe-6", 0, false, []byte("original"), 8, false); err != nil {
		t.Fatalf("seed upload: %v", err)
	}

	_, err := uploadFragment(t, client, "stripe-6", 0, false, []byte("replacement"), 11, false)
	requireCode(t, err, codes.AlreadyExists)

	frag, err := store.GetFragment("stripe-6", 0)
	if err != nil {
		t.Fatalf("GetFragment: %v", err)
	}
	if string(frag.Data) != "original" {
		t.Fatalf("rejected upload changed stored data to %q", frag.Data)
	}
}

func TestUploadReplace(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	if _, err := uploadFragment(t, client, "stripe-7", 4, true, []byte("original"), 8, false); err != nil {
		t.Fatalf("seed upload: %v", err)
	}

	resp, err := uploadFragment(t, client, "stripe-7", 4, true, []byte("replacement"), 11, true)
	if err != nil {
		t.Fatalf("replace upload: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("replace rejected: %s", resp.GetMessage())
	}

	frag, err := store.GetFragment("stripe-7", 4)
	if err != nil {
		t.Fatalf("GetFragment: %v", err)
	}
	if string(frag.Data) != "replacement" {
		t.Fatalf("stored data = %q, want replacement", frag.Data)
	}
}

func TestUploadSizeAndChecksumValidation(t *testing.T) {
	store := newTestStore()
	cfg := config.Config{MaxFragmentSize: 8, MaxMessageSize: 8}
	client, _ := newFragmentTestServer(t, cfg, store)

	t.Run("fragment too large", func(t *testing.T) {
		stream := openUpload(t, client)
		sendChunk(t, stream, chunkFor("stripe-8", 0, false, false, fragmentChecksum([]byte("123456789")), 9, 0, []byte("123456789"), true))
		_, err := stream.CloseAndRecv()
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("chunk too large", func(t *testing.T) {
		payload := []byte("123456789")
		stream := openUpload(t, client)
		sendChunk(t, stream, chunkFor("stripe-8", 0, false, false, fragmentChecksum(payload), 8, 0, payload, true))
		_, err := stream.CloseAndRecv()
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		stream := openUpload(t, client)
		sendChunk(t, stream, chunkFor("stripe-8", 0, false, false, fragmentChecksum([]byte("wrong")), 4, 0, []byte("data"), true))
		_, err := stream.CloseAndRecv()
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("checksum missing", func(t *testing.T) {
		stream := openUpload(t, client)
		sendChunk(t, stream, chunkFor("stripe-8", 0, false, false, "", 4, 0, []byte("data"), true))
		_, err := stream.CloseAndRecv()
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("short payload", func(t *testing.T) {
		stream := openUpload(t, client)
		sendChunk(t, stream, chunkFor("stripe-8", 0, false, false, fragmentChecksum([]byte("data")), 8, 0, []byte("data"), true))
		_, err := stream.CloseAndRecv()
		requireCode(t, err, codes.InvalidArgument)
	})
}

func TestUploadEmptyFragment(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	resp, err := uploadFragment(t, client, "stripe-9", 1, false, nil, 1, false)
	if err != nil {
		t.Fatalf("upload empty fragment: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("empty fragment rejected: %s", resp.GetMessage())
	}

	frag, err := store.GetFragment("stripe-9", 1)
	if err != nil {
		t.Fatalf("GetFragment: %v", err)
	}
	if len(frag.Data) != 0 {
		t.Fatalf("stored data = %q, want empty", frag.Data)
	}
	if frag.Checksum != fragmentChecksum(nil) {
		t.Fatalf("stored checksum = %q, want checksum of the empty string", frag.Checksum)
	}
}

func TestUploadShardIndexAndParityValidation(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	data := []byte("payload")
	checksum := fragmentChecksum(data)

	t.Run("out of range", func(t *testing.T) {
		stream := openUpload(t, client)
		sendChunk(t, stream, chunkFor("stripe-10", 5, false, false, checksum, int64(len(data)), 0, data, true))
		_, err := stream.CloseAndRecv()
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("data shard marked parity", func(t *testing.T) {
		stream := openUpload(t, client)
		sendChunk(t, stream, chunkFor("stripe-10", 2, true, false, checksum, int64(len(data)), 0, data, true))
		_, err := stream.CloseAndRecv()
		requireCode(t, err, codes.InvalidArgument)
	})

	t.Run("parity shard", func(t *testing.T) {
		resp, err := uploadFragment(t, client, "stripe-10", 3, true, data, len(data), false)
		if err != nil || !resp.GetSuccess() {
			t.Fatalf("parity upload: resp=%+v err=%v", resp, err)
		}
	})

	t.Run("replication copy at parity index", func(t *testing.T) {
		resp, err := uploadFragment(t, client, "stripe-10", 4, false, data, len(data), false)
		if err != nil || !resp.GetSuccess() {
			t.Fatalf("replication copy upload: resp=%+v err=%v", resp, err)
		}
	})
}

func TestUploadRejectsLegacyIdentity(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	stream := openUpload(t, client)
	sendChunk(t, stream, &pb.FragmentChunk{
		Key:      "legacy-key",
		Index:    0,
		Data:     []byte("payload"),
		Checksum: fragmentChecksum([]byte("payload")),
		Last:     true,
	})
	_, err := stream.CloseAndRecv()
	requireCode(t, err, codes.InvalidArgument)
}

func TestUploadEndsWithoutLastChunk(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	data := []byte("payload")
	stream := openUpload(t, client)
	sendChunk(t, stream, chunkFor("stripe-11", 0, false, false, fragmentChecksum(data), int64(len(data)), 0, data[:4], false))

	_, err := stream.CloseAndRecv()
	requireCode(t, err, codes.InvalidArgument)
}

func TestUploadConcurrentSameFragmentRejected(t *testing.T) {
	store := newTestStore()
	client, srv := newFragmentTestServer(t, config.Config{}, store)

	data := []byte("concurrent payload")
	checksum := fragmentChecksum(data)
	total := int64(len(data))

	first := openUpload(t, client)
	sendChunk(t, first, chunkFor("stripe-12", 0, false, false, checksum, total, 0, data[:5], false))

	deadline := time.Now().Add(5 * time.Second)
	for srv.staging.active() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := srv.staging.active(); got != 1 {
		t.Fatalf("staging active = %d, want 1", got)
	}

	second := openUpload(t, client)
	sendChunk(t, second, chunkFor("stripe-12", 0, false, false, checksum, total, 0, data[:5], false))
	_, err := second.CloseAndRecv()
	requireCode(t, err, codes.Aborted)

	sendChunk(t, first, chunkFor("stripe-12", 0, false, false, checksum, total, int64(len(data[:5])), data[5:], true))
	resp, err := first.CloseAndRecv()
	if err != nil || !resp.GetSuccess() {
		t.Fatalf("winner upload: resp=%+v err=%v", resp, err)
	}

	if got := srv.staging.active(); got != 0 {
		t.Fatalf("staging active = %d after uploads finished, want 0", got)
	}
}

func downloadChunks(t *testing.T, client pb.StorageServiceClient, stripeID string, shardIndex int32) ([]*pb.FragmentChunk, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	stream, err := client.DownloadFragment(ctx, &pb.FragmentRequest{StripeId: stripeID, ShardIndex: shardIndex})
	if err != nil {
		return nil, err
	}

	var chunks []*pb.FragmentChunk
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return chunks, nil
		}
		if err != nil {
			return chunks, err
		}
		chunks = append(chunks, chunk)
	}
}

// reassemble verifies the wire metadata on every chunk and returns the payload.
func reassemble(t *testing.T, chunks []*pb.FragmentChunk) []byte {
	t.Helper()

	var data []byte
	var totalSize int64
	for i, chunk := range chunks {
		if i == 0 {
			totalSize = chunk.GetTotalSize()
		}
		if chunk.GetTotalSize() != totalSize {
			t.Fatalf("chunk %d total_size = %d, want %d", i, chunk.GetTotalSize(), totalSize)
		}
		if chunk.GetOffset() != int64(len(data)) {
			t.Fatalf("chunk %d offset = %d, want %d", i, chunk.GetOffset(), len(data))
		}
		if chunk.GetLast() != (i == len(chunks)-1) {
			t.Fatalf("chunk %d last = %v", i, chunk.GetLast())
		}
		data = append(data, chunk.GetData()...)
	}
	if int64(len(data)) != totalSize {
		t.Fatalf("reassembled %d bytes, want total_size %d", len(data), totalSize)
	}
	return data
}

func TestDownloadStreamsChunks(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{UploadChunkSize: 5}, store)

	data := []byte("a seventeen-byte payload")
	if _, err := uploadFragment(t, client, "stripe-20", 2, false, data, len(data), false); err != nil {
		t.Fatalf("upload: %v", err)
	}

	chunks, err := downloadChunks(t, client, "stripe-20", 2)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("downloaded %d chunks, want a multi-chunk stream", len(chunks))
	}
	for i, chunk := range chunks {
		if chunk.GetStripeId() != "stripe-20" || chunk.GetShardIndex() != 2 {
			t.Fatalf("chunk %d identity = %s/%d", i, chunk.GetStripeId(), chunk.GetShardIndex())
		}
		if chunk.GetChecksum() != fragmentChecksum(data) {
			t.Fatalf("chunk %d checksum = %q", i, chunk.GetChecksum())
		}
		if chunk.GetIsParity() {
			t.Fatalf("chunk %d marked parity", i)
		}
	}
	if got := reassemble(t, chunks); string(got) != string(data) {
		t.Fatalf("downloaded data = %q, want %q", got, data)
	}
}

func TestDownloadUploadRoundTrip(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{UploadChunkSize: 3}, store)

	data := []byte("round-trip payload")
	if _, err := uploadFragment(t, client, "stripe-21", 0, false, data, 4, false); err != nil {
		t.Fatalf("upload: %v", err)
	}

	chunks, err := downloadChunks(t, client, "stripe-21", 0)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if got := reassemble(t, chunks); string(got) != string(data) {
		t.Fatalf("round trip = %q, want %q", got, data)
	}
}

func TestDownloadEmptyFragment(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	if _, err := uploadFragment(t, client, "stripe-22", 1, false, nil, 1, false); err != nil {
		t.Fatalf("upload: %v", err)
	}

	chunks, err := downloadChunks(t, client, "stripe-22", 1)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("downloaded %d chunks for an empty fragment, want 1", len(chunks))
	}
	chunk := chunks[0]
	if len(chunk.GetData()) != 0 || chunk.GetOffset() != 0 || chunk.GetTotalSize() != 0 || !chunk.GetLast() {
		t.Fatalf("empty fragment chunk = %+v", chunk)
	}
	if chunk.GetChecksum() != fragmentChecksum(nil) {
		t.Fatalf("empty fragment checksum = %q", chunk.GetChecksum())
	}
}

func TestDownloadMissingFragment(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	_, err := downloadChunks(t, client, "stripe-23", 0)
	requireCode(t, err, codes.NotFound)
}

func TestDownloadCorruptedStoredFragment(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	err := store.PutFragment(storage.Fragment{
		StripeID:   "stripe-24",
		ShardIndex: 0,
		Data:       []byte("corrupt payload"),
		Checksum:   fragmentChecksum([]byte("original payload")),
	})
	if err != nil {
		t.Fatalf("PutFragment: %v", err)
	}

	_, err = downloadChunks(t, client, "stripe-24", 0)
	requireCode(t, err, codes.Internal)
}

func TestDownloadValidatesIdentity(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	_, err := downloadChunks(t, client, "", 0)
	requireCode(t, err, codes.InvalidArgument)

	_, err = downloadChunks(t, client, "stripe-25", 5)
	requireCode(t, err, codes.InvalidArgument)
}

func TestDownloadHonorsCancellation(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{UploadChunkSize: 1}, store)

	data := make([]byte, 64<<10)
	if _, err := uploadFragment(t, client, "stripe-26", 0, false, data, len(data), false); err != nil {
		t.Fatalf("upload: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.DownloadFragment(ctx, &pb.FragmentRequest{StripeId: "stripe-26", ShardIndex: 0})
	if err != nil {
		t.Fatalf("DownloadFragment: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	cancel()

	for {
		if _, err := stream.Recv(); err != nil {
			requireCode(t, err, codes.Canceled)
			return
		}
	}
}

func TestRepairScaffold(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{}, store)

	resp, err := client.Repair(context.Background(), &pb.RepairRequest{Key: "doc"})
	if err != nil {
		t.Fatalf("Repair failed: %v", err)
	}
	if !resp.GetSuccess() {
		t.Fatalf("Repair rejected a valid key: %s", resp.GetMessage())
	}

	badRepair, err := client.Repair(context.Background(), &pb.RepairRequest{Key: "  "})
	if err != nil {
		t.Fatalf("Repair(blank) RPC failed: %v", err)
	}
	if badRepair.GetSuccess() {
		t.Fatal("Repair accepted a blank key")
	}
}

func TestUploadRejectsOversizedGRPCMessage(t *testing.T) {
	store := newTestStore()
	cfg := config.Config{MaxMessageSize: 1024}
	client, _ := newFragmentTestServer(t, cfg, store, grpc.MaxRecvMsgSize(cfg.MaxMessageSize))

	data := make([]byte, 2048)
	_, err := uploadFragment(t, client, "stripe-13", 0, false, data, len(data), false)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted for an oversized gRPC message, got %v", err)
	}
	if _, err := store.GetFragment("stripe-13", 0); err == nil {
		t.Fatal("oversized message was stored")
	}
}

func TestUploadHonorsFragmentTimeout(t *testing.T) {
	store := newTestStore()
	client, _ := newFragmentTestServer(t, config.Config{FragmentTimeout: time.Nanosecond}, store)

	data := []byte("slow")
	stream := openUpload(t, client)
	sendChunk(t, stream, chunkFor("stripe-14", 0, false, false, fragmentChecksum(data), int64(len(data)), 0, data, true))

	time.Sleep(time.Millisecond)
	_, err := stream.CloseAndRecv()
	requireCode(t, err, codes.DeadlineExceeded)
}
