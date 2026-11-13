package bench

import (
	"context"
	"sync"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeAPI is a controllable StorageService subset used by the package tests.
type fakeAPI struct {
	mu sync.Mutex

	puts      int64
	gets      int64
	unavail   int64
	block     chan struct{}
	stats     pb.StatsResponse
	snapshots int64
	checks    int64
	health    func(call int64) *pb.ClusterHealthResponse
	healthN   int64
}

func (f *fakeAPI) Put(ctx context.Context, req *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	f.mu.Lock()
	f.puts++
	remaining := f.unavail
	if remaining > 0 {
		f.unavail--
	}
	block := f.block
	f.mu.Unlock()
	if remaining > 0 {
		return nil, status.Error(codes.Unavailable, "leader changed")
	}
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-block:
		}
	}
	return &pb.PutResponse{Success: true, Mode: req.GetMode(), Version: 1}, nil
}

func (f *fakeAPI) Get(ctx context.Context, req *pb.GetRequest, _ ...grpc.CallOption) (*pb.GetResponse, error) {
	f.mu.Lock()
	f.gets++
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-block:
		}
	}
	return &pb.GetResponse{Success: true, Value: []byte("value")}, nil
}

func (f *fakeAPI) Stats(context.Context, *pb.StatsRequest, ...grpc.CallOption) (*pb.StatsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &pb.StatsResponse{
		Keys:          f.stats.Keys,
		Stripes:       f.stats.Stripes,
		Fragments:     f.stats.Fragments + uint64(f.puts),
		FragmentBytes: f.stats.FragmentBytes,
		StorageBytes:  f.stats.StorageBytes,
	}, nil
}

func (f *fakeAPI) ForceSnapshot(context.Context, *pb.ForceSnapshotRequest, ...grpc.CallOption) (*pb.ForceSnapshotResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots++
	return &pb.ForceSnapshotResponse{Success: true, LastIndex: 42}, nil
}

func (f *fakeAPI) CheckpointStore(context.Context, *pb.CheckpointRequest, ...grpc.CallOption) (*pb.CheckpointResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checks++
	return &pb.CheckpointResponse{Success: true, BytesAfter: int64(f.stats.StorageBytes)}, nil
}

func (f *fakeAPI) ClusterHealth(context.Context, *pb.ClusterHealthRequest, ...grpc.CallOption) (*pb.ClusterHealthResponse, error) {
	f.mu.Lock()
	call := f.healthN
	f.healthN++
	fn := f.health
	f.mu.Unlock()
	if fn == nil {
		return &pb.ClusterHealthResponse{Success: true}, nil
	}
	return fn(call), nil
}

func fakeNodes(count int, api *fakeAPI) Nodes {
	nodes := make(Nodes, count)
	for i := 0; i < count; i++ {
		nodes[i] = Node{ID: "node" + string(rune('1'+i)), API: api}
	}
	return nodes
}
