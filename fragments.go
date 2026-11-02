package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fragmentKey namespaces fragments so they cannot overwrite the base key.
func fragmentKey(key string, index int32, isParity bool) string {
	kind := "d"
	if isParity {
		kind = "p"
	}
	return fmt.Sprintf("%s#%s%d", key, kind, index)
}

// UploadFragment stores each chunk of a streamed fragment.
func (s *server) UploadFragment(stream pb.StorageService_UploadFragmentServer) error {
	count := 0
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.FragmentResponse{
				Success: true,
				Message: fmt.Sprintf("stored %d fragment chunks", count),
			})
		}
		if err != nil {
			return err
		}
		if chunk.GetKey() == "" {
			return status.Error(codes.InvalidArgument, "fragment key is required")
		}

		key := fragmentKey(chunk.GetKey(), chunk.GetIndex(), chunk.GetIsParity())
		if err := s.store.Put(key, chunk.GetData(), chunk.GetIsParity()); err != nil {
			return status.Errorf(codes.Internal, "store fragment: %v", err)
		}
		count++
	}
}

// DownloadFragment streams a stored fragment back to the caller.
func (s *server) DownloadFragment(req *pb.FragmentRequest, stream pb.StorageService_DownloadFragmentServer) error {
	if req.GetKey() == "" {
		return status.Error(codes.InvalidArgument, "fragment key is required")
	}

	for _, isParity := range []bool{false, true} {
		key := fragmentKey(req.GetKey(), req.GetIndex(), isParity)
		value, err := s.store.Get(key)
		if err != nil {
			continue
		}
		return stream.Send(&pb.FragmentChunk{
			Key:      req.GetKey(),
			Index:    req.GetIndex(),
			Data:     value,
			IsParity: isParity,
		})
	}

	return status.Error(codes.NotFound, "fragment not found")
}

// Repair is the entry point for the failure-management subsystem. It is a
// deliberate scaffold: reconstruction and rebalancing land with that subsystem.
func (s *server) Repair(ctx context.Context, req *pb.RepairRequest) (*pb.RepairResponse, error) {
	if strings.TrimSpace(req.GetKey()) == "" {
		return &pb.RepairResponse{Success: false, Message: "key is required"}, nil
	}

	log.Printf("repair requested for key %q (reconstruction not yet implemented)", req.GetKey())
	return &pb.RepairResponse{
		Success: true,
		Message: "repair request accepted; reconstruction is provided by the failure-management subsystem",
	}, nil
}
