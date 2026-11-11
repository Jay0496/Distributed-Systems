package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	minShardIndex = 0
	maxShardIndex = 4
)

// fragmentChecksum returns the lowercase SHA-256 hex digest of data.
func fragmentChecksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// validateFragmentID enforces the v2 identity: a non-empty stripe ID and a
// global shard index in 0..4.
func validateFragmentID(stripeID string, shardIndex int32) error {
	if strings.TrimSpace(stripeID) == "" {
		return status.Error(codes.InvalidArgument, "fragment stripe_id is required")
	}
	if shardIndex < minShardIndex || shardIndex > maxShardIndex {
		return status.Errorf(codes.InvalidArgument, "invalid shard_index %d (expected %d..%d)", shardIndex, minShardIndex, maxShardIndex)
	}
	return nil
}

// validateShardParity derives what it can without knowing the redundancy mode.
// Indices 0-2 are always RS data shards, so they can never be marked parity.
// Indices 3-4 may be RS parity or a full replication copy, so the sender's flag
// is authoritative for those indices.
func validateShardParity(shardIndex int32, isParity bool) error {
	if shardIndex <= 2 && isParity {
		return status.Errorf(codes.InvalidArgument, "shard_index %d is a data shard and cannot be marked parity", shardIndex)
	}
	return nil
}

// validateFragmentChecksum requires a 64-character hexadecimal SHA-256 digest.
func validateFragmentChecksum(checksum string) error {
	if checksum == "" {
		return status.Error(codes.InvalidArgument, "fragment checksum is required")
	}
	if len(checksum) != sha256.Size*2 {
		return status.Errorf(codes.InvalidArgument, "fragment checksum must be %d hex characters", sha256.Size*2)
	}
	if _, err := hex.DecodeString(checksum); err != nil {
		return status.Error(codes.InvalidArgument, "fragment checksum must be hexadecimal")
	}
	return nil
}

// UploadFragment stages a chunked fragment and commits it atomically once the
// final chunk arrives, the total size matches, and the checksum verifies.
func (s *Server) UploadFragment(stream pb.StorageService_UploadFragmentServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), s.cfg.FragmentTimeout)
	defer cancel()

	var (
		id     fragmentID
		state  *uploadState
		staged bool
	)
	defer func() {
		if staged {
			s.staging.end(id)
		}
	}()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if state == nil {
				return status.Error(codes.InvalidArgument, "fragment upload contained no chunks")
			}
			return status.Error(codes.InvalidArgument, "fragment upload ended before a chunk with last=true")
		}
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}

		if err := s.validateChunk(chunk); err != nil {
			return err
		}

		if state == nil {
			id = fragmentID{stripeID: chunk.GetStripeId(), shardIndex: int(chunk.GetShardIndex())}
			state, err = s.staging.begin(
				id,
				chunk.GetTotalSize(),
				strings.ToLower(chunk.GetChecksum()),
				chunk.GetReplace(),
				chunk.GetIsParity(),
			)
			if err != nil {
				switch {
				case errors.Is(err, errUploadInFlight):
					return status.Errorf(codes.Aborted, "fragment %s/%d is already being uploaded", id.stripeID, id.shardIndex)
				case errors.Is(err, errTooManyUploads):
					return status.Error(codes.ResourceExhausted, err.Error())
				default:
					return status.Errorf(codes.Internal, "stage fragment: %v", err)
				}
			}
			staged = true
		} else if err := validateChunkConsistency(chunk, id, state); err != nil {
			return err
		}

		if err := state.appendData(chunk.GetOffset(), chunk.GetData()); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}

		if !chunk.GetLast() {
			continue
		}

		if int64(len(state.data)) != state.totalSize {
			return status.Errorf(codes.InvalidArgument,
				"fragment size %d does not match total_size %d", len(state.data), state.totalSize)
		}
		if got := fragmentChecksum(state.data); !strings.EqualFold(got, state.checksum) {
			return status.Errorf(codes.InvalidArgument,
				"fragment checksum mismatch: got %s, want %s", got, state.checksum)
		}

		alreadyStored, err := s.commitFragment(ctx, id, state)
		if err != nil {
			return err
		}
		message := fmt.Sprintf("stored fragment %s/%d (%d bytes)", id.stripeID, id.shardIndex, len(state.data))
		if alreadyStored {
			message = fmt.Sprintf("fragment %s/%d already stored with matching checksum", id.stripeID, id.shardIndex)
		}
		return stream.SendAndClose(&pb.FragmentResponse{Success: true, Message: message})
	}
}

// validateChunk checks the fields that must be valid on every chunk.
func (s *Server) validateChunk(chunk *pb.FragmentChunk) error {
	if err := validateFragmentID(chunk.GetStripeId(), chunk.GetShardIndex()); err != nil {
		return err
	}
	if err := validateShardParity(chunk.GetShardIndex(), chunk.GetIsParity()); err != nil {
		return err
	}
	if err := validateFragmentChecksum(chunk.GetChecksum()); err != nil {
		return err
	}
	if chunk.GetTotalSize() < 0 {
		return status.Errorf(codes.InvalidArgument, "total_size %d must not be negative", chunk.GetTotalSize())
	}
	if chunk.GetTotalSize() > int64(s.cfg.MaxFragmentSize) {
		return status.Errorf(codes.InvalidArgument,
			"total_size %d exceeds MAX_FRAGMENT_SIZE %d", chunk.GetTotalSize(), s.cfg.MaxFragmentSize)
	}
	if chunk.GetOffset() < 0 {
		return status.Errorf(codes.InvalidArgument, "offset %d must not be negative", chunk.GetOffset())
	}
	if len(chunk.GetData()) > s.cfg.MaxMessageSize {
		return status.Errorf(codes.InvalidArgument,
			"chunk data is %d bytes, exceeds MAX_MESSAGE_SIZE %d", len(chunk.GetData()), s.cfg.MaxMessageSize)
	}
	return nil
}

// validateChunkConsistency rejects a stream whose repeated fields change
// mid-upload. Only data and offset may vary between chunks.
func validateChunkConsistency(chunk *pb.FragmentChunk, id fragmentID, state *uploadState) error {
	if chunk.GetStripeId() != id.stripeID || int(chunk.GetShardIndex()) != id.shardIndex {
		return status.Errorf(codes.InvalidArgument,
			"fragment identity changed mid-upload: got %s/%d, want %s/%d",
			chunk.GetStripeId(), chunk.GetShardIndex(), id.stripeID, id.shardIndex)
	}
	if chunk.GetTotalSize() != state.totalSize {
		return status.Errorf(codes.InvalidArgument,
			"total_size changed mid-upload: got %d, want %d", chunk.GetTotalSize(), state.totalSize)
	}
	if !strings.EqualFold(chunk.GetChecksum(), state.checksum) {
		return status.Error(codes.InvalidArgument, "checksum changed mid-upload")
	}
	if chunk.GetReplace() != state.replace {
		return status.Error(codes.InvalidArgument, "replace changed mid-upload")
	}
	if chunk.GetIsParity() != state.isParity {
		return status.Error(codes.InvalidArgument, "is_parity changed mid-upload")
	}
	return nil
}

// commitFragment applies duplicate rules and writes the staged buffer in one
// upsert. It returns true when an identical fragment was already present.
//
// Replacing a fragment with a different checksum requires replace=true, which
// only repair paths set. The transport has no view of key metadata, so it does
// not decide whether a stripe is current; superseded-stripe cleanup belongs to
// metadata garbage collection.
func (s *Server) commitFragment(ctx context.Context, id fragmentID, state *uploadState) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, status.FromContextError(err).Err()
	}

	refs, err := s.store.ListFragments(id.stripeID)
	if err != nil {
		return false, status.Errorf(codes.Internal, "look up fragment %s/%d: %v", id.stripeID, id.shardIndex, err)
	}
	var existingChecksum string
	found := false
	for _, ref := range refs {
		if ref.ShardIndex == id.shardIndex {
			existingChecksum = ref.Checksum
			found = true
			break
		}
	}

	if found {
		if strings.EqualFold(existingChecksum, state.checksum) {
			return true, nil
		}
		if !state.replace {
			return false, status.Errorf(codes.AlreadyExists,
				"fragment %s/%d already exists with a different checksum", id.stripeID, id.shardIndex)
		}
	}

	fragment := storage.Fragment{
		StripeID:   id.stripeID,
		ShardIndex: id.shardIndex,
		IsParity:   state.isParity,
		Data:       state.data,
		Checksum:   state.checksum,
	}
	if err := s.store.PutFragment(fragment); err != nil {
		return false, status.Errorf(codes.Internal, "store fragment %s/%d: %v", id.stripeID, id.shardIndex, err)
	}
	return false, nil
}

// DownloadFragment streams a stored fragment in bounded chunks after verifying
// its stored checksum. The legacy key/index fields are ignored.
func (s *Server) DownloadFragment(req *pb.FragmentRequest, stream pb.StorageService_DownloadFragmentServer) error {
	ctx, cancel := context.WithTimeout(stream.Context(), s.cfg.FragmentTimeout)
	defer cancel()

	if err := validateFragmentID(req.GetStripeId(), req.GetShardIndex()); err != nil {
		return err
	}

	stripeID := req.GetStripeId()
	shardIndex := int(req.GetShardIndex())
	frag, err := s.store.GetFragment(stripeID, shardIndex)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return status.Errorf(codes.NotFound, "fragment %s/%d not found", stripeID, shardIndex)
		}
		return status.Errorf(codes.Internal, "read fragment %s/%d: %v", stripeID, shardIndex, err)
	}

	checksum := frag.Checksum
	if checksum == "" {
		checksum = fragmentChecksum(frag.Data)
	}
	if !strings.EqualFold(checksum, fragmentChecksum(frag.Data)) {
		return status.Errorf(codes.Internal,
			"stored fragment %s/%d failed checksum verification", stripeID, shardIndex)
	}

	totalSize := int64(len(frag.Data))
	if totalSize == 0 {
		return stream.Send(&pb.FragmentChunk{
			StripeId:   stripeID,
			ShardIndex: int32(shardIndex),
			IsParity:   frag.IsParity,
			Checksum:   checksum,
			Offset:     0,
			TotalSize:  0,
			Last:       true,
		})
	}

	chunkSize := s.cfg.UploadChunkSize
	for offset := int64(0); offset < totalSize; offset += int64(chunkSize) {
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}

		end := offset + int64(chunkSize)
		if end > totalSize {
			end = totalSize
		}
		chunk := &pb.FragmentChunk{
			StripeId:   stripeID,
			ShardIndex: int32(shardIndex),
			IsParity:   frag.IsParity,
			Checksum:   checksum,
			Offset:     offset,
			TotalSize:  totalSize,
			Data:       frag.Data[offset:end],
			Last:       end == totalSize,
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}
	return nil
}

// Repair is the entry point for the failure-management subsystem. It is a
// deliberate scaffold: reconstruction and rebalancing land with that subsystem.
func (s *Server) Repair(ctx context.Context, req *pb.RepairRequest) (*pb.RepairResponse, error) {
	if strings.TrimSpace(req.GetKey()) == "" {
		return &pb.RepairResponse{Success: false, Message: "key is required"}, nil
	}

	log.Printf("repair requested for key %q (reconstruction not yet implemented)", req.GetKey())
	return &pb.RepairResponse{
		Success: true,
		Message: "repair request accepted; reconstruction is provided by the failure-management subsystem",
	}, nil
}
