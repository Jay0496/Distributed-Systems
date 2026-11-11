package server

import (
	"context"
	"errors"

	"distributed-kv-store/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Read-path errors. They are sentinel values so handlers can map each failure
// to the documented gRPC status without parsing error strings.
var (
	// ErrInsufficientShards means fewer than three RS shards or zero valid
	// replication copies were available.
	ErrInsufficientShards = errors.New("insufficient valid shards")
	// ErrDataLoss means enough shards arrived but no subset reconstructed a
	// value matching the committed size and checksum.
	ErrDataLoss = errors.New("read data loss")
)

// readStatusError maps read-path failures to the documented gRPC statuses.
//
// GetResponse has no message field, so application failures are returned as
// status errors rather than success=false responses. Forwarding propagates the
// leader's status unchanged.
func readStatusError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrInsufficientShards):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrDataLoss):
		return status.Error(codes.DataLoss, err.Error())
	case errors.Is(err, storage.ErrNotFound):
		return status.Error(codes.NotFound, "key not found")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		// Store read errors and any unexpected metadata failure must never be
		// mistaken for a missing key.
		return status.Error(codes.Internal, err.Error())
	}
}
