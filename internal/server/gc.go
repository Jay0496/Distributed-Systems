package server

import (
	"context"
	"log/slog"
	"time"

	"distributed-kv-store/internal/rpc"
	"distributed-kv-store/internal/storage"

	pb "distributed-kv-store/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gcTimeout bounds the best-effort deletion of a superseded stripe's fragments.
// The task 09 repair loop retries any leftovers.
const gcTimeout = 5 * time.Second

// deleteStripeFragments removes one stripe's local and remote fragments. It is
// used for cleanup after a failed write and for overwrite garbage collection.
// Only fragment rows are touched; stripe and placement metadata are removed by
// the put_meta apply on every node.
func (s *Server) deleteStripeFragments(ctx context.Context, stripeID string, placements []storage.ShardPlacement) {
	if stripeID == "" {
		return
	}

	owners := make(map[string]struct{}, len(placements))
	for _, placement := range placements {
		if placement.NodeID != "" {
			owners[placement.NodeID] = struct{}{}
		}
	}

	for owner := range owners {
		if err := ctx.Err(); err != nil {
			slog.Info("fragment cleanup cancelled",
				"stripe_id", stripeID, "error", err, "request_id", rpc.RequestID(ctx))
			return
		}

		if owner == s.cfg.NodeID {
			if err := s.deleteLocalStripeFragments(stripeID); err != nil {
				slog.Warn("local fragment cleanup failed",
					"stripe_id", stripeID, "error", err, "request_id", rpc.RequestID(ctx))
			}
			continue
		}

		target, ok := s.cfg.Peers[owner]
		if !ok {
			slog.Warn("no address for fragment owner", "stripe_id", stripeID, "owner", owner)
			continue
		}
		if err := s.deleteRemoteStripeFragments(ctx, target, stripeID); err != nil {
			slog.Warn("remote fragment cleanup failed",
				"stripe_id", stripeID, "owner", owner, "error", err, "request_id", rpc.RequestID(ctx))
		}
	}
}

// deleteLocalStripeFragments deletes every local fragment for a stripe without
// touching the local metadata tables.
func (s *Server) deleteLocalStripeFragments(stripeID string) error {
	refs, err := s.store.ListFragments(stripeID)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if err := s.store.DeleteFragment(stripeID, ref.ShardIndex); err != nil {
			return err
		}
	}
	return nil
}

// deleteRemoteStripeFragments asks one owner to delete a stripe's fragments.
func (s *Server) deleteRemoteStripeFragments(ctx context.Context, target, stripeID string) error {
	opCtx, cancel := context.WithTimeout(ctx, gcTimeout)
	defer cancel()

	conn, err := s.getConn(opCtx, target)
	if err != nil {
		return err
	}
	_, err = pb.NewStorageServiceClient(conn).DeleteStripeFragments(opCtx, &pb.DeleteStripeFragmentsRequest{
		StripeId: stripeID,
	})
	if status.Code(err) == codes.Unavailable {
		s.dropConn(target)
	}
	return err
}

// garbageCollectOldStripe removes the superseded stripe's fragments everywhere
// it was placed. The metadata commit already removed its stripes/placements
// rows on every node, so this is fragment-only cleanup. It runs asynchronously:
// a slow or unreachable owner must not delay a successful client response.
func (s *Server) garbageCollectOldStripe(currentStripeID string, old *storage.KeyMeta) {
	if old == nil || old.StripeID == "" || old.StripeID == currentStripeID {
		return
	}
	oldMeta := *old
	root := s.rootCtx
	if root == nil {
		root = context.Background()
	}
	go func() {
		ctx, cancel := context.WithTimeout(root, gcTimeout)
		defer cancel()
		s.deleteStripeFragments(ctx, oldMeta.StripeID, oldMeta.Placement)
		slog.Info("garbage-collected stripe",
			"stripe_id", oldMeta.StripeID, "positions", len(oldMeta.Placement))
	}()
}
