package proto_test

import (
	"testing"

	pb "distributed-kv-store/proto"
)

// TestFragmentChunkV2Fields pins the additive fragment transport fields. The
// legacy fields stay present so old clients still compile and old messages
// remain decodable.
func TestFragmentChunkV2Fields(t *testing.T) {
	chunk := &pb.FragmentChunk{
		StripeId:   "stripe-1",
		ShardIndex: 3,
		Checksum:   "deadbeef",
		Offset:     1024,
		TotalSize:  2048,
		Last:       true,
		Replace:    true,
		Data:       []byte("parity"),
	}

	if got := chunk.GetStripeId(); got != "stripe-1" {
		t.Errorf("StripeId = %q, want stripe-1", got)
	}
	if got := chunk.GetShardIndex(); got != 3 {
		t.Errorf("ShardIndex = %d, want 3", got)
	}
	if got := chunk.GetChecksum(); got != "deadbeef" {
		t.Errorf("Checksum = %q, want deadbeef", got)
	}
	if got := chunk.GetOffset(); got != 1024 {
		t.Errorf("Offset = %d, want 1024", got)
	}
	if got := chunk.GetTotalSize(); got != 2048 {
		t.Errorf("TotalSize = %d, want 2048", got)
	}
	if !chunk.GetLast() {
		t.Error("Last = false, want true")
	}
	if !chunk.GetReplace() {
		t.Error("Replace = false, want true")
	}

	legacy := &pb.FragmentChunk{Key: "doc", Index: 1, IsParity: true}
	if got := legacy.GetKey(); got != "doc" {
		t.Errorf("legacy Key = %q, want doc", got)
	}
	if got := legacy.GetIndex(); got != 1 {
		t.Errorf("legacy Index = %d, want 1", got)
	}
	if !legacy.GetIsParity() {
		t.Error("legacy IsParity = false, want true")
	}
}

// TestFragmentRequestV2Fields checks the stripe-addressed download request.
func TestFragmentRequestV2Fields(t *testing.T) {
	req := &pb.FragmentRequest{StripeId: "stripe-1", ShardIndex: 4}
	if got := req.GetStripeId(); got != "stripe-1" {
		t.Errorf("StripeId = %q, want stripe-1", got)
	}
	if got := req.GetShardIndex(); got != 4 {
		t.Errorf("ShardIndex = %d, want 4", got)
	}

	legacy := &pb.FragmentRequest{Key: "doc", Index: 2}
	if got := legacy.GetKey(); got != "doc" {
		t.Errorf("legacy Key = %q, want doc", got)
	}
	if got := legacy.GetIndex(); got != 2 {
		t.Errorf("legacy Index = %d, want 2", got)
	}
}

// TestFragmentOperationMessages checks the new delete, list, stat, and info
// messages exist with the expected accessors.
func TestFragmentOperationMessages(t *testing.T) {
	info := &pb.FragmentInfo{
		StripeId:   "stripe-1",
		ShardIndex: 2,
		IsParity:   false,
		Size:       4096,
		Checksum:   "cafe",
	}
	if info.GetStripeId() != "stripe-1" || info.GetShardIndex() != 2 || info.GetIsParity() ||
		info.GetSize() != 4096 || info.GetChecksum() != "cafe" {
		t.Errorf("FragmentInfo accessors returned %+v", info)
	}

	deleteOne := &pb.DeleteFragmentRequest{StripeId: "stripe-1", ShardIndex: 0}
	if deleteOne.GetStripeId() != "stripe-1" || deleteOne.GetShardIndex() != 0 {
		t.Errorf("DeleteFragmentRequest accessors returned %+v", deleteOne)
	}

	deleteStripe := &pb.DeleteStripeFragmentsRequest{StripeId: "stripe-1"}
	if deleteStripe.GetStripeId() != "stripe-1" {
		t.Errorf("DeleteStripeFragmentsRequest = %+v", deleteStripe)
	}

	list := &pb.ListFragmentsRequest{}
	if list.GetStripeId() != "" {
		t.Errorf("empty ListFragmentsRequest StripeId = %q, want empty", list.GetStripeId())
	}

	op := &pb.FragmentOpResponse{Success: true, Message: "absent"}
	if !op.GetSuccess() || op.GetMessage() != "absent" {
		t.Errorf("FragmentOpResponse = %+v", op)
	}
}
