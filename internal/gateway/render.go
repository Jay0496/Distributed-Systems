package gateway

import (
	"encoding/json"
	"fmt"
	"io"

	pb "distributed-kv-store/proto"
)

// writeJSON renders one machine-readable value. JSON is the stable interface
// used by scripts and benchmarks; text output is only for humans.
func writeJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode JSON output: %w", err)
	}
	return nil
}

type putOutput struct {
	Node         string `json:"node"`
	Key          string `json:"key"`
	Success      bool   `json:"success"`
	Message      string `json:"message"`
	StripeID     string `json:"stripe_id"`
	Mode         string `json:"mode"`
	Version      int64  `json:"version"`
	Degraded     bool   `json:"degraded"`
	ShardsStored int32  `json:"shards_stored"`
	Checksum     string `json:"checksum"`
}

func renderPut(out io.Writer, node, key string, resp *pb.PutResponse, asJSON bool) error {
	if asJSON {
		return writeJSON(out, putOutput{
			Node:         node,
			Key:          key,
			Success:      resp.GetSuccess(),
			Message:      resp.GetMessage(),
			StripeID:     resp.GetStripeId(),
			Mode:         resp.GetMode(),
			Version:      resp.GetVersion(),
			Degraded:     resp.GetDegraded(),
			ShardsStored: resp.GetShardsStored(),
			Checksum:     resp.GetChecksum(),
		})
	}

	degraded := ""
	if resp.GetDegraded() {
		degraded = " degraded=true"
	}
	fmt.Fprintf(out, "put committed on %s: key=%q version=%d mode=%s shards=%d/5 stripe=%s%s\n",
		node, key, resp.GetVersion(), resp.GetMode(), resp.GetShardsStored(), resp.GetStripeId(), degraded)
	return nil
}

type getOutput struct {
	Node       string `json:"node"`
	Key        string `json:"key"`
	Success    bool   `json:"success"`
	Value      string `json:"value"`
	Mode       string `json:"mode"`
	StripeID   string `json:"stripe_id"`
	Version    int64  `json:"version"`
	ShardsUsed int32  `json:"shards_used"`
	Repaired   bool   `json:"repaired"`
	Checksum   string `json:"checksum"`
}

func renderGet(out, errOut io.Writer, node, key string, resp *pb.GetResponse, asJSON bool) error {
	if asJSON {
		return writeJSON(out, getOutput{
			Node:       node,
			Key:        key,
			Success:    resp.GetSuccess(),
			Value:      string(resp.GetValue()),
			Mode:       resp.GetMode(),
			StripeID:   resp.GetStripeId(),
			Version:    resp.GetVersion(),
			ShardsUsed: resp.GetShardsUsed(),
			Repaired:   resp.GetRepaired(),
			Checksum:   resp.GetChecksum(),
		})
	}

	// The value stays on stdout so it can be piped; forwarding metadata goes to
	// stderr so the demo can still show which node served the request.
	if _, err := fmt.Fprintf(out, "%s\n", resp.GetValue()); err != nil {
		return err
	}
	fmt.Fprintf(errOut, "served by %s (mode=%s version=%d shards=%d repaired=%v)\n",
		node, resp.GetMode(), resp.GetVersion(), resp.GetShardsUsed(), resp.GetRepaired())
	return nil
}

type deleteOutput struct {
	Node        string `json:"node"`
	Key         string `json:"key"`
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	Found       bool   `json:"found"`
	OldStripeID string `json:"old_stripe_id"`
}

func renderDelete(out io.Writer, node, key string, resp *pb.DeleteResponse, asJSON bool) error {
	if asJSON {
		return writeJSON(out, deleteOutput{
			Node:        node,
			Key:         key,
			Success:     resp.GetSuccess(),
			Message:     resp.GetMessage(),
			Found:       resp.GetFound(),
			OldStripeID: resp.GetOldStripeId(),
		})
	}

	if !resp.GetFound() {
		fmt.Fprintf(out, "delete on %s: key %q was already absent\n", node, key)
		return nil
	}
	fmt.Fprintf(out, "delete on %s: key %q removed (old stripe %s)\n", node, key, resp.GetOldStripeId())
	return nil
}

type modeOutput struct {
	Node        string `json:"node"`
	DefaultMode string `json:"default_mode"`
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	LeaderID    string `json:"leader_id"`
}

func renderMode(out io.Writer, node string, resp *pb.ConfigResponse, set bool, asJSON bool) error {
	if asJSON {
		return writeJSON(out, modeOutput{
			Node:        node,
			DefaultMode: resp.GetDefaultMode(),
			Success:     resp.GetSuccess(),
			Message:     resp.GetMessage(),
			LeaderID:    resp.GetLeaderId(),
		})
	}

	if set {
		fmt.Fprintf(out, "default mode set to %s on %s\n", resp.GetDefaultMode(), node)
		return nil
	}
	fmt.Fprintf(out, "default mode: %s (served by %s)\n", resp.GetDefaultMode(), node)
	return nil
}
