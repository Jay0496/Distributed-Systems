package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"

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

type statusNodeOutput struct {
	NodeID       string `json:"node_id"`
	GRPCAddr     string `json:"grpc_addr"`
	RaftState    string `json:"raft_state"`
	Healthy      bool   `json:"healthy"`
	Leader       bool   `json:"leader"`
	LastSeenUnix int64  `json:"last_seen_unix"`
	Fragments    uint64 `json:"fragments"`
	StorageBytes uint64 `json:"storage_bytes"`
	Error        string `json:"error,omitempty"`
}

type statusOutput struct {
	Node             string             `json:"node"`
	Success          bool               `json:"success"`
	LeaderID         string             `json:"leader_id"`
	Mode             string             `json:"mode"`
	Degraded         bool               `json:"degraded"`
	HealthyNodes     uint32             `json:"healthy_nodes"`
	TotalNodes       uint32             `json:"total_nodes"`
	DegradedStripes  uint64             `json:"degraded_stripes"`
	RepairQueueDepth uint64             `json:"repair_queue_depth"`
	Message          string             `json:"message"`
	Nodes            []statusNodeOutput `json:"nodes"`
}

func renderStatus(out io.Writer, node string, resp *pb.ClusterHealthResponse, asJSON bool) error {
	nodes := append([]*pb.NodeHealth(nil), resp.GetNodes()...)
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetNodeId() < nodes[j].GetNodeId()
	})

	if asJSON {
		rendered := statusOutput{
			Node:             node,
			Success:          resp.GetSuccess(),
			LeaderID:         resp.GetLeaderId(),
			Mode:             resp.GetMode(),
			Degraded:         resp.GetDegraded(),
			HealthyNodes:     resp.GetHealthyNodes(),
			TotalNodes:       resp.GetTotalNodes(),
			DegradedStripes:  resp.GetDegradedStripes(),
			RepairQueueDepth: resp.GetRepairQueueDepth(),
			Message:          resp.GetMessage(),
			Nodes:            make([]statusNodeOutput, 0, len(nodes)),
		}
		for _, entry := range nodes {
			rendered.Nodes = append(rendered.Nodes, statusNodeOutput{
				NodeID:       entry.GetNodeId(),
				GRPCAddr:     entry.GetGrpcAddr(),
				RaftState:    entry.GetRaftState(),
				Healthy:      entry.GetHealthy(),
				Leader:       entry.GetLeader(),
				LastSeenUnix: entry.GetLastSeenUnix(),
				Fragments:    entry.GetFragments(),
				StorageBytes: entry.GetStorageBytes(),
				Error:        entry.GetError(),
			})
		}
		return writeJSON(out, rendered)
	}

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "leader: %s\n", emptyDash(resp.GetLeaderId()))
	fmt.Fprintf(w, "mode: %s\n", emptyDash(resp.GetMode()))
	fmt.Fprintf(w, "health: %d/%d nodes healthy, degraded=%t, degraded_stripes=%d, repair_queue=%d\n",
		resp.GetHealthyNodes(), resp.GetTotalNodes(), resp.GetDegraded(),
		resp.GetDegradedStripes(), resp.GetRepairQueueDepth())
	fmt.Fprintf(w, "served by: %s\n\n", node)
	fmt.Fprintln(w, "NODE\tHEALTHY\tLEADER\tRAFT STATE\tFRAGMENTS\tSTORAGE\tERROR")
	for _, entry := range nodes {
		state := entry.GetRaftState()
		if state == "" {
			state = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			entry.GetNodeId(),
			yesNo(entry.GetHealthy()),
			yesNo(entry.GetLeader()),
			state,
			entry.GetFragments(),
			humanBytes(entry.GetStorageBytes()),
			emptyDash(entry.GetError()),
		)
	}
	return w.Flush()
}

type healthOutput struct {
	Node             string `json:"node"`
	Healthy          bool   `json:"healthy"`
	LeaderID         string `json:"leader_id"`
	Mode             string `json:"mode"`
	Degraded         bool   `json:"degraded"`
	HealthyNodes     uint32 `json:"healthy_nodes"`
	TotalNodes       uint32 `json:"total_nodes"`
	DegradedStripes  uint64 `json:"degraded_stripes"`
	RepairQueueDepth uint64 `json:"repair_queue_depth"`
	Message          string `json:"message"`
}

func renderHealth(out io.Writer, node string, resp *pb.ClusterHealthResponse, healthy, asJSON bool) error {
	if asJSON {
		return writeJSON(out, healthOutput{
			Node:             node,
			Healthy:          healthy,
			LeaderID:         resp.GetLeaderId(),
			Mode:             resp.GetMode(),
			Degraded:         resp.GetDegraded(),
			HealthyNodes:     resp.GetHealthyNodes(),
			TotalNodes:       resp.GetTotalNodes(),
			DegradedStripes:  resp.GetDegradedStripes(),
			RepairQueueDepth: resp.GetRepairQueueDepth(),
			Message:          resp.GetMessage(),
		})
	}

	fmt.Fprintf(out, "healthy=%t leader=%s mode=%s nodes=%d/%d degraded=%t degraded_stripes=%d repair_queue=%d message=%q\n",
		healthy, emptyDash(resp.GetLeaderId()), emptyDash(resp.GetMode()),
		resp.GetHealthyNodes(), resp.GetTotalNodes(), resp.GetDegraded(),
		resp.GetDegradedStripes(), resp.GetRepairQueueDepth(), resp.GetMessage())
	return nil
}

type statsOutput struct {
	Node             string `json:"node"`
	NodeID           string `json:"node_id"`
	Mode             string `json:"mode"`
	Leader           bool   `json:"leader"`
	Keys             uint64 `json:"keys"`
	Stripes          uint64 `json:"stripes"`
	Fragments        uint64 `json:"fragments"`
	StorageBytes     uint64 `json:"storage_bytes"`
	RepairQueueDepth uint64 `json:"repair_queue_depth"`
	RepairsCompleted uint64 `json:"repairs_completed"`
	RepairsFailed    uint64 `json:"repairs_failed"`
	UptimeSeconds    int64  `json:"uptime_seconds"`
}

func renderStats(out io.Writer, node string, resp *pb.StatsResponse, asJSON bool) error {
	if asJSON {
		return writeJSON(out, statsOutput{
			Node:             node,
			NodeID:           resp.GetNodeId(),
			Mode:             resp.GetMode(),
			Leader:           resp.GetLeader(),
			Keys:             resp.GetKeys(),
			Stripes:          resp.GetStripes(),
			Fragments:        resp.GetFragments(),
			StorageBytes:     resp.GetStorageBytes(),
			RepairQueueDepth: resp.GetRepairQueueDepth(),
			RepairsCompleted: resp.GetRepairsCompleted(),
			RepairsFailed:    resp.GetRepairsFailed(),
			UptimeSeconds:    resp.GetUptimeSeconds(),
		})
	}

	fmt.Fprintf(out, "stats from %s (node=%s leader=%t mode=%s): keys=%d stripes=%d fragments=%d storage=%s repair_queue=%d repairs_completed=%d repairs_failed=%d uptime=%ds\n",
		node, emptyDash(resp.GetNodeId()), resp.GetLeader(), emptyDash(resp.GetMode()),
		resp.GetKeys(), resp.GetStripes(), resp.GetFragments(),
		humanBytes(resp.GetStorageBytes()), resp.GetRepairQueueDepth(),
		resp.GetRepairsCompleted(), resp.GetRepairsFailed(), resp.GetUptimeSeconds())
	return nil
}

type repairOutput struct {
	Node     string `json:"node"`
	Key      string `json:"key"`
	StripeID string `json:"stripe_id,omitempty"`
	Force    bool   `json:"force"`
	Success  bool   `json:"success"`
	Message  string `json:"message"`
}

func renderRepair(out io.Writer, node, key, stripeID string, force bool, resp *pb.RepairResponse, asJSON bool) error {
	if asJSON {
		return writeJSON(out, repairOutput{
			Node:     node,
			Key:      key,
			StripeID: stripeID,
			Force:    force,
			Success:  resp.GetSuccess(),
			Message:  resp.GetMessage(),
		})
	}

	detail := ""
	if stripeID != "" {
		detail = " stripe=" + stripeID
	}
	fmt.Fprintf(out, "repair accepted on %s: key=%q%s force=%t: %s\n",
		node, key, detail, force, resp.GetMessage())
	return nil
}

func yesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}

func emptyDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func humanBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit && exp < 3; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
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
