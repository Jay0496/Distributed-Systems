package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "distributed-kv-store/proto"
)

// fakeService records CLI requests and returns programmable responses.
type fakeService struct {
	putReq  *pb.PutRequest
	putNode string
	putResp *pb.PutResponse
	putErr  error

	getKey  string
	getNode string
	getResp *pb.GetResponse
	getErr  error

	deleteReq  *pb.DeleteRequest
	deleteNode string
	deleteResp *pb.DeleteResponse
	deleteErr  error

	getConfigCalls int
	configNode     string
	configResp     *pb.ConfigResponse
	configErr      error

	setConfigMode  string
	setConfigCalls int
	setConfigResp  *pb.ConfigResponse
	setConfigErr   error

	healthNode  string
	healthResp  *pb.ClusterHealthResponse
	healthErr   error
	healthCalls int

	statsNode string
	statsResp *pb.StatsResponse
	statsErr  error

	repairReq  *pb.RepairRequest
	repairNode string
	repairResp *pb.RepairResponse
	repairErr  error
}

func (f *fakeService) Put(_ context.Context, req *pb.PutRequest) (string, *pb.PutResponse, error) {
	f.putReq = req
	if f.putErr != nil {
		return f.putNode, nil, f.putErr
	}
	resp := f.putResp
	if resp == nil {
		resp = &pb.PutResponse{
			Success: true, Message: "write committed", StripeId: "stripe-1",
			Mode: "rs32", Version: 1, ShardsStored: 5, Checksum: "abc",
		}
	}
	return f.putNode, resp, nil
}

func (f *fakeService) Get(_ context.Context, key string) (string, *pb.GetResponse, error) {
	f.getKey = key
	if f.getErr != nil {
		return f.getNode, nil, f.getErr
	}
	resp := f.getResp
	if resp == nil {
		resp = &pb.GetResponse{
			Success: true, Value: []byte("hello"), Mode: "rs32",
			StripeId: "stripe-1", Version: 2, ShardsUsed: 3, Checksum: "abc",
		}
	}
	return f.getNode, resp, nil
}

func (f *fakeService) Delete(_ context.Context, req *pb.DeleteRequest) (string, *pb.DeleteResponse, error) {
	f.deleteReq = req
	if f.deleteErr != nil {
		return f.deleteNode, nil, f.deleteErr
	}
	resp := f.deleteResp
	if resp == nil {
		resp = &pb.DeleteResponse{Success: true, Found: true, Message: "deleted", OldStripeId: "stripe-1"}
	}
	return f.deleteNode, resp, nil
}

func (f *fakeService) GetConfig(context.Context) (string, *pb.ConfigResponse, error) {
	f.getConfigCalls++
	if f.configErr != nil {
		return f.configNode, nil, f.configErr
	}
	resp := f.configResp
	if resp == nil {
		resp = &pb.ConfigResponse{Success: true, DefaultMode: "rs32", LeaderId: "node1"}
	}
	return f.configNode, resp, nil
}

func (f *fakeService) SetConfig(_ context.Context, mode string) (string, *pb.ConfigResponse, error) {
	f.setConfigCalls++
	f.setConfigMode = mode
	if f.setConfigErr != nil {
		return f.configNode, nil, f.setConfigErr
	}
	resp := f.setConfigResp
	if resp == nil {
		resp = &pb.ConfigResponse{Success: true, DefaultMode: mode, LeaderId: "node1"}
	}
	return f.configNode, resp, nil
}

func (f *fakeService) ClusterHealth(context.Context) (string, *pb.ClusterHealthResponse, error) {
	f.healthCalls++
	if f.healthErr != nil {
		return f.healthNode, nil, f.healthErr
	}
	resp := f.healthResp
	if resp == nil {
		resp = healthyClusterResponse()
	}
	return f.healthNode, resp, nil
}

func (f *fakeService) Stats(context.Context) (string, *pb.StatsResponse, error) {
	if f.statsErr != nil {
		return f.statsNode, nil, f.statsErr
	}
	resp := f.statsResp
	if resp == nil {
		resp = &pb.StatsResponse{
			NodeId: "node1", Mode: "rs32", Leader: true,
			Keys: 4, Stripes: 4, Fragments: 20, StorageBytes: 2 * 1024 * 1024,
			RepairQueueDepth: 2, RepairsCompleted: 3, RepairsFailed: 1, UptimeSeconds: 90,
		}
	}
	return f.statsNode, resp, nil
}

func (f *fakeService) Repair(_ context.Context, req *pb.RepairRequest) (string, *pb.RepairResponse, error) {
	f.repairReq = req
	if f.repairErr != nil {
		return f.repairNode, nil, f.repairErr
	}
	resp := f.repairResp
	if resp == nil {
		resp = &pb.RepairResponse{Success: true, Message: "repair accepted for 5 shards"}
	}
	return f.repairNode, resp, nil
}

func healthyClusterResponse() *pb.ClusterHealthResponse {
	resp := &pb.ClusterHealthResponse{
		Success: true, LeaderId: "node1", Mode: "rs32",
		HealthyNodes: 5, TotalNodes: 5,
		Message: "5/5 nodes healthy, 0 degraded stripes",
	}
	for i := 1; i <= 5; i++ {
		resp.Nodes = append(resp.Nodes, &pb.NodeHealth{
			NodeId:       "node" + string(rune('0'+i)),
			GrpcAddr:     "node" + string(rune('0'+i)) + ":5005" + string(rune('0'+i)),
			Healthy:      true,
			Leader:       i == 1,
			RaftState:    "Follower",
			Fragments:    uint64(i),
			StorageBytes: uint64(i) * 1024,
		})
	}
	return resp
}

func newTestCLI(t *testing.T, svc Service) (*CLI, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}
	return NewCLI(svc, out, errOut), out, errOut
}

func TestCLIPutParsesInterspersedFlags(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"flags before positionals", []string{"put", "--mode", "replication", "--version", "4", "k", "v"}},
		{"flags after positionals", []string{"put", "k", "v", "--mode", "replication", "--version", "4"}},
		{"equals form", []string{"put", "k", "v", "--mode=replication", "--version=4"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &fakeService{putNode: "node1"}
			cli, _, _ := newTestCLI(t, svc)
			if err := cli.Run(context.Background(), tc.args); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if svc.putReq == nil {
				t.Fatal("Put was not called")
			}
			if svc.putReq.GetKey() != "k" || string(svc.putReq.GetValue()) != "v" {
				t.Fatalf("request = %+v", svc.putReq)
			}
			if svc.putReq.GetMode() != "replication" {
				t.Fatalf("mode = %q, want replication", svc.putReq.GetMode())
			}
			if svc.putReq.GetExpectedVersion() != 4 {
				t.Fatalf("expected_version = %d, want 4", svc.putReq.GetExpectedVersion())
			}
		})
	}
}

func TestCLIPutValueFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value.bin")
	if err := os.WriteFile(path, []byte("from-file"), 0o600); err != nil {
		t.Fatalf("write value file: %v", err)
	}

	svc := &fakeService{putNode: "node1"}
	cli, _, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"put", "k", "--value-file", path}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := string(svc.putReq.GetValue()); got != "from-file" {
		t.Fatalf("value = %q, want from-file", got)
	}
}

func TestCLIPutRejectsMissingValueOrFile(t *testing.T) {
	svc := &fakeService{}
	cli, _, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"put", "k"}); err == nil {
		t.Fatal("put without a value succeeded")
	}
	if svc.putReq != nil {
		t.Fatal("Put was called for an invalid request")
	}
}

func TestCLIPutRejectsValueAndFileTogether(t *testing.T) {
	path := filepath.Join(t.TempDir(), "value.bin")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatalf("write value file: %v", err)
	}
	cli, _, _ := newTestCLI(t, &fakeService{})
	if err := cli.Run(context.Background(), []string{"put", "k", "v", "--value-file", path}); err == nil {
		t.Fatal("put accepted both a positional value and --value-file")
	}
}

func TestCLIPutRejectsInvalidMode(t *testing.T) {
	svc := &fakeService{}
	cli, _, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"put", "k", "v", "--mode", "triple"}); err == nil {
		t.Fatal("put accepted an invalid mode")
	}
	if svc.putReq != nil {
		t.Fatal("Put was called for an invalid mode")
	}
}

func TestCLIPutRejectedResponseIsError(t *testing.T) {
	svc := &fakeService{
		putNode: "node1",
		putResp: &pb.PutResponse{Success: false, Message: "version conflict"},
	}
	cli, _, _ := newTestCLI(t, svc)
	err := cli.Run(context.Background(), []string{"put", "k", "v", "--version", "2"})
	if err == nil || !strings.Contains(err.Error(), "version conflict") {
		t.Fatalf("error = %v, want a version conflict", err)
	}
}

func TestCLIPutJSON(t *testing.T) {
	cli, out, _ := newTestCLI(t, &fakeService{putNode: "node1"})
	if err := cli.Run(context.Background(), []string{"put", "k", "v", "--json"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out.String())
	}
	if got["node"] != "node1" || got["key"] != "k" {
		t.Fatalf("JSON = %v", got)
	}
}

func TestCLIGetTextPrintsValueAndServedNode(t *testing.T) {
	cli, out, errOut := newTestCLI(t, &fakeService{getNode: "node3"})
	if err := cli.Run(context.Background(), []string{"get", "hello"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := out.String(); got != "hello\n" {
		t.Fatalf("stdout = %q, want hello", got)
	}
	if !strings.Contains(errOut.String(), "served by node3") {
		t.Fatalf("stderr = %q, want the serving node", errOut.String())
	}
}

func TestCLIGetJSON(t *testing.T) {
	cli, out, _ := newTestCLI(t, &fakeService{getNode: "node3"})
	if err := cli.Run(context.Background(), []string{"get", "hello", "--json"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out.String())
	}
	if got["value"] != "hello" || got["node"] != "node3" {
		t.Fatalf("JSON = %v", got)
	}
}

func TestCLIDeleteUsesVersionGuard(t *testing.T) {
	svc := &fakeService{deleteNode: "node2"}
	cli, out, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"delete", "k", "--version", "7"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if svc.deleteReq.GetKey() != "k" || svc.deleteReq.GetExpectedVersion() != 7 {
		t.Fatalf("delete request = %+v", svc.deleteReq)
	}
	if !strings.Contains(out.String(), "delete on node2") {
		t.Fatalf("stdout = %q", out.String())
	}
}

func TestCLIDeleteAlreadyAbsent(t *testing.T) {
	svc := &fakeService{
		deleteNode: "node2",
		deleteResp: &pb.DeleteResponse{Success: true, Found: false, Message: "absent"},
	}
	cli, out, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"delete", "k"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(out.String(), "already absent") {
		t.Fatalf("stdout = %q", out.String())
	}
}

func TestCLIModeReadsDefault(t *testing.T) {
	svc := &fakeService{configNode: "node2"}
	cli, out, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"mode"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if svc.getConfigCalls != 1 || svc.setConfigCalls != 0 {
		t.Fatalf("GetConfig calls = %d, SetConfig calls = %d", svc.getConfigCalls, svc.setConfigCalls)
	}
	if !strings.Contains(out.String(), "default mode: rs32") {
		t.Fatalf("stdout = %q", out.String())
	}
}

func TestCLIModeSetsDefault(t *testing.T) {
	svc := &fakeService{configNode: "node2"}
	cli, out, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"mode", "replication"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if svc.setConfigCalls != 1 || svc.setConfigMode != "replication" {
		t.Fatalf("SetConfig calls = %d mode = %q", svc.setConfigCalls, svc.setConfigMode)
	}
	if !strings.Contains(out.String(), "default mode set to replication") {
		t.Fatalf("stdout = %q", out.String())
	}
}

func TestCLIModeRejectsInvalidAndExtraArguments(t *testing.T) {
	cli, _, _ := newTestCLI(t, &fakeService{})
	if err := cli.Run(context.Background(), []string{"mode", "triple"}); err == nil {
		t.Fatal("mode accepted an invalid mode")
	}
	if err := cli.Run(context.Background(), []string{"mode", "rs32", "extra"}); err == nil {
		t.Fatal("mode accepted two arguments")
	}
}

func TestCLIHelp(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"--help"}, {"put", "--help"}, {"get", "-h"}} {
		cli, out, _ := newTestCLI(t, &fakeService{})
		if err := cli.Run(context.Background(), args); err != nil {
			t.Fatalf("Run(%v): %v", args, err)
		}
		if !strings.Contains(out.String(), "Usage:") {
			t.Fatalf("Run(%v) output = %q", args, out.String())
		}
	}
}

func TestCLIRejectsUnknownCommandAndFlag(t *testing.T) {
	cli, _, _ := newTestCLI(t, &fakeService{})
	if err := cli.Run(context.Background(), []string{"explode"}); err == nil {
		t.Fatal("unknown command succeeded")
	}
	if err := cli.Run(context.Background(), []string{"get", "k", "--bogus"}); err == nil {
		t.Fatal("unknown flag succeeded")
	}
}

func TestCLIReturnsServiceErrors(t *testing.T) {
	svc := &fakeService{getErr: errors.New("all attempts failed")}
	cli, _, _ := newTestCLI(t, svc)
	err := cli.Run(context.Background(), []string{"get", "k"})
	if err == nil || !strings.Contains(err.Error(), "all attempts failed") {
		t.Fatalf("error = %v", err)
	}
}

func TestCLIRequiresCommand(t *testing.T) {
	cli, out, _ := newTestCLI(t, &fakeService{})
	if err := cli.Run(context.Background(), nil); err == nil {
		t.Fatal("no command succeeded")
	}
	if !strings.Contains(out.String(), "Usage:") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestCLIStatusRendersFiveNodes(t *testing.T) {
	svc := &fakeService{healthNode: "node2"}
	cli, out, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"status"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	text := out.String()
	for _, want := range []string{"leader: node1", "mode: rs32", "5/5 nodes healthy", "NODE", "node5", "served by: node2"} {
		if !strings.Contains(text, want) {
			t.Fatalf("status output missing %q:\n%s", want, text)
		}
	}
}

func TestCLIStatusJSON(t *testing.T) {
	cli, out, _ := newTestCLI(t, &fakeService{healthNode: "node2"})
	if err := cli.Run(context.Background(), []string{"status", "--json"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got struct {
		Node  string `json:"node"`
		Nodes []struct {
			NodeID  string `json:"node_id"`
			Healthy bool   `json:"healthy"`
		} `json:"nodes"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out.String())
	}
	if got.Node != "node2" || len(got.Nodes) != 5 {
		t.Fatalf("JSON = %+v", got)
	}
}

func TestCLIHealthUnhealthyExitsNonZero(t *testing.T) {
	resp := healthyClusterResponse()
	resp.Degraded = true
	resp.HealthyNodes = 4
	svc := &fakeService{healthNode: "node2", healthResp: resp}
	cli, out, _ := newTestCLI(t, svc)

	err := cli.Run(context.Background(), []string{"health"})
	if err == nil || !strings.Contains(err.Error(), "not healthy") {
		t.Fatalf("error = %v, want an unhealthy error", err)
	}
	if !strings.Contains(out.String(), "healthy=false") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestCLIHealthHealthy(t *testing.T) {
	cli, out, _ := newTestCLI(t, &fakeService{healthNode: "node2"})
	if err := cli.Run(context.Background(), []string{"health"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(out.String(), "healthy=true") || strings.Count(strings.TrimSpace(out.String()), "\n") != 0 {
		t.Fatalf("health output is not a single healthy line: %q", out.String())
	}
}

func TestCLIStatsRendersFields(t *testing.T) {
	svc := &fakeService{statsNode: "node1"}
	cli, out, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"stats", "--json"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out.String())
	}
	if got["keys"] != float64(4) || got["fragments"] != float64(20) || got["leader"] != true {
		t.Fatalf("JSON = %v", got)
	}
}

func TestCLIRepairForwardsFlags(t *testing.T) {
	svc := &fakeService{repairNode: "node2"}
	cli, out, _ := newTestCLI(t, svc)
	if err := cli.Run(context.Background(), []string{"repair", "k", "--stripe", "stripe-1", "--force"}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if svc.repairReq.GetKey() != "k" || svc.repairReq.GetStripeId() != "stripe-1" || !svc.repairReq.GetForce() {
		t.Fatalf("repair request = %+v", svc.repairReq)
	}
	if !strings.Contains(out.String(), "repair accepted on node2") {
		t.Fatalf("stdout = %q", out.String())
	}
}

func TestCLIRepairRejectedResponseIsError(t *testing.T) {
	svc := &fakeService{
		repairNode: "node2",
		repairResp: &pb.RepairResponse{Success: false, Message: "key not found"},
	}
	cli, _, _ := newTestCLI(t, svc)
	err := cli.Run(context.Background(), []string{"repair", "k"})
	if err == nil || !strings.Contains(err.Error(), "key not found") {
		t.Fatalf("error = %v", err)
	}
}
