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
