package main

import "testing"

// TestRunHelpWithoutGatewayNodes covers the container acceptance check:
// `docker run --rm --entrypoint /app/gateway <image> --help` must work before a
// cluster configuration exists.
func TestRunHelpWithoutGatewayNodes(t *testing.T) {
	t.Setenv("GATEWAY_NODES", "")
	if err := run([]string{"--help"}); err != nil {
		t.Fatalf("run --help: %v", err)
	}
}
