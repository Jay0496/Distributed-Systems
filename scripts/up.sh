#!/usr/bin/env bash
# Build and start the five-node cluster, then wait until it is healthy.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"

timeout=120
while [[ $# -gt 0 ]]; do
    case "$1" in
        --timeout) [[ $# -ge 2 ]] || die '--timeout requires a value'; timeout="$2"; shift 2 ;;
        --help|-h)
            show_help 'Usage: scripts/up.sh [--timeout SECONDS] [--help]

Builds the deployment image, starts node1-node5 in the background, waits for all
five health checks, prints docker compose ps, and asks the gateway for cluster
status. No Go installation is required on the host.

Options:
  --timeout SECONDS   Health and status timeout (default 120)
  --help              Show this help

Examples:
  ./scripts/up.sh
  ./scripts/up.sh --timeout 180

Exit codes:
  0  all five nodes are healthy and the cluster is not degraded
  1  Docker/Compose is unavailable, startup failed, or the timeout expired'
            exit 0
            ;;
        *) die "unknown option: $1 (try --help)" ;;
    esac
done

assert_docker_compose
log "$(auth_status_text)"
compose up --build -d
wait_node_health "$timeout"
wait_cluster_healthy "$timeout"
compose ps
gateway status
log 'cluster is ready'
