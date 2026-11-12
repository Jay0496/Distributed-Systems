#!/usr/bin/env bash
# Run unit, integration, chaos, or benchmark suites in Docker only.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"

unit=0
integration=0
chaos=0
bench=0
keep=0
timeout=120
while [[ $# -gt 0 ]]; do
    case "$1" in
        --unit) unit=1; shift ;;
        --integration) integration=1; shift ;;
        --chaos) chaos=1; shift ;;
        --bench) bench=1; shift ;;
        --keep) keep=1; shift ;;
        --timeout) [[ $# -ge 2 ]] || die '--timeout requires a value'; timeout="$2"; shift 2 ;;
        --help|-h)
            show_help 'Usage: scripts/test.sh [--unit] [--integration] [--chaos] [--bench] [--keep] [--timeout SECONDS] [--help]

Runs Go tests inside golang:1.22 containers. No host Go installation is needed.

Suites (combine flags to run more than one; --unit is the default):
  --unit          go test -race ./... (unit and component tests)
  --integration   starts the cluster, waits for health, then runs the
                  integration-tagged suite on distributed-systems_cluster-net
  --chaos         runs the chaos-tagged failure suite
  --bench         runs the benchmark-tagged suite

Options:
  --keep              Leave a cluster started by this script running
  --timeout SECONDS   Health timeout for integration (default 120)
  --help              Show this help

Examples:
  ./scripts/test.sh
  ./scripts/test.sh --integration --keep
  ./scripts/test.sh --unit --chaos

Exit codes:
  0  every selected suite passed
  1  Docker failed or a test failed'
            exit 0
            ;;
        *) die "unknown option: $1 (try --help)" ;;
    esac
done

if [[ "$unit" -eq 0 && "$integration" -eq 0 && "$chaos" -eq 0 && "$bench" -eq 0 ]]; then
    unit=1
fi

run_go_test() {
    local label="$1"
    local network="$2"
    shift 2
    local -a args=(run --rm)
    if [[ "$network" -eq 1 ]]; then
        args+=(--network "$COMPOSE_NETWORK")
    fi
    if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
        args+=(-e AUTH_TOKEN)
    fi
    args+=(-v "$REPO_ROOT:/app" -w /app golang:1.22 go test "$@")
    section "$label"
    log "docker ${args[*]}"
    docker "${args[@]}"
}

started_cluster=0
cleanup() {
    if [[ "$started_cluster" -eq 1 && "$keep" -eq 0 ]]; then
        log 'stopping the cluster started by this script (use --keep to leave it running)'
        docker compose down --remove-orphans || true
    elif [[ "$started_cluster" -eq 1 && "$keep" -eq 1 ]]; then
        log 'leaving the cluster running (--keep)'
    fi
}
trap cleanup EXIT

assert_docker_compose

if [[ "$integration" -eq 1 ]]; then
    if [[ "$(healthy_node_count)" -lt 5 ]]; then
        log 'cluster is not fully healthy; starting it'
        compose up --build -d
        started_cluster=1
    fi
    wait_node_health "$timeout"
    wait_cluster_healthy "$timeout"
    run_go_test 'integration tests' 1 -tags=integration -count=1 -v ./tests/...
fi

if [[ "$unit" -eq 1 ]]; then
    run_go_test 'unit and component tests' 0 -race -count=1 ./...
fi

if [[ "$chaos" -eq 1 ]]; then
    run_go_test 'chaos tests' 0 -race -tags=chaos -count=1 -v ./tests/chaos/...
fi

if [[ "$bench" -eq 1 ]]; then
    run_go_test 'benchmark-tagged tests' 0 -race -tags=bench -count=1 ./tests/bench/...
fi

log 'all selected test suites passed'
