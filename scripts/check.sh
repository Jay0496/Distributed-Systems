#!/usr/bin/env bash
# Run the free local equivalent of the former GitHub Actions CI job.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"

skip_chaos=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-chaos) skip_chaos=1; shift ;;
        --help|-h)
            show_help 'Usage: scripts/check.sh [--skip-chaos] [--help]

Runs the local Docker check suite. No host Go installation is required.

Checks:
  build                 go build ./...
  vet                   go vet ./...
  vet integration       go vet -tags=integration ./...
  vet chaos             go vet -tags=chaos ./...
  fast tests            go test -race -count=1 ./...
  chaos tests           go test -race -tags=chaos -count=1 ./tests/chaos/...
  format check          gofmt over a CRLF-normalized copy of the working tree

Options:
  --skip-chaos          Skip the chaos suite for a faster check
  --help                Show this help

Examples:
  ./scripts/check.sh
  ./scripts/check.sh --skip-chaos

Exit codes:
  0  every check passed
  1  Docker failed or a check failed'
            exit 0
            ;;
        *) die "unknown option: $1 (try --help)" ;;
    esac
done

assert_docker_compose

run_check() {
    local label="$1"
    shift
    section "$label"
    log "docker run --rm -v ${REPO_ROOT}:/app -w /app golang:1.22 $*"
    docker run --rm -v "${REPO_ROOT}:/app" -w /app golang:1.22 "$@"
}

run_check 'build' go build ./...
run_check 'vet' go vet ./...
run_check 'vet integration-tagged files' go vet -tags=integration ./...
run_check 'vet chaos-tagged files' go vet -tags=chaos ./...
run_check 'fast tests' go test -race -count=1 ./...
if [[ "$skip_chaos" -eq 0 ]]; then
    run_check 'chaos tests' go test -race -tags=chaos -count=1 ./tests/chaos/...
fi
run_check 'format check' bash /app/scripts/format-check.sh

log 'all local checks passed'
