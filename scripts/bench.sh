#!/usr/bin/env bash
# Run the benchmark tool inside the deployment image.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"

if [[ $# -gt 0 && ( "$1" == "--help" || "$1" == "-h" || "$1" == "help" ) ]]; then
    show_help 'Usage: scripts/bench.sh [bench arguments] [--help]

Runs the one-shot bench tool from the deployment image with the tools profile
environment (GATEWAY_NODES, AUTH_TOKEN, MAX_MESSAGE_SIZE). This is a placeholder
runner; the benchmark task adds the real flags and reporting.

Examples:
  ./scripts/bench.sh
  ./scripts/bench.sh --help
  ./scripts/bench.sh --scenario rs32

Exit codes:
  0  the bench tool exited successfully
  1  Docker/Compose failed or the bench tool failed'
    exit 0
fi

assert_docker_compose
args=(run --rm)
if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
    args+=(-e AUTH_TOKEN)
fi
args+=(bench "$@")
compose "${args[@]}"
