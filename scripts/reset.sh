#!/usr/bin/env bash
# Destructively reset the cluster, then optionally rebuild the image.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"

build=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --build) build=1; shift ;;
        --help|-h)
            show_help 'Usage: scripts/reset.sh [--build] [--help]

PERMANENTLY deletes the cluster containers, the Compose network, and the five
named data volumes (node1-data ... node5-data). Use scripts/down.sh when you
only want to stop the cluster and keep its data.

Options:
  --build   Rebuild the deployment image after reset (does not start the cluster)
  --help    Show this help

Examples:
  ./scripts/reset.sh
  ./scripts/reset.sh --build

Exit codes:
  0  the reset completed
  1  Docker/Compose failed'
            exit 0
            ;;
        *) die "unknown option: $1 (try --help)" ;;
    esac
done

assert_docker_compose
warn 'RESET IS DESTRUCTIVE: removing containers, the Compose network, and node1-data ... node5-data.'
compose down --volumes --remove-orphans
if [[ "$build" -eq 1 ]]; then
    compose build
fi
log 'cluster reset complete; run ./scripts/up.sh to start again'
