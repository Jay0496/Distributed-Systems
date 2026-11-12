#!/usr/bin/env bash
# Stop the cluster while keeping data volumes by default.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"

volumes=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --volumes) volumes=1; shift ;;
        --help|-h)
            show_help 'Usage: scripts/down.sh [--volumes] [--help]

Stops and removes the cluster containers. Named data volumes are preserved
unless --volumes is supplied.

Options:
  --volumes   Remove node1-data ... node5-data (destructive)
  --help      Show this help

Examples:
  ./scripts/down.sh
  ./scripts/down.sh --volumes

Exit codes:
  0  the cluster stopped
  1  Docker/Compose failed'
            exit 0
            ;;
        *) die "unknown option: $1 (try --help)" ;;
    esac
done

assert_docker_compose
if [[ "$volumes" -eq 1 ]]; then
    warn 'Removing containers AND the five named data volumes. All cluster data will be deleted.'
    compose down --volumes --remove-orphans
else
    log 'Volumes are preserved; use --volumes to delete them.'
    compose down --remove-orphans
fi
log 'cluster stopped'
