#!/usr/bin/env bash
# Shared bash helpers for the deployment scripts. Source this file:
#   . "$(dirname "$0")/common.sh"
# It defines functions only; callers keep their own set -euo pipefail.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
COMPOSE_NETWORK="distributed-systems_cluster-net"
NODE_SERVICES=(node1 node2 node3 node4 node5)
AUTH_TOKEN_VALUE="${AUTH_TOKEN:-}"
GATEWAY_NODES_VALUE="node1=node1:50051,node2=node2:50052,node3=node3:50053,node4=node4:50054,node5=node5:50055"

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

warn() {
    printf '[%s] WARNING: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
}

section() {
    printf '\n=== %s ===\n' "$*"
}

die() {
    printf '[%s] ERROR: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
    exit 1
}

assert_docker_compose() {
    command -v docker >/dev/null 2>&1 || die 'docker was not found on PATH. Install Docker with the Compose plugin.'
    docker compose version >/dev/null 2>&1 || die 'docker compose (v2) is unavailable. Install the Docker Compose plugin and retry.'
}

compose() {
    log "docker compose $*"
    docker compose "$@"
}

compose_allow_failure() {
    log "docker compose $*"
    docker compose "$@" || true
}

show_help() {
    printf '%s\n' "$1"
}

auth_status_text() {
    if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
        printf 'auth: enabled'
    else
        printf 'auth: disabled'
    fi
}

host_path() {
    if command -v cygpath >/dev/null 2>&1; then
        cygpath -w "$1"
    else
        printf '%s' "$1"
    fi
}

# -e AUTH_TOKEN copies the value from the caller's environment without putting
# the token on the command line, in logs, or in error messages.
gateway() {
    local -a args=(run --rm -T)
    if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
        args+=(-e AUTH_TOKEN)
    fi
    args+=(gateway "$@")
    compose "${args[@]}"
}

gateway_capture() {
    local -a args=(run --rm -T)
    if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
        args+=(-e AUTH_TOKEN)
    fi
    args+=(gateway "$@")
    log "docker compose ${args[*]}"
    docker compose "${args[@]}" 2>/dev/null
}

gateway_volume() {
    local volume="$1"
    shift
    local -a args=(run --rm -T -v "${volume}")
    if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
        args+=(-e AUTH_TOKEN)
    fi
    args+=(gateway "$@")
    compose "${args[@]}"
}

gateway_capture_volume() {
    local volume="$1"
    shift
    local -a args=(run --rm -T -v "${volume}")
    if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
        args+=(-e AUTH_TOKEN)
    fi
    args+=(gateway "$@")
    log "docker compose ${args[*]}"
    docker compose "${args[@]}" 2>/dev/null
}

# json_get extracts a top-level scalar from the gateway's pretty-printed JSON.
# jq is used when present; the sed fallback keeps the demo usable without it.
json_get() {
    local json="$1"
    local key="$2"
    if command -v jq >/dev/null 2>&1; then
        jq -r --arg key "$key" '.[$key]' <<<"$json"
        return
    fi
    sed -n "s/.*\"${key}\": *\"\{0,1\}\([^\",]*\)\"\{0,1\}.*/\1/p" <<<"$json" | head -n 1
}

compose_rows() {
    docker compose ps --all --format json 2>/dev/null || true
}

healthy_node_count() {
    local rows
    rows="$(compose_rows)"
    if [[ -z "${rows//[[:space:]]/}" ]]; then
        printf '0\n'
        return
    fi

    if command -v jq >/dev/null 2>&1; then
        if jq -e 'type == "array"' >/dev/null 2>&1 <<<"$rows"; then
            jq '[.[] | select(.Service | test("^node[1-5]$")) | select(.Health == "healthy")] | length' <<<"$rows"
        else
            jq -s '[.[] | select(.Service | test("^node[1-5]$")) | select(.Health == "healthy")] | length' <<<"$rows"
        fi
        return
    fi

    # Fallback for hosts without jq: ask Docker inspect for the health status of
    # the node containers Compose knows about.
    local ids id count=0
    ids="$(docker compose ps -q "${NODE_SERVICES[@]}" 2>/dev/null || true)"
    for id in $ids; do
        if [[ "$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$id" 2>/dev/null || true)" == "healthy" ]]; then
            count=$((count + 1))
        fi
    done
    printf '%s\n' "$count"
}

show_cluster_diagnostics() {
    warn 'Cluster did not become healthy in time. Current status:'
    docker compose ps || true
    warn 'Recent node logs:'
    docker compose logs --tail 40 node1 node2 node3 node4 node5 || true
}

wait_node_health() {
    local timeout="${1:-120}"
    local deadline=$(( $(date +%s) + timeout ))
    local last=-1 healthy

    while :; do
        healthy="$(healthy_node_count)"
        if [[ "$healthy" != "$last" ]]; then
            log "healthy nodes: ${healthy}/5"
            last="$healthy"
        fi
        if [[ "$healthy" -ge 5 ]]; then
            log 'all five nodes report healthy'
            return 0
        fi
        if [[ "$(date +%s)" -ge "$deadline" ]]; then
            show_cluster_diagnostics
            die "timed out after ${timeout}s waiting for five healthy nodes"
        fi
        sleep 2
    done
}

wait_cluster_healthy() {
    local timeout="${1:-120}"
    local deadline=$(( $(date +%s) + timeout ))
    local status health_line last=''

    while :; do
        if status="$(gateway_capture status 2>/dev/null)"; then
            health_line="$(grep -m1 '^health:' <<<"$status" || true)"
            if [[ -n "$health_line" && "$health_line" != "$last" ]]; then
                log "$health_line"
                last="$health_line"
            fi
            if grep -Eq '^health: 5/5 nodes healthy, degraded=false' <<<"$status"; then
                return 0
            fi
        else
            log 'status probe retrying'
        fi
        if [[ "$(date +%s)" -ge "$deadline" ]]; then
            show_cluster_diagnostics
            die "timed out after ${timeout}s waiting for a healthy, non-degraded cluster"
        fi
        sleep 3
    done
}

format_bytes() {
    awk -v bytes="$1" 'BEGIN {
        if (bytes < 1024) printf "%.0f B", bytes;
        else if (bytes < 1048576) printf "%.2f KiB", bytes/1024;
        else printf "%.2f MiB", bytes/1048576;
    }'
}
