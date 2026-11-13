#!/usr/bin/env bash
# Run the benchmark harness inside Docker with CPU sampling.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"

mode="both"
runs=3
duration="60s"
concurrency=32
keys=10000
value_size=4096
read_ratio="0.5"
warmup="10s"
values=1000
output="bench-results"
repair_timeout=300
skip_load=0
skip_storage=0
skip_recovery=0
keep=0

usage() {
    show_help 'Usage: scripts/bench.sh [--mode rs32|replication|both] [--runs 3]
                         [--duration 60s] [--concurrency 32] [--keys 10000]
                         [--value-size 4096] [--read-ratio 0.5] [--warmup 10s]
                         [--values 1000] [--output bench-results]
                         [--repair-timeout 300] [--skip-load] [--skip-storage]
                         [--skip-recovery] [--keep] [--help]

Runs entirely in Docker. Results are written under bench-results/<run-id>/ as
meta.json, run.json, latency.csv, throughput.csv, storage.csv, recovery.csv,
cpu.csv, and summary.md.

Storage measurement is destructive: it resets the five data volumes before each
mode so the measurement starts clean. Recovery seeds keys before stopping nodes.

Options:
  --mode rs32|replication|both  Modes to measure (default both)
  --runs N                      Repetitions per scenario (default 3)
  --duration 60s                Measured load duration
  --concurrency 32              Load workers
  --keys 10000                  Load key space
  --value-size 4096             Payload size in bytes
  --read-ratio 0.5              Read fraction in the load mix
  --warmup 10s                  Discarded warmup
  --values 1000                 Keys for storage and recovery seed
  --output bench-results        Host results directory
  --repair-timeout 300          Seconds to wait for degraded to clear
  --skip-load                   Skip load scenarios
  --skip-storage                Skip storage measurements
  --skip-recovery               Skip stop/restart scenarios
  --keep                        Accepted for parity; the cluster is left running
  --help                        Show this help

Examples:
  ./scripts/bench.sh --runs 1 --duration 15s
  ./scripts/bench.sh --mode replication --skip-storage --skip-recovery
  ./scripts/bench.sh --mode both --runs 3 --skip-recovery

Exit codes:
  0  every selected scenario completed
  1  Docker, the cluster, or a benchmark command failed'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode) mode="$2"; shift 2 ;;
        --runs) runs="$2"; shift 2 ;;
        --duration) duration="$2"; shift 2 ;;
        --concurrency) concurrency="$2"; shift 2 ;;
        --keys) keys="$2"; shift 2 ;;
        --value-size) value_size="$2"; shift 2 ;;
        --read-ratio) read_ratio="$2"; shift 2 ;;
        --warmup) warmup="$2"; shift 2 ;;
        --values) values="$2"; shift 2 ;;
        --output) output="$2"; shift 2 ;;
        --repair-timeout) repair_timeout="$2"; shift 2 ;;
        --skip-load) skip_load=1; shift ;;
        --skip-storage) skip_storage=1; shift ;;
        --skip-recovery) skip_recovery=1; shift ;;
        --keep) keep=1; shift ;;
        --help|-h) usage; exit 0 ;;
        *) die "unknown option: $1 (try --help)" ;;
    esac
done

assert_docker_compose

if [[ "$skip_load" -eq 1 && "$skip_storage" -eq 1 && "$skip_recovery" -eq 1 ]]; then
    die 'every scenario was skipped; omit at least one --skip switch'
fi

if [[ "$output" = /* ]]; then
    host_out="$output"
else
    host_out="$REPO_ROOT/$output"
fi
mkdir -p "$host_out"

case "$mode" in
    both) modes=(rs32 replication) ;;
    rs32|replication) modes=("$mode") ;;
    *) die "invalid --mode $mode (want rs32, replication, or both)" ;;
esac

run_stamp="$(date -u '+%Y%m%d-%H%M%S')"

ensure_cluster() {
    if [[ "$(healthy_node_count)" -lt 5 ]]; then
        log 'cluster is not fully healthy; starting it'
        compose up --build -d
    fi
    wait_node_health 180
    wait_cluster_healthy 180
}

reset_cluster() {
    warn 'resetting the cluster and removing all five data volumes for a clean storage measurement'
    compose_allow_failure down -v --remove-orphans
    compose up -d
    wait_node_health 180
    wait_cluster_healthy 180
}

declare -A cpu_sum=()
declare -A cpu_count=()

reset_cpu_samples() {
    cpu_sum=()
    cpu_count=()
}

add_cpu_snapshot() {
    local line name percent
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        name="$(printf '%s' "$line" | sed -n 's/.*"Name":"\([^"]*\)".*/\1/p')"
        [[ "$name" =~ ^node[1-5]$ ]] || continue
        percent="$(printf '%s' "$line" | sed -n 's/.*"CPUPerc":"\([0-9.]*\)%.*/\1/p')"
        [[ -z "$percent" ]] && continue
        cpu_sum["$name"]="$(awk -v sum="${cpu_sum[$name]:-0}" -v value="$percent" 'BEGIN { printf "%.6f", sum + value }')"
        cpu_count["$name"]=$(( ${cpu_count[$name]:-0} + 1 ))
    done < <(docker stats --no-stream --format '{{json .}}' 2>/dev/null || true)
}

write_cpu_csv() {
    local directory="$1" node path
    if [[ "${#cpu_sum[@]}" -eq 0 ]]; then
        return 0
    fi
    path="$directory/cpu.csv"
    printf 'node,avg_cpu_percent,samples\n' > "$path"
    for node in $(printf '%s\n' "${!cpu_sum[@]}" | sort); do
        awk -v node="$node" -v sum="${cpu_sum[$node]}" -v count="${cpu_count[$node]}" \
            'BEGIN { printf "%s,%.2f,%d\n", node, sum / count, count }' >> "$path"
    done
}

run_bench_with_cpu() {
    local run_id="$1" scenario="$2"
    shift 2
    local run_dir="$host_out/$run_id"
    mkdir -p "$run_dir"
    reset_cpu_samples
    add_cpu_snapshot

    local -a compose_args=(run --rm -T)
    if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
        compose_args+=(-e AUTH_TOKEN)
    fi
    compose_args+=(-v "${host_out}:/results" bench "$@")
    compose_args+=(--output /results --run-id "$run_id")

    log "bench scenario ${scenario} (run ${run_id})"
    docker compose "${compose_args[@]}" > "$run_dir/console.txt" 2>&1 &
    local pid=$!
    while kill -0 "$pid" 2>/dev/null; do
        sleep 2
        add_cpu_snapshot
    done
    local status=0
    wait "$pid" || status=$?
    add_cpu_snapshot
    write_cpu_csv "$run_dir"
    cat "$run_dir/console.txt"
    if [[ "$status" -ne 0 ]]; then
        die "bench scenario ${scenario} failed with exit code ${status}"
    fi
    update_bench_summary "$run_id"
}

run_bench_direct() {
    local -a args=(run --rm -T)
    if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
        args+=(-e AUTH_TOKEN)
    fi
    args+=(-v "${host_out}:/results" bench "$@")
    compose "${args[@]}"
}

update_bench_summary() {
    local run_id="$1"
    local -a args=(run --rm -T)
    if [[ -n "$AUTH_TOKEN_VALUE" ]]; then
        args+=(-e AUTH_TOKEN)
    fi
    args+=(-v "${host_out}:/results" bench report --input "/results/${run_id}")
    docker compose "${args[@]}" >/dev/null
}

bench_status() {
    gateway_capture status --json
}

wait_degraded_ts() {
    local timeout="$1" deadline=$(( $(date +%s) + timeout )) json degraded
    while :; do
        if json="$(bench_status 2>/dev/null)"; then
            degraded="$(json_get "$json" degraded)"
            if [[ "$degraded" == "true" ]]; then
                date -u '+%Y-%m-%dT%H:%M:%SZ'
                return 0
            fi
        fi
        [[ "$(date +%s)" -ge "$deadline" ]] && die "timed out after ${timeout}s waiting for degraded state"
        sleep 2
    done
}

wait_healthy_again() {
    local timeout="$1" deadline=$(( $(date +%s) + timeout )) json degraded healthy
    while :; do
        if json="$(bench_status 2>/dev/null)"; then
            degraded="$(json_get "$json" degraded)"
            healthy="$(json_get "$json" healthy_nodes)"
            if [[ "$degraded" == "false" && "${healthy:-0}" -ge 5 ]]; then
                return 0
            fi
        fi
        [[ "$(date +%s)" -ge "$deadline" ]] && die "timed out after ${timeout}s waiting for the cluster to recover"
        sleep 2
    done
}

# The tools profile is not started by `up`, so build its image explicitly; this
# keeps the bench binary in step with the checked-out source.
compose build bench
ensure_cluster

if [[ "$skip_load" -eq 0 ]]; then    for run in $(seq 1 "$runs"); do
        for bench_mode in "${modes[@]}"; do
            run_bench_with_cpu "${run_stamp}-load-${bench_mode}-run${run}" "load-${bench_mode}" \
                run --mode "$bench_mode" --concurrency "$concurrency" --duration "$duration" \
                --warmup "$warmup" --keys "$keys" --value-size "$value_size" --read-ratio "$read_ratio"
        done
    done
fi

if [[ "$skip_storage" -eq 0 ]]; then
    for run in $(seq 1 "$runs"); do
        for bench_mode in "${modes[@]}"; do
            reset_cluster
            run_bench_with_cpu "${run_stamp}-storage-${bench_mode}-run${run}" "storage-${bench_mode}" \
                storage --mode "$bench_mode" --values "$values" --value-size "$value_size"
        done
    done
fi

if [[ "$skip_recovery" -eq 0 ]]; then
    ensure_cluster
    for run in $(seq 1 "$runs"); do
        run_bench_direct recovery --seed-only --seed-keys "$values" \
            --value-size "$value_size" --key-prefix "recovery-${run_stamp}-run${run}"

        for scenario in one-node two-node; do
            if [[ "$scenario" == "one-node" ]]; then
                nodes=(node4)
            else
                nodes=(node4 node5)
            fi
            section "recovery scenario ${scenario}"
            stop_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
            compose stop "${nodes[@]}"
            degraded_at="$(wait_degraded_ts 60)"
            restart_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
            compose start "${nodes[@]}"
            run_bench_with_cpu "${run_stamp}-recovery-${scenario}-run${run}" "recovery-${scenario}" \
                recovery --measure-only --scenario "$scenario" \
                --start-time "$stop_at" --degraded-time "$degraded_at" \
                --restart-time "$restart_at" --timeout "${repair_timeout}s"
            wait_healthy_again "$repair_timeout"
        done
    done
fi

ensure_cluster
section 'benchmark summary'
log "results directory: ${host_out}"
find "$host_out" -mindepth 1 -maxdepth 1 -type d -printf '  %f\n' | sort
