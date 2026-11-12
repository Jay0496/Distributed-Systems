#!/usr/bin/env bash
# Run the end-to-end demo: RS write/read, replication comparison, two-node
# failure, automatic repair, and delete.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
. "$SCRIPT_DIR/common.sh"

mode=rs32
skip_failure=0
keep=0
value_size=4096
repair_timeout=120
timeout=120
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode) [[ $# -ge 2 ]] || die '--mode requires rs32 or replication'; mode="$2"; shift 2 ;;
        --skip-failure) skip_failure=1; shift ;;
        --keep) keep=1; shift ;;
        --value-size) [[ $# -ge 2 ]] || die '--value-size requires a value'; value_size="$2"; shift 2 ;;
        --repair-timeout) [[ $# -ge 2 ]] || die '--repair-timeout requires a value'; repair_timeout="$2"; shift 2 ;;
        --timeout) [[ $# -ge 2 ]] || die '--timeout requires a value'; timeout="$2"; shift 2 ;;
        --help|-h)
            show_help 'Usage: scripts/demo.sh [--mode rs32|replication] [--skip-failure] [--keep] [--value-size BYTES] [--repair-timeout SECONDS] [--timeout SECONDS] [--help]

Runs a complete demonstration without host Go:
  1. start the cluster if needed and wait for health
  2. write and read a deterministic value in RS mode
  3. show per-node fragment counts and bytes with sqlite3
  4. write the same key in replication mode and compare storage amplification
  5. stop node4 and node5, prove reads/writes still work, show degraded status
  6. restart the nodes and wait for repair instead of grepping logs
  7. delete the key and confirm it is gone
  8. print a summary table and the auth state

Options:
  --mode rs32|replication   Mode for the storage comparison (default rs32)
  --skip-failure            Skip the two-node failure and repair section
  --keep                    Leave a cluster started by this script running
  --value-size BYTES        Demo value size (default 4096)
  --repair-timeout SECONDS  Repair wait after restart (default 120)
  --timeout SECONDS         Initial health wait (default 120)
  --help                    Show this help

Examples:
  ./scripts/demo.sh
  ./scripts/demo.sh --mode replication --skip-failure

Exit codes:
  0  every demo check passed
  1  the cluster was unhealthy or a demo step failed'
            exit 0
            ;;
        *) die "unknown option: $1 (try --help)" ;;
    esac
done

[[ "$mode" == "rs32" || "$mode" == "replication" ]] || die "--mode must be rs32 or replication"
[[ "$value_size" -gt 0 ]] || die "--value-size must be greater than zero"

assert_docker_compose

# With two nodes stopped, the leader may wait for the dead owners' read/upload
# timeouts before it can answer a forwarded call. Five retries cover every node
# in the ring so the demo reliably reaches the current leader; this is a demo
# setting, not a change to the gateway's production default.
export GATEWAY_RETRIES=5
export GATEWAY_TIMEOUT=45s

key='demo-key'
failure_key='demo-key-failure'
degraded_key='demo-key-degraded'
checks=()
failed=0
started_cluster=0
nodes_stopped=0
temp_value_file=''
temp_value_file="$(mktemp "${TMPDIR:-/tmp}/kv-demo-XXXXXX")"
pattern='distributed-systems-demo-'
expected_value="$(yes "$pattern" | tr -d '\n' | head -c "$value_size" || true)"
printf '%s' "$expected_value" > "$temp_value_file"
value_volume="$(host_path "$temp_value_file"):/demo/value:ro"

add_check() {
    local step="$1"
    local detail="$2"
    checks+=("PASS|${step}|${detail}")
    log "PASS: ${step} - ${detail}"
}

cleanup() {
    if [[ "$nodes_stopped" -eq 1 ]]; then
        log 'restarting node4 and node5 before exit'
        docker compose start node4 node5 || true
        nodes_stopped=0
    fi
    if [[ -n "$temp_value_file" && -f "$temp_value_file" ]]; then
        rm -f "$temp_value_file"
    fi
    if [[ "$started_cluster" -eq 1 && "$keep" -eq 0 ]]; then
        log 'stopping the cluster started by this script (use --keep to leave it running)'
        docker compose down --remove-orphans || true
    elif [[ "$started_cluster" -eq 1 && "$keep" -eq 1 ]]; then
        log 'leaving the cluster running (--keep)'
    fi
}
trap cleanup EXIT

fragment_stats() {
    local stripe="$1"
    local node raw count bytes
    for node in "${NODE_SERVICES[@]}"; do
        raw="$(docker compose exec -T "$node" sqlite3 /data/node.db \
            "SELECT COUNT(*), COALESCE(SUM(length(data)),0) FROM fragments WHERE stripe_id='${stripe}';")"
        IFS='|' read -r count bytes <<<"$raw"
        printf '%s|%s|%s\n' "$node" "$count" "$bytes"
    done
}

show_fragment_table() {
    local stats="$1"
    local node count bytes
    printf '%-8s %10s %12s\n' NODE FRAGMENTS BYTES
    while IFS='|' read -r node count bytes; do
        [[ -n "$node" ]] || continue
        printf '%-8s %10s %12s\n' "$node" "$count" "$(format_bytes "$bytes")"
    done <<<"$stats"
}

if [[ "$(healthy_node_count)" -lt 5 ]]; then
    log 'cluster is not fully healthy; starting it'
    compose up --build -d
    started_cluster=1
fi
wait_node_health "$timeout"
wait_cluster_healthy "$timeout"
add_check 'cluster health' 'five nodes healthy and not degraded'
log "$(auth_status_text)"

# ---- storage comparison -----------------------------------------------------
gateway mode rs32 >/dev/null
put_json="$(gateway_capture_volume "$value_volume" put "$key" --value-file /demo/value --mode rs32 --json)"
put_success="$(json_get "$put_json" success)"
put_mode="$(json_get "$put_json" mode)"
put_stripe="$(json_get "$put_json" stripe_id)"
put_version="$(json_get "$put_json" version)"
put_shards="$(json_get "$put_json" shards_stored)"
[[ "$put_success" == "true" ]] || die "RS put was rejected: $(json_get "$put_json" message)"
[[ "$put_mode" == "rs32" ]] || die "expected rs32, got $put_mode"
add_check 'rs32 write' "version=${put_version} shards=${put_shards}/5 stripe=${put_stripe}"

get_json="$(gateway_capture get "$key" --json)"
get_value="$(json_get "$get_json" value)"
get_shards="$(json_get "$get_json" shards_used)"
[[ "$get_value" == "$expected_value" ]] || die 'RS read-back value did not match the write'
add_check 'rs32 read' "verified ${value_size} bytes with ${get_shards} shards"

rs_stats="$(fragment_stats "$put_stripe")"
printf '\nPer-node fragments for the rs32 stripe:\n'
show_fragment_table "$rs_stats"
rs_total=0
rs_fragments=0
while IFS='|' read -r node count bytes; do
    [[ -n "$node" ]] || continue
    rs_fragments=$((rs_fragments + count))
    rs_total=$((rs_total + bytes))
done <<<"$rs_stats"
[[ "$rs_fragments" -eq 5 ]] || die "expected five RS shards, found $rs_fragments"
add_check 'rs32 placement' "${rs_fragments} shards, $(format_bytes "$rs_total") total"

gateway mode replication >/dev/null
rep_json="$(gateway_capture_volume "$value_volume" put "$key" --value-file /demo/value --mode replication --json)"
rep_success="$(json_get "$rep_json" success)"
rep_mode="$(json_get "$rep_json" mode)"
rep_stripe="$(json_get "$rep_json" stripe_id)"
rep_version="$(json_get "$rep_json" version)"
rep_shards="$(json_get "$rep_json" shards_stored)"
[[ "$rep_success" == "true" ]] || die "replication put was rejected: $(json_get "$rep_json" message)"
[[ "$rep_mode" == "replication" ]] || die "expected replication, got $rep_mode"
add_check 'replication write' "version=${rep_version} copies=${rep_shards}/5 stripe=${rep_stripe}"

rep_stats="$(fragment_stats "$rep_stripe")"
printf 'Per-node fragments for the replication stripe:\n'
show_fragment_table "$rep_stats"
rep_total=0
rep_fragments=0
while IFS='|' read -r node count bytes; do
    [[ -n "$node" ]] || continue
    rep_fragments=$((rep_fragments + count))
    rep_total=$((rep_total + bytes))
done <<<"$rep_stats"
[[ "$rep_fragments" -eq 5 ]] || die "expected five replication copies, found $rep_fragments"
add_check 'replication placement' "${rep_fragments} copies, $(format_bytes "$rep_total") total"

rs_multiplier="$(awk -v total="$rs_total" -v size="$value_size" 'BEGIN { printf "%.2f", total/size }')"
rep_multiplier="$(awk -v total="$rep_total" -v size="$value_size" 'BEGIN { printf "%.2f", total/size }')"
rs_overhead="$(awk -v m="$rs_multiplier" 'BEGIN { printf "%.0f", (m-1)*100 }')"
rep_overhead="$(awk -v m="$rep_multiplier" 'BEGIN { printf "%.0f", (m-1)*100 }')"
ratio="$(awk -v rep="$rep_total" -v rs="$rs_total" 'BEGIN { printf "%.2f", rep/rs }')"
printf '\n'
printf 'rs32:        %s stored for %s original (%sx, about %s%% extra)\n' \
    "$(format_bytes "$rs_total")" "$(format_bytes "$value_size")" "$rs_multiplier" "$rs_overhead"
printf 'replication: %s stored for %s original (%sx, about %s%% extra)\n' \
    "$(format_bytes "$rep_total")" "$(format_bytes "$value_size")" "$rep_multiplier" "$rep_overhead"
printf 'replication / rs32 total ratio: %sx\n' "$ratio"
add_check 'storage comparison' 'rs32 stores about 1.67x, replication stores 5x'

# ---- failure and repair -----------------------------------------------------
if [[ "$skip_failure" -eq 0 ]]; then
    gateway mode rs32 >/dev/null
    failure_json="$(gateway_capture_volume "$value_volume" put "$failure_key" --value-file /demo/value --mode rs32 --json)"
    failure_success="$(json_get "$failure_json" success)"
    failure_stripe="$(json_get "$failure_json" stripe_id)"
    [[ "$failure_success" == "true" ]] || die "failure-test put was rejected: $(json_get "$failure_json" message)"
    add_check 'failure baseline' "wrote ${failure_key} with five shards"

    compose stop node4 node5
    nodes_stopped=1
    log 'waiting for the detector to report degraded mode'

    deadline=$(( $(date +%s) + 45 ))
    degraded=0
    status=''
    while [[ "$(date +%s)" -lt "$deadline" ]]; do
        if status="$(gateway_capture status 2>/dev/null)"; then
            if grep -q 'degraded=true' <<<"$status"; then
                degraded=1
                break
            fi
        else
            log 'status probe waiting for a leader'
        fi
        sleep 2
    done
    [[ "$degraded" -eq 1 ]] || die 'cluster did not report degraded mode after stopping node4 and node5'
    add_check 'degraded status' "$(grep -m1 '^health:' <<<"$status")"

    down_get_json="$(gateway_capture get "$failure_key" --json)"
    down_get_value="$(json_get "$down_get_json" value)"
    down_get_shards="$(json_get "$down_get_json" shards_used)"
    [[ "$down_get_value" == "$expected_value" ]] || die 'read with two nodes down did not match'
    add_check 'read with two nodes down' "reconstructed from ${down_get_shards} shards"

    degraded_json="$(gateway_capture_volume "$value_volume" put "$degraded_key" --value-file /demo/value --mode rs32 --json)"
    degraded_success="$(json_get "$degraded_json" success)"
    degraded_shards="$(json_get "$degraded_json" shards_stored)"
    degraded_flag="$(json_get "$degraded_json" degraded)"
    [[ "$degraded_success" == "true" ]] || die "degraded write failed: $(json_get "$degraded_json" message)"
    add_check 'write with two nodes down' "committed ${degraded_shards}/5 shards, degraded=${degraded_flag}"

    compose start node4 node5
    nodes_stopped=0

    log 'waiting for automatic repair (not a log-line grep)'
    wait_cluster_healthy "$repair_timeout"
    add_check 'repair' 'degraded mode cleared automatically'

    restored_stats="$(fragment_stats "$failure_stripe")"
    printf 'Per-node fragments after repair:\n'
    show_fragment_table "$restored_stats"
    restored_fragments=0
    while IFS='|' read -r node count bytes; do
        [[ -n "$node" ]] || continue
        restored_fragments=$((restored_fragments + count))
    done <<<"$restored_stats"
    [[ "$restored_fragments" -eq 5 ]] || die "expected five restored shards, found $restored_fragments"
    add_check 'fragment refill' "${restored_fragments} shards present after restart"
else
    log 'skipping the failure and repair section (--skip-failure)'
fi

# ---- delete -----------------------------------------------------------------
gateway delete "$key" >/dev/null
if gateway_capture get "$key" >/dev/null 2>&1; then
    die 'get still returned a value after delete'
fi
add_check 'delete' 'key is gone after delete'

if [[ "$skip_failure" -eq 0 ]]; then
    gateway delete "$failure_key" >/dev/null
    gateway delete "$degraded_key" >/dev/null
fi

section 'Demo summary'
printf '%-24s %-6s %s\n' STEP RESULT DETAIL
for check in "${checks[@]}"; do
    IFS='|' read -r result step detail <<<"$check"
    printf '%-24s %-6s %s\n' "$step" "$result" "$detail"
done
printf '%s\n' "$(auth_status_text)"
printf 'value size: %s; storage mode tested: %s; failure section: %s\n' \
    "$(format_bytes "$value_size")" "$mode" "$([[ "$skip_failure" -eq 1 ]] && echo skipped || echo included)"
