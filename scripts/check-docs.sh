#!/usr/bin/env bash
# Check the documentation set for broken relative links, stale strings, and
# runnable command blocks. This is a documentation guard, not a test runner.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

show_help() {
    cat <<'EOF'
Usage: scripts/check-docs.sh [--help]

Checks every Markdown file outside .git and .opencode:
  - relative links resolve to a real file or directory
  - no stale strings remain in the documentation
  - <!-- runnable --> powershell/bash blocks are listed for manual execution

Options:
  --help    Show this help

Examples:
  ./scripts/check-docs.sh

Exit codes:
  0  every check passed
  1  a broken link, stale string, or malformed runnable block was found
EOF
}

if [[ $# -gt 0 ]]; then
    case "$1" in
        --help|-h) show_help; exit 0 ;;
        *) echo "unknown option: $1 (try --help)" >&2; exit 1 ;;
    esac
fi

# Assemble the unfinished-work markers so this guard does not itself trip the
# release grep for the marker text.
stale_strings=(
    'go run main.go'
    'NewRouter(100)'
    'not yet ''implemented'
    'legacy''_kv'
    'TO''DO'
)

failures=0
link_count=0
runnable_count=0
md_count=0

fail() {
    failures=$((failures + 1))
    printf 'FAIL: %s\n' "$1"
}

# Markdown files, NUL-delimited. The loop below runs in the current shell via
# process substitution, so counters survive.
while IFS= read -r -d '' file; do
    md_count=$((md_count + 1))
    rel="${file#"$REPO_ROOT"/}"
    dir="$(dirname -- "$file")"

    # ---- links ---------------------------------------------------------
    while IFS= read -r match; do
        [[ -n "$match" ]] || continue
        link_count=$((link_count + 1))

        target="${match#*](}"
        target="${target%)}"
        # Trim surrounding whitespace.
        target="${target#"${target%%[![:space:]]*}"}"
        target="${target%"${target##*[![:space:]]}"}"

        if [[ "$target" == '<'*'>' ]]; then
            target="${target:1:${#target}-2}"
        else
            # Drop a Markdown title: [text](path "title").
            target="${target%%[[:space:]]*}"
        fi

        [[ -n "$target" ]] || continue
        [[ "$target" == \#* ]] && continue
        [[ "$target" =~ ^[a-zA-Z][a-zA-Z0-9+.-]*: ]] && continue

        path="${target%%#*}"
        [[ -n "$path" ]] || continue
        path="${path//%20/ }"

        if [[ "$path" == /* || "$path" =~ ^[A-Za-z]: ]]; then
            resolved="$path"
        else
            resolved="$dir/$path"
        fi
        if [[ ! -e "$resolved" ]]; then
            fail "$rel: broken link -> $target"
        fi
    done < <(grep -oE '!?\[[^]]*\]\([^)]+\)' "$file" || true)

    # ---- stale strings --------------------------------------------------
    for stale in "${stale_strings[@]}"; do
        matches="$(grep -nF -- "$stale" "$file" || true)"
        [[ -n "$matches" ]] || continue
        while IFS= read -r result; do
            fail "$rel:${result%%:*}: stale string -> $stale"
        done <<< "$matches"
    done

    # ---- runnable blocks ------------------------------------------------
    block="$(awk -v file="$rel" '
        function problem(message) {
            print "FAIL|" file ":" FNR ": " message
        }
        {
            if ($0 ~ /^[[:space:]]*<!-- runnable -->[[:space:]]*$/) {
                pending = 1
                tagline = FNR
                next
            }
            if (!pending) {
                next
            }
            if ($0 ~ /^[[:space:]]*$/) {
                next
            }
            if ($0 !~ /^[[:space:]]*```/) {
                problem("runnable tag at line " tagline " is not followed by a code fence")
                pending = 0
                next
            }
            lang = $0
            sub(/^[[:space:]]*```/, "", lang)
            sub(/[[:space:]].*$/, "", lang)
            if (lang != "powershell" && lang != "bash" && lang != "sh") {
                problem("runnable block uses unsupported language " lang)
                pending = 0
                next
            }
            print "RUNNABLE|" file ":" (FNR + 1) " (" lang ")"
            pending = 0
            closed = 0
            while ((getline line) > 0) {
                if (line ~ /^[[:space:]]*```[[:space:]]*$/) {
                    closed = 1
                    break
                }
                print "    " line
            }
            if (!closed) {
                problem("runnable block has no closing fence")
            }
        }
    ' "$file")"

    [[ -n "$block" ]] || continue
    while IFS= read -r out; do
        case "$out" in
            FAIL\|*)
                fail "${out#FAIL|}"
                ;;
            RUNNABLE\|*)
                runnable_count=$((runnable_count + 1))
                printf '\nrunnable: %s\n' "${out#RUNNABLE|}"
                ;;
            *)
                printf '%s\n' "$out"
                ;;
        esac
    done <<< "$block"
done < <(find "$REPO_ROOT" -type f -name '*.md' \
    -not -path '*/.git/*' -not -path '*/.opencode/*' -print0 | sort -z)

printf '\nchecked %d Markdown files, %d relative links, %d runnable blocks\n' \
    "$md_count" "$link_count" "$runnable_count"

if [[ "$failures" -gt 0 ]]; then
    printf 'documentation check failed with %d problem(s)\n' "$failures" >&2
    exit 1
fi

printf 'documentation check passed\n'
