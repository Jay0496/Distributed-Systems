#!/usr/bin/env bash
# Check gofmt over a CRLF-normalized copy of the working tree.
#
# The repository is usually checked out with CRLF on Windows, which makes a raw
# `gofmt -l .` report every untouched file. Copy the tree inside the container,
# normalize line endings, and check that snapshot instead.
set -euo pipefail

SOURCE="${1:-/app}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

tar --exclude=.git -cf - -C "$SOURCE" . | tar -xf - -C "$tmp"
find "$tmp" -type f -name '*.go' -exec sed -i 's/\r$//' {} +

cd "$tmp"
bad="$(gofmt -l .)"
if [ -n "$bad" ]; then
    printf 'gofmt would rewrite these files:\n%s\n' "$bad"
    exit 1
fi

printf 'format check passed\n'
