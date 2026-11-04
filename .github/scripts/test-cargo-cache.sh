#!/usr/bin/env bash
# Exercise real BuildKit cache mounts without touching the default builder's caches.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work="$(mktemp -d)"
builder="cpr-cache-test-$$"
cleanup() { docker buildx rm "$builder" >/dev/null 2>&1 || true; rm -rf "$work"; }
trap cleanup EXIT
docker buildx create --name "$builder" --driver docker-container >/dev/null
mkdir -p "$work/source/registry/cache/test" "$work/source/git" "$work/source/target/amd64/release/deps"
printf 'crate-data' > "$work/source/registry/cache/test/example.crate"
printf 'compiled-data' > "$work/source/target/amd64/release/deps/example.rlib"
mkdir "$work/snapshot"
for item in registry git target; do tar -czf "$work/snapshot/$item.tar.gz" -C "$work/source/$item" .; done
export GITHUB_OUTPUT="$work/output"
bash "$root/.github/actions/cargo-cache/cargo-cache.sh" restore "$builder" "$work/snapshot" amd64
grep -q 'usable=true' "$GITHUB_OUTPUT"
: > "$GITHUB_OUTPUT"
bash "$root/.github/actions/cargo-cache/cargo-cache.sh" snapshot "$builder" "$work/export" amd64
grep -q 'usable=true' "$GITHUB_OUTPUT"
for item in registry git target; do mkdir "$work/$item"; tar -xzf "$work/export/$item.tar.gz" -C "$work/$item"; done
cmp "$work/source/registry/cache/test/example.crate" "$work/registry/cache/test/example.crate"
cmp "$work/source/target/amd64/release/deps/example.rlib" "$work/target/amd64/release/deps/example.rlib"
# Invalid and wrong-architecture snapshots must not be restored.
: > "$GITHUB_OUTPUT"
bash "$root/.github/actions/cargo-cache/cargo-cache.sh" restore "$builder" "$work/export" arm64
grep -q 'usable=false' "$GITHUB_OUTPUT"
printf 'invalid' > "$work/export/target.tar.gz"
: > "$GITHUB_OUTPUT"
bash "$root/.github/actions/cargo-cache/cargo-cache.sh" restore "$builder" "$work/export" amd64
grep -q 'usable=false' "$GITHUB_OUTPUT"
echo 'Cargo cache archive roundtrip passed'
