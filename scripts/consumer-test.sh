#!/usr/bin/env bash
# Installs the packed package into a fresh project on one Effect version, then sends one
# event through the Node sink to a local ingest route. Usage: consumer-test.sh <effect-version>
set -euo pipefail

version="${1:?usage: consumer-test.sh <effect-version>}"
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/alchemy-analytics-consumer.XXXXXX")"

(cd "$root" && pnpm pack --pack-destination "$work" >/dev/null)
tarball="$(ls "$work"/alchemy-analytics-*.tgz)"

cat >"$work/package.json" <<JSON
{
  "name": "consumer",
  "private": true,
  "type": "module",
  "dependencies": {
    "alchemy-analytics": "file:$tarball",
    "effect": "$version"
  }
}
JSON

cp "$root/scripts/consumer-run.mjs" "$work/run.mjs"

cd "$work"
pnpm install --ignore-workspace --config.minimumReleaseAge=0 >install.log 2>&1 || { cat install.log; exit 1; }
echo "consumer on effect@$(node -p "require('./node_modules/effect/package.json').version")"
node run.mjs
