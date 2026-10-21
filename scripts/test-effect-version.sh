#!/usr/bin/env bash
# Runs the full gate and the packed-consumer test against one Effect version in a temp
# copy of the repo. Usage: test-effect-version.sh <effect-version>
set -euo pipefail

version="${1:?usage: test-effect-version.sh <effect-version>}"
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/alchemy-analytics-effect.XXXXXX")"

(cd "$root" && git ls-files -co --exclude-standard -z | tar --null -T - -cf -) | tar -xf - -C "$work"

cd "$work"
node -e '
const fs = require("node:fs");
const version = process.argv[1];
const pkg = JSON.parse(fs.readFileSync("package.json", "utf8"));
for (const name of ["effect", "@effect/vitest", "@effect/platform-node"]) {
  pkg.devDependencies[name] = version;
}
fs.writeFileSync("package.json", JSON.stringify(pkg, null, 2));
' "$version"

pnpm install --no-frozen-lockfile --config.minimumReleaseAge=0
echo "unit tests on effect@$(node -p "require('./node_modules/effect/package.json').version")"
pnpm typecheck
pnpm test
bash scripts/consumer-test.sh "$version"
