#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
# Fixed offline correctness gate followed by structural architecture metrics.
# The benchmark and its fixtures stay frozen during optimisation.
export TZ=UTC
export LC_ALL=C
bun run typecheck
bun test
exec bun run ./tests/benchmarks/architecture.ts
