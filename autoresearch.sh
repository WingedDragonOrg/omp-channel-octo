#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
# A successful measurement may score zero. Safety violations or a broken
# workload exit nonzero; production continuity is reported by the metrics.
exec bun tests/benchmarks/restart.ts
