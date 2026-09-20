#!/usr/bin/env bash
set -euo pipefail

# Run only local fixtures; no Octo credentials, installed OMP, or model calls.
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
export TZ=UTC
export LC_ALL=C
exec bun run tests/benchmarks/autoresearch.ts
