#!/usr/bin/env bash
set -euo pipefail
exec bun run "$(dirname "${BASH_SOURCE[0]}")/tests/fixtures/stability-benchmark.ts"
