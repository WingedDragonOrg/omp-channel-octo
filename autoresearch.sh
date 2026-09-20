#!/usr/bin/env bash
set -euo pipefail
exec bun run tests/fixtures/resource-benchmark.ts
