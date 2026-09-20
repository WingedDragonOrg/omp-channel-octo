#!/usr/bin/env bash
set -euo pipefail

# Offline ask-card geometry, screenshots, and real-renderer answer interactions.
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
export TZ=UTC
export LC_ALL=C
exec bun run tests/benchmarks/ask-card-ui.mjs
