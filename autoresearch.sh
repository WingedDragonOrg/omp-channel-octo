#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
export TZ=UTC
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
# All renderer assets are local and pinned by bun.lock. No package downloads or live API calls.
# Requires the Chromium revision installed by: bunx playwright install chromium
exec bun tests/benchmarks/ask-card-ui.mjs
