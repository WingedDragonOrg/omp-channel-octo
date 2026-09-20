#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
# All transports and workers are local fixtures; no credentials or live services.
# Fail closed on behavior regressions before emitting the UI coverage metric.
bun test ./tests/core-switch-command.test.ts ./tests/core-ask-card.test.ts
bun run ./tests/command-card.benchmark.ts
