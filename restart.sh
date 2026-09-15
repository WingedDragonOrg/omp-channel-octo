#!/usr/bin/env bash
# Restart the resident omp-channel-octo service through Super, wait until it is healthy,
# then print the resulting state. Stream logs with `super logs -f omp-channel-octo`.
set -euo pipefail

service="${1:-omp-channel-octo}"

# The Super client reads its endpoint and credentials from this env file.
if [[ -f "${HOME}/.super/env.sh" ]]; then
  # shellcheck disable=SC1091
  . "${HOME}/.super/env.sh"
fi

super -y restart --wait-healthy --timeout 60 "${service}"
super info "${service}"
