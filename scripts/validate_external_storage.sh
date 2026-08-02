#!/usr/bin/env bash
# Backward-compatible wrapper.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec "$ROOT_DIR/scripts/validate_storage.sh"
