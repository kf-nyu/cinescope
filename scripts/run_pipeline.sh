#!/usr/bin/env bash
# Run the full CineScope pipeline and save the entire console log to one file.
# Still prints to the terminal (tee). Stops on first failure.
#
# Usage:
#   make pipeline
#   bash scripts/run_pipeline.sh
#   PIPELINE_LOG=/tmp/my.log bash scripts/run_pipeline.sh
#
# Steps: init-storage → validate-storage → download → download-oscars →
#        inspect → test → baseline → cast-crew → oscars

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p "$ROOT_DIR/outputs/logs"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
LOG="${PIPELINE_LOG:-$ROOT_DIR/outputs/logs/pipeline_${STAMP}.log}"

STEPS=(
  init-storage
  validate-storage
  download
  download-oscars
  inspect
  test
  baseline
  cast-crew
  oscars
)

{
  echo "================================================================================"
  echo "CineScope full pipeline"
  echo "Started (UTC):  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Host:           $(hostname 2>/dev/null || echo unknown)"
  echo "User:           $(whoami 2>/dev/null || echo unknown)"
  echo "Repo:           $ROOT_DIR"
  echo "Log file:       $LOG"
  echo "Steps:          ${STEPS[*]}"
  echo "================================================================================"
  echo
} | tee "$LOG"

run_step() {
  local step="$1"
  {
    echo
    echo "--------------------------------------------------------------------------------"
    echo ">>> make ${step}   ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
    echo "--------------------------------------------------------------------------------"
  } | tee -a "$LOG"

  # Preserve make's exit status through tee.
  set +e
  make "$step" 2>&1 | tee -a "$LOG"
  local status=${PIPESTATUS[0]}
  set -e

  if (( status != 0 )); then
    {
      echo
      echo "ERROR: make ${step} failed with exit code ${status}"
      echo "Full log: $LOG"
    } | tee -a "$LOG" >&2
    exit "$status"
  fi
}

for step in "${STEPS[@]}"; do
  run_step "$step"
done

{
  echo
  echo "================================================================================"
  echo "Pipeline complete (UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Full log: $LOG"
  echo "================================================================================"
} | tee -a "$LOG"

echo
echo "Saved full pipeline log to: $LOG"
