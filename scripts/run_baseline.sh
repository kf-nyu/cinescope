#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/storage.sh"
cinescope_load_env "$ROOT_DIR"

"$ROOT_DIR/scripts/validate_storage.sh"

if [[ -n "${JAVA_HOME:-}" ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi

BASICS="${CINESCOPE_DATA_ROOT}/raw/imdb/title.basics.tsv.gz"
RATINGS="${CINESCOPE_DATA_ROOT}/raw/imdb/title.ratings.tsv.gz"

cinescope_is_file "$BASICS" || cinescope_fail "missing $BASICS"
cinescope_is_file "$RATINGS" || cinescope_fail "missing $RATINGS"

mkdir -p \
  "$ROOT_DIR/outputs/metrics" \
  "$ROOT_DIR/outputs/plans" \
  "$ROOT_DIR/outputs/logs" \
  "$ROOT_DIR/outputs/charts"

LOG="$ROOT_DIR/outputs/logs/run_baseline.log"
export PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON="$ROOT_DIR/.venv/bin/python"
else
  PYTHON="python3"
fi
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

echo "Running baseline Spark job (master=${SPARK_MASTER:-local[*]}, backend=${CINESCOPE_STORAGE_BACKEND})..."
set +e
"$PYTHON" -m cinescope.jobs.build_movies_ratings 2>&1 | tee "$LOG"
status=${PIPESTATUS[0]}
set -e

if (( status != 0 )); then
  echo "Baseline job failed. See $LOG" >&2
  exit "$status"
fi

OUTPUT="${CINESCOPE_DATA_ROOT}/bronze/movies_ratings"
echo
echo "Parquet location: $OUTPUT"
cinescope_du_human "$OUTPUT"
echo "Metrics:         $ROOT_DIR/outputs/metrics/baseline_metrics.json"
echo "Plan:            $ROOT_DIR/outputs/plans/movies_ratings_plan.txt"
echo "Log:             $LOG"
