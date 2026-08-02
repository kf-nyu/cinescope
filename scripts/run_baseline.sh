#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/storage.sh"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/spark_job.sh"
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
JOB="$ROOT_DIR/src/cinescope/jobs/build_movies_ratings.py"

echo "Running baseline Spark job (master=${SPARK_MASTER:-local[*]}, backend=${CINESCOPE_STORAGE_BACKEND})..."
set +e
cinescope_run_spark_job "$ROOT_DIR" "$JOB" "$LOG"
status=$?
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
