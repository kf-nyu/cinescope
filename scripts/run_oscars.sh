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

OSCARS="${CINESCOPE_DATA_ROOT}/raw/oscars/oscars.csv"
cinescope_is_file "$OSCARS" || cinescope_fail "missing $OSCARS — run: make download-oscars"

mkdir -p \
  "$ROOT_DIR/outputs/metrics" \
  "$ROOT_DIR/outputs/plans" \
  "$ROOT_DIR/outputs/logs"

LOG="$ROOT_DIR/outputs/logs/run_oscars.log"
export PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON="$ROOT_DIR/.venv/bin/python"
else
  PYTHON="python3"
fi
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

echo "Running Oscar feature Spark job (master=${SPARK_MASTER:-local[*]}, backend=${CINESCOPE_STORAGE_BACKEND})..."
set +e
"$PYTHON" -m cinescope.jobs.build_oscar_features 2>&1 | tee "$LOG"
status=${PIPESTATUS[0]}
set -e

if (( status != 0 )); then
  echo "Oscar job failed. See $LOG" >&2
  exit "$status"
fi

echo
echo "Nominations:  ${CINESCOPE_DATA_ROOT}/bronze/oscars_nominations"
echo "Film features:${CINESCOPE_DATA_ROOT}/silver/movie_oscar_features"
echo "Awards join:  ${CINESCOPE_DATA_ROOT}/silver/movies_awards_enriched"
cinescope_du_human "${CINESCOPE_DATA_ROOT}/silver/movies_awards_enriched"
echo "Metrics:      $ROOT_DIR/outputs/metrics/oscar_metrics.json"
echo "Log:          $LOG"
