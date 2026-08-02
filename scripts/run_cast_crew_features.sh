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

MOVIES="${CINESCOPE_DATA_ROOT}/bronze/movies_ratings"
PRINCIPALS="${CINESCOPE_DATA_ROOT}/raw/imdb/title.principals.tsv.gz"
NAMES="${CINESCOPE_DATA_ROOT}/raw/imdb/name.basics.tsv.gz"

cinescope_is_dir "$MOVIES" || cinescope_fail "missing bronze movies_ratings at $MOVIES (run make baseline first)"
cinescope_is_file "$PRINCIPALS" || cinescope_fail "missing $PRINCIPALS"
cinescope_is_file "$NAMES" || cinescope_fail "missing $NAMES"

mkdir -p \
  "$ROOT_DIR/outputs/metrics" \
  "$ROOT_DIR/outputs/plans" \
  "$ROOT_DIR/outputs/logs"

LOG="$ROOT_DIR/outputs/logs/run_cast_crew.log"
export PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON="$ROOT_DIR/.venv/bin/python"
else
  PYTHON="python3"
fi
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

echo "Running cast/crew feature Spark job (master=${SPARK_MASTER:-local[*]}, backend=${CINESCOPE_STORAGE_BACKEND})..."
set +e
"$PYTHON" -m cinescope.jobs.build_cast_crew_features 2>&1 | tee "$LOG"
status=${PIPESTATUS[0]}
set -e

if (( status != 0 )); then
  echo "Cast/crew job failed. See $LOG" >&2
  exit "$status"
fi

CAST_OUT="${CINESCOPE_DATA_ROOT}/silver/cast_crew_features"
ENRICHED_OUT="${CINESCOPE_DATA_ROOT}/silver/movies_enriched"

echo
echo "Cast/crew features: $CAST_OUT"
cinescope_du_human "$CAST_OUT"
echo "Movies enriched:   $ENRICHED_OUT"
cinescope_du_human "$ENRICHED_OUT"
echo "Metrics:           $ROOT_DIR/outputs/metrics/cast_crew_metrics.json"
echo "Log:               $LOG"
