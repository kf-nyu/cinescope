#!/usr/bin/env bash
# Local-only final analytics/model run after the raw and baseline layers exist.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck source=scripts/lib/storage.sh
source "$ROOT_DIR/scripts/lib/storage.sh"
cinescope_load_env "$ROOT_DIR"

if cinescope_is_hdfs; then
  cinescope_fail "final-artifacts is local-only; analytics and ML do not run on Dataproc JupyterHub"
fi

PYTHON="$ROOT_DIR/.venv/bin/python"
JUPYTER="$ROOT_DIR/.venv/bin/jupyter"
[[ -x "$PYTHON" ]] || cinescope_fail "missing .venv Python; run make setup"
[[ -x "$JUPYTER" ]] || cinescope_fail "missing Jupyter executable; run make setup"

export PYTHONPATH="$ROOT_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"
export MPLBACKEND=Agg

make test
make cast-crew
make oscars

RUN_DIR="$ROOT_DIR/outputs/notebooks"
mkdir -p "$RUN_DIR"

run_notebook() {
  local notebook="$1"
  local base
  base="$(basename "$notebook" .ipynb)"
  "$JUPYTER" nbconvert \
    --to notebook \
    --execute "$notebook" \
    --output-dir "$RUN_DIR" \
    --output "${base}_executed.ipynb" \
    --ExecutePreprocessor.timeout=-1 \
    --ExecutePreprocessor.kernel_name=python3
}

run_notebook "$ROOT_DIR/notebooks/02_core_analytics.ipynb"
run_notebook "$ROOT_DIR/notebooks/03_train_hit_model.ipynb"
run_notebook "$ROOT_DIR/notebooks/04_train_awards_model.ipynb"

"$PYTHON" "$ROOT_DIR/scripts/validate_final_artifacts.py" \
  --allow-report-placeholders

REPORT_DIR="$ROOT_DIR/outputs/report"
mkdir -p "$REPORT_DIR"
"$PYTHON" "$ROOT_DIR/scripts/finalize_report.py" \
  --output "$REPORT_DIR/CineScope_Final_Report.md"
"$PYTHON" "$ROOT_DIR/scripts/validate_final_artifacts.py" \
  --report "$REPORT_DIR/CineScope_Final_Report.md"

echo "Corrected final artifacts generated and validated."
echo "Executed notebooks: $RUN_DIR"
echo "Resolved report: $REPORT_DIR/CineScope_Final_Report.md"