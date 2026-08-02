#!/usr/bin/env bash
# Remove CineScope data created on NYU Dataproc for the current account.
# Intended for students, instructors, and TAs after a demo or graded run.
#
# Deletes (for this NetID / HDFS user only):
#   - HDFS:  hdfs:///user/<user>/cinescope-data  (raw, bronze, silver, warehouse, …)
#   - Local: SPARK_LOCAL_DIR and CINESCOPE_DOWNLOAD_STAGING under /tmp
#   - Local: repo outputs/logs, outputs/metrics/*.json, outputs/plans/*.txt
#
# Does NOT delete your HDFS home, other users' data, or the Git clone
# (pass --remove-clone to delete the clone after cleanup).
#
# Usage (from the repo root, with .env configured for HDFS):
#   make clean-hpc
#   # or:
#   bash scripts/cleanup_hpc.sh
#   bash scripts/cleanup_hpc.sh --yes          # skip interactive confirm
#   bash scripts/cleanup_hpc.sh --remove-clone # also rm -rf the clone when done

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/storage.sh"
cinescope_load_env "$ROOT_DIR"

SKIP_CONFIRM=0
REMOVE_CLONE=0
for arg in "$@"; do
  case "$arg" in
    --yes|-y) SKIP_CONFIRM=1 ;;
    --remove-clone) REMOVE_CLONE=1 ;;
    -h|--help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *)
      cinescope_fail "Unknown argument: $arg (try --help)"
      ;;
  esac
done

if [[ "${CINESCOPE_STORAGE_BACKEND:-local}" != "hdfs" ]]; then
  cinescope_fail "cleanup_hpc.sh requires CINESCOPE_STORAGE_BACKEND=hdfs in .env (refuse to wipe a local laptop data root)."
fi

: "${CINESCOPE_DATA_ROOT:?Set CINESCOPE_DATA_ROOT (or CINESCOPE_NETID) in .env}"
: "${SPARK_LOCAL_DIR:?Set SPARK_LOCAL_DIR in .env}"
: "${CINESCOPE_DOWNLOAD_STAGING:=}"

command -v hdfs >/dev/null 2>&1 || cinescope_fail "hdfs command not found (run this on Dataproc)"

case "$CINESCOPE_DATA_ROOT" in
  hdfs:///user/*/cinescope-data|hdfs:///user/*/cinescope-data/)
    ;;
  *)
    cinescope_fail "Refusing to delete unexpected data root (expected …/cinescope-data): $CINESCOPE_DATA_ROOT"
    ;;
esac

echo "This will permanently delete CineScope artifacts for this account:"
echo "  HDFS data root : $CINESCOPE_DATA_ROOT"
echo "  Local Spark tmp: $SPARK_LOCAL_DIR"
if [[ -n "${CINESCOPE_DOWNLOAD_STAGING}" ]]; then
  echo "  Download stage : $CINESCOPE_DOWNLOAD_STAGING"
fi
echo "  Repo run logs  : $ROOT_DIR/outputs/{logs,metrics,plans}"
if [[ "$REMOVE_CLONE" -eq 1 ]]; then
  echo "  Git clone      : $ROOT_DIR  (--remove-clone)"
fi
echo

if [[ "$SKIP_CONFIRM" -eq 0 ]]; then
  read -r -p "Type YES to proceed: " answer
  if [[ "$answer" != "YES" ]]; then
    echo "Aborted."
    exit 1
  fi
fi

echo "Removing HDFS tree (if present)…"
if hdfs dfs -test -e "$CINESCOPE_DATA_ROOT" 2>/dev/null; then
  hdfs dfs -rm -r -skipTrash "$CINESCOPE_DATA_ROOT"
  echo "  deleted $CINESCOPE_DATA_ROOT"
else
  echo "  (already absent) $CINESCOPE_DATA_ROOT"
fi

echo "Removing local scratch…"
rm -rf "$SPARK_LOCAL_DIR"
echo "  cleared $SPARK_LOCAL_DIR"
if [[ -n "${CINESCOPE_DOWNLOAD_STAGING}" ]]; then
  rm -rf "$CINESCOPE_DOWNLOAD_STAGING"
  echo "  cleared $CINESCOPE_DOWNLOAD_STAGING"
fi

echo "Clearing generated repo outputs…"
rm -rf "$ROOT_DIR/outputs/logs/"*
rm -f "$ROOT_DIR/outputs/metrics/"*.json \
  "$ROOT_DIR/outputs/metrics/raw_data_inventory.txt"
rm -f "$ROOT_DIR/outputs/plans/"*.txt
rm -rf "$ROOT_DIR/outputs/charts/generated"
echo "  cleared outputs under $ROOT_DIR/outputs"

echo
echo "HPC CineScope data cleanup complete for this account."
echo "Verify with: hdfs dfs -ls $(dirname "$CINESCOPE_DATA_ROOT")"

if [[ "$REMOVE_CLONE" -eq 1 ]]; then
  echo "Removing Git clone at $ROOT_DIR …"
  cd "$HOME"
  rm -rf "$ROOT_DIR"
  echo "Clone removed. Done."
fi
