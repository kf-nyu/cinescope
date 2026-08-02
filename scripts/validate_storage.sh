#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck disable=SC1091
source "$ROOT_DIR/scripts/lib/storage.sh"
cinescope_load_env "$ROOT_DIR"

: "${CINESCOPE_DATA_ROOT:?Set CINESCOPE_DATA_ROOT in .env}"
: "${SPARK_LOCAL_DIR:?Set SPARK_LOCAL_DIR in .env}"
: "${SPARK_WAREHOUSE_DIR:?Set SPARK_WAREHOUSE_DIR in .env}"
: "${SPARK_CHECKPOINT_DIR:?Set SPARK_CHECKPOINT_DIR in .env}"

MIN_FREE_GB="${CINESCOPE_MIN_FREE_GB:-}"

if cinescope_is_hdfs; then
  command -v hdfs >/dev/null 2>&1 || cinescope_fail "hdfs command not found (are you on NYU Dataproc?)"
  case "$CINESCOPE_DATA_ROOT" in
    hdfs://*|/user/*) ;;
    *) cinescope_fail "HDFS CINESCOPE_DATA_ROOT must be hdfs:///user/... or /user/..." ;;
  esac
  # spark.local.dir must remain on the local filesystem
  case "$SPARK_LOCAL_DIR" in
    hdfs://*) cinescope_fail "SPARK_LOCAL_DIR must be a local path, not HDFS." ;;
  esac
  cinescope_assert_not_repo "$SPARK_LOCAL_DIR" "$ROOT_DIR"
  cinescope_is_dir "$CINESCOPE_DATA_ROOT" || cinescope_fail "HDFS data root missing: $CINESCOPE_DATA_ROOT (run make init-storage)"
  echo "Storage validation passed (HDFS)."
  echo "  backend:      hdfs"
  echo "  hdfs user:    ${CINESCOPE_HDFS_USER}"
  echo "  data root:    $CINESCOPE_DATA_ROOT"
  echo "  spark master: ${SPARK_MASTER:-yarn}"
  echo "  local scratch:$SPARK_LOCAL_DIR"
  exit 0
fi

# ---- local backend ----
cinescope_assert_not_repo "$CINESCOPE_DATA_ROOT" "$ROOT_DIR"
cinescope_assert_not_repo "$SPARK_LOCAL_DIR" "$ROOT_DIR"
cinescope_assert_not_repo "$SPARK_WAREHOUSE_DIR" "$ROOT_DIR"
cinescope_assert_not_repo "$SPARK_CHECKPOINT_DIR" "$ROOT_DIR"

if [[ -n "${CINESCOPE_SSD_VOLUME:-}" ]]; then
  if ! mount | grep -Fq "on ${CINESCOPE_SSD_VOLUME} "; then
    cinescope_fail "CINESCOPE_SSD_VOLUME is set but not mounted at ${CINESCOPE_SSD_VOLUME}. Unset it to use a plain local directory."
  fi
  case "$CINESCOPE_DATA_ROOT" in
    "$CINESCOPE_SSD_VOLUME"/*) ;;
    *) cinescope_fail "CINESCOPE_DATA_ROOT must be inside CINESCOPE_SSD_VOLUME when the volume is set." ;;
  esac
  for path_var in SPARK_LOCAL_DIR SPARK_WAREHOUSE_DIR SPARK_CHECKPOINT_DIR; do
    path_val="${!path_var}"
    case "$path_val" in
      "$CINESCOPE_SSD_VOLUME"/*) ;;
      *) cinescope_fail "$path_var must be inside CINESCOPE_SSD_VOLUME when the volume is set." ;;
    esac
  done
  MIN_FREE_GB="${MIN_FREE_GB:-200}"
else
  # Partner-friendly local directory (e.g. ~/cinescope-data). Still refuse the repo.
  MIN_FREE_GB="${MIN_FREE_GB:-50}"
fi

[[ -d "$CINESCOPE_DATA_ROOT" ]] || cinescope_fail "data root does not exist: $CINESCOPE_DATA_ROOT (run make init-storage)"
[[ -w "$CINESCOPE_DATA_ROOT" ]] || cinescope_fail "data root is not writable: $CINESCOPE_DATA_ROOT"

avail_kb="$(df -k "$CINESCOPE_DATA_ROOT" | awk 'NR==2 {print $4}')"
avail_gb=$((avail_kb / 1024 / 1024))
if (( avail_gb < MIN_FREE_GB )); then
  cinescope_fail "need at least ${MIN_FREE_GB} GB free at data root (found ${avail_gb} GB). Override with CINESCOPE_MIN_FREE_GB if intentional."
fi

echo "Storage validation passed (local)."
echo "  backend:   local"
if [[ -n "${CINESCOPE_SSD_VOLUME:-}" ]]; then
  echo "  volume:    $CINESCOPE_SSD_VOLUME"
fi
echo "  data root: $CINESCOPE_DATA_ROOT"
echo "  free:      ${avail_gb} GB (minimum ${MIN_FREE_GB} GB)"
