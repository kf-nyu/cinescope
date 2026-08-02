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

cinescope_assert_not_repo "$CINESCOPE_DATA_ROOT" "$ROOT_DIR"

if cinescope_is_hdfs; then
  command -v hdfs >/dev/null 2>&1 || cinescope_fail "hdfs command not found"
  # Local scratch for Spark shuffle on executors / driver
  mkdir -p "$SPARK_LOCAL_DIR"
  for sub in raw/imdb raw/oscars bronze silver gold samples checkpoints warehouse models exports; do
    cinescope_mkdir "${CINESCOPE_DATA_ROOT}/${sub}"
  done
  # warehouse/checkpoints may already be under data root; ensure explicit env targets exist
  cinescope_mkdir "$SPARK_WAREHOUSE_DIR"
  cinescope_mkdir "$SPARK_CHECKPOINT_DIR"
  echo "Initialized HDFS storage at: $CINESCOPE_DATA_ROOT"
  hdfs dfs -ls -d "$CINESCOPE_DATA_ROOT" || true
  exit 0
fi

if [[ -n "${CINESCOPE_SSD_VOLUME:-}" ]]; then
  if ! mount | grep -Fq "on ${CINESCOPE_SSD_VOLUME} "; then
    cinescope_fail "CINESCOPE_SSD_VOLUME is set but not mounted at ${CINESCOPE_SSD_VOLUME}"
  fi
  case "$CINESCOPE_DATA_ROOT" in
    "$CINESCOPE_SSD_VOLUME"/*) ;;
    *) cinescope_fail "CINESCOPE_DATA_ROOT must be inside CINESCOPE_SSD_VOLUME when set." ;;
  esac
fi

mkdir -p \
  "$CINESCOPE_DATA_ROOT/raw/imdb" \
  "$CINESCOPE_DATA_ROOT/raw/oscars" \
  "$CINESCOPE_DATA_ROOT/bronze" \
  "$CINESCOPE_DATA_ROOT/silver" \
  "$CINESCOPE_DATA_ROOT/gold" \
  "$CINESCOPE_DATA_ROOT/samples" \
  "$SPARK_LOCAL_DIR" \
  "$SPARK_CHECKPOINT_DIR" \
  "$SPARK_WAREHOUSE_DIR" \
  "$CINESCOPE_DATA_ROOT/models" \
  "$CINESCOPE_DATA_ROOT/exports"

test -w "$CINESCOPE_DATA_ROOT" || cinescope_fail "data root is not writable: $CINESCOPE_DATA_ROOT"

echo "Initialized local storage at: $CINESCOPE_DATA_ROOT"
df -h "$CINESCOPE_DATA_ROOT"
