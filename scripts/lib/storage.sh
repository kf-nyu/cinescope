#!/usr/bin/env bash
# Shared storage helpers for local disk and HDFS (NYU Dataproc).
# shellcheck shell=bash

cinescope_load_env() {
  local root_dir="$1"
  if [[ -f "${root_dir}/.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    source "${root_dir}/.env"
    set +a
  fi
  CINESCOPE_STORAGE_BACKEND="${CINESCOPE_STORAGE_BACKEND:-local}"
  SPARK_MASTER="${SPARK_MASTER:-local[*]}"
  cinescope_apply_hdfs_defaults
}

# Derive HDFS paths from CINESCOPE_NETID (or whoami on Dataproc).
# Dataproc HDFS homes look like: /user/<netid>_nyu_edu/
cinescope_apply_hdfs_defaults() {
  if [[ "${CINESCOPE_STORAGE_BACKEND:-local}" != "hdfs" ]]; then
    return 0
  fi

  if [[ -z "${CINESCOPE_HDFS_USER:-}" ]]; then
    if [[ -n "${CINESCOPE_NETID:-}" ]]; then
      CINESCOPE_HDFS_USER="${CINESCOPE_NETID}_nyu_edu"
    else
      local u
      u="$(whoami 2>/dev/null || true)"
      if [[ "$u" == *_nyu_edu ]]; then
        CINESCOPE_HDFS_USER="$u"
        CINESCOPE_NETID="${u%_nyu_edu}"
      elif [[ -n "$u" && "$u" != "root" ]]; then
        # Fallback: treat whoami as the short NetID
        CINESCOPE_NETID="${CINESCOPE_NETID:-$u}"
        CINESCOPE_HDFS_USER="${CINESCOPE_NETID}_nyu_edu"
      fi
    fi
  fi

  if [[ -z "${CINESCOPE_HDFS_USER:-}" ]]; then
    cinescope_fail "HDFS mode requires CINESCOPE_NETID (or CINESCOPE_HDFS_USER) in .env"
  fi

  # Export so child Python processes see the derived values.
  export CINESCOPE_NETID="${CINESCOPE_NETID:-}"
  export CINESCOPE_HDFS_USER

  local root="hdfs:///user/${CINESCOPE_HDFS_USER}/cinescope-data"
  local scratch_id="${CINESCOPE_NETID:-$CINESCOPE_HDFS_USER}"

  [[ -n "${CINESCOPE_DATA_ROOT:-}" ]] || export CINESCOPE_DATA_ROOT="$root"
  [[ -n "${SPARK_WAREHOUSE_DIR:-}" ]] || export SPARK_WAREHOUSE_DIR="${root}/warehouse"
  [[ -n "${SPARK_CHECKPOINT_DIR:-}" ]] || export SPARK_CHECKPOINT_DIR="${root}/checkpoints"
  [[ -n "${SPARK_LOCAL_DIR:-}" ]] || export SPARK_LOCAL_DIR="/tmp/cinescope-spark-${scratch_id}"
  [[ -n "${CINESCOPE_DOWNLOAD_STAGING:-}" ]] || export CINESCOPE_DOWNLOAD_STAGING="/tmp/cinescope-download-${scratch_id}"
}

cinescope_fail() {
  echo "Error: $*" >&2
  exit 1
}

cinescope_is_hdfs() {
  [[ "${CINESCOPE_STORAGE_BACKEND:-local}" == "hdfs" ]]
}

cinescope_assert_not_repo() {
  local path_val="$1"
  local root_dir="$2"
  case "$path_val" in
    "$root_dir"|"$root_dir"/*|/)
      cinescope_fail "path must not be / or inside the Git repository: $path_val"
      ;;
  esac
}

cinescope_mkdir() {
  local path_val="$1"
  if cinescope_is_hdfs; then
    hdfs dfs -mkdir -p "$path_val"
  else
    mkdir -p "$path_val"
  fi
}

cinescope_path_exists() {
  local path_val="$1"
  if cinescope_is_hdfs; then
    hdfs dfs -test -e "$path_val"
  else
    [[ -e "$path_val" ]]
  fi
}

cinescope_is_file() {
  local path_val="$1"
  if cinescope_is_hdfs; then
    hdfs dfs -test -f "$path_val"
  else
    [[ -f "$path_val" ]]
  fi
}

cinescope_is_dir() {
  local path_val="$1"
  if cinescope_is_hdfs; then
    hdfs dfs -test -d "$path_val"
  else
    [[ -d "$path_val" ]]
  fi
}

cinescope_put_file() {
  # Usage: cinescope_put_file <local_file> <dest_uri_or_path>
  local src="$1"
  local dest="$2"
  if cinescope_is_hdfs; then
    hdfs dfs -mkdir -p "$(dirname "$dest")"
    hdfs dfs -put -f "$src" "$dest"
  else
    mkdir -p "$(dirname "$dest")"
    cp -f "$src" "$dest"
  fi
}

cinescope_du_human() {
  local path_val="$1"
  if cinescope_is_hdfs; then
    hdfs dfs -du -h -s "$path_val" 2>/dev/null || echo "(size unavailable) $path_val"
  else
    du -sh "$path_val" 2>/dev/null || echo "(missing) $path_val"
  fi
}
