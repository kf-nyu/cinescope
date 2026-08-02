#!/usr/bin/env bash
# Launch a CineScope Spark job locally (python) or on Dataproc (spark-submit batch).
# shellcheck shell=bash
#
# NYU Dataproc is batch-oriented: use spark-submit, not Jupyter / pyspark shells.
# Client deploy-mode keeps the driver on the login node so .venv, .env, and
# outputs/ under the Git clone continue to work.

cinescope_python() {
  local root_dir="$1"
  if [[ -x "${root_dir}/.venv/bin/python" ]]; then
    echo "${root_dir}/.venv/bin/python"
  else
    echo "python3"
  fi
}

# Resolve a namenode URI with authority (not bare hdfs://).
# Prefer SPARK_HADOOP_FS_DEFAULT; else `hdfs getconf -confKey fs.defaultFS`.
cinescope_resolve_hadoop_fs_default() {
  local fs="${SPARK_HADOOP_FS_DEFAULT:-}"
  if [[ -z "$fs" ]] && command -v hdfs >/dev/null 2>&1; then
    fs="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || true)"
  fi
  fs="$(echo "$fs" | tr -d '[:space:]')"

  case "$fs" in
    hdfs://[A-Za-z0-9._-]*|hdfs://[A-Za-z0-9._-]*:[0-9]*)
      export SPARK_HADOOP_FS_DEFAULT="$fs"
      ;;
    *)
      cinescope_fail "Could not resolve a valid HDFS namenode URI for spark.hadoop.fs.defaultFS (got '${fs:-empty}'). Set SPARK_HADOOP_FS_DEFAULT in .env (e.g. hdfs://nyu-dataproc-m) or ensure 'hdfs getconf -confKey fs.defaultFS' works."
      ;;
  esac
}

cinescope_prepare_yarn_batch() {
  export SPARK_HOME="${SPARK_HOME:-/usr/lib/spark}"
  if [[ -d "$SPARK_HOME/bin" ]]; then
    export PATH="$SPARK_HOME/bin:$PATH"
  fi
  command -v spark-submit >/dev/null 2>&1 || \
    cinescope_fail "spark-submit not found (set SPARK_HOME, expected /usr/lib/spark on Dataproc)"

  cinescope_resolve_hadoop_fs_default

  # Dataproc may default to cluster mode; python jobs via spark-submit need client.
  export SPARK_SUBMIT_DEPLOY_MODE="${SPARK_SUBMIT_DEPLOY_MODE:-client}"
}

# Usage: cinescope_run_spark_job <root_dir> <job_script.py> <log_path>
# Local: python job_script.py
# HDFS+YARN: spark-submit --deploy-mode client …
cinescope_run_spark_job() {
  local root_dir="$1"
  local job_script="$2"
  local log_path="$3"

  [[ -f "$job_script" ]] || cinescope_fail "job script not found: $job_script"

  local python
  python="$(cinescope_python "$root_dir")"
  export PYTHONPATH="${root_dir}/src${PYTHONPATH:+:$PYTHONPATH}"
  export PYSPARK_PYTHON="$python"
  export PYSPARK_DRIVER_PYTHON="$python"

  local master="${SPARK_MASTER:-local[*]}"
  local status=0

  if cinescope_is_hdfs && [[ "$master" == yarn* ]]; then
    cinescope_prepare_yarn_batch
    echo "Submitting batch Spark job via spark-submit (master=yarn, deploy-mode=client, fs.defaultFS=${SPARK_HADOOP_FS_DEFAULT})..."
    set +e
    spark-submit \
      --master yarn \
      --deploy-mode client \
      --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-4g}" \
      --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-200}" \
      --conf "spark.pyspark.python=${PYSPARK_PYTHON}" \
      --conf "spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON}" \
      --conf "spark.hadoop.fs.defaultFS=${SPARK_HADOOP_FS_DEFAULT}" \
      "$job_script" 2>&1 | tee "$log_path"
    status=${PIPESTATUS[0]}
    set -e
  else
    echo "Running Spark job locally (master=${master}, backend=${CINESCOPE_STORAGE_BACKEND:-local})..."
    set +e
    "$python" "$job_script" 2>&1 | tee "$log_path"
    status=${PIPESTATUS[0]}
    set -e
  fi

  return "$status"
}
