"""Spark session factory for local disk or YARN / HDFS deployments."""

from __future__ import annotations

import os
from typing import Any

from pyspark.sql import SparkSession

from cinescope.paths import CineScopePaths, get_paths


def build_spark_session(
    *,
    app_name: str = "cinescope",
    paths: CineScopePaths | None = None,
    **overrides: Any,
) -> SparkSession:
    """Create a Spark session.

    - Local Mac / laptop: ``SPARK_MASTER=local[*]`` (default), data on local disk.
    - NYU Dataproc: ``SPARK_MASTER=yarn``, data root on HDFS; ``spark.local.dir``
      remains a *local* scratch path on executors.
    """
    if paths is None:
        paths = get_paths(create_dirs=True, validate_mount=True)

    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "24g")
    shuffle_partitions = os.environ.get("SPARK_SHUFFLE_PARTITIONS", "96")
    master = os.environ.get("SPARK_MASTER", "local[*]").strip() or "local[*]"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.local.dir", str(paths.spark_local_dir))
        .config("spark.sql.warehouse.dir", str(paths.spark_warehouse_dir))
    )

    if paths.backend == "hdfs":
        # Prefer explicit HDFS when running on Dataproc / YARN.
        builder = builder.config(
            "spark.hadoop.fs.defaultFS",
            os.environ.get("SPARK_HADOOP_FS_DEFAULT", "hdfs://"),
        )

    for key, value in overrides.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setCheckpointDir(str(paths.spark_checkpoint_dir))
    spark.sparkContext.setLogLevel("WARN")
    return spark
