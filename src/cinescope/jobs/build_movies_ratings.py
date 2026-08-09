"""Baseline Spark job: movies joined with ratings → bronze Parquet."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from cinescope.paths import get_paths
from cinescope.schemas import TITLE_BASICS_SCHEMA, TITLE_RATINGS_SCHEMA
from cinescope.spark_session import build_spark_session
from cinescope.transformations import (
    build_movies_ratings,
    prepare_title_basics,
    prepare_title_ratings,
)
from cinescope.validation import directory_size_bytes, validate_movies_ratings


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read_tsv(spark: SparkSession, path: Path, schema) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .option("nullValue", "\\N")
        .option("emptyValue", "")
        .option("compression", "gzip")
        .schema(schema)
        .csv(str(path))
    )


def run() -> dict:
    started = time.perf_counter()
    paths = get_paths(create_dirs=True, validate_mount=True)
    repo = _repo_root()
    metrics_dir = repo / "outputs" / "metrics"
    plans_dir = repo / "outputs" / "plans"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    plans_dir.mkdir(parents=True, exist_ok=True)

    basics_path = paths.raw_imdb_dir / "title.basics.tsv.gz"
    ratings_path = paths.raw_imdb_dir / "title.ratings.tsv.gz"
    for required in (basics_path, ratings_path):
        if not required.is_file():
            raise FileNotFoundError(f"Required IMDb input missing: {required}")

    spark = build_spark_session(app_name="cinescope-movies-ratings", paths=paths)
    try:
        titles_raw = _read_tsv(spark, basics_path, TITLE_BASICS_SCHEMA)
        ratings_raw = _read_tsv(spark, ratings_path, TITLE_RATINGS_SCHEMA)

        title_count = titles_raw.count()
        rating_count = ratings_raw.count()

        movies = prepare_title_basics(titles_raw)
        ratings = prepare_title_ratings(ratings_raw)
        movie_count = movies.count()

        joined = build_movies_ratings(movies, ratings)
        # Materialize once for validation + write metrics.
        joined.cache()
        joined_count = joined.count()

        plan_text = joined._jdf.queryExecution().executedPlan().toString()
        plan_path = plans_dir / "movies_ratings_plan.txt"
        plan_path.write_text(plan_text + "\n", encoding="utf-8")

        output_path = paths.movies_ratings_dir
        (
            joined.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(output_path))
        )

        validation = validate_movies_ratings(
            joined,
            output_path=str(output_path),
            data_root=str(paths.data_root),
            backend=paths.backend,
        )
        validation.raise_if_failed()

        # Re-read check from a fresh path (same session is fine; proves Parquet readable).
        reread = spark.read.parquet(str(output_path))
        reread_count = reread.count()
        if reread_count != joined_count:
            raise RuntimeError(
                f"Parquet reread count mismatch: wrote {joined_count}, read {reread_count}"
            )

        duration_s = round(time.perf_counter() - started, 3)
        output_size = directory_size_bytes(str(output_path))
        input_sizes = {
            "title.basics.tsv.gz": basics_path.size_bytes(),
            "title.ratings.tsv.gz": ratings_path.size_bytes(),
        }

        metrics = {
            "job": "build_movies_ratings",
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "duration_seconds": duration_s,
            "input_paths": {
                "title_basics": str(basics_path),
                "title_ratings": str(ratings_path),
            },
            "input_file_sizes_bytes": input_sizes,
            "row_counts": {
                "title_basics": title_count,
                "title_ratings": rating_count,
                "movies": movie_count,
                "joined_movies_ratings": joined_count,
                "parquet_reread": reread_count,
            },
            "partitions": {
                "input_title_basics": titles_raw.rdd.getNumPartitions(),
                "input_title_ratings": ratings_raw.rdd.getNumPartitions(),
                "output": reread.rdd.getNumPartitions(),
            },
            "output_path": str(output_path),
            "output_size_bytes": output_size,
            "output_schema": reread.schema.jsonValue(),
            "validation": {
                "passed": validation.passed,
                "errors": validation.errors,
                "warnings": validation.warnings,
                "details": validation.details,
            },
            "plan_path": str(plan_path),
        }

        metrics_path = metrics_dir / "baseline_metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")

        print(json.dumps(metrics, indent=2))
        return metrics
    finally:
        spark.stop()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
