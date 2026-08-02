"""Spark job: cast/crew reputation features + enriched movies table."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from cinescope.paths import get_paths
from cinescope.schemas import (
    HIGHLY_RATED_THRESHOLD,
    KNOWN_PERSON_MIN_AVG_RATING,
    KNOWN_PERSON_MIN_MOVIES,
    KNOWN_PERSON_MIN_TOTAL_VOTES,
    NAME_BASICS_SCHEMA,
    RELEVANT_PRINCIPAL_CATEGORIES,
    TITLE_PRINCIPALS_SCHEMA,
)
from cinescope.spark_session import build_spark_session
from cinescope.transformations import (
    aggregate_cast_crew_features,
    attach_known_people,
    build_known_people_lookup,
    build_movies_enriched,
    person_movie_history,
    prepare_name_basics,
    prepare_title_principals,
    restrict_principals_to_movies,
)
from cinescope.validation import (
    directory_size_bytes,
    validate_cast_crew_features,
    validate_movies_enriched,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _read_tsv(spark, path: Path, schema):
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

    principals_path = paths.raw_imdb_dir / "title.principals.tsv.gz"
    names_path = paths.raw_imdb_dir / "name.basics.tsv.gz"
    movies_path = paths.movies_ratings_dir
    for required in (principals_path, names_path):
        if not required.is_file():
            raise FileNotFoundError(f"Required IMDb input missing: {required}")
    if not movies_path.exists():
        raise FileNotFoundError(
            f"Bronze movies_ratings missing: {movies_path}. Run the baseline job first."
        )

    spark = build_spark_session(app_name="cinescope-cast-crew", paths=paths)
    try:
        movies = spark.read.parquet(str(movies_path))
        movies_count = movies.count()
        movies.cache()

        principals_raw = _read_tsv(spark, principals_path, TITLE_PRINCIPALS_SCHEMA)
        names_raw = _read_tsv(spark, names_path, NAME_BASICS_SCHEMA)

        # Sample physical-plan evidence before expensive counts where possible.
        raw_principals_count = principals_raw.count()

        principals_clean = prepare_title_principals(principals_raw)
        names_clean = prepare_name_basics(names_raw)

        movie_principals = restrict_principals_to_movies(principals_clean, movies)
        movie_principals.cache()

        movie_related_count = movie_principals.count()
        after_role_filter_count = movie_related_count  # role filter already applied
        distinct_people = movie_principals.select("nconst").distinct().count()
        distinct_movies_with_principals = (
            movie_principals.select("tconst").distinct().count()
        )

        # Join names for documentation / optional enrichment of person grain.
        principals_named = movie_principals.join(
            names_clean.select("nconst", "primary_name", "birth_year"),
            on="nconst",
            how="left",
        )

        history = person_movie_history(movie_principals, movies)
        history.cache()
        rows_before_agg = principals_named.count()

        known_people = build_known_people_lookup(history)
        known_people.cache()
        known_count = known_people.count()

        # Explicit broadcast-join plan evidence on the large principals side.
        broadcast_demo = attach_known_people(
            movie_principals.select("tconst", "nconst", "category"),
            known_people,
        )
        broadcast_plan = broadcast_demo._jdf.queryExecution().executedPlan().toString()
        (plans_dir / "known_people_broadcast_plan.txt").write_text(
            broadcast_plan + "\n", encoding="utf-8"
        )

        cast_crew = aggregate_cast_crew_features(
            movie_principals, history, known_people
        )
        cast_crew.cache()
        rows_after_agg = cast_crew.count()
        movies_with_features = rows_after_agg
        movies_without_features = movies_count - movies_with_features

        main_plan = cast_crew._jdf.queryExecution().executedPlan().toString()
        (plans_dir / "cast_crew_plan.txt").write_text(main_plan + "\n", encoding="utf-8")

        out_cast = paths.cast_crew_features_dir
        (
            cast_crew.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(out_cast))
        )

        enriched = build_movies_enriched(movies, cast_crew)
        enriched.cache()
        enriched_count = enriched.count()

        out_enriched = paths.movies_enriched_dir
        (
            enriched.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(out_enriched))
        )

        cast_validation = validate_cast_crew_features(
            cast_crew,
            output_path=str(out_cast),
            data_root=str(paths.data_root),
            backend=paths.backend,
        )
        cast_validation.raise_if_failed()

        enriched_validation = validate_movies_enriched(
            enriched,
            baseline_count=movies_count,
            output_path=str(out_enriched),
            data_root=str(paths.data_root),
            backend=paths.backend,
        )
        enriched_validation.raise_if_failed()

        # Separate-process-style reread within a fresh read of the same session.
        reread_cast = spark.read.parquet(str(out_cast))
        reread_enriched = spark.read.parquet(str(out_enriched))
        if reread_cast.count() != rows_after_agg:
            raise RuntimeError("cast_crew Parquet reread count mismatch")
        if reread_enriched.count() != enriched_count:
            raise RuntimeError("movies_enriched Parquet reread count mismatch")

        duration_s = round(time.perf_counter() - started, 3)
        metrics = {
            "job": "build_cast_crew_features",
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "duration_seconds": duration_s,
            "parameters": {
                "relevant_categories": list(RELEVANT_PRINCIPAL_CATEGORIES),
                "highly_rated_threshold": HIGHLY_RATED_THRESHOLD,
                "known_person_min_movies": KNOWN_PERSON_MIN_MOVIES,
                "known_person_min_total_votes": KNOWN_PERSON_MIN_TOTAL_VOTES,
                "known_person_min_avg_rating": KNOWN_PERSON_MIN_AVG_RATING,
            },
            "input_paths": {
                "title_principals": str(principals_path),
                "name_basics": str(names_path),
                "movies_ratings": str(movies_path),
            },
            "input_file_sizes_bytes": {
                "title.principals.tsv.gz": principals_path.size_bytes(),
                "name.basics.tsv.gz": names_path.size_bytes(),
                "movies_ratings_parquet": directory_size_bytes(str(movies_path)),
            },
            "row_counts": {
                "movies_ratings": movies_count,
                "raw_principals": raw_principals_count,
                "movie_related_principals": movie_related_count,
                "principals_after_role_filter": after_role_filter_count,
                "distinct_people": distinct_people,
                "distinct_movies_with_principals": distinct_movies_with_principals,
                "known_people": known_count,
                "rows_before_aggregation": rows_before_agg,
                "rows_after_aggregation": rows_after_agg,
                "movies_with_cast_crew_features": movies_with_features,
                "movies_without_cast_crew_features": movies_without_features,
                "movies_enriched": enriched_count,
            },
            "partitions": {
                "cast_crew_output": reread_cast.rdd.getNumPartitions(),
                "enriched_output": reread_enriched.rdd.getNumPartitions(),
            },
            "output_paths": {
                "cast_crew_features": str(out_cast),
                "movies_enriched": str(out_enriched),
            },
            "output_sizes_bytes": {
                "cast_crew_features": directory_size_bytes(str(out_cast)),
                "movies_enriched": directory_size_bytes(str(out_enriched)),
            },
            "output_schema_cast_crew": reread_cast.schema.jsonValue(),
            "validation_cast_crew": {
                "passed": cast_validation.passed,
                "errors": cast_validation.errors,
                "warnings": cast_validation.warnings,
                "details": cast_validation.details,
            },
            "validation_enriched": {
                "passed": enriched_validation.passed,
                "errors": enriched_validation.errors,
                "warnings": enriched_validation.warnings,
                "details": enriched_validation.details,
            },
            "plan_paths": {
                "cast_crew": str(plans_dir / "cast_crew_plan.txt"),
                "known_people_broadcast": str(
                    plans_dir / "known_people_broadcast_plan.txt"
                ),
            },
            "join_notes": {
                "movie_id_broadcast": "principals restricted via broadcast of movie tconsts",
                "known_people_broadcast": "explicit F.broadcast on known-person lookup",
                "history_window": "rangeBetween(unboundedPreceding, -1) on start_year",
                "enriched_join": "left join from movies_ratings to cast_crew_features",
            },
        }

        metrics_path = metrics_dir / "cast_crew_metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(metrics, indent=2))

        for df in (movies, movie_principals, history, known_people, cast_crew, enriched):
            df.unpersist()
        return metrics
    finally:
        spark.stop()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
