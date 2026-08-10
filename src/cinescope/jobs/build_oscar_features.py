"""Spark job: Oscar nominations → film-level awards features + enriched movies."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from cinescope.paths import get_paths
from cinescope.schemas import (
    CAST_CREW_FEATURE_SEMANTICS_VERSION,
    OSCARS_NOMINATIONS_SCHEMA,
)
from cinescope.ml.features import validate_feature_artifact_metadata
from cinescope.spark_session import build_spark_session
from cinescope.transformations import (
    aggregate_movie_oscar_features,
    build_movies_awards_enriched,
    prepare_oscars_nominations,
)
from cinescope.validation import (
    directory_size_bytes,
    validate_movie_oscar_features,
    validate_movies_awards_enriched,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def run() -> dict:
    started = time.perf_counter()
    paths = get_paths(create_dirs=True, validate_mount=True)
    repo = _repo_root()
    metrics_dir = repo / "outputs" / "metrics"
    plans_dir = repo / "outputs" / "plans"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    plans_dir.mkdir(parents=True, exist_ok=True)

    oscars_path = paths.raw_oscars_dir / "oscars.csv"
    movies_path = paths.movies_enriched_dir
    feature_semantics_version = None
    if movies_path.exists():
        cast_metrics_path = metrics_dir / "cast_crew_metrics.json"
        if not cast_metrics_path.is_file():
            raise RuntimeError(
                "movies_enriched exists without cast_crew_metrics.json; "
                "rerun the corrected cast/crew job"
            )
        cast_metadata = json.loads(cast_metrics_path.read_text(encoding="utf-8"))
        validate_feature_artifact_metadata(cast_metadata)
        feature_semantics_version = CAST_CREW_FEATURE_SEMANTICS_VERSION
    if not movies_path.exists():
        # Fall back to bronze if cast/crew enriched table is absent.
        movies_path = paths.movies_ratings_dir
    if not oscars_path.is_file():
        raise FileNotFoundError(
            f"Oscar input missing: {oscars_path}. Run scripts/download_oscars.sh first."
        )
    if not movies_path.exists():
        raise FileNotFoundError(
            f"Movies table missing: {movies_path}. Run baseline (and preferably cast-crew) first."
        )

    spark = build_spark_session(app_name="cinescope-oscars", paths=paths)
    try:
        raw = (
            spark.read.option("header", "true")
            .option("sep", "\t")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .schema(OSCARS_NOMINATIONS_SCHEMA)
            .csv(str(oscars_path))
        )
        raw_count = raw.count()

        nominations = prepare_oscars_nominations(raw)
        nominations.cache()
        nomination_rows = nominations.count()
        films_with_film_id = nominations.select("tconst").distinct().count()

        out_noms = paths.oscars_nominations_dir
        (
            nominations.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(out_noms))
        )

        oscar_features = aggregate_movie_oscar_features(nominations)
        oscar_features.cache()
        feature_rows = oscar_features.count()

        plan_text = oscar_features._jdf.queryExecution().executedPlan().toString()
        (plans_dir / "oscar_features_plan.txt").write_text(
            plan_text + "\n", encoding="utf-8"
        )

        out_feat = paths.movie_oscar_features_dir
        (
            oscar_features.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(out_feat))
        )

        movies = spark.read.parquet(str(movies_path))
        movies_count = movies.count()
        enriched = build_movies_awards_enriched(movies, oscar_features)
        enriched.cache()
        enriched_count = enriched.count()

        matched = enriched.filter("was_oscar_nominated = true").count()
        winners = enriched.filter("was_oscar_winner = true").count()

        out_enriched = paths.movies_awards_enriched_dir
        (
            enriched.write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(str(out_enriched))
        )

        feat_validation = validate_movie_oscar_features(
            oscar_features,
            output_path=str(out_feat),
            data_root=str(paths.data_root),
            backend=paths.backend,
        )
        feat_validation.raise_if_failed()

        enriched_validation = validate_movies_awards_enriched(
            enriched,
            baseline_count=movies_count,
            output_path=str(out_enriched),
            data_root=str(paths.data_root),
            backend=paths.backend,
        )
        enriched_validation.raise_if_failed()

        reread_feat = spark.read.parquet(str(out_feat))
        reread_enriched = spark.read.parquet(str(out_enriched))
        if reread_feat.count() != feature_rows:
            raise RuntimeError("movie_oscar_features Parquet reread mismatch")
        if reread_enriched.count() != enriched_count:
            raise RuntimeError("movies_awards_enriched Parquet reread mismatch")

        duration_s = round(time.perf_counter() - started, 3)
        metrics = {
            "job": "build_oscar_features",
            "feature_semantics_version": feature_semantics_version,
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "spark_version": spark.version,
            "duration_seconds": duration_s,
            "source": {
                "dataset": "DLu/oscar_data oscars.csv",
                "url": "https://github.com/DLu/oscar_data",
                "join_key": "FilmId → tconst",
            },
            "input_paths": {
                "oscars_csv": str(oscars_path),
                "movies_table": str(movies_path),
            },
            "input_file_sizes_bytes": {
                "oscars.csv": oscars_path.size_bytes(),
                "movies_table": directory_size_bytes(str(movies_path)),
            },
            "row_counts": {
                "raw_oscar_rows": raw_count,
                "nomination_rows_with_tconst": nomination_rows,
                "distinct_films_in_oscars": films_with_film_id,
                "movie_oscar_feature_rows": feature_rows,
                "movies_input": movies_count,
                "movies_awards_enriched": enriched_count,
                "movies_with_oscar_nomination": matched,
                "movies_with_oscar_win": winners,
                "movies_without_oscar_history": movies_count - matched,
            },
            "output_paths": {
                "oscars_nominations": str(out_noms),
                "movie_oscar_features": str(out_feat),
                "movies_awards_enriched": str(out_enriched),
            },
            "output_sizes_bytes": {
                "oscars_nominations": directory_size_bytes(str(out_noms)),
                "movie_oscar_features": directory_size_bytes(str(out_feat)),
                "movies_awards_enriched": directory_size_bytes(str(out_enriched)),
            },
            "validation_movie_oscar_features": {
                "passed": feat_validation.passed,
                "errors": feat_validation.errors,
                "warnings": feat_validation.warnings,
                "details": feat_validation.details,
            },
            "validation_movies_awards_enriched": {
                "passed": enriched_validation.passed,
                "errors": enriched_validation.errors,
                "warnings": enriched_validation.warnings,
                "details": enriched_validation.details,
            },
            "plan_path": str(plans_dir / "oscar_features_plan.txt"),
            "leakage_note": (
                "Oscar outcomes are post-release labels/features. Use them for "
                "awards-recognition modeling and analytics; exclude from "
                "pre-release hit prediction feature sets."
            ),
        }

        metrics_path = metrics_dir / "oscar_metrics.json"
        metrics_path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(metrics, indent=2))

        nominations.unpersist()
        oscar_features.unpersist()
        enriched.unpersist()
        return metrics
    finally:
        spark.stop()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
