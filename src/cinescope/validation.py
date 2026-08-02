"""Validation checks for bronze and silver CineScope tables."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cinescope.schemas import MOVIES_RATINGS_OUTPUT_COLUMNS


@dataclass
class ValidationResult:
    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def raise_if_failed(self) -> None:
        if not self.passed:
            joined = "; ".join(self.errors)
            raise RuntimeError(f"Validation failed: {joined}")


def _check_output_path(
    errors: list[str],
    details: dict[str, Any],
    output_path: Path | str | None,
    data_root: Path | str | None = None,
    *,
    backend: str = "local",
) -> None:
    if output_path is None:
        return
    out = str(output_path)
    details["output_path"] = out
    details["storage_backend"] = backend
    if backend == "hdfs":
        if not (out.startswith("hdfs://") or out.startswith("/user/")):
            errors.append(f"output path is not an HDFS URI: {out}")
        if data_root is not None and not out.rstrip("/").startswith(
            str(data_root).rstrip("/")
        ):
            errors.append(f"output path is not under data root {data_root}: {out}")
        return

    resolved = Path(out).expanduser().resolve()
    details["output_path"] = str(resolved)
    repo = Path(__file__).resolve().parents[2]
    try:
        resolved.relative_to(repo)
        errors.append(f"output path must not be inside the Git repository: {resolved}")
    except ValueError:
        pass
    if data_root is not None:
        root = Path(str(data_root)).expanduser().resolve()
        try:
            resolved.relative_to(root)
        except ValueError:
            errors.append(f"output path is not under data root {root}: {resolved}")


def _check_unique_tconst(
    df: DataFrame, errors: list[str], details: dict[str, Any]
) -> int:
    row_count = df.count()
    details["row_count"] = row_count
    if "tconst" not in df.columns:
        errors.append("missing tconst column")
        return row_count
    null_tconst = df.filter(F.col("tconst").isNull()).count()
    details["null_tconst"] = null_tconst
    if null_tconst:
        errors.append(f"tconst has {null_tconst} null values")
    distinct_tconst = df.select("tconst").distinct().count()
    details["distinct_tconst"] = distinct_tconst
    if distinct_tconst != row_count:
        errors.append(
            f"tconst is not unique ({distinct_tconst} distinct / {row_count} rows)"
        )
    return row_count


def validate_movies_ratings(
    df: DataFrame,
    *,
    output_path: Path | str | None = None,
    data_root: Path | str | None = None,
    ssd_volume: Path | str | None = None,  # backward-compatible alias for data_root
    backend: str = "local",
    min_year: int = 1870,
    max_year: int = 2035,
) -> ValidationResult:
    """Run critical quality checks on the joined movies/ratings table."""
    errors: list[str] = []
    warnings: list[str] = []
    details: dict[str, Any] = {}

    missing_cols = [
        c for c in MOVIES_RATINGS_OUTPUT_COLUMNS if c not in df.columns
    ]
    if missing_cols:
        errors.append(f"missing required columns: {missing_cols}")

    row_count = _check_unique_tconst(df, errors, details)
    if row_count == 0:
        errors.append("joined count is zero")

    if "average_rating" in df.columns:
        bad_rating = df.filter(
            F.col("average_rating").isNotNull()
            & ((F.col("average_rating") < 1) | (F.col("average_rating") > 10))
        ).count()
        details["invalid_average_rating"] = bad_rating
        if bad_rating:
            errors.append(f"average_rating outside [1, 10]: {bad_rating} rows")

    if "num_votes" in df.columns:
        bad_votes = df.filter(
            F.col("num_votes").isNotNull() & (F.col("num_votes") < 0)
        ).count()
        details["invalid_num_votes"] = bad_votes
        if bad_votes:
            errors.append(f"num_votes negative: {bad_votes} rows")

    if "runtime_minutes" in df.columns:
        bad_runtime = df.filter(
            F.col("runtime_minutes").isNotNull() & (F.col("runtime_minutes") <= 0)
        ).count()
        details["invalid_runtime_minutes"] = bad_runtime
        if bad_runtime:
            warnings.append(
                f"runtime_minutes non-positive when present: {bad_runtime} rows"
            )

    if "start_year" in df.columns:
        bad_year = df.filter(
            F.col("start_year").isNotNull()
            & ((F.col("start_year") < min_year) | (F.col("start_year") > max_year))
        ).count()
        details["invalid_start_year"] = bad_year
        if bad_year:
            warnings.append(
                f"start_year outside [{min_year}, {max_year}]: {bad_year} rows"
            )

    root = data_root if data_root is not None else ssd_volume
    _check_output_path(errors, details, output_path, root, backend=backend)

    null_counts = {}
    for col_name in MOVIES_RATINGS_OUTPUT_COLUMNS:
        if col_name in df.columns:
            null_counts[col_name] = df.filter(F.col(col_name).isNull()).count()
    details["null_counts"] = null_counts

    return ValidationResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        details=details,
    )


def validate_cast_crew_features(
    df: DataFrame,
    *,
    output_path: Path | str | None = None,
    data_root: Path | str | None = None,
    ssd_volume: Path | str | None = None,
    backend: str = "local",
) -> ValidationResult:
    """Validate film-level cast/crew feature table."""
    errors: list[str] = []
    warnings: list[str] = []
    details: dict[str, Any] = {}

    _check_unique_tconst(df, errors, details)
    root = data_root if data_root is not None else ssd_volume
    _check_output_path(errors, details, output_path, root, backend=backend)

    for col_name in (
        "principal_count",
        "cast_count",
        "director_count",
        "writer_count",
        "producer_count",
        "known_cast_count",
        "known_director_count",
    ):
        if col_name in df.columns:
            bad = df.filter(F.col(col_name) < 0).count()
            details[f"negative_{col_name}"] = bad
            if bad:
                errors.append(f"{col_name} has {bad} negative values")

    for col_name in (
        "principal_prior_movie_count_mean",
        "cast_prior_movie_count_mean",
        "director_prior_movie_count_mean",
    ):
        if col_name in df.columns:
            bad = df.filter(F.col(col_name).isNotNull() & (F.col(col_name) < 0)).count()
            details[f"negative_{col_name}"] = bad
            if bad:
                errors.append(f"{col_name} has {bad} negative values")

    return ValidationResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        details=details,
    )


def validate_movies_enriched(
    df: DataFrame,
    *,
    baseline_count: int,
    output_path: Path | str | None = None,
    data_root: Path | str | None = None,
    ssd_volume: Path | str | None = None,
    backend: str = "local",
) -> ValidationResult:
    """Validate enriched movies table preserves baseline row count and uniqueness."""
    errors: list[str] = []
    warnings: list[str] = []
    details: dict[str, Any] = {}

    row_count = _check_unique_tconst(df, errors, details)
    details["baseline_count"] = baseline_count
    if row_count != baseline_count:
        errors.append(
            f"enriched row count {row_count} != baseline movie count {baseline_count}"
        )

    for col_name in MOVIES_RATINGS_OUTPUT_COLUMNS:
        if col_name not in df.columns:
            errors.append(f"enriched table missing baseline column: {col_name}")

    root = data_root if data_root is not None else ssd_volume
    _check_output_path(errors, details, output_path, root, backend=backend)
    return ValidationResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        details=details,
    )


def validate_movie_oscar_features(
    df: DataFrame,
    *,
    output_path: Path | str | None = None,
    data_root: Path | str | None = None,
    ssd_volume: Path | str | None = None,
    backend: str = "local",
) -> ValidationResult:
    """Validate film-level Oscar feature table."""
    errors: list[str] = []
    warnings: list[str] = []
    details: dict[str, Any] = {}

    _check_unique_tconst(df, errors, details)
    root = data_root if data_root is not None else ssd_volume
    _check_output_path(errors, details, output_path, root, backend=backend)

    if "oscar_nomination_count" in df.columns and "oscar_win_count" in df.columns:
        bad = df.filter(F.col("oscar_win_count") > F.col("oscar_nomination_count")).count()
        details["wins_exceed_nominations"] = bad
        if bad:
            errors.append(
                f"oscar_win_count exceeds oscar_nomination_count in {bad} rows"
            )

    for col_name in (
        "oscar_nomination_count",
        "oscar_win_count",
        "best_picture_nomination_count",
        "acting_nomination_count",
    ):
        if col_name in df.columns:
            bad = df.filter(F.col(col_name) < 0).count()
            details[f"negative_{col_name}"] = bad
            if bad:
                errors.append(f"{col_name} has {bad} negative values")

    return ValidationResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        details=details,
    )


def validate_movies_awards_enriched(
    df: DataFrame,
    *,
    baseline_count: int,
    output_path: Path | str | None = None,
    data_root: Path | str | None = None,
    ssd_volume: Path | str | None = None,
    backend: str = "local",
) -> ValidationResult:
    """Validate awards-enriched table matches baseline movie count."""
    return validate_movies_enriched(
        df,
        baseline_count=baseline_count,
        output_path=output_path,
        data_root=data_root,
        ssd_volume=ssd_volume,
        backend=backend,
    )


def directory_size_bytes(path: Path | str) -> int:
    """Sum file sizes under a local directory (0 for missing / non-local paths)."""
    text = str(path)
    if text.startswith("hdfs://"):
        return _hdfs_size_bytes(text)
    p = Path(text)
    if not p.exists():
        return 0
    total = 0
    for root, _dirs, files in os_walk(p):
        for name in files:
            total += (root / name).stat().st_size
    return total


def _hdfs_size_bytes(uri: str) -> int:
    import subprocess

    result = subprocess.run(
        ["hdfs", "dfs", "-du", "-s", uri],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return 0
    # Format: "<size>  <path>" or "<size>  <disk_space>  <path>"
    parts = result.stdout.strip().split()
    try:
        return int(parts[0])
    except (ValueError, IndexError):
        return 0


def os_walk(path: Path):
    import os

    for root, dirs, files in os.walk(path):
        yield Path(root), dirs, files
