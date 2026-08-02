"""Validation check tests using local Spark fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from cinescope.schemas import TITLE_BASICS_SCHEMA, TITLE_RATINGS_SCHEMA
from cinescope.transformations import (
    build_movies_ratings,
    prepare_title_basics,
    prepare_title_ratings,
)
from cinescope.validation import validate_movies_ratings


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("cinescope-validation-tests")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _joined(spark: SparkSession):
    basics = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .schema(TITLE_BASICS_SCHEMA)
        .csv(str(FIXTURES / "title.basics.tsv"))
    )
    ratings = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .schema(TITLE_RATINGS_SCHEMA)
        .csv(str(FIXTURES / "title.ratings.tsv"))
    )
    return build_movies_ratings(
        prepare_title_basics(basics),
        prepare_title_ratings(ratings),
    )


def test_validation_flags_invalid_rating(spark):
    """Fail validation when average_rating is outside the IMDb 1–10 range."""
    df = _joined(spark)
    result = validate_movies_ratings(df)
    assert result.details["row_count"] > 0
    assert result.details["invalid_average_rating"] >= 1
    assert not result.passed
    assert any("average_rating" in e for e in result.errors)


def test_validation_passes_for_clean_subset(spark, tmp_path):
    """Pass validation for a clean unique-tconst subset under the data root."""
    df = _joined(spark).filter(F.col("tconst") == "tt0000001")
    data_root = tmp_path / "cinescope-data"
    out = data_root / "bronze" / "movies_ratings"
    out.mkdir(parents=True)
    result = validate_movies_ratings(
        df,
        output_path=out,
        data_root=data_root,
        backend="local",
    )
    assert result.passed
    assert result.details["null_tconst"] == 0
    assert result.details["distinct_tconst"] == 1


def test_validation_rejects_output_outside_data_root(spark, tmp_path):
    """Reject outputs that are not under the configured data root."""
    df = _joined(spark).filter(F.col("tconst") == "tt0000001")
    data_root = tmp_path / "cinescope-data"
    data_root.mkdir()
    result = validate_movies_ratings(
        df,
        output_path=Path("/tmp/not-under-data-root"),
        data_root=data_root,
        backend="local",
    )
    assert not result.passed
    assert any("data root" in e for e in result.errors)


def test_required_columns_present(spark):
    """Report null counts for required bronze output columns."""
    df = _joined(spark).filter(F.col("tconst") == "tt0000001")
    result = validate_movies_ratings(df)
    assert "null_counts" in result.details
    assert "tconst" in result.details["null_counts"]
