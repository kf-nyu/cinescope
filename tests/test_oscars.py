"""Oscar nomination cleaning, aggregation, and enrichment tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from cinescope.schemas import OSCARS_NOMINATIONS_SCHEMA
from cinescope.transformations import (
    aggregate_movie_oscar_features,
    build_movies_awards_enriched,
    prepare_oscars_nominations,
)
from cinescope.validation import validate_movie_oscar_features, validate_movies_awards_enriched


FIXTURES = Path(__file__).parent / "fixtures"

MOVIES_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("primary_title", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("start_year", IntegerType(), True),
        StructField("runtime_minutes", IntegerType(), True),
        StructField("genres", ArrayType(StringType()), True),
        StructField("average_rating", DoubleType(), True),
        StructField("num_votes", LongType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("cinescope-oscar-tests")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _oscars(spark: SparkSession):
    raw = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .schema(OSCARS_NOMINATIONS_SCHEMA)
        .csv(str(FIXTURES / "oscars.tsv"))
    )
    return prepare_oscars_nominations(raw)


def test_explode_multi_film_ids_and_drop_missing(spark):
    """Explode pipe-separated FilmIds and drop rows without a usable tconst."""
    noms = _oscars(spark)
    ids = {r.tconst for r in noms.select("tconst").distinct().collect()}
    assert "tt0019071" in ids
    assert "tt0019553" in ids
    # SciTech row without film id dropped
    assert noms.filter(F.col("nominee_name") == "Some Engineer").count() == 0


def test_winner_flag_parsing(spark):
    """Map Winner True/empty strings to boolean is_winner flags."""
    noms = _oscars(spark)
    shape = noms.filter(
        (F.col("tconst") == "tt5580390")
        & (F.col("canonical_category") == "BEST PICTURE")
    ).collect()[0]
    assert shape.is_winner is True
    get_out_bp = noms.filter(
        (F.col("tconst") == "tt5052448")
        & (F.col("canonical_category") == "BEST PICTURE")
    ).collect()[0]
    assert get_out_bp.is_winner is False


def test_aggregate_one_row_per_movie(spark):
    """Aggregate nomination rows to exactly one film-level Oscar feature row."""
    features = aggregate_movie_oscar_features(_oscars(spark))
    assert features.count() == features.select("tconst").distinct().count()
    shape = features.filter(F.col("tconst") == "tt5580390").collect()[0]
    assert shape.oscar_nomination_count == 2  # picture + directing
    assert shape.oscar_win_count == 2
    assert shape.best_picture_won is True
    assert shape.directing_win_count == 1

    get_out = features.filter(F.col("tconst") == "tt5052448").collect()[0]
    assert get_out.was_oscar_nominated is True
    assert get_out.was_oscar_winner is True  # writing win
    assert get_out.best_picture_nominated is True
    assert get_out.best_picture_won is False
    assert get_out.writing_win_count == 1
    assert get_out.acting_nomination_count == 1


def test_awards_enriched_keeps_non_oscar_movies(spark):
    """Left-join Oscars so non-nominated movies remain with zeroed award counts."""
    movies_path = FIXTURES / "movies_for_oscars.tsv"
    movies = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .schema(
            StructType(
                [
                    StructField("tconst", StringType(), True),
                    StructField("primary_title", StringType(), True),
                    StructField("original_title", StringType(), True),
                    StructField("start_year", IntegerType(), True),
                    StructField("runtime_minutes", IntegerType(), True),
                    StructField("genres", StringType(), True),
                    StructField("average_rating", DoubleType(), True),
                    StructField("num_votes", LongType(), True),
                ]
            )
        )
        .csv(str(movies_path))
        .withColumn("genres", F.split(F.col("genres"), ","))
    )
    features = aggregate_movie_oscar_features(_oscars(spark))
    enriched = build_movies_awards_enriched(movies, features)
    assert enriched.count() == 2
    none = enriched.filter(F.col("tconst") == "tt0000001").collect()[0]
    assert none.oscar_nomination_count == 0
    assert none.was_oscar_nominated is False
    result = validate_movies_awards_enriched(enriched, baseline_count=2)
    assert result.passed


def test_oscar_feature_validation(spark):
    """Confirm film-level Oscar features pass uniqueness and count checks."""
    features = aggregate_movie_oscar_features(_oscars(spark))
    result = validate_movie_oscar_features(features)
    assert result.passed
