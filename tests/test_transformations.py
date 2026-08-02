"""Transformation and join tests using local Spark and fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from cinescope.schemas import (
    MOVIES_RATINGS_OUTPUT_COLUMNS,
    TITLE_BASICS_SCHEMA,
    TITLE_RATINGS_SCHEMA,
)
from cinescope.transformations import (
    build_movies_ratings,
    cast_title_basics,
    cast_title_ratings,
    filter_movies,
    normalize_genres,
    prepare_title_basics,
    prepare_title_ratings,
    replace_imdb_nulls,
)


FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("cinescope-tests")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _read_fixture(spark: SparkSession, name: str, schema):
    path = FIXTURES / name
    return (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .schema(schema)
        .csv(str(path))
    )


def test_replace_imdb_nulls(spark):
    """Convert exact IMDb \\N string sentinels to Spark nulls."""
    df = _read_fixture(spark, "title.basics.tsv", TITLE_BASICS_SCHEMA)
    cleaned = replace_imdb_nulls(df)
    row = cleaned.filter(cleaned.tconst == "tt0000003").collect()[0]
    assert row.startYear is None
    assert row.endYear is None
    assert row.runtimeMinutes is None
    assert row.genres is None


def test_cast_title_basics_and_ratings(spark):
    """Cast year/runtime/rating/vote columns to the expected numeric types."""
    basics = replace_imdb_nulls(
        _read_fixture(spark, "title.basics.tsv", TITLE_BASICS_SCHEMA)
    )
    ratings = replace_imdb_nulls(
        _read_fixture(spark, "title.ratings.tsv", TITLE_RATINGS_SCHEMA)
    )
    typed_b = cast_title_basics(basics)
    typed_r = cast_title_ratings(ratings)

    movie = typed_b.filter(typed_b.tconst == "tt0000001").collect()[0]
    assert movie.startYear == 1999
    assert movie.runtimeMinutes == 120
    assert movie.isAdult == 0

    rating = typed_r.filter(typed_r.tconst == "tt0000001").collect()[0]
    assert rating.averageRating == pytest.approx(8.2)
    assert rating.numVotes == 1500


def test_filter_movies_excludes_non_movies(spark):
    """Keep titleType == movie and drop series/shorts from the bronze path."""
    df = cast_title_basics(
        replace_imdb_nulls(_read_fixture(spark, "title.basics.tsv", TITLE_BASICS_SCHEMA))
    )
    movies = filter_movies(df)
    types = {r.titleType for r in movies.select("titleType").distinct().collect()}
    assert types == {"movie"}
    ids = {r.tconst for r in movies.select("tconst").collect()}
    assert "tt0000002" not in ids
    assert "tt0000006" not in ids
    assert "tt0000001" in ids


def test_normalize_genres_splits_without_explode(spark):
    """Split comma-separated genres into arrays without exploding film rows."""
    df = replace_imdb_nulls(
        _read_fixture(spark, "title.basics.tsv", TITLE_BASICS_SCHEMA)
    )
    movies = normalize_genres(filter_movies(cast_title_basics(df)))
    row = movies.filter(movies.tconst == "tt0000001").collect()[0]
    assert row.genres == ["Drama", "Comedy"]
    null_genres = movies.filter(movies.tconst == "tt0000003").collect()[0]
    assert null_genres.genres is None
    assert movies.count() == 4  # still one row per movie


def test_join_rated_movies_and_output_columns(spark):
    """Inner-join movies to ratings and emit the bronze output column set."""
    movies = prepare_title_basics(
        _read_fixture(spark, "title.basics.tsv", TITLE_BASICS_SCHEMA)
    )
    ratings = prepare_title_ratings(
        _read_fixture(spark, "title.ratings.tsv", TITLE_RATINGS_SCHEMA)
    )
    joined = build_movies_ratings(movies, ratings)
    assert set(joined.columns) == set(MOVIES_RATINGS_OUTPUT_COLUMNS)

    ids = {r.tconst for r in joined.select("tconst").collect()}
    # Rated movie joins; unrated movie excluded by inner join; non-movies excluded.
    assert "tt0000001" in ids
    assert "tt0000004" in ids  # rated even if runtime unusual
    assert "tt0000005" not in ids  # unrated movie
    assert "tt0000002" not in ids  # tv series
    # Ratings fixture includes tt0000003 with null rating fields → inner join keeps it.
    assert "tt0000003" in ids
