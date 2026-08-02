"""Cast/crew feature engineering tests using local Spark fixtures."""

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

from cinescope.schemas import NAME_BASICS_SCHEMA, TITLE_PRINCIPALS_SCHEMA
from cinescope.transformations import (
    aggregate_cast_crew_features,
    build_known_people_lookup,
    build_movies_enriched,
    filter_relevant_principals,
    person_movie_history,
    prepare_name_basics,
    prepare_title_principals,
    replace_imdb_nulls,
    restrict_principals_to_movies,
)
from cinescope.validation import validate_cast_crew_features, validate_movies_enriched


FIXTURES = Path(__file__).parent / "fixtures"

MOVIES_SCHEMA = StructType(
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


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("cinescope-cast-crew-tests")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _movies(spark: SparkSession):
    df = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .schema(MOVIES_SCHEMA)
        .csv(str(FIXTURES / "movies_ratings.tsv"))
    )
    return df.withColumn(
        "genres",
        F.when(F.col("genres").isNull(), F.lit(None).cast(ArrayType(StringType()))).otherwise(
            F.split(F.col("genres"), ",")
        ),
    )


def _principals(spark: SparkSession):
    raw = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .schema(TITLE_PRINCIPALS_SCHEMA)
        .csv(str(FIXTURES / "title.principals.tsv"))
    )
    return prepare_title_principals(raw)


def _names(spark: SparkSession):
    raw = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .schema(NAME_BASICS_SCHEMA)
        .csv(str(FIXTURES / "name.basics.tsv"))
    )
    return prepare_name_basics(raw)


def test_filter_relevant_principals_drops_self(spark):
    """Keep filmmaking roles and drop irrelevant categories such as self."""
    raw = (
        spark.read.option("header", "true")
        .option("sep", "\t")
        .schema(TITLE_PRINCIPALS_SCHEMA)
        .csv(str(FIXTURES / "title.principals.tsv"))
    )
    cleaned = filter_relevant_principals(replace_imdb_nulls(raw))
    cats = {r.category for r in cleaned.select("category").distinct().collect()}
    assert "self" not in cats
    assert "actress" in cats
    assert "director" in cats


def test_missing_birth_year_and_job_characters(spark):
    """Preserve IMDb \\N sentinels as null for birth year, job, and characters."""
    names = _names(spark)
    bob = names.filter(F.col("nconst") == "nm1000002").collect()[0]
    assert bob.birth_year is None
    principals = _principals(spark)
    row = principals.filter(
        (F.col("tconst") == "tt1000001") & (F.col("nconst") == "nm1000003")
    ).collect()[0]
    assert row.job is None
    assert row.characters is None


def test_person_with_no_prior_movies(spark):
    """People with no earlier rated films get zero/null prior reputation features."""
    movies = _movies(spark)
    principals = restrict_principals_to_movies(_principals(spark), movies)
    hist = person_movie_history(principals, movies)
    newbie = hist.filter(
        (F.col("nconst") == "nm1000005") & (F.col("tconst") == "tt1000003")
    ).collect()[0]
    assert newbie.prior_movie_count == 0
    assert newbie.prior_average_rating is None
    assert newbie.prior_total_votes == 0


def test_current_movie_excluded_from_historical_features(spark):
    """Exclude the current movie (and later years) from leakage-safe priors."""
    movies = _movies(spark)
    principals = restrict_principals_to_movies(_principals(spark), movies)
    hist = person_movie_history(principals, movies)

    # Alice: early hit 2000 (8.0), mid 2010 — at mid career, prior should be only 2000 film.
    mid = hist.filter(
        (F.col("nconst") == "nm1000001") & (F.col("tconst") == "tt1000002")
    ).collect()[0]
    assert mid.prior_movie_count == 1
    assert mid.prior_average_rating == pytest.approx(8.0)
    assert mid.prior_total_votes == 5000
    assert mid.prior_highly_rated_movie_count == 1

    # Dana director across three films: at 2020 film, priors are 2000 and 2010 only.
    later = hist.filter(
        (F.col("nconst") == "nm1000003") & (F.col("tconst") == "tt1000003")
    ).collect()[0]
    assert later.prior_movie_count == 2
    assert later.prior_average_rating == pytest.approx(7.75)
    # Must not include the current movie's 6.5 rating in the average.
    assert later.prior_average_rating != pytest.approx(6.5)


def test_multiple_roles_and_aggregation_one_row_per_movie(spark):
    """Aggregate multi-role principals to one row per movie with distinct counts."""
    movies = _movies(spark)
    principals = restrict_principals_to_movies(_principals(spark), movies)
    hist = person_movie_history(principals, movies)
    known = build_known_people_lookup(hist)
    features = aggregate_cast_crew_features(principals, hist, known)

    assert features.count() == features.select("tconst").distinct().count()
    later = features.filter(F.col("tconst") == "tt1000003").collect()[0]
    assert later.director_count == 1
    assert later.writer_count == 1
    assert later.cast_count == 1
    # Dana appears as director and writer; principal_count counts distinct people.
    assert later.principal_count == 2


def test_movie_with_no_principals_kept_in_enriched(spark):
    """Keep movies without principals in the enriched table with zeroed counts."""
    movies = _movies(spark)
    principals = restrict_principals_to_movies(_principals(spark), movies)
    hist = person_movie_history(principals, movies)
    known = build_known_people_lookup(hist)
    features = aggregate_cast_crew_features(principals, hist, known)
    enriched = build_movies_enriched(movies, features)

    assert enriched.count() == movies.count()
    orphan = enriched.filter(F.col("tconst") == "tt1000004").collect()[0]
    assert orphan.principal_count == 0
    assert orphan.cast_count == 0
    assert orphan.has_known_cast is False

    result = validate_movies_enriched(enriched, baseline_count=movies.count())
    assert result.passed


def test_cast_crew_validation_unique_tconst(spark):
    """Validate that cast/crew feature output has unique non-null tconst values."""
    movies = _movies(spark)
    principals = restrict_principals_to_movies(_principals(spark), movies)
    hist = person_movie_history(principals, movies)
    known = build_known_people_lookup(hist)
    features = aggregate_cast_crew_features(principals, hist, known)
    result = validate_cast_crew_features(features)
    assert result.passed
    assert result.details["distinct_tconst"] == result.details["row_count"]
