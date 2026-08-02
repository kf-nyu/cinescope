"""Unit tests for analytics helpers (local Spark fixtures)."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from cinescope.analytics import (
    genre_decade_stats,
    rating_anomalies,
    runtime_bucket_stats,
)
from cinescope.ml.features import (
    HIT_RATING_MIN,
    HIT_VOTES_MIN,
    add_genre_flags,
    with_hit_label,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("cinescope-analytics-tests")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _movies(spark: SparkSession):
    schema = T.StructType(
        [
            T.StructField("tconst", T.StringType(), False),
            T.StructField("primary_title", T.StringType(), True),
            T.StructField("start_year", T.IntegerType(), True),
            T.StructField("runtime_minutes", T.IntegerType(), True),
            T.StructField("genres", T.ArrayType(T.StringType()), True),
            T.StructField("average_rating", T.DoubleType(), True),
            T.StructField("num_votes", T.LongType(), True),
        ]
    )
    rows = [
        ("tt1", "A", 1994, 90, ["Drama", "Crime"], 8.5, 50),
        ("tt2", "B", 1995, 120, ["Drama"], 7.2, 5000),
        ("tt3", "C", 2001, 100, ["Comedy"], 6.0, 2000),
        ("tt4", "D", 2002, 45, ["Horror"], 8.2, 40),
        ("tt5", "E", 2010, 150, ["Action"], 7.5, 10000),
    ]
    return spark.createDataFrame(rows, schema=schema)


def test_with_decade_and_genre_stats(spark):
    """Genre×decade aggregation returns one row per decade-genre pair."""
    stats = genre_decade_stats(_movies(spark)).collect()
    decades = {r["decade"] for r in stats}
    assert 1990 in decades and 2000 in decades
    drama_1990 = [
        r for r in stats if r["decade"] == 1990 and r["genre"] == "Drama"
    ]
    assert len(drama_1990) == 1
    assert drama_1990[0]["film_count"] == 2


def test_runtime_buckets(spark):
    """Runtime bucketing groups films into fixed-width bins."""
    buckets = {
        r["runtime_bucket"]: r["film_count"]
        for r in runtime_bucket_stats(_movies(spark), bucket_minutes=15).collect()
    }
    assert 90 in buckets or 105 in buckets


def test_rating_anomalies_high_rating_low_votes(spark):
    """Anomaly helper flags high-rated, low-vote titles."""
    outliers = rating_anomalies(
        _movies(spark), high_rating=8.0, vote_percentile=0.5
    ).collect()
    ids = {r["tconst"] for r in outliers}
    assert "tt1" in ids or "tt4" in ids


def test_hit_label_and_genre_flags(spark):
    """Hit label and genre flags follow provisional thresholds."""
    df = with_hit_label(add_genre_flags(_movies(spark)))
    rows = {r["tconst"]: r for r in df.collect()}
    assert rows["tt2"]["is_hit"] == 1  # 7.2 and 5000
    assert rows["tt1"]["is_hit"] == 0  # high rating but low votes
    assert rows["tt2"]["genre_drama"] == 1
    assert HIT_RATING_MIN == 7.0 and HIT_VOTES_MIN == 1000
