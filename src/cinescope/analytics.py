"""Core analytical transforms for proposal §2.3 insights (local Spark)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_decade(df: DataFrame, year_col: str = "start_year") -> DataFrame:
    """Add integer decade column (e.g. 1994 → 1990)."""
    return df.withColumn(
        "decade",
        (F.floor(F.col(year_col) / 10) * 10).cast("int"),
    )


def genre_decade_stats(movies: DataFrame) -> DataFrame:
    """Median rating and vote volume by genre × decade (exploded genres)."""
    exploded = (
        with_decade(movies)
        .withColumn("genre", F.explode_outer("genres"))
        .filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
        .filter(F.col("decade").isNotNull())
    )
    return (
        exploded.groupBy("decade", "genre")
        .agg(
            F.count(F.lit(1)).alias("film_count"),
            F.expr("percentile_approx(average_rating, 0.5)").alias("median_rating"),
            F.sum("num_votes").alias("total_votes"),
            F.expr("percentile_approx(num_votes, 0.5)").alias("median_votes"),
        )
        .orderBy("decade", F.desc("film_count"))
    )


def director_prior_vs_rating(movies: DataFrame) -> DataFrame:
    """Films with non-null director prior rating — for scatter / correlation."""
    return (
        movies.filter(F.col("director_prior_rating_mean").isNotNull())
        .select(
            "tconst",
            "primary_title",
            "start_year",
            "average_rating",
            "num_votes",
            "director_prior_rating_mean",
            "director_prior_movie_count_mean",
            "director_prior_votes_sum",
        )
    )


def runtime_bucket_stats(
    movies: DataFrame,
    *,
    bucket_minutes: int = 15,
    min_runtime: int = 40,
    max_runtime: int = 240,
) -> DataFrame:
    """Median rating and volume by runtime bucket (sweet-spot analysis)."""
    clipped = movies.filter(
        F.col("runtime_minutes").isNotNull()
        & (F.col("runtime_minutes") >= min_runtime)
        & (F.col("runtime_minutes") <= max_runtime)
    ).withColumn(
        "runtime_bucket",
        (F.floor(F.col("runtime_minutes") / bucket_minutes) * bucket_minutes).cast(
            "int"
        ),
    )
    return (
        clipped.groupBy("runtime_bucket")
        .agg(
            F.count(F.lit(1)).alias("film_count"),
            F.expr("percentile_approx(average_rating, 0.5)").alias("median_rating"),
            F.avg("average_rating").alias("mean_rating"),
            F.sum("num_votes").alias("total_votes"),
        )
        .orderBy("runtime_bucket")
    )


def rating_anomalies(
    movies: DataFrame,
    *,
    high_rating: float = 8.0,
    vote_percentile: float = 0.10,
) -> DataFrame:
    """High rating with unusually low votes (niche vs mainstream outliers)."""
    threshold_row = movies.agg(
        F.expr(f"percentile_approx(num_votes, {vote_percentile})").alias("vote_cut")
    ).collect()[0]
    vote_cut = float(threshold_row["vote_cut"] or 0)
    return (
        movies.filter(
            (F.col("average_rating") >= high_rating)
            & (F.col("num_votes") <= vote_cut)
            & F.col("num_votes").isNotNull()
        )
        .select(
            "tconst",
            "primary_title",
            "start_year",
            "average_rating",
            "num_votes",
            "runtime_minutes",
        )
        .withColumn("vote_cut_used", F.lit(vote_cut))
        .orderBy(F.desc("average_rating"), "num_votes")
    )


def director_prior_correlation(movies: DataFrame) -> float | None:
    """Pearson correlation between director prior mean rating and film rating."""
    row = (
        movies.filter(
            F.col("director_prior_rating_mean").isNotNull()
            & F.col("average_rating").isNotNull()
        )
        .select(
            F.corr("director_prior_rating_mean", "average_rating").alias("corr")
        )
        .collect()[0]
    )
    value = row["corr"]
    return float(value) if value is not None else None
