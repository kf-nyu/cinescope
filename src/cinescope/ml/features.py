"""MLlib feature helpers — pre-release features only (leakage-safe for hit model)."""

from __future__ import annotations

from typing import Sequence

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Provisional hit definition (may be retuned after evaluation).
HIT_RATING_MIN = 7.0
HIT_VOTES_MIN = 1000

# Must never enter hit / awards *feature* vectors.
POST_RELEASE_EXCLUDE = (
    "average_rating",
    "num_votes",
    "is_hit",
)

OSCAR_EXCLUDE = (
    "oscar_nomination_count",
    "oscar_win_count",
    "was_oscar_winner",
    "was_oscar_nominated",
    "first_oscar_ceremony",
    "last_oscar_ceremony",
    "best_picture_nomination_count",
    "best_picture_win_count",
    "best_picture_won",
    "best_picture_nominated",
    "acting_nomination_count",
    "acting_win_count",
    "directing_nomination_count",
    "directing_win_count",
    "writing_nomination_count",
    "writing_win_count",
)

# Numeric / boolean columns safe as pre-release predictors when present.
PRE_RELEASE_NUMERIC = (
    "start_year",
    "runtime_minutes",
    "principal_count",
    "cast_count",
    "director_count",
    "writer_count",
    "producer_count",
    "cast_prior_movie_count_mean",
    "cast_prior_rating_mean",
    "cast_prior_votes_sum",
    "director_prior_movie_count_mean",
    "director_prior_rating_mean",
    "director_prior_votes_sum",
    "writer_prior_movie_count_mean",
    "writer_prior_rating_mean",
    "principal_prior_movie_count_mean",
    "principal_prior_rating_mean",
    "principal_prior_votes_sum",
    "principal_max_prior_rating",
    "known_cast_count",
    "known_director_count",
)

PRE_RELEASE_BOOL = (
    "has_known_cast",
    "has_known_director",
)

# Dominant genres for multi-hot (stable list for reproducibility).
TOP_GENRES = (
    "Drama",
    "Comedy",
    "Action",
    "Romance",
    "Crime",
    "Thriller",
    "Horror",
    "Adventure",
    "Documentary",
    "Animation",
)


def with_hit_label(
    df: DataFrame,
    *,
    rating_min: float = HIT_RATING_MIN,
    votes_min: int = HIT_VOTES_MIN,
) -> DataFrame:
    """Binary hit label from post-release rating/votes (label only, not a feature)."""
    return df.withColumn(
        "is_hit",
        (
            (F.col("average_rating") >= rating_min)
            & (F.col("num_votes") >= votes_min)
        ).cast("int"),
    )


def with_awards_label(df: DataFrame) -> DataFrame:
    """Binary awards-recognition label: any Oscar nomination."""
    return df.withColumn(
        "label_awards",
        F.coalesce(F.col("was_oscar_nominated").cast("int"), F.lit(0)),
    )


def add_genre_flags(
    df: DataFrame, genres: Sequence[str] = TOP_GENRES
) -> DataFrame:
    """Add genre_<name> 0/1 columns from array genres."""
    out = df
    for g in genres:
        col_name = f"genre_{g.lower()}"
        out = out.withColumn(
            col_name,
            F.array_contains(F.col("genres"), g).cast("int"),
        )
        out = out.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))
    return out


def available_pre_release_columns(df: DataFrame) -> list[str]:
    """Intersect configured pre-release columns with the frame schema."""
    present = set(df.columns)
    cols: list[str] = []
    for c in PRE_RELEASE_NUMERIC:
        if c in present:
            cols.append(c)
    for c in PRE_RELEASE_BOOL:
        if c in present:
            cols.append(c)
    for g in TOP_GENRES:
        name = f"genre_{g.lower()}"
        if name in present:
            cols.append(name)
    return cols


def prepare_modeling_frame(df: DataFrame) -> DataFrame:
    """Hit + awards labels and genre flags; booleans cast to int."""
    out = with_hit_label(df)
    out = with_awards_label(out)
    out = add_genre_flags(out)
    for c in PRE_RELEASE_BOOL:
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast("int"))
    return out


def build_feature_pipeline(
    feature_cols: Sequence[str],
    *,
    label_col: str,
) -> Pipeline:
    """Impute numeric nulls → assemble features. Label column must already exist."""
    # Imputer requires DoubleType-ish; cast in notebook or here via select.
    imputer = Imputer(
        inputCols=list(feature_cols),
        outputCols=[f"{c}_imp" for c in feature_cols],
        strategy="mean",
    )
    assembler = VectorAssembler(
        inputCols=[f"{c}_imp" for c in feature_cols],
        outputCol="features",
        handleInvalid="keep",
    )
    return Pipeline(stages=[imputer, assembler])


def leakage_notes() -> dict:
    """Document label rules and feature exclusions for metrics JSON."""
    return {
        "hit_label": {
            "rule": f"average_rating >= {HIT_RATING_MIN} AND num_votes >= {HIT_VOTES_MIN}",
            "column": "is_hit",
        },
        "awards_label": {
            "rule": "was_oscar_nominated",
            "column": "label_awards",
        },
        "excluded_from_features": list(POST_RELEASE_EXCLUDE) + list(OSCAR_EXCLUDE),
        "pre_release_numeric": list(PRE_RELEASE_NUMERIC),
        "pre_release_bool": list(PRE_RELEASE_BOOL),
        "top_genres": list(TOP_GENRES),
    }
