"""Reusable Spark transformations for IMDb cleaning and cast/crew features."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from cinescope.schemas import (
    CAST_CATEGORIES,
    HIGHLY_RATED_THRESHOLD,
    KNOWN_PERSON_MIN_AVG_RATING,
    KNOWN_PERSON_MIN_MOVIES,
    KNOWN_PERSON_MIN_TOTAL_VOTES,
    RELEVANT_PRINCIPAL_CATEGORIES,
)


IMDB_NULL = r"\N"


def replace_imdb_nulls(df: DataFrame) -> DataFrame:
    """Convert exact IMDb ``\\N`` string sentinels to Spark null."""
    for field in df.schema.fields:
        col = F.col(field.name)
        df = df.withColumn(
            field.name,
            F.when(col == IMDB_NULL, F.lit(None)).otherwise(col),
        )
    return df


def cast_title_basics(df: DataFrame) -> DataFrame:
    """Cast title.basics string columns to typed values after null replacement."""
    return (
        df.withColumn("isAdult", F.col("isAdult").cast("int"))
        .withColumn("startYear", F.col("startYear").cast("int"))
        .withColumn("endYear", F.col("endYear").cast("int"))
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast("int"))
    )


def cast_title_ratings(df: DataFrame) -> DataFrame:
    """Cast title.ratings string columns to typed values after null replacement."""
    return df.withColumn(
        "averageRating", F.col("averageRating").cast("double")
    ).withColumn("numVotes", F.col("numVotes").cast("long"))


def cast_title_principals(df: DataFrame) -> DataFrame:
    """Cast principals fields and normalize to snake_case column names."""
    return (
        df.withColumn("ordering", F.col("ordering").cast("int"))
        .withColumnRenamed("tconst", "tconst")
        .withColumnRenamed("nconst", "nconst")
        .withColumnRenamed("category", "category")
        .withColumnRenamed("job", "job")
        .withColumnRenamed("characters", "characters")
        .withColumnRenamed("ordering", "ordering")
    )


def cast_name_basics(df: DataFrame) -> DataFrame:
    """Cast name.basics numeric years after null replacement."""
    return df.withColumn("birthYear", F.col("birthYear").cast("int")).withColumn(
        "deathYear", F.col("deathYear").cast("int")
    )


def normalize_name_fields(df: DataFrame) -> DataFrame:
    """Normalize name.basics to snake_case and split list-like string fields."""
    return (
        df.withColumnRenamed("primaryName", "primary_name")
        .withColumnRenamed("birthYear", "birth_year")
        .withColumnRenamed("deathYear", "death_year")
        .withColumnRenamed("primaryProfession", "primary_profession")
        .withColumnRenamed("knownForTitles", "known_for_titles")
        .withColumn(
            "primary_profession",
            F.when(
                F.col("primary_profession").isNull()
                | (F.trim(F.col("primary_profession")) == ""),
                F.lit(None).cast("array<string>"),
            ).otherwise(F.split(F.col("primary_profession"), ",")),
        )
        .withColumn(
            "known_for_titles",
            F.when(
                F.col("known_for_titles").isNull()
                | (F.trim(F.col("known_for_titles")) == ""),
                F.lit(None).cast("array<string>"),
            ).otherwise(F.split(F.col("known_for_titles"), ",")),
        )
    )


def filter_relevant_principals(df: DataFrame) -> DataFrame:
    """Keep filmmaking roles used for reputation / cast-crew features."""
    return df.filter(F.col("category").isin(list(RELEVANT_PRINCIPAL_CATEGORIES)))


def filter_movies(df: DataFrame) -> DataFrame:
    """Keep only feature-film rows (``titleType == movie``)."""
    return df.filter(F.col("titleType") == "movie")


def normalize_genres(df: DataFrame) -> DataFrame:
    """Split comma-separated genres into an array without exploding rows."""
    return df.withColumn(
        "genres",
        F.when(
            F.col("genres").isNull() | (F.trim(F.col("genres")) == ""),
            F.lit(None).cast("array<string>"),
        ).otherwise(F.split(F.col("genres"), ",")),
    )


def prepare_title_basics(df: DataFrame) -> DataFrame:
    """Full title.basics cleaning pipeline for the baseline job."""
    cleaned = replace_imdb_nulls(df)
    typed = cast_title_basics(cleaned)
    movies = filter_movies(typed)
    return normalize_genres(movies)


def prepare_title_ratings(df: DataFrame) -> DataFrame:
    """Full title.ratings cleaning pipeline for the baseline job."""
    return cast_title_ratings(replace_imdb_nulls(df))


def prepare_title_principals(df: DataFrame) -> DataFrame:
    """Clean principals: nulls, casts, relevant-role filter."""
    return filter_relevant_principals(cast_title_principals(replace_imdb_nulls(df)))


def prepare_name_basics(df: DataFrame) -> DataFrame:
    """Clean names: nulls, casts, snake_case normalization."""
    return normalize_name_fields(cast_name_basics(replace_imdb_nulls(df)))


def build_movies_ratings(movies: DataFrame, ratings: DataFrame) -> DataFrame:
    """Join cleaned movies with ratings and project the bronze output schema."""
    ratings_proj = ratings.select(
        F.col("tconst"),
        F.col("averageRating").alias("average_rating"),
        F.col("numVotes").alias("num_votes"),
    )
    movies_proj = movies.select(
        F.col("tconst"),
        F.col("primaryTitle").alias("primary_title"),
        F.col("originalTitle").alias("original_title"),
        F.col("startYear").alias("start_year"),
        F.col("runtimeMinutes").alias("runtime_minutes"),
        F.col("genres"),
    )
    return movies_proj.join(ratings_proj, on="tconst", how="inner")


def restrict_principals_to_movies(
    principals: DataFrame, movies: DataFrame
) -> DataFrame:
    """Keep principals whose ``tconst`` appears in the rated-movies table."""
    movie_ids = F.broadcast(movies.select("tconst").distinct())
    return principals.join(movie_ids, on="tconst", how="inner")


def person_movie_history(principals: DataFrame, movies: DataFrame) -> DataFrame:
    """Build distinct person-movie rows with leakage-safe prior reputation features.

    Priors use only rated movies with ``start_year`` strictly less than the
    current movie's year (Spark ``rangeBetween(..., -1)``). The current movie's
    own rating/votes are never included.
    """
    person_movies = (
        principals.select("tconst", "nconst")
        .dropna(subset=["tconst", "nconst"])
        .dropDuplicates(["tconst", "nconst"])
        .join(
            movies.select(
                "tconst",
                "start_year",
                "average_rating",
                "num_votes",
            ),
            on="tconst",
            how="inner",
        )
    )

    # Rows without a usable year cannot form a strict temporal history.
    with_year = person_movies.filter(F.col("start_year").isNotNull())
    without_year = person_movies.filter(F.col("start_year").isNull()).select(
        "tconst",
        "nconst",
        "start_year",
        "average_rating",
        "num_votes",
        F.lit(0).cast("long").alias("prior_movie_count"),
        F.lit(None).cast("double").alias("prior_average_rating"),
        F.lit(0).cast("long").alias("prior_total_votes"),
        F.lit(0).cast("long").alias("prior_highly_rated_movie_count"),
    )

    w = (
        Window.partitionBy("nconst")
        .orderBy(F.col("start_year").asc())
        .rangeBetween(Window.unboundedPreceding, -1)
    )
    highly_rated = (
        F.col("average_rating") >= F.lit(HIGHLY_RATED_THRESHOLD)
    ).cast("int")

    hist = (
        with_year.withColumn("prior_movie_count", F.count(F.lit(1)).over(w).cast("long"))
        .withColumn("prior_average_rating", F.avg("average_rating").over(w))
        .withColumn("prior_total_votes", F.coalesce(F.sum("num_votes").over(w), F.lit(0)).cast("long"))
        .withColumn(
            "prior_highly_rated_movie_count",
            F.coalesce(F.sum(highly_rated).over(w), F.lit(0)).cast("long"),
        )
        .select(
            "tconst",
            "nconst",
            "start_year",
            "average_rating",
            "num_votes",
            "prior_movie_count",
            "prior_average_rating",
            "prior_total_votes",
            "prior_highly_rated_movie_count",
        )
    )
    return hist.unionByName(without_year)


def build_known_people_lookup(person_history: DataFrame) -> DataFrame:
    """Small data-derived lookup of well-established people for broadcast join demo.

    Rule (documented): career aggregates over rated movies in the bronze table —
    at least ``KNOWN_PERSON_MIN_MOVIES`` movies, ``KNOWN_PERSON_MIN_TOTAL_VOTES``
    cumulative votes, and ``KNOWN_PERSON_MIN_AVG_RATING`` mean rating.
    """
    career = person_history.groupBy("nconst").agg(
        F.countDistinct("tconst").alias("career_movie_count"),
        F.avg("average_rating").alias("career_average_rating"),
        F.sum("num_votes").alias("career_total_votes"),
    )
    return career.filter(
        (F.col("career_movie_count") >= F.lit(KNOWN_PERSON_MIN_MOVIES))
        & (F.col("career_total_votes") >= F.lit(KNOWN_PERSON_MIN_TOTAL_VOTES))
        & (F.col("career_average_rating") >= F.lit(KNOWN_PERSON_MIN_AVG_RATING))
    ).select(
        "nconst",
        F.lit(True).alias("is_known_person"),
        "career_movie_count",
        "career_average_rating",
        "career_total_votes",
    )


def attach_known_people(
    principals: DataFrame, known_people: DataFrame
) -> DataFrame:
    """Broadcast-join known-person flags onto principals (explicit hint)."""
    return principals.join(
        F.broadcast(known_people.select("nconst", "is_known_person")),
        on="nconst",
        how="left",
    ).withColumn(
        "is_known_person", F.coalesce(F.col("is_known_person"), F.lit(False))
    )


def aggregate_cast_crew_features(
    principals: DataFrame, person_history: DataFrame, known_people: DataFrame
) -> DataFrame:
    """Aggregate person-level history to exactly one row per movie."""
    enriched = (
        principals.select("tconst", "nconst", "category")
        .join(person_history.select(
            "tconst",
            "nconst",
            "prior_movie_count",
            "prior_average_rating",
            "prior_total_votes",
            "prior_highly_rated_movie_count",
        ), on=["tconst", "nconst"], how="left")
    )
    enriched = attach_known_people(enriched, known_people)

    is_cast = F.col("category").isin(list(CAST_CATEGORIES))
    is_director = F.col("category") == "director"
    is_writer = F.col("category") == "writer"
    is_producer = F.col("category") == "producer"

    # Deduplicate person within role group so multi-ordering rows do not inflate means.
    role_person = (
        enriched.withColumn(
            "role_group",
            F.when(is_cast, F.lit("cast"))
            .when(is_director, F.lit("director"))
            .when(is_writer, F.lit("writer"))
            .when(is_producer, F.lit("producer"))
            .otherwise(F.lit("other")),
        )
        .dropDuplicates(["tconst", "nconst", "role_group"])
    )

    film = role_person.groupBy("tconst").agg(
        F.countDistinct("nconst").alias("principal_count"),
        F.countDistinct(F.when(F.col("role_group") == "cast", F.col("nconst"))).alias(
            "cast_count"
        ),
        F.countDistinct(
            F.when(F.col("role_group") == "director", F.col("nconst"))
        ).alias("director_count"),
        F.countDistinct(
            F.when(F.col("role_group") == "writer", F.col("nconst"))
        ).alias("writer_count"),
        F.countDistinct(
            F.when(F.col("role_group") == "producer", F.col("nconst"))
        ).alias("producer_count"),
        F.avg(
            F.when(F.col("role_group") == "cast", F.col("prior_movie_count"))
        ).alias("cast_prior_movie_count_mean"),
        F.avg(
            F.when(F.col("role_group") == "cast", F.col("prior_average_rating"))
        ).alias("cast_prior_rating_mean"),
        F.sum(
            F.when(F.col("role_group") == "cast", F.col("prior_total_votes"))
        ).alias("cast_prior_votes_sum"),
        F.avg(
            F.when(F.col("role_group") == "director", F.col("prior_movie_count"))
        ).alias("director_prior_movie_count_mean"),
        F.avg(
            F.when(F.col("role_group") == "director", F.col("prior_average_rating"))
        ).alias("director_prior_rating_mean"),
        F.sum(
            F.when(F.col("role_group") == "director", F.col("prior_total_votes"))
        ).alias("director_prior_votes_sum"),
        F.avg(
            F.when(F.col("role_group") == "writer", F.col("prior_movie_count"))
        ).alias("writer_prior_movie_count_mean"),
        F.avg(
            F.when(F.col("role_group") == "writer", F.col("prior_average_rating"))
        ).alias("writer_prior_rating_mean"),
        F.avg("prior_movie_count").alias("principal_prior_movie_count_mean"),
        F.avg("prior_average_rating").alias("principal_prior_rating_mean"),
        F.sum("prior_total_votes").alias("principal_prior_votes_sum"),
        F.max("prior_average_rating").alias("principal_max_prior_rating"),
        F.countDistinct(
            F.when(
                (F.col("role_group") == "cast") & F.col("is_known_person"),
                F.col("nconst"),
            )
        ).alias("known_cast_count"),
        F.countDistinct(
            F.when(
                (F.col("role_group") == "director") & F.col("is_known_person"),
                F.col("nconst"),
            )
        ).alias("known_director_count"),
    )

    return (
        film.withColumn("has_known_cast", F.col("known_cast_count") > 0)
        .withColumn("has_known_director", F.col("known_director_count") > 0)
        .fillna(
            {
                "principal_count": 0,
                "cast_count": 0,
                "director_count": 0,
                "writer_count": 0,
                "producer_count": 0,
                "known_cast_count": 0,
                "known_director_count": 0,
                "cast_prior_votes_sum": 0,
                "director_prior_votes_sum": 0,
                "principal_prior_votes_sum": 0,
            }
        )
    )


def build_movies_enriched(movies: DataFrame, cast_crew: DataFrame) -> DataFrame:
    """Left-join cast/crew features onto movies; keep all rated movies."""
    feature_cols = [c for c in cast_crew.columns if c != "tconst"]
    joined = movies.join(cast_crew, on="tconst", how="left")
    # Default numeric counts to 0 when a movie has no principals.
    fill_zero = [
        c
        for c in feature_cols
        if c.endswith("_count")
        or c.endswith("_sum")
        or c in ("has_known_cast", "has_known_director")
    ]
    for col_name in fill_zero:
        if col_name.startswith("has_"):
            joined = joined.withColumn(
                col_name, F.coalesce(F.col(col_name), F.lit(False))
            )
        else:
            joined = joined.withColumn(
                col_name, F.coalesce(F.col(col_name), F.lit(0))
            )
    return joined


def prepare_oscars_nominations(df: DataFrame) -> DataFrame:
    """Clean Oscar nomination rows and explode pipe-separated FilmId values.

    Rows without a usable IMDb ``tt…`` FilmId are dropped (special/technical
    awards often have no film id). ``Winner`` empty → false; ``True`` → true.
    """
    from cinescope.schemas import BEST_PICTURE_CATEGORIES

    cleaned = (
        df.withColumn("ceremony", F.col("Ceremony").cast("int"))
        .withColumn("award_year", F.col("Year"))
        .withColumn("award_class", F.col("Class"))
        .withColumn("canonical_category", F.col("CanonicalCategory"))
        .withColumn("category", F.col("Category"))
        .withColumn("film_title_raw", F.col("Film"))
        .withColumn("film_id_raw", F.col("FilmId"))
        .withColumn("nominee_name", F.col("Name"))
        .withColumn("nominees", F.col("Nominees"))
        .withColumn("nominee_ids_raw", F.col("NomineeIds"))
        .withColumn(
            "is_winner",
            F.when(F.lower(F.trim(F.col("Winner"))) == "true", F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
        .withColumn("detail", F.col("Detail"))
        .withColumn("note", F.col("Note"))
    )

    exploded = (
        cleaned.withColumn(
            "tconst",
            F.explode_outer(F.split(F.col("film_id_raw"), r"\|")),
        )
        .withColumn("tconst", F.trim(F.col("tconst")))
        .filter(F.col("tconst").isNotNull() & (F.col("tconst") != ""))
        .filter(F.col("tconst").rlike(r"^tt\d+$"))
    )

    return exploded.select(
        "tconst",
        "ceremony",
        "award_year",
        "award_class",
        "canonical_category",
        "category",
        "film_title_raw",
        "nominee_name",
        "nominees",
        "nominee_ids_raw",
        "is_winner",
        "detail",
        "note",
        F.col("canonical_category")
        .isin(list(BEST_PICTURE_CATEGORIES))
        .alias("is_best_picture_category"),
        (F.col("award_class") == "Acting").alias("is_acting_category"),
        (F.col("award_class") == "Directing").alias("is_directing_category"),
        (F.col("award_class") == "Writing").alias("is_writing_category"),
    )


def aggregate_movie_oscar_features(nominations: DataFrame) -> DataFrame:
    """Aggregate nomination-grain Oscars to exactly one row per film ``tconst``."""
    return nominations.groupBy("tconst").agg(
        F.count(F.lit(1)).alias("oscar_nomination_count"),
        F.sum(F.col("is_winner").cast("int")).alias("oscar_win_count"),
        F.max(F.col("is_winner")).alias("was_oscar_winner"),
        F.lit(True).alias("was_oscar_nominated"),
        F.min("ceremony").alias("first_oscar_ceremony"),
        F.max("ceremony").alias("last_oscar_ceremony"),
        F.sum(F.col("is_best_picture_category").cast("int")).alias(
            "best_picture_nomination_count"
        ),
        F.sum(
            (
                F.col("is_best_picture_category") & F.col("is_winner")
            ).cast("int")
        ).alias("best_picture_win_count"),
        F.max(
            F.col("is_best_picture_category") & F.col("is_winner")
        ).alias("best_picture_won"),
        F.max(F.col("is_best_picture_category")).alias("best_picture_nominated"),
        F.sum(F.col("is_acting_category").cast("int")).alias(
            "acting_nomination_count"
        ),
        F.sum(
            (F.col("is_acting_category") & F.col("is_winner")).cast("int")
        ).alias("acting_win_count"),
        F.sum(F.col("is_directing_category").cast("int")).alias(
            "directing_nomination_count"
        ),
        F.sum(
            (F.col("is_directing_category") & F.col("is_winner")).cast("int")
        ).alias("directing_win_count"),
        F.sum(F.col("is_writing_category").cast("int")).alias(
            "writing_nomination_count"
        ),
        F.sum(
            (F.col("is_writing_category") & F.col("is_winner")).cast("int")
        ).alias("writing_win_count"),
    )


def build_movies_awards_enriched(
    movies: DataFrame, oscar_features: DataFrame
) -> DataFrame:
    """Left-join film-level Oscar features onto the enriched movie table.

    Movies with no Oscar history keep identity/cast features; Oscar counts
    default to 0 and boolean flags to false.
    """
    joined = movies.join(oscar_features, on="tconst", how="left")
    count_defaults = [
        "oscar_nomination_count",
        "oscar_win_count",
        "best_picture_nomination_count",
        "best_picture_win_count",
        "acting_nomination_count",
        "acting_win_count",
        "directing_nomination_count",
        "directing_win_count",
        "writing_nomination_count",
        "writing_win_count",
    ]
    bool_defaults = [
        "was_oscar_nominated",
        "was_oscar_winner",
        "best_picture_nominated",
        "best_picture_won",
    ]
    for col_name in count_defaults:
        if col_name in joined.columns:
            joined = joined.withColumn(
                col_name, F.coalesce(F.col(col_name), F.lit(0)).cast("long")
            )
    for col_name in bool_defaults:
        if col_name in joined.columns:
            joined = joined.withColumn(
                col_name, F.coalesce(F.col(col_name), F.lit(False))
            )
    return joined
