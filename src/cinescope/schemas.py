"""Explicit Spark schemas for IMDb TSV sources.

Raw fields are read as strings so IMDb ``\\N`` sentinels can be converted
to null before numeric casting. Do not rely on inferSchema for production reads.
"""

from pyspark.sql.types import StringType, StructField, StructType

TITLE_BASICS_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", StringType(), True),
        StructField("startYear", StringType(), True),
        StructField("endYear", StringType(), True),
        StructField("runtimeMinutes", StringType(), True),
        StructField("genres", StringType(), True),
    ]
)

TITLE_RATINGS_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("averageRating", StringType(), True),
        StructField("numVotes", StringType(), True),
    ]
)

TITLE_PRINCIPALS_SCHEMA = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("ordering", StringType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True),
    ]
)

NAME_BASICS_SCHEMA = StructType(
    [
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", StringType(), True),
        StructField("deathYear", StringType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True),
    ]
)

MOVIES_RATINGS_OUTPUT_COLUMNS = (
    "tconst",
    "primary_title",
    "original_title",
    "start_year",
    "runtime_minutes",
    "genres",
    "average_rating",
    "num_votes",
)

# Roles kept before the large principals ↔ names / history joins.
RELEVANT_PRINCIPAL_CATEGORIES = (
    "actor",
    "actress",
    "director",
    "writer",
    "producer",
    "composer",
    "cinematographer",
    "editor",
)

CAST_CATEGORIES = ("actor", "actress")

# Highly rated prior movie threshold (documented).
HIGHLY_RATED_THRESHOLD = 7.0

# Data-derived "known person" lookup thresholds (documented).
KNOWN_PERSON_MIN_MOVIES = 10
KNOWN_PERSON_MIN_TOTAL_VOTES = 100_000
KNOWN_PERSON_MIN_AVG_RATING = 7.0

# Oscar nominations (DLu/oscar_data). File is tab-separated despite .csv suffix.
OSCARS_NOMINATIONS_SCHEMA = StructType(
    [
        StructField("Ceremony", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("Class", StringType(), True),
        StructField("CanonicalCategory", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Film", StringType(), True),
        StructField("FilmId", StringType(), True),
        StructField("Name", StringType(), True),
        StructField("Nominees", StringType(), True),
        StructField("NomineeIds", StringType(), True),
        StructField("Winner", StringType(), True),
        StructField("Detail", StringType(), True),
        StructField("Note", StringType(), True),
        StructField("Citation", StringType(), True),
    ]
)

BEST_PICTURE_CATEGORIES = (
    "BEST PICTURE",
    "UNIQUE AND ARTISTIC PICTURE",
)
