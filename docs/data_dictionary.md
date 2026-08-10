# CineScope Data Dictionary

Source: [IMDb non-commercial datasets](https://datasets.imdbws.com/)

## `title.basics` (raw → typed)

| Field | Spark type (after cast) | Meaning | Transformation | Null behavior |
|---|---|---|---|---|
| `tconst` | string | IMDb title id (join key) | preserved | required; nulls fail validation |
| `titleType` | string | title category | filter `== "movie"` | non-movies dropped |
| `primaryTitle` | string | primary display title | renamed `primary_title` | may be null |
| `originalTitle` | string | original title | renamed `original_title` | may be null |
| `isAdult` | int | adult flag | cast from string | `\N` → null before cast |
| `startYear` | int | release year | renamed `start_year` | `\N` → null |
| `endYear` | int | series end year | cast; not in bronze output | `\N` → null |
| `runtimeMinutes` | int | runtime | renamed `runtime_minutes` | `\N` → null |
| `genres` | array\<string\> | genre list | split on `,` (no explode) | `\N` / empty → null array |

## `title.ratings` (raw → typed)

| Field | Spark type (after cast) | Meaning | Transformation | Null behavior |
|---|---|---|---|---|
| `tconst` | string | IMDb title id (join key) | join key to basics | required |
| `averageRating` | double | mean user rating | renamed `average_rating` | `\N` → null |
| `numVotes` | long | vote count | renamed `num_votes` | `\N` → null |

## `title.principals` (raw → typed)

| Field | Spark type | Meaning | Transformation |
|---|---|---|---|
| `tconst` | string | title id | filter to rated-movie ids early |
| `ordering` | int | billing order | cast |
| `nconst` | string | person id | join key to names / history |
| `category` | string | role | keep relevant filmmaking roles only |
| `job` | string | detailed job | `\N` → null |
| `characters` | string | character list JSON-ish | `\N` → null |

**Roles kept:** actor, actress, director, writer, producer, composer, cinematographer, editor.

## `name.basics` (raw → typed / normalized)

| Field | Spark type | Meaning |
|---|---|---|
| `nconst` | string | person id |
| `primary_name` | string | display name |
| `birth_year` / `death_year` | int | years (`\N` → null) |
| `primary_profession` | array\<string\> | split professions |
| `known_for_titles` | array\<string\> | split title ids |

## Bronze: `movies_ratings`

Path: `$CINESCOPE_DATA_ROOT/bronze/movies_ratings/`

Inner join on `tconst` after movie filter.

| Field | Spark type | Meaning |
|---|---|---|
| `tconst` | string | IMDb title id |
| `primary_title` / `original_title` | string | titles |
| `start_year` | int | year |
| `runtime_minutes` | int | runtime |
| `genres` | array\<string\> | genres |
| `average_rating` | double | rating |
| `num_votes` | long | votes |

## Person-level history (intermediate)

For each `(nconst, tconst)`:

| Feature | Definition |
|---|---|
| `prior_movie_count` | count of that person's rated movies with `start_year` **strictly before** current |
| `prior_average_rating` | mean rating of those prior movies |
| `prior_total_votes` | sum of votes on prior movies |
| `prior_highly_rated_movie_count` | count of prior movies with `average_rating >= 7.0` |

Current-movie and later-film rows are never included. IMDb ratings and vote totals are current snapshots rather than historical snapshots; prior rating fields are documented retrospective proxies, and prior vote totals are excluded from predictive models. Missing `start_year` → prior counts 0 / rating null.

## Silver: `cast_crew_features`

Path: `$CINESCOPE_DATA_ROOT/silver/cast_crew_features/`

One row per `tconst`. Aggregations use distinct people within role groups (cast = actor+actress).

| Feature | Aggregation |
|---|---|
| `*_count` | distinct people in role / overall |
| `*_prior_movie_count_mean` | mean of person priors |
| `*_prior_rating_mean` | mean of person prior average ratings |
| `*_prior_votes_sum` | sum of person prior vote totals |
| `principal_max_prior_rating` | max person prior average rating |
| `known_*` / `has_known_*` | point-in-time flags derived separately for each film from strict prior history |

**Point-in-time known-person rule:** before the current film, at least 10 rated movies and a prior mean rating of at least 7.0. Current and future films never affect the flag.

The job also builds a separate full-corpus lookup using ≥10 movies, ≥100,000 total votes, and ≥7.0 mean rating solely to demonstrate an explicit broadcast join in the Spark physical plan. That lookup is descriptive and does not enter `cast_crew_features` or either model.

## Silver: `movies_enriched`

Path: `$CINESCOPE_DATA_ROOT/silver/movies_enriched/`

Left join of bronze movies to cast/crew features. Movies without principals keep identity/rating fields; count features default to 0 / false.

## Oscar nominations (`DLu/oscar_data`)

Source: [github.com/DLu/oscar_data](https://github.com/DLu/oscar_data) (`oscars.csv`, tab-separated). Join key: `FilmId` → IMDb `tconst`. Multi-film nominations explode on `|`.

| Raw field | Meaning |
|---|---|
| `Ceremony` / `Year` | Ceremony number / award year string |
| `Class` | Acting, Directing, Writing, Title, … |
| `CanonicalCategory` | Normalized category (e.g. `BEST PICTURE`) |
| `FilmId` | IMDb title id(s) |
| `Winner` | empty = nominee; `True` = winner |

## Bronze: `oscars_nominations`

Path: `$CINESCOPE_DATA_ROOT/bronze/oscars_nominations/`

One row per (film × nomination) after exploding `FilmId`. Rows without a usable `tt…` id are dropped.

## Silver: `movie_oscar_features`

Path: `$CINESCOPE_DATA_ROOT/silver/movie_oscar_features/`

One row per `tconst` among Oscar-linked films.

| Feature | Meaning |
|---|---|
| `oscar_nomination_count` / `oscar_win_count` | totals |
| `was_oscar_nominated` / `was_oscar_winner` | flags |
| `best_picture_*` | Best Picture (incl. early Unique and Artistic Picture) |
| `acting_*` / `directing_*` / `writing_*` | class-level counts |
| `first_oscar_ceremony` / `last_oscar_ceremony` | ceremony span |

## Silver: `movies_awards_enriched`

Path: `$CINESCOPE_DATA_ROOT/silver/movies_awards_enriched/`

Left join of `movies_enriched` to `movie_oscar_features`. Non-Oscar films keep counts at 0 / false.

**Leakage note:** Oscar wins/nominations are post-release outcomes. Use for awards modeling and analytics; exclude from pre-release hit-prediction feature sets.

## Predictive model contract

Both classifiers use a strict whitelist of release metadata, role counts, genre flags, prior film counts, prior-film rating proxies, and point-in-time known-person flags. They exclude:

- `average_rating`, `num_votes`, and the derived hit label
- every Oscar outcome field
- full-career person aggregates used by the broadcast demonstration
- prior vote totals, because historical IMDb vote snapshots are unavailable

Model selection uses chronological train and validation cohorts. The selected model and decision threshold are then evaluated once on later test years. Primary metrics are PR AUC, ROC AUC, positive-class precision, recall, and positive-class F1.
