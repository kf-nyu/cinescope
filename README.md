# CineScope

**Public repository:** [https://github.com/kf-nyu/cinescope](https://github.com/kf-nyu/cinescope)

PySpark pipeline for IMDb film analytics (NYU Big Data course project).

Same GitHub repository for **local laptops** and **NYU Dataproc (HDFS + YARN)**. Only `.env` changes per machine — never commit `.env`.

## Summary

CineScope downloads official IMDb non-commercial datasets, cleans them with Spark, builds a rated-movies bronze table, engineers leakage-safe cast/crew reputation features, joins Academy Awards data, and writes enriched Parquet tables for analytics and later modeling.

## Architecture

```text
Official IMDb datasets
        ↓
title.basics + title.ratings → bronze/movies_ratings
        ↓
title.principals + name.basics
  + historical person reputation (pre-release only)
        + point-in-time known-person flags
        ↓
silver/cast_crew_features (1 row per movie)
        ↓
silver/movies_enriched (left join; all rated movies)
        ↓
Academy Awards (DLu/oscar_data FilmId → tconst)
        ↓
bronze/oscars_nominations
silver/movie_oscar_features
silver/movies_awards_enriched
```

## Storage backends (same repo)

| Backend | When | Data root |
|---|---|---|
| **local + external volume** | Large external disk | `$CINESCOPE_SSD_VOLUME/cinescope-data` |
| **local directory** | Laptop without external disk | `~/cinescope-data` |
| **hdfs** | NYU Dataproc | derived from `CINESCOPE_NETID` |

| Location | Contents |
|---|---|
| **Git repo** | Source, tests, notebooks, docs (no large data) |
| **`CINESCOPE_DATA_ROOT`** | Raw IMDb/Oscars, Parquet, warehouse/checkpoints |
| **`SPARK_LOCAL_DIR`** | Spark shuffle scratch — always a **local** filesystem path |

Large data must never live inside the Git repository. Do not commit `.env`, logs, or machine-specific metrics/plans.

---

## Getting started — Local (laptop / desktop)

### Prerequisites

- Python 3.11
- Java 17 (set `JAVA_HOME`)
- Enough free disk at `CINESCOPE_DATA_ROOT` (~**50 GB** plain local folder, or ~**200 GB** if using an external volume)

### 1. Clone and create the virtualenv

```bash
git clone https://github.com/kf-nyu/cinescope.git cinescope
cd cinescope

export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"  # example

make setup
source .venv/bin/activate
export PYTHONPATH="$PWD/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
```

### 2. Configure `.env`

```bash
cp .env.example .env
# edit paths for your machine — never commit .env
```

**Option A — external volume** under `/Volumes/...`:

```bash
CINESCOPE_STORAGE_BACKEND=local
SPARK_MASTER=local[*]
CINESCOPE_SSD_VOLUME="/Volumes/YourVolumeName"
CINESCOPE_DATA_ROOT="/Volumes/YourVolumeName/cinescope-data"
SPARK_LOCAL_DIR="/Volumes/YourVolumeName/cinescope-data/spark-temp"
SPARK_WAREHOUSE_DIR="/Volumes/YourVolumeName/cinescope-data/warehouse"
SPARK_CHECKPOINT_DIR="/Volumes/YourVolumeName/cinescope-data/checkpoints"
CINESCOPE_MIN_FREE_GB=200
```

**Option B — plain home directory** (no external drive):

```bash
CINESCOPE_STORAGE_BACKEND=local
SPARK_MASTER=local[*]
# CINESCOPE_SSD_VOLUME=   # leave unset
CINESCOPE_DATA_ROOT="${HOME}/cinescope-data"
SPARK_LOCAL_DIR="${HOME}/cinescope-data/spark-temp"
SPARK_WAREHOUSE_DIR="${HOME}/cinescope-data/warehouse"
SPARK_CHECKPOINT_DIR="${HOME}/cinescope-data/checkpoints"
CINESCOPE_MIN_FREE_GB=50
```

### 3. Initialize storage and run the pipeline

```bash
make init-storage
make validate-storage
make download
make inspect
make test
make baseline
make cast-crew
make download-oscars
make oscars
```

Or one combined run with a full console log: `make pipeline` → `outputs/logs/pipeline_<UTC>.log`.

---

## Getting started — NYU HPC (Dataproc / HDFS)

Same repository. On Dataproc your login is `netid_nyu_edu`; the short NetID is written into `.env` automatically. More detail: [`deploy/hpc/README.md`](deploy/hpc/README.md). **Local Mac does not need a NetID** (use `.env.example` instead).

### Prerequisites

- NYU HPC account with Dataproc access ([SSH](https://dataproc.hpc.nyu.edu/ssh))
- `hdfs` available on the login node

### 1. One-shot setup (copy-paste on Dataproc)

Clones the repo, derives NetID from `whoami`, creates `.env` from `.env.hpc.example`, and builds the venv:

```bash
git clone https://github.com/kf-nyu/cinescope.git cinescope
cd cinescope
bash scripts/setup_hpc.sh

source .venv/bin/activate
export PYTHONPATH="$PWD/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
```

Equivalent manual steps (if you prefer not to use the script):

```bash
git clone https://github.com/kf-nyu/cinescope.git cinescope
cd cinescope

# Dataproc login is <netid>_nyu_edu → short NetID
NETID="${USER%_nyu_edu}"
echo "CINESCOPE_NETID=$NETID"

cp .env.hpc.example .env
sed -i "s/^CINESCOPE_NETID=.*/CINESCOPE_NETID=${NETID}/" .env
# never commit .env

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH="$PWD/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
```

Paths derived from that NetID:

```text
CINESCOPE_HDFS_USER      = ${CINESCOPE_NETID}_nyu_edu
CINESCOPE_DATA_ROOT      = hdfs:///user/${CINESCOPE_HDFS_USER}/cinescope-data
SPARK_LOCAL_DIR          = /tmp/cinescope-spark-${CINESCOPE_NETID}
```

### 2. Run the pipeline

One command runs all steps and saves the **entire** console log (no copy-paste):

```bash
make pipeline
# → outputs/logs/pipeline_<UTC-timestamp>.log
```

Or step by step (same sequence):

```bash
make init-storage
make validate-storage
make download
make download-oscars
make inspect
make test
make baseline
make cast-crew
make oscars
```

Pipeline targets use **batch** `spark-submit --master yarn --deploy-mode client` (not Jupyter / interactive shells). `fs.defaultFS` comes from `hdfs getconf` unless you set `SPARK_HADOOP_FS_DEFAULT` in `.env`.

### 3. Tear down (instructors / TAs / students)

After a run on Dataproc, delete **this account’s** CineScope HDFS data and `/tmp` scratch:

```bash
make clean-hpc
# type YES when prompted
```

Or non-interactive / also remove the Git clone:

```bash
bash scripts/cleanup_hpc.sh --yes
bash scripts/cleanup_hpc.sh --yes --remove-clone
```

This only removes `…/cinescope-data` under the configured HDFS user plus local scratch for that NetID — not the whole HDFS home.

---

## Privacy / public-repo notes

Safe to publish:

- Source under `src/`, tests, fixtures, Makefile, generic `.env*.example`, docs that use placeholders

Keep private (gitignored — do not force-add):

- `.env` (NetID, local absolute paths)
- `docs/internal/` (local planning notes — not published)
- `outputs/logs/`, `outputs/metrics/*.json`, `outputs/plans/*.txt` (often contain absolute home/volume paths)
- Notebook **outputs** (re-run locally; committed notebooks should be output-cleared)
- `.venv/`

Readers should copy an example env file and set their own paths / `CINESCOPE_NETID`.

## Data sources

- IMDb: [https://datasets.imdbws.com/](https://datasets.imdbws.com/)
- Oscars + IMDb ids: [https://github.com/DLu/oscar_data](https://github.com/DLu/oscar_data)

## Cast/crew design notes

- **Row explosion:** `title.principals` is multi-row per title; aggregate back to one row per `tconst`.
- **Roles kept:** actor, actress, director, writer, producer, composer, cinematographer, editor.
- **Leakage control:** priors use only movies with `start_year` strictly before the current film.
- **Point-in-time known person:** model features use only prior movie count and prior mean rating available at that film.
- **Broadcast evidence:** a separate full-corpus lookup demonstrates explicit `F.broadcast`; it never enters model features.
- **Vote snapshots:** prior vote totals remain available for retrospective analytics but are excluded from predictive models because IMDb does not provide historical vote snapshots.
- **Enriched table:** left join so movies without principals are retained.

## Generated outputs

**Under `CINESCOPE_DATA_ROOT`:** `raw/`, `bronze/`, `silver/` Parquet trees.

**Local repo `outputs/` (machine-specific; gitignored metrics/plans/logs):** JSON metrics, Spark plans, and charts under `outputs/charts/generated/`.

**Notebooks (local Spark):** after silver tables exist, run `02_core_analytics`, `03_train_hit_model`, `04_train_awards_model`. The notebooks reject cast/crew artifacts created before feature-semantics version 2.

## Safety warnings

- Never use the Git repository (or `/`) as `CINESCOPE_DATA_ROOT`.
- If `CINESCOPE_SSD_VOLUME` is set, it must be mounted and contain the data root.
- On HDFS, `SPARK_LOCAL_DIR` must remain a local scratch path.

## Current status

**Done**
- IMDb + Oscars ETL (local + Dataproc batch), cast/crew features, awards enrichment, unit tests
- Dataproc full pipeline ≈ **23 min** (ETL only — no analytics/MLlib on the cluster)
- Point-in-time known cast/director semantics and strict model feature exclusions
- Local notebook `02_core_analytics` — five non-circular findings, label sensitivity, and charts
- Local notebooks `03` / `04` — weighted Logistic Regression / GBT comparison, chronological validation, PR curves, and validation-selected thresholds

**In progress**
- Rerun cast/crew, Oscar enrichment, analytics, and both models against the full reference dataset
- Replace the superseded July 31 model metrics with feature-semantics version 2 results
- Final business report and presentation

**Runtime**
- Dataproc = ETL batch only  
- Analytics / MLlib = local Spark notebooks (not JupyterHub)

**Evidence status**
- The July 31 ETL metrics and physical plans remain valid reference evidence.
- The July 31 model metrics are superseded because their known-person flags used full-career outcomes.
- Final report and slide metrics must come from the corrected chronological rerun.

## Out of scope (for now)

Not in the current pipeline: review-text sentiment, Hadoop MapReduce Streaming, commercial cloud (GCP/Azure), or GPU training.
