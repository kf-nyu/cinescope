# CineScope on NYU Dataproc (HDFS + YARN)

**Repository:** [https://github.com/kf-nyu/cinescope](https://github.com/kf-nyu/cinescope)

Same GitHub repository as local development. On Dataproc, NetID comes from your login (`whoami` → `netid_nyu_edu`).

## Prerequisites

1. NYU HPC account + Dataproc access for the Big Data course  
2. SSH into Dataproc (see [NYU Dataproc docs](https://services.rt.nyu.edu/docs/cloud/dataproc/intro/))  
3. `hdfs` available on the login node  

## Setup (copy-paste)

```bash
git clone https://github.com/kf-nyu/cinescope.git cinescope
cd cinescope
bash scripts/setup_hpc.sh

source .venv/bin/activate
export PYTHONPATH="$PWD/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
```

`setup_hpc.sh` will:

1. Read login user via `whoami` (e.g. `ab1234_nyu_edu`)
2. Set short NetID (`ab1234`)
3. Copy `.env.hpc.example` → `.env` and fill `CINESCOPE_NETID=…`
4. Create `.venv` and `pip install -r requirements.txt`

Never commit `.env`. Re-run with `bash scripts/setup_hpc.sh --force` to overwrite `.env`.

### NetID variable

| Variable | Meaning |
|---|---|
| `CINESCOPE_NETID` | Short NetID only (filled by `setup_hpc.sh` from login) |
| `CINESCOPE_HDFS_USER` | Optional override; default `${CINESCOPE_NETID}_nyu_edu` |

Derived when paths are left blank:

```text
hdfs:///user/${CINESCOPE_HDFS_USER}/cinescope-data
```

## Run

Preferred — one command, full console log saved under `outputs/logs/`:

```bash
make pipeline
# writes outputs/logs/pipeline_<UTC-timestamp>.log (also prints live)
```

Or the same steps individually:

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

`make baseline` / `cast-crew` / `oscars` submit **batch** jobs with:

```text
spark-submit --master yarn --deploy-mode client …
```

They auto-detect `spark.hadoop.fs.defaultFS` from `hdfs getconf` (or `SPARK_HADOOP_FS_DEFAULT` in `.env`). Do **not** use Jupyter or an interactive `pyspark` shell on Dataproc.

`make test` uses local pip PySpark and temporarily unsets `SPARK_HOME` so it does not conflict with `/usr/lib/spark`.

## Analytics / MLlib (not on Dataproc)

Course Dataproc is **ETL batch only**. Run §2.3 insights and hit/awards models on a **local** laptop against silver Parquet (SSD or local data root):

- `notebooks/02_core_analytics.ipynb`
- `notebooks/03_train_hit_model.ipynb`
- `notebooks/04_train_awards_model.ipynb`

Do **not** use JupyterHub (~1GB quota) for Spark MLlib. Hadoop MapReduce Streaming is **not** part of the initial ETL.

## Local vs HDFS

| Location | Contents |
|---|---|
| **HDFS** data root | raw IMDb/Oscars, bronze/silver Parquet, warehouse, checkpoints |
| **Local** `SPARK_LOCAL_DIR` | Spark shuffle / temp |
| **Git clone** | code, tests, small `outputs/` artifacts |

## Tear down (delete this account’s CineScope data)

Instructors, TAs, or students can wipe artifacts created by this project for **their** Dataproc account:

```bash
make clean-hpc
# confirm by typing YES
```

What is removed:

| Target | Path |
|---|---|
| HDFS project tree | `hdfs:///user/<you>/cinescope-data` |
| Local Spark scratch | `/tmp/cinescope-spark-<netid>` |
| Download staging | `/tmp/cinescope-download-<netid>` |
| Repo run artifacts | `outputs/logs`, metrics JSON, plan texts |

Optional — also delete the clone after cleanup:

```bash
bash scripts/cleanup_hpc.sh --yes --remove-clone
```

Requires `.env` with `CINESCOPE_STORAGE_BACKEND=hdfs` (same as a normal HPC run). Does **not** delete other users’ HDFS homes.

## Notes

- Dataproc Spark is for **batch** `spark-submit` (client deploy-mode). Not Jupyter / shells.  
- First YARN job after idle may wait for Dataproc autoscaling.  
- If `fs.defaultFS` resolution fails, set `SPARK_HADOOP_FS_DEFAULT=hdfs://nyu-dataproc-m` (or your cluster’s `hdfs getconf` value) in `.env`.  
- Large uploads: NYU Dataproc ingest + `hadoop distcp` if home quota is tight.  
- Never commit `.env`. Never store NetIDs in tracked files.
