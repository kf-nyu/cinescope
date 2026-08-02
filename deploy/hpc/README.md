# CineScope on NYU Dataproc (HDFS + YARN)

**Repository:** [https://github.com/kf-nyu/cinescope](https://github.com/kf-nyu/cinescope)

Same GitHub repository as local development. Set **`CINESCOPE_NETID`**; HDFS locations are derived automatically.

## Prerequisites

1. NYU HPC account + Dataproc access for the Big Data course  
2. SSH into Dataproc (see [NYU Dataproc docs](https://services.rt.nyu.edu/docs/cloud/dataproc/intro/))  
3. `hdfs` available on the login node  

## Setup

```bash
git clone https://github.com/kf-nyu/cinescope.git cinescope
cd cinescope

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH="$PWD/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

cp .env.hpc.example .env
# Set CINESCOPE_NETID=your_short_netid  (never commit .env)
```

### NetID variable

| Variable | Meaning |
|---|---|
| `CINESCOPE_NETID` | Short NetID only (e.g. `ab1234` from `ab1234@nyu.edu`) |
| `CINESCOPE_HDFS_USER` | Optional override; default `${CINESCOPE_NETID}_nyu_edu` |

Derived when paths are left blank:

```text
hdfs:///user/${CINESCOPE_HDFS_USER}/cinescope-data
```

## Run

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

- First YARN job after idle may wait for Dataproc autoscaling.  
- Large uploads: NYU Dataproc ingest + `hadoop distcp` if home quota is tight.  
- Never commit `.env`. Never store NetIDs in tracked files.
