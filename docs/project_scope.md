# CineScope Project Scope

## Required

- IMDb ingestion from official non-commercial datasets
- Spark ETL with explicit schemas and null/type cleaning
- Parquet conversion on external SSD storage
- Large joins (titles ↔ ratings; principals ↔ names / history)
- Film-level feature table (including cast/crew reputation)
- Awards enrichment (Oscar nominations/wins joined to films)
- Five analytical findings
- Hit-prediction model
- Performance optimization evidence (plans, timings, broadcast join)
- Professional report and slides

## Completed so far

- Bronze `movies_ratings`
- Silver `cast_crew_features` and `movies_enriched`
- Leakage-safe historical person features
- Broadcast known-person demonstration
- Bronze `oscars_nominations`, silver `movie_oscar_features`, `movies_awards_enriched`
- Flexible storage backends: local SSD, local directory, or HDFS (NYU Dataproc)
- Unit tests and storage safety guards
- Dataproc batch ETL (`make pipeline` / `spark-submit`); full pipeline ~23 min on YARN
- Local `02_core_analytics` — proposal §2.3 insights + charts
- Local `03` / `04` — provisional GBT hit + awards models (untuned; default threshold ≈0.5)

## Runtime split

| Where | What |
|---|---|
| **NYU Dataproc** | ETL only (batch `spark-submit`). No Jupyter / interactive Spark shells. |
| **Local Mac + data root** | Analytics charts + Spark MLlib training notebooks |
| **JupyterHub (~1GB)** | Do **not** run Spark MLlib or load silver Parquet |

## Optional (still deferred)

- Review sentiment
- Hadoop MapReduce Streaming (not required for initial ETL)
- GPU neural network
- Delta Lake
- Box-office data
- Franchise analysis
- Cloud execution (GCP / Azure)

## Next

- Layer 5: logistic regression, HP tuning, threshold / PR sweep; confirm provisional labels
- Confirm Oscar source wording (DLu vs Kaggle) in the proposal text
- Business report + slides (after Layer 5 is locked)

## Boundary

Do not begin review sentiment, cloud deployment, GPU training, or final slide/report formatting until analytics/ML notebooks and labels are locked.
