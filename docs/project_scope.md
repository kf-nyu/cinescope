# CineScope Project Scope

## Grading - 435 points

- Technique/Execution: 125 points
- Technologies: 125 points
- Report (Analysis, presentation, execution): 125 points
- Innovation/Applicability: 40 points
- Source code/ code quality: 20 points

## Required

**Audience and standard:** Write the report and presentation as a professional-grade business product for a high-level corporate chief, client, or investor. Lead with decisions, analytical insight, business value, and recommendations. Use technical implementation as supporting evidence rather than presenting a query or code summary.

- IMDb ingestion from official non-commercial datasets
- Spark ETL with explicit schemas and null/type cleaning
- Parquet conversion on external SSD storage
- Large joins (titles ↔ ratings; principals ↔ names / history)
- Film-level feature table (including cast/crew reputation)
- Awards enrichment (Oscar nominations/wins joined to films)
- Five analytical findings
- Hit-prediction model
- Performance optimization evidence (plans, timings, broadcast join)
- Professional report and slides. Smart analysis and insights into the problem we are solving is expected, not just a summarization of some data query.
- Charts, visualization, analytical insights, will get more points.

## Completed so far

- Bronze `movies_ratings`
- Silver `cast_crew_features` and `movies_enriched`
- Leakage-safe historical person features
- Broadcast known-person demonstration
- Bronze `oscars_nominations`, silver `movie_oscar_features`, `movies_awards_enriched`
- Flexible storage backends: local SSD, local directory, or HDFS (NYU Dataproc)
- Unit tests and storage safety guards
- Dataproc batch ETL (`make pipeline` / `spark-submit`); full pipeline ~23 min on YARN
- Corrected point-in-time known cast/director features; full-career lookup retained only for broadcast-plan evidence
- Strict model feature contract excluding outcomes, Oscar fields, full-career fields, and retrospective vote totals
- Local `02_core_analytics`: five corrected insights, hit-label sensitivity, and charts
- Local `03` and `04`: weighted Logistic Regression and GBT comparison, chronological cohorts, PR curves, and validation-selected thresholds
- Oscar source confirmed as `DLu/oscar_data`

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

## Final status

- Feature-semantics version 2 silver tables validated at one row per rated movie
- Notebooks `02` to `04` executed with saved outputs and no execution errors
- Final metrics, charts, plans, logs, report, and presentation reconciled
- Full reference run completed with 50 passing tests
- Final presentation allows 12 minutes of speaking and 3 minutes for questions

## Boundary

Review sentiment, cloud deployment, GPU training, and other optional work remain future extensions rather than submission requirements.
