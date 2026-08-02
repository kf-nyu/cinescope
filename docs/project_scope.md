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

## Optional (still deferred)

- Review sentiment
- Hadoop Streaming review processing
- GPU neural network
- Delta Lake
- Box-office data
- Franchise analysis
- Cloud execution (GCP / Azure)

## Next (partner review)

- Awards-recognition MLlib model (labels from Oscar features)
- Hit-prediction model using **pre-release** features only (exclude Oscar outcomes to avoid leakage)
- Confirm Oscar source choice (GitHub DLu/oscar_data vs Kaggle scrape) with Isha

## Boundary

Do not begin review sentiment, cloud deployment, GPU training, or final slide/report formatting until agreed with the project partner.
