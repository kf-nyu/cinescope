# CineScope Final Execution Summary

This page summarizes the final reference run. Generated metrics, plans, logs, charts, models, and executed notebooks are not stored in Git because they contain local paths or reproducible output.

## Validation

- Final run date: August 9, 2026
- Spark version: 3.5.9
- Automated tests: 50 passed
- Final movie table: 348,676 rows with 348,676 distinct non-null `tconst` values
- Cast and crew feature table: 347,460 rows, one row per represented film
- Point-in-time feature definition: version 2
- All jobs and notebooks completed without execution errors

## Data scale

| Item | Final value |
|---|---:|
| Raw principal relationships | 100,895,615 |
| Movie-related principal relationships | 5,059,936 |
| Distinct people represented | 1,425,195 |
| Rated movies | 348,676 |
| Movies with cast and crew features | 347,460 |
| Movies without principal features | 1,216 |
| Oscar-linked films in the source | 5,264 |
| Rated films with an Oscar nomination | 3,903 |
| Rated films with an Oscar win | 1,091 |

## Analytics

| Result | Final value |
|---|---:|
| Director prior-rating correlation | 0.468 |
| High-rating, bottom-decile-vote candidates | 6,712 |
| Audience-reception hit prevalence | 4.02% |
| Peak-volume runtime bucket | 90 minutes |
| Films in the peak-volume runtime bucket | 103,636 |
| Highest director-experience lift | 1.41 times the eligible baseline |

## Chronological model results

Both outcomes compare class-weighted Logistic Regression and Gradient-Boosted Trees. Validation PR AUC selects the model parameters. A predefined validation sweep selects the tested threshold with the highest positive-class F1. The selected model and threshold are then evaluated once on later test years.

| Outcome | Selected model | PR AUC | ROC AUC | Precision | Recall | Positive F1 | Threshold |
|---|---|---:|---:|---:|---:|---:|---:|
| Audience-reception hit | GBTClassifier | 0.225 | 0.849 | 0.231 | 0.412 | 0.296 | 0.825 |
| Oscar nomination | GBTClassifier | 0.120 | 0.950 | 0.061 | 0.700 | 0.113 | 0.750 |

## Performance evidence

- Baseline title and rating job: about 25.3 seconds in the local reference run
- Final cast and crew feature job: about 123.2 seconds
- Final Oscar feature job: about 7.5 seconds
- Dataproc batch ETL reference: about 23 minutes
- The physical plan confirms `BroadcastHashJoin` and `BroadcastExchange` for the small descriptive known-person lookup.
- The descriptive broadcast lookup is separate from predictive features. Point-in-time model flags use only strict prior history.

The timings come from different environments and are not a controlled local-versus-cluster benchmark.

## Reproduction

```bash
make setup
# Configure .env and make the raw datasets available.
make pipeline
```

After the pipeline finishes, run notebooks `02`, `03`, and `04` in order to reproduce the analytics, models, metrics, and charts.
