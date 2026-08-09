# CineScope

## A Scalable Analytics Platform for Film Success and Awards Recognition

**Course:** Big Data, Summer 2026  
**Team:** Isha Dave and Kenji Funaki  
**Report status:** Canonical narrative source. `scripts/finalize_report.py` produces the submission copy from feature-semantics version 2 metrics.

## Executive Summary

Film studios and streaming platforms make expensive decisions before audience reaction is known. CineScope investigates how much useful signal can be extracted from information available before release, including genre, runtime, creative-team structure, and the prior experience of cast and crew. The project combines official IMDb non-commercial datasets with historical Academy Award records and processes them through an Apache Spark pipeline that runs both on local storage and on NYU Dataproc with HDFS and YARN.

The reference execution processed approximately 100.9 million cast and crew relationships and produced a film-level analytical population of 348,676 rated movies. This relationship table, rather than the final film table, creates the principal big-data challenge: each film has multiple people, each person has multiple films, and prior-career features must be computed in time order without allowing the current or a future film to influence an earlier prediction.

CineScope produces five groups of business evidence. It describes how genre performance changes across decades, measures the association between director track record and later film ratings, profiles runtime without claiming a universal causal optimum, identifies high-rating films with limited audience reach, and measures hit-rate lift across pre-release director-experience bands. The corrected predictive analysis compares weighted Logistic Regression and Gradient-Boosted Trees using chronological train, validation, and test cohorts. Model choice and classification thresholds are determined on validation years; later test years are evaluated once.

The final corrected results are:

- Audience-reception hit model: test PR AUC `{{FINAL_HIT_PR_AUC}}`, ROC AUC `{{FINAL_HIT_ROC_AUC}}`, precision `{{FINAL_HIT_PRECISION}}`, recall `{{FINAL_HIT_RECALL}}`, and positive-class F1 `{{FINAL_HIT_F1}}` at threshold `{{FINAL_HIT_THRESHOLD}}`.
- Oscar nomination model: test PR AUC `{{FINAL_AWARDS_PR_AUC}}`, ROC AUC `{{FINAL_AWARDS_ROC_AUC}}`, precision `{{FINAL_AWARDS_PRECISION}}`, recall `{{FINAL_AWARDS_RECALL}}`, and positive-class F1 `{{FINAL_AWARDS_F1}}` at threshold `{{FINAL_AWARDS_THRESHOLD}}`.
- Director prior-rating association: Pearson correlation `{{FINAL_DIRECTOR_CORRELATION}}`, interpreted as an association rather than a causal effect.
- High-rating, low-vote niche candidates: `{{FINAL_NICHE_CANDIDATE_COUNT}}` films.

The business recommendation is to use CineScope as a ranking and research aid, not as an automatic greenlight system. Its scores can prioritize projects for deeper review and make tradeoffs explicit, but they do not include production budget, marketing spend, distribution, release strategy, or box-office revenue. Those omitted variables are central to commercial success.

## Project Overview and Key Takeaways

CineScope converts fragmented film, audience, creative-team, and awards records into three useful analytical outputs:

| Analytical output | Potential use | Result |
|---|---|---|
| Project screening | Prioritize projects for creative, market, and prestige review | Ranked audience-reception and Oscar-recognition scores |
| Portfolio benchmarking | Compare projects with relevant genre and era cohorts | Contextual rating, reach, and runtime profiles |
| Catalog discovery | Surface highly rated films with limited mainstream reach | Niche-candidate review queue |

### Findings and Practical Implications

| Finding | Practical implication | Appropriate use |
|---|---|---|
| Genre performance changes across decades | Timeless genre averages can mislead | Benchmark within comparable eras and audience contexts |
| Creative-team history contains signal | Prior work can improve screening discipline | Use track record as one diligence input, never as a veto |
| Runtime patterns are descriptive, not causal | Editing to an aggregate "sweet spot" is not evidence-based | Evaluate runtime within genre, format, and positioning |
| Rare outcomes require threshold choices | One cutoff cannot serve every business team | Set thresholds according to the cost of missed opportunities and false alarms |
| IMDb reception is not financial return | Current scores cannot support profitability conclusions | Add budget, marketing, distribution, and revenue before ROI analysis |

### Appropriate Scope

The current system is best understood as an analytical prototype and prioritization aid. It can help organize evidence and focus attention, but its scores should be interpreted alongside comparable-film context, feature explanations, and explicit data limitations.

## 1. Business Problem

Film decisions are made under uncertainty. Before release, decision-makers may know a project's genre, planned runtime, credited creative team, release period, and the team's previous work. They do not yet know the film's eventual IMDb rating, vote count, or awards outcome. CineScope asks three focused questions:

1. Which observable film and creative-team characteristics are associated with audience reception and reach?
2. Can pre-release information rank films by the likelihood of becoming an IMDb audience-reception hit?
3. Can the same information rank films by the likelihood of receiving an Oscar nomination?

The word "hit" has a narrow project definition. It means an IMDb average rating of at least 7.0 and at least 1,000 votes. This combines perceived quality with a minimum level of audience reach. It does not mean profitability, box-office revenue, return on investment, or streaming retention. The report therefore uses "audience-reception hit" whenever the distinction matters.

Oscar recognition is defined as at least one mapped nomination. Wins and Best Picture outcomes are valuable descriptive fields, but they are too sparse to serve as the primary rare-event model under the project deadline.

### Analytical Objectives

The analysis is useful when it supports a more disciplined shortlist, explains why a title receives attention, and makes threshold tradeoffs visible. A high aggregate model score is not sufficient by itself. For this reason, CineScope emphasizes chronological testing, rare-event metrics, and transparent interpretation.

## 2. Why Big-Data Technology Is Required

IMDb publishes separate normalized datasets for titles, ratings, people, and principal cast and crew relationships. The final modeling table is moderate in size, but producing it requires operations over much larger source tables:

| Source | Reference scale | Role in CineScope |
|---|---:|---|
| `title.basics` | approximately 12.7 million rows | Film type, release year, runtime, and genres |
| `title.ratings` | approximately 1.7 million rows | Rating and vote outcomes |
| `title.principals` | approximately 100.9 million rows | Many-to-many film and person relationships |
| `name.basics` | approximately 13 million rows | Person identity and profession data |
| `DLu/oscar_data` | approximately 12,000 rows | Nomination, category, winner, and IMDb identifiers |

A person can appear in many films, and a film can have many principal contributors. Computing each person's history before every film creates a shuffle-heavy workload with temporal windows and repeated aggregation. Spark distributes these operations across partitions, while Parquet reduces repeated I/O after the raw TSV sources have been cleaned.

The project demonstrates two execution modes. Local Spark uses a Mac and external SSD for development, analytics, and ML. The same ETL jobs run in batch mode on NYU Dataproc using HDFS and YARN. This is a portability and scalability demonstration; it is not a claim that the course cluster must have lower wall-clock time than a well-provisioned local machine for this dataset.

## 3. Architecture

```text
Official IMDb TSV files + DLu Oscar data
                    |
                    v
          Explicit schema and cleaning
                    |
                    v
      Bronze: movies_ratings, nominations
                    |
                    v
 Temporal person history + film aggregation
                    |
                    v
 Silver: cast_crew_features, movies_enriched,
         movie_oscar_features, movies_awards_enriched
                    |
                    v
       Analytics + chronological ML evaluation
                    |
                    v
       JSON metrics, PNG charts, saved models
```

### 3.1 Bronze Layer

The baseline job reads IMDb title and rating TSV files with explicit Spark schemas. IMDb's `\N` missing-value marker is converted to null before numeric casting. Titles are filtered to movies and joined to ratings by `tconst`. The inner join intentionally limits the analytical population to rated movies.

The Oscar job reads the tab-separated `oscars.csv` file from `DLu/oscar_data`. Some records contain multiple IMDb identifiers separated by `|`; these are exploded before aggregation. Rows without a usable IMDb title identifier cannot be joined and are excluded from film-level labels.

### 3.2 Silver Layer

The cast and crew job restricts the large principals table to rated movie identifiers early. It retains relevant creative roles and deduplicates person-role combinations before film-level aggregation. The output remains one row per film.

For each `(person, film)` pair, a Spark window orders films by release year. Prior counts and prior ratings use only films with `start_year` strictly less than the current film's year. Current-film and future-film rows are excluded. Films with missing years receive zero or null prior features because no defensible temporal history can be established.

Known cast and director flags are also point-in-time. A person qualifies at a particular film only when their earlier record contains at least 10 rated films with a prior mean rating of at least 7.0. The same person can therefore be unknown early in a career and known later.

### 3.3 Broadcast Join Demonstration

The pipeline separately creates a small full-corpus known-person lookup and joins it to the large principals side using `F.broadcast`. The Spark physical plan confirms a `BroadcastHashJoin`, demonstrating how a small dimension avoids repartitioning a much larger relationship table.

This full-corpus lookup is used only for physical-plan evidence. It does not enter the predictive feature table because it uses lifetime outcomes and would reveal future information for earlier films. The report does not claim a numerical broadcast speedup because the project did not run a controlled broadcast-versus-shuffle timing experiment.

## 4. Methodological Corrections and Validity Controls

An audit of the provisional July model found that four known-person fields were originally derived from full-career ratings and votes. Those fields included current and future outcomes while being described as pre-release predictors. The provisional July model metrics are therefore superseded.

The final implementation corrects this issue at its source:

- Film-level known-person flags are derived independently for each film from strict prior history.
- Full-career lookup fields are isolated from persisted modeling features.
- Predictive code rejects direct outcomes, Oscar fields, full-career aggregates, and retrospective prior-vote totals.
- Model notebooks reject silver artifacts generated before feature-semantics version 2.
- Regression tests verify that a person can cross the known-person threshold only after accumulating the required earlier record.

IMDb supplies current ratings and vote totals, not a historical snapshot for every release date. The final models exclude prior vote totals because later voting can alter those values. Prior-film average ratings remain retrospective reputation proxies and are disclosed as such. A stricter prospective production system would require timestamped rating snapshots.

## 5. Analytical Findings

### 5.1 Genre and Decade

The first analysis explodes genre arrays and calculates film count, median IMDb rating, total votes, and median votes by genre and decade. Chart cells with fewer than 50 films are removed to prevent very small groups from driving the visual narrative.

**Final finding:** `{{FINAL_GENRE_FINDING}}`

**Business interpretation:** Genre expectations are era-dependent. A decision process should compare a project with relevant films from a similar period rather than treating a genre's historical average as timeless.

### 5.2 Director Track Record

The second analysis compares a director's prior-film mean rating with the current film's rating. The final correlation is `{{FINAL_DIRECTOR_CORRELATION}}`.

**Business interpretation:** Prior creative reputation contains useful signal, but the relationship is not causal. Established directors may receive larger budgets, stronger scripts, better distribution, and greater marketing support. Director track record should support due diligence rather than justify a hiring decision on its own.

### 5.3 Runtime Profile

The runtime analysis groups films into 15-minute buckets between 40 and 240 minutes and excludes buckets containing fewer than 50 films. It plots both film volume and median rating.

**Final finding:** `{{FINAL_RUNTIME_FINDING}}`

**Business interpretation:** The chart describes where films are concentrated and how ratings differ across those groups. It does not prove that changing a screenplay's length will change its rating. Runtime interacts with genre, format, budget, and audience expectations.

### 5.4 Niche Candidates

The anomaly analysis identifies films with ratings of at least 8.0 and vote counts in the bottom decile. The corrected run identifies `{{FINAL_NICHE_CANDIDATE_COUNT}}` candidates.

**Business interpretation:** These are not automatically fraudulent or manipulated ratings. They may include niche, regional, documentary, festival, recent, or otherwise low-discovery films. For a streaming platform, the list is a discovery queue for editorial review and catalog acquisition research.

### 5.5 Pre-Release Signal Lift

The fifth analysis orders eligible films by director prior film count and creates four quantile bands. It compares each band's hit rate with the eligible baseline. This replaces the earlier vote-quartile chart, which was circular because vote count is part of the hit definition.

**Final finding:** `{{FINAL_SIGNAL_LIFT_FINDING}}`

**Business interpretation:** The result measures association between prior experience and later audience reception. Even a strong lift would not imply that experience alone determines success.

## 6. Predictive Modeling

### 6.1 Feature Contract

Candidate predictors include release year, runtime, role counts, genre indicators, prior movie counts, prior-film rating proxies, and corrected point-in-time known-person flags. The following information is forbidden:

- Current film rating, vote count, and hit label
- Oscar nomination and win fields
- Full-career person aggregates
- Prior vote totals derived from the current IMDb snapshot

### 6.2 Chronological Evaluation

A random split can train on later films while testing on earlier films. CineScope instead uses chronological partitions.

| Outcome | Training years | Validation years | Test years |
|---|---|---|---|
| Audience-reception hit | 1960-2013 | 2014-2018 | 2019-2023 |
| Oscar nomination | 1960-2007 | 2008-2013 | 2014-2019 |

Training rows receive inverse-frequency class weights so positive and negative classes contribute equal aggregate weight. Candidate Logistic Regression and GBT configurations are fitted on training years. Validation PR AUC selects the model and hyperparameters. A validation threshold sweep selects the operating point that maximizes positive-class F1. The selected pipeline and threshold are then applied once to the later test period.

### 6.3 Final Model Comparison

| Outcome | Selected algorithm | PR AUC | ROC AUC | Precision | Recall | Positive F1 | Threshold |
|---|---|---:|---:|---:|---:|---:|---:|
| Audience-reception hit | `{{FINAL_HIT_ALGORITHM}}` | `{{FINAL_HIT_PR_AUC}}` | `{{FINAL_HIT_ROC_AUC}}` | `{{FINAL_HIT_PRECISION}}` | `{{FINAL_HIT_RECALL}}` | `{{FINAL_HIT_F1}}` | `{{FINAL_HIT_THRESHOLD}}` |
| Oscar nomination | `{{FINAL_AWARDS_ALGORITHM}}` | `{{FINAL_AWARDS_PR_AUC}}` | `{{FINAL_AWARDS_ROC_AUC}}` | `{{FINAL_AWARDS_PRECISION}}` | `{{FINAL_AWARDS_RECALL}}` | `{{FINAL_AWARDS_F1}}` | `{{FINAL_AWARDS_THRESHOLD}}` |

PR AUC is the primary ranking metric because both outcomes are rare. ROC AUC is retained for comparability, while precision, recall, and positive-class F1 describe behavior at the chosen operating threshold. Overall accuracy and frequency-weighted F1 are not headline results because the large negative class can make them look strong even when most positive films are missed.

## 7. Business Recommendations and Future Work

### Recommended Use

1. **Use scores for ranking, not automatic approval.** The models can prioritize projects for human review but should not independently approve or reject a film.
2. **Keep the two outcomes separate.** Audience reception and Oscar recognition have different labels, prevalence, and decision costs.
3. **Choose thresholds by use case.** A broad screening process may favor recall, while a limited awards-focused review may favor precision.
4. **Show context with every score.** Include comparable genre-era cohorts, important features, and known limitations.

### Analytical Applications

5. **Benchmark within genre and era.** Avoid comparing a current project with one timeless industry average.
6. **Treat creative-team history as supporting evidence.** Use prior work to focus diligence while recognizing budget, script, access, and selection effects.
7. **Review niche candidates manually.** Send high-rating, low-reach titles to editorial or acquisition specialists rather than labeling them anomalous in a negative sense.

### Future Development

8. **Add economic and distribution data before making commercial claims.** Budget, marketing, release footprint, distribution channel, revenue, and streaming engagement are required before modeling ROI.
9. **Validate prospectively.** Score future release cohorts before outcomes are known and compare predictions with later audience and award results.
10. **Evaluate stability over time.** Add rolling temporal validation, calibration checks, and monitoring before considering operational use.

### Development Roadmap

| Stage | Deliverable | Purpose |
|---|---|---|
| Current | Transparent ranking prototype and analytical report | Demonstrate scalable analysis and defensible evaluation |
| Next | Economic data enrichment and prospective score tracking | Test whether broader business outcomes can be modeled |
| Later | Monitoring, repeatable refreshes, and workflow integration | Assess operational feasibility |

## 8. Limitations

- IMDb users are self-selected and do not represent every audience.
- Older films have had more time to accumulate votes, creating exposure-age bias.
- Current IMDb ratings are retrospective snapshots and may differ from ratings available at historical decision time.
- The hit label measures IMDb reception and reach, not financial success.
- Oscar rows without usable IMDb identifiers cannot contribute positive labels.
- Films outside the selected Oscar cohort may have incomplete or structurally different eligibility.
- Correlation does not establish causation.
- Class weighting and threshold choice encode business preferences and must be revisited for a real deployment.
- The final evaluation uses one chronological holdout design. Broader rolling-origin validation would provide a stronger estimate of performance stability.
- The project is an analytical prototype. It does not include monitoring, scheduled retraining, model calibration, an API, or a production decision workflow.

These limitations define which conclusions the current evidence can support and which questions require additional data or validation.

## 9. Performance Engineering and Reproducibility

The July 31 reference ETL execution produced 348,676 rated films. The baseline join completed in approximately 25.3 seconds locally, cast and crew feature engineering in approximately 121.4 seconds, and Oscar aggregation in approximately 8.0 seconds. The full Dataproc batch pipeline took approximately 23 minutes. These timings describe different environments and should not be interpreted as a controlled benchmark.

Physical plans are saved under `outputs/plans/`, metrics under `outputs/metrics/`, charts under `outputs/charts/generated/`, and models under the configured data root. The repository keeps machine-specific generated evidence out of Git because it contains absolute local and HDFS paths.

The final reproducibility sequence is:

```bash
make test
make baseline
make cast-crew
make oscars

# Then execute locally in order:
# notebooks/02_core_analytics.ipynb
# notebooks/03_train_hit_model.ipynb
# notebooks/04_train_awards_model.ipynb
```

Before submission, the final report and presentation must be reconciled against `analytics_metrics.json`, `hit_model_metrics.json`, and `awards_model_metrics.json` generated by feature-semantics version 2.

## 10. Conclusion

CineScope demonstrates an end-to-end big-data workflow in which distributed processing is justified by the source relationships and temporal feature construction, not merely by the final row count. The project integrates IMDb and Oscar data, materializes reusable Parquet tables, demonstrates a broadcast join, computes point-in-time person history, and evaluates rare-event classifiers on later film cohorts.

Its main practical lesson is that honest validation matters as much as model choice. Removing future-derived features, separating validation from test decisions, and reporting positive-class metrics produces a more credible business tool even if headline performance declines.

CineScope can organize evidence, rank opportunities, and make screening criteria more consistent, but it should not allocate capital or replace creative judgment. The most useful next steps are to add economic and distribution data, validate predictions prospectively, and test performance stability over time. These additions would extend the current analytical prototype toward a broader film-planning tool while preserving its transparent and evidence-based design.

## References

1. IMDb. "IMDb Non-Commercial Datasets." https://developer.imdb.com/non-commercial-datasets/
2. IMDb. "IMDb Datasets." https://datasets.imdbws.com/
3. DLu. "Oscar Award Data with IMDb Film Identifiers." https://github.com/DLu/oscar_data
4. Apache Spark. "Spark SQL, DataFrames and Datasets Guide." https://spark.apache.org/docs/3.5.6/sql-programming-guide.html
5. Apache Spark. "MLlib Classification and Regression." https://spark.apache.org/docs/3.5.6/ml-classification-regression.html

## Appendix A: Submission Reconciliation Checklist

- [ ] Resolve every metric placeholder from version 2 JSON outputs.
- [ ] Verify every displayed result against its exact JSON key.
- [ ] Confirm all three temporal partitions contain positive labels.
- [ ] Confirm model choice and threshold originate from validation results.
- [ ] Confirm no forbidden field appears in either `feature_cols` list.
- [ ] Confirm the final chart filenames match the report and deck.
- [ ] Update reference runtimes only if the corrected full rerun materially changes them.
- [ ] Export and inspect DOCX and PDF versions page by page.

## Appendix B: Superseded Results

The July 31 GBT model metrics are retained only as project-history evidence. They used random train/test splits, default thresholds, frequency-weighted F1, and full-career known-person flags. They must not appear as final model performance in the report or presentation.