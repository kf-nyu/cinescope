.PHONY: setup setup-hpc init-storage validate-storage download download-oscars inspect inspect-counts test baseline cast-crew enriched oscars pipeline final-artifacts validate-final clean-generated clean-hpc

ROOT_DIR := $(shell pwd)
PYTHON := $(ROOT_DIR)/.venv/bin/python
PIP := $(ROOT_DIR)/.venv/bin/pip

setup:
	python3.11 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Setup complete. Activate with: source .venv/bin/activate"

# Dataproc only: derive NetID from login, write .env from .env.hpc.example, create venv.
setup-hpc:
	bash scripts/setup_hpc.sh

init-storage:
	bash scripts/initialize_storage.sh

validate-storage:
	bash scripts/validate_storage.sh

download:
	bash scripts/download_imdb.sh

download-oscars:
	bash scripts/download_oscars.sh

inspect:
	bash scripts/inspect_imdb.sh

inspect-counts:
	bash scripts/inspect_imdb.sh --count-rows

# Unset cluster SPARK_HOME so pip PySpark unit tests do not mix with /usr/lib/spark.
test:
	@if [ -n "$$JAVA_HOME" ]; then export PATH="$$JAVA_HOME/bin:$$PATH"; fi; \
	unset SPARK_HOME SPARK_CONF_DIR; \
	PYSPARK_PYTHON=$(PYTHON) PYSPARK_DRIVER_PYTHON=$(PYTHON) \
	PYTHONPATH=$(ROOT_DIR)/src $(PYTHON) -m pytest -q

baseline:
	bash scripts/run_baseline.sh

cast-crew:
	bash scripts/run_cast_crew_features.sh

enriched:
	bash scripts/run_cast_crew_features.sh

oscars:
	bash scripts/run_oscars.sh

# Full pipeline; tees all console output to outputs/logs/pipeline_<UTC>.log
pipeline:
	bash scripts/run_pipeline.sh

# Local Spark only: regenerate corrected silver outputs, execute notebooks 02-04,
# and validate versioned metrics. Report placeholders remain allowed at this stage.
final-artifacts:
	bash scripts/run_final_artifacts.sh

# Submission gate: requires corrected metrics/charts and no FINAL placeholders.
validate-final:
	$(PYTHON) scripts/validate_final_artifacts.py --report outputs/report/CineScope_Final_Report.md

clean-generated:
	rm -rf outputs/logs/* \
		outputs/metrics/baseline_metrics.json \
		outputs/metrics/cast_crew_metrics.json \
		outputs/metrics/oscar_metrics.json \
		outputs/plans/movies_ratings_plan.txt \
		outputs/plans/cast_crew_plan.txt \
		outputs/plans/known_people_broadcast_plan.txt \
		outputs/plans/oscar_features_plan.txt \
		outputs/charts/generated \
		outputs/notebooks \
		outputs/report
	@echo "Cleaned generated local artifacts. Raw IMDb / Parquet on the SSD was not deleted."

# Wipe this account's CineScope HDFS tree + /tmp scratch on Dataproc.
# Requires .env with CINESCOPE_STORAGE_BACKEND=hdfs. Prompts for YES unless CLEAN_HPC_YES=1.
clean-hpc:
	@if [ "$${CLEAN_HPC_YES:-0}" = "1" ]; then \
		bash scripts/cleanup_hpc.sh --yes; \
	else \
		bash scripts/cleanup_hpc.sh; \
	fi
