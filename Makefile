.PHONY: setup init-storage validate-storage download download-oscars inspect inspect-counts test baseline cast-crew enriched oscars clean-generated clean-hpc

ROOT_DIR := $(shell pwd)
PYTHON := $(ROOT_DIR)/.venv/bin/python
PIP := $(ROOT_DIR)/.venv/bin/pip

setup:
	python3.11 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Setup complete. Activate with: source .venv/bin/activate"

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

test:
	@if [ -n "$$JAVA_HOME" ]; then export PATH="$$JAVA_HOME/bin:$$PATH"; fi; \
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

clean-generated:
	rm -rf outputs/logs/* \
		outputs/metrics/baseline_metrics.json \
		outputs/metrics/cast_crew_metrics.json \
		outputs/metrics/oscar_metrics.json \
		outputs/plans/movies_ratings_plan.txt \
		outputs/plans/cast_crew_plan.txt \
		outputs/plans/known_people_broadcast_plan.txt \
		outputs/plans/oscar_features_plan.txt \
		outputs/charts/generated
	@echo "Cleaned generated local artifacts. Raw IMDb / Parquet on the SSD was not deleted."

# Wipe this account's CineScope HDFS tree + /tmp scratch on Dataproc.
# Requires .env with CINESCOPE_STORAGE_BACKEND=hdfs. Prompts for YES unless CLEAN_HPC_YES=1.
clean-hpc:
	@if [ "$${CLEAN_HPC_YES:-0}" = "1" ]; then \
		bash scripts/cleanup_hpc.sh --yes; \
	else \
		bash scripts/cleanup_hpc.sh; \
	fi
