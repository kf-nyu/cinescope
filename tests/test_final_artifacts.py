"""Dependency-free tests for final report and artifact tooling."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path


from scripts.finalize_report import genre_finding, runtime_finding, signal_lift_finding
from scripts.validate_final_artifacts import validate_model


class FinalReportFindingTests(unittest.TestCase):
    def test_qualitative_findings_are_derived_from_metrics(self):
        analytics = {
            "genre_summary": [
                {"genre": "Drama", "rating_change": 0.3},
                {"genre": "Comedy", "rating_change": -0.2},
            ],
            "runtime_profile": {
                "peak_volume_bucket_minutes": 90,
                "peak_volume_film_count": 1234,
                "peak_volume_median_rating": 6.7,
            },
            "pre_release_signal_lift": [
                {"signal_band": 1, "lift_vs_eligible": 0.5},
                {"signal_band": 4, "lift_vs_eligible": 1.8},
            ],
        }

        self.assertIn("Drama", genre_finding(analytics))
        self.assertIn("Comedy", genre_finding(analytics))
        self.assertIn("90-minute", runtime_finding(analytics))
        self.assertIn("1,234", runtime_finding(analytics))
        self.assertIn("1.80x", signal_lift_finding(analytics))


class FinalArtifactContractTests(unittest.TestCase):
    def test_valid_model_contract_passes(self):
        with tempfile.TemporaryDirectory() as directory:
            charts_dir = Path(directory)
            chart = charts_dir / "model.png"
            chart.touch()
            metrics = {
                "schema_version": 2,
                "feature_cols": ["runtime_minutes", "genre_drama"],
                "partitions": {
                    "train": {
                        "rows": 100,
                        "positives": 10,
                        "min_year": 1960,
                        "max_year": 2010,
                    },
                    "validation": {
                        "rows": 20,
                        "positives": 2,
                        "min_year": 2011,
                        "max_year": 2015,
                    },
                    "test": {
                        "rows": 20,
                        "positives": 2,
                        "min_year": 2016,
                        "max_year": 2020,
                    },
                },
                "candidate_results": [{"candidate_id": "lr"}],
                "selected_candidate": {"candidate_id": "lr"},
                "threshold_selection": {
                    "source": "validation",
                    "selected": {"threshold": 0.4},
                },
                "test_metrics": {
                    "pr_auc": 0.5,
                    "roc_auc": 0.8,
                    "precision": 0.5,
                    "recall": 0.5,
                    "positive_f1": 0.5,
                    "tp": 1,
                    "fp": 1,
                    "fn": 1,
                    "tn": 17,
                },
                "charts": [str(chart)],
            }
            errors: list[str] = []

            validate_model(metrics, "model", charts_dir, errors)

            self.assertEqual(errors, [])

    def test_forbidden_feature_fails_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics = {
                "schema_version": 2,
                "feature_cols": ["average_rating"],
                "partitions": {},
                "candidate_results": [],
                "selected_candidate": {},
                "threshold_selection": {},
                "test_metrics": {},
                "charts": [],
            }
            errors: list[str] = []

            validate_model(metrics, "model", Path(directory), errors)

            self.assertTrue(
                any("forbidden features" in error for error in errors),
                errors,
            )


if __name__ == "__main__":
    unittest.main()