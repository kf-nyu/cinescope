"""Resolve the CineScope business-report template from version 2 metrics."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]


def load_version2(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema_version") != 2:
        raise RuntimeError(f"Expected schema_version 2: {path}")
    return value


def format_metric(value: Any, digits: int = 3) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):.{digits}f}"


def genre_finding(analytics: dict[str, Any]) -> str:
    rows = analytics.get("genre_summary") or []
    if not rows:
        raise RuntimeError("analytics metrics have no genre_summary")
    increase = max(rows, key=lambda row: float(row["rating_change"]))
    decrease = min(rows, key=lambda row: float(row["rating_change"]))
    return (
        f"Among the eight highest-volume genres, {increase['genre']} had the "
        f"largest median-rating increase ({float(increase['rating_change']):+.2f}) "
        f"between its first and latest qualifying decades, while "
        f"{decrease['genre']} had the largest decrease "
        f"({float(decrease['rating_change']):+.2f})."
    )


def runtime_finding(analytics: dict[str, Any]) -> str:
    profile = analytics.get("runtime_profile") or {}
    return (
        f"The {int(profile['peak_volume_bucket_minutes'])}-minute bucket contained "
        f"the most films ({int(profile['peak_volume_film_count']):,}) and had a "
        f"median IMDb rating of {float(profile['peak_volume_median_rating']):.2f}; "
        "this is a volume profile, not a causal optimum."
    )


def signal_lift_finding(analytics: dict[str, Any]) -> str:
    rows = analytics.get("pre_release_signal_lift") or []
    if not rows:
        raise RuntimeError("analytics metrics have no pre_release_signal_lift")
    strongest = max(rows, key=lambda row: float(row["lift_vs_eligible"]))
    weakest = min(rows, key=lambda row: float(row["lift_vs_eligible"]))
    return (
        f"Director-experience band {int(strongest['signal_band'])} had the strongest "
        f"audience-hit lift at {float(strongest['lift_vs_eligible']):.2f}x the eligible "
        f"baseline, compared with {float(weakest['lift_vs_eligible']):.2f}x in band "
        f"{int(weakest['signal_band'])}."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--metrics-dir",
        type=Path,
        default=ROOT / "outputs" / "metrics",
    )
    parser.add_argument(
        "--template",
        type=Path,
        default=ROOT / "docs" / "final_report.md",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "outputs" / "report" / "CineScope_Final_Report.md",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    analytics = load_version2(args.metrics_dir / "analytics_metrics.json")
    hit = load_version2(args.metrics_dir / "hit_model_metrics.json")
    awards = load_version2(args.metrics_dir / "awards_model_metrics.json")

    hit_test = hit["test_metrics"]
    awards_test = awards["test_metrics"]
    replacements = {
        "FINAL_HIT_ALGORITHM": str(hit["selected_candidate"]["algorithm"]),
        "FINAL_HIT_PR_AUC": format_metric(hit_test["pr_auc"]),
        "FINAL_HIT_ROC_AUC": format_metric(hit_test["roc_auc"]),
        "FINAL_HIT_PRECISION": format_metric(hit_test["precision"]),
        "FINAL_HIT_RECALL": format_metric(hit_test["recall"]),
        "FINAL_HIT_F1": format_metric(hit_test["positive_f1"]),
        "FINAL_HIT_THRESHOLD": format_metric(
            hit["threshold_selection"]["selected"]["threshold"]
        ),
        "FINAL_AWARDS_ALGORITHM": str(
            awards["selected_candidate"]["algorithm"]
        ),
        "FINAL_AWARDS_PR_AUC": format_metric(awards_test["pr_auc"]),
        "FINAL_AWARDS_ROC_AUC": format_metric(awards_test["roc_auc"]),
        "FINAL_AWARDS_PRECISION": format_metric(awards_test["precision"]),
        "FINAL_AWARDS_RECALL": format_metric(awards_test["recall"]),
        "FINAL_AWARDS_F1": format_metric(awards_test["positive_f1"]),
        "FINAL_AWARDS_THRESHOLD": format_metric(
            awards["threshold_selection"]["selected"]["threshold"]
        ),
        "FINAL_DIRECTOR_CORRELATION": format_metric(
            analytics["director_prior_rating_corr"]
        ),
        "FINAL_NICHE_CANDIDATE_COUNT": f"{int(analytics['anomaly_count_rating_ge_8_bottom_decile_votes']):,}",
        "FINAL_GENRE_FINDING": genre_finding(analytics),
        "FINAL_RUNTIME_FINDING": runtime_finding(analytics),
        "FINAL_SIGNAL_LIFT_FINDING": signal_lift_finding(analytics),
    }

    report = args.template.read_text(encoding="utf-8")
    for key, value in replacements.items():
        report = report.replace("{{" + key + "}}", value)

    unresolved = sorted(set(re.findall(r"\{\{FINAL_[A-Z0-9_]+\}\}", report)))
    if unresolved:
        raise RuntimeError("Unresolved report placeholders: " + ", ".join(unresolved))

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(f"Wrote resolved report: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())