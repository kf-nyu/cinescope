"""Fail-closed validation for final CineScope metrics, charts, and report."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_FEATURES = {
    "average_rating",
    "num_votes",
    "is_hit",
    "label_awards",
    "oscar_nomination_count",
    "oscar_win_count",
    "was_oscar_winner",
    "was_oscar_nominated",
    "first_oscar_ceremony",
    "last_oscar_ceremony",
    "best_picture_nomination_count",
    "best_picture_win_count",
    "best_picture_won",
    "best_picture_nominated",
    "acting_nomination_count",
    "acting_win_count",
    "directing_nomination_count",
    "directing_win_count",
    "writing_nomination_count",
    "writing_win_count",
    "career_movie_count",
    "career_average_rating",
    "career_total_votes",
    "is_known_person",
    "cast_prior_votes_sum",
    "director_prior_votes_sum",
    "principal_prior_votes_sum",
}


def load_json(path: Path, errors: list[str]) -> dict[str, Any]:
    if not path.is_file():
        errors.append(f"missing metrics file: {path}")
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"invalid JSON {path}: {exc}")
        return {}
    if not isinstance(value, dict):
        errors.append(f"expected JSON object: {path}")
        return {}
    return value


def validate_version(metrics: dict[str, Any], name: str, errors: list[str]) -> None:
    if metrics.get("schema_version") != 2:
        errors.append(f"{name} must have schema_version 2")


def validate_charts(
    metrics: dict[str, Any], name: str, charts_dir: Path, errors: list[str]
) -> None:
    charts = metrics.get("charts")
    if not isinstance(charts, list) or not charts:
        errors.append(f"{name} has no chart list")
        return
    for chart in charts:
        candidate = Path(str(chart))
        if not candidate.is_file():
            candidate = charts_dir / candidate.name
        if not candidate.is_file():
            errors.append(f"{name} chart is missing: {Path(str(chart)).name}")


def validate_partition_order(
    partitions: dict[str, Any], name: str, errors: list[str]
) -> None:
    expected = ("train", "validation", "test")
    if any(part not in partitions for part in expected):
        errors.append(f"{name} is missing train/validation/test partition metadata")
        return
    summaries = [partitions[part] for part in expected]
    for part, summary in zip(expected, summaries):
        if int(summary.get("rows") or 0) <= 0:
            errors.append(f"{name} {part} partition is empty")
        if int(summary.get("positives") or 0) <= 0:
            errors.append(f"{name} {part} partition has no positives")
    train_max = summaries[0].get("max_year")
    validation_min = summaries[1].get("min_year")
    validation_max = summaries[1].get("max_year")
    test_min = summaries[2].get("min_year")
    if None in (train_max, validation_min, validation_max, test_min):
        errors.append(f"{name} partition years are incomplete")
    elif not (train_max < validation_min and validation_max < test_min):
        errors.append(f"{name} partition years overlap or are out of order")


def validate_model(
    metrics: dict[str, Any], name: str, charts_dir: Path, errors: list[str]
) -> None:
    validate_version(metrics, name, errors)
    features = set(metrics.get("feature_cols") or [])
    leaked = sorted(features & FORBIDDEN_FEATURES)
    if leaked:
        errors.append(f"{name} contains forbidden features: {', '.join(leaked)}")
    if not features:
        errors.append(f"{name} has no feature list")

    validate_partition_order(metrics.get("partitions") or {}, name, errors)

    candidates = metrics.get("candidate_results") or []
    selected = metrics.get("selected_candidate") or {}
    candidate_ids = {item.get("candidate_id") for item in candidates}
    if not candidates or selected.get("candidate_id") not in candidate_ids:
        errors.append(f"{name} selected candidate is not in candidate results")

    threshold_block = metrics.get("threshold_selection") or {}
    if threshold_block.get("source") != "validation":
        errors.append(f"{name} threshold source must be validation")
    threshold = (threshold_block.get("selected") or {}).get("threshold")
    if threshold is None or not 0 <= float(threshold) <= 1:
        errors.append(f"{name} selected threshold is invalid")
    test_metrics = metrics.get("test_metrics") or {}
    for key in ("pr_auc", "roc_auc", "precision", "recall", "positive_f1"):
        value = test_metrics.get(key)
        if value is None or not 0 <= float(value) <= 1:
            errors.append(f"{name} test metric {key} is missing or invalid")
    for key in ("tp", "fp", "fn", "tn"):
        value = test_metrics.get(key)
        if value is None or int(value) < 0:
            errors.append(f"{name} confusion count {key} is missing or invalid")

    validate_charts(metrics, name, charts_dir, errors)


def validate_executed_notebooks(
    notebooks_dir: Path,
    source_dir: Path,
    errors: list[str],
) -> None:
    """Require executed notebooks to match current source and contain no errors."""
    names = (
        "02_core_analytics",
        "03_train_hit_model",
        "04_train_awards_model",
    )
    for name in names:
        source_path = source_dir / f"{name}.ipynb"
        executed_path = notebooks_dir / f"{name}_executed.ipynb"
        if not source_path.is_file():
            errors.append(f"missing source notebook: {source_path}")
            continue
        if not executed_path.is_file():
            errors.append(f"missing executed notebook: {executed_path}")
            continue
        try:
            source = json.loads(source_path.read_text(encoding="utf-8"))
            executed = json.loads(executed_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"invalid notebook JSON for {name}: {exc}")
            continue

        source_cells = source.get("cells") or []
        executed_cells = executed.get("cells") or []
        source_content = [
            (cell.get("cell_type"), cell.get("source") or [])
            for cell in source_cells
        ]
        executed_content = [
            (cell.get("cell_type"), cell.get("source") or [])
            for cell in executed_cells
        ]
        if source_content != executed_content:
            errors.append(f"executed notebook source is stale: {executed_path}")

        outputs = [
            output
            for cell in executed_cells
            for output in (cell.get("outputs") or [])
        ]
        if not outputs:
            errors.append(f"executed notebook has no saved outputs: {executed_path}")
        if any(output.get("output_type") == "error" for output in outputs):
            errors.append(f"executed notebook contains an error output: {executed_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--metrics-dir",
        type=Path,
        default=ROOT / "outputs" / "metrics",
    )
    parser.add_argument(
        "--charts-dir",
        type=Path,
        default=ROOT / "outputs" / "charts" / "generated",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "docs" / "final_report.md",
    )
    parser.add_argument(
        "--notebooks-dir",
        type=Path,
        default=ROOT / "outputs" / "notebooks",
    )
    parser.add_argument(
        "--source-notebooks-dir",
        type=Path,
        default=ROOT / "notebooks",
    )
    parser.add_argument("--allow-report-placeholders", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    errors: list[str] = []

    cast = load_json(args.metrics_dir / "cast_crew_metrics.json", errors)
    if cast and cast.get("feature_semantics_version") != 2:
        errors.append("cast/crew metrics must have feature_semantics_version 2")

    oscars = load_json(args.metrics_dir / "oscar_metrics.json", errors)
    if oscars and oscars.get("feature_semantics_version") != 2:
        errors.append("Oscar metrics must propagate feature_semantics_version 2")
    if cast and oscars:
        cast_completed = cast.get("completed_at_utc")
        oscar_completed = oscars.get("completed_at_utc")
        if cast_completed and oscar_completed and oscar_completed < cast_completed:
            errors.append("Oscar enrichment predates the corrected cast/crew artifact")

    analytics = load_json(args.metrics_dir / "analytics_metrics.json", errors)
    if analytics:
        validate_version(analytics, "analytics", errors)
        validate_charts(analytics, "analytics", args.charts_dir, errors)

    hit = load_json(args.metrics_dir / "hit_model_metrics.json", errors)
    awards = load_json(args.metrics_dir / "awards_model_metrics.json", errors)
    if hit:
        validate_model(hit, "hit model", args.charts_dir, errors)
    if awards:
        validate_model(awards, "awards model", args.charts_dir, errors)

    validate_executed_notebooks(
        args.notebooks_dir,
        args.source_notebooks_dir,
        errors,
    )

    if not args.report.is_file():
        errors.append(f"missing report source: {args.report}")
    elif not args.allow_report_placeholders:
        report = args.report.read_text(encoding="utf-8")
        placeholders = sorted(set(re.findall(r"\{\{FINAL_[A-Z0-9_]+\}\}", report)))
        if placeholders:
            errors.append(
                "report contains unresolved placeholders: " + ", ".join(placeholders)
            )

    if errors:
        print("Final artifact validation FAILED:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Final artifact validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())