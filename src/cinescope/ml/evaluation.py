"""Shared evaluation helpers for rare-event film classifiers."""

from __future__ import annotations

from typing import Any, Sequence

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def temporal_split(
    df: DataFrame,
    *,
    year_col: str = "start_year",
    train_end: int,
    validation_end: int,
    test_end: int,
    min_year: int | None = None,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Return disjoint year-ordered frames, excluding null and out-of-range years."""
    if not train_end < validation_end < test_end:
        raise ValueError("Expected train_end < validation_end < test_end")

    eligible = df.filter(F.col(year_col).isNotNull())
    if min_year is not None:
        eligible = eligible.filter(F.col(year_col) >= F.lit(min_year))
    eligible = eligible.filter(F.col(year_col) <= F.lit(test_end))

    train = eligible.filter(F.col(year_col) <= F.lit(train_end))
    validation = eligible.filter(
        (F.col(year_col) > F.lit(train_end))
        & (F.col(year_col) <= F.lit(validation_end))
    )
    test = eligible.filter(
        (F.col(year_col) > F.lit(validation_end))
        & (F.col(year_col) <= F.lit(test_end))
    )
    return train, validation, test


def partition_summary(
    df: DataFrame,
    *,
    label_col: str,
    year_col: str = "start_year",
) -> dict[str, int | float | None]:
    """Collect cohort size, years, and positive prevalence for one partition."""
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.min(year_col).alias("min_year"),
        F.max(year_col).alias("max_year"),
        F.sum(F.when(F.col(label_col) == 1, 1).otherwise(0)).alias("positives"),
    ).collect()[0]
    rows = int(row["rows"] or 0)
    positives = int(row["positives"] or 0)
    return {
        "rows": rows,
        "min_year": row["min_year"],
        "max_year": row["max_year"],
        "positives": positives,
        "positive_rate": positives / rows if rows else None,
    }


def with_balanced_class_weights(
    df: DataFrame,
    *,
    label_col: str,
    output_col: str = "class_weight",
) -> tuple[DataFrame, dict[str, float]]:
    """Weight each class so positive and negative total weight are equal."""
    counts = {
        int(row[label_col]): int(row["count"])
        for row in df.groupBy(label_col).count().collect()
        if row[label_col] is not None
    }
    if set(counts) != {0, 1}:
        raise ValueError(
            f"Expected binary labels 0 and 1 in {label_col}; found {sorted(counts)}"
        )

    total = counts[0] + counts[1]
    weights = {
        "negative": total / (2.0 * counts[0]),
        "positive": total / (2.0 * counts[1]),
    }
    weighted = df.withColumn(
        output_col,
        F.when(F.col(label_col) == 1, F.lit(weights["positive"]))
        .when(F.col(label_col) == 0, F.lit(weights["negative"]))
        .otherwise(F.lit(None).cast("double")),
    )
    return weighted, weights


def with_positive_score(
    predictions: DataFrame,
    *,
    probability_col: str = "probability",
    output_col: str = "positive_score",
) -> DataFrame:
    """Extract the positive-class score from a Spark probability vector."""
    return predictions.withColumn(
        output_col,
        vector_to_array(F.col(probability_col))[1],
    )


def positive_class_metrics(
    *,
    tp: int,
    fp: int,
    fn: int,
    tn: int,
) -> dict[str, int | float | None]:
    """Compute metrics whose F1 explicitly refers to the positive class."""
    total = tp + fp + fn + tn
    precision = tp / (tp + fp) if tp + fp else None
    recall = tp / (tp + fn) if tp + fn else None
    if precision is None or recall is None or precision + recall == 0:
        positive_f1 = None
    else:
        positive_f1 = 2 * precision * recall / (precision + recall)
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "positive_f1": positive_f1,
        "accuracy": (tp + tn) / total if total else None,
        "prevalence": (tp + fn) / total if total else None,
    }


def evaluate_binary_predictions(
    predictions: DataFrame,
    *,
    label_col: str,
    prediction_col: str = "prediction",
    score_col: str = "positive_score",
) -> dict[str, int | float | None]:
    """Evaluate ranking and hard predictions without weighted multiclass F1."""
    row = predictions.agg(
        F.sum(
            F.when(
                (F.col(label_col) == 1) & (F.col(prediction_col) == 1), 1
            ).otherwise(0)
        ).alias("tp"),
        F.sum(
            F.when(
                (F.col(label_col) == 0) & (F.col(prediction_col) == 1), 1
            ).otherwise(0)
        ).alias("fp"),
        F.sum(
            F.when(
                (F.col(label_col) == 1) & (F.col(prediction_col) == 0), 1
            ).otherwise(0)
        ).alias("fn"),
        F.sum(
            F.when(
                (F.col(label_col) == 0) & (F.col(prediction_col) == 0), 1
            ).otherwise(0)
        ).alias("tn"),
    ).collect()[0]
    metrics = positive_class_metrics(
        tp=int(row["tp"] or 0),
        fp=int(row["fp"] or 0),
        fn=int(row["fn"] or 0),
        tn=int(row["tn"] or 0),
    )
    metrics["roc_auc"] = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol=score_col,
        metricName="areaUnderROC",
    ).evaluate(predictions)
    metrics["pr_auc"] = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol=score_col,
        metricName="areaUnderPR",
    ).evaluate(predictions)
    return metrics


def threshold_sweep(
    predictions: DataFrame,
    *,
    label_col: str,
    thresholds: Sequence[float],
    score_col: str = "positive_score",
) -> list[dict[str, Any]]:
    """Evaluate positive-class metrics for candidate score thresholds."""
    values = sorted({float(value) for value in thresholds})
    if not values or values[0] < 0 or values[-1] > 1:
        raise ValueError("Thresholds must contain values in [0, 1]")

    threshold_df = predictions.sparkSession.createDataFrame(
        [(value,) for value in values],
        ["threshold"],
    )
    expanded = (
        predictions.select(label_col, score_col)
        .crossJoin(F.broadcast(threshold_df))
        .withColumn(
            "_prediction",
            (F.col(score_col) >= F.col("threshold")).cast("int"),
        )
    )
    rows = (
        expanded.groupBy("threshold")
        .agg(
            F.sum(
                F.when(
                    (F.col(label_col) == 1) & (F.col("_prediction") == 1), 1
                ).otherwise(0)
            ).alias("tp"),
            F.sum(
                F.when(
                    (F.col(label_col) == 0) & (F.col("_prediction") == 1), 1
                ).otherwise(0)
            ).alias("fp"),
            F.sum(
                F.when(
                    (F.col(label_col) == 1) & (F.col("_prediction") == 0), 1
                ).otherwise(0)
            ).alias("fn"),
            F.sum(
                F.when(
                    (F.col(label_col) == 0) & (F.col("_prediction") == 0), 1
                ).otherwise(0)
            ).alias("tn"),
        )
        .orderBy("threshold")
        .collect()
    )
    results: list[dict[str, Any]] = []
    for row in rows:
        metrics = positive_class_metrics(
            tp=int(row["tp"] or 0),
            fp=int(row["fp"] or 0),
            fn=int(row["fn"] or 0),
            tn=int(row["tn"] or 0),
        )
        results.append({"threshold": float(row["threshold"]), **metrics})
    return results


def select_threshold(
    results: Sequence[dict[str, Any]],
    *,
    minimum_recall: float | None = None,
) -> dict[str, Any]:
    """Choose a validation threshold by positive F1, optionally requiring recall."""
    candidates = [
        result
        for result in results
        if result.get("positive_f1") is not None
        and (
            minimum_recall is None
            or float(result.get("recall") or 0) >= minimum_recall
        )
    ]
    if not candidates:
        raise ValueError("No threshold satisfies the selection criteria")
    return max(
        candidates,
        key=lambda result: (
            float(result["positive_f1"]),
            float(result.get("recall") or 0),
            float(result.get("precision") or 0),
            float(result["threshold"]),
        ),
    )


def apply_threshold(
    predictions: DataFrame,
    *,
    threshold: float,
    score_col: str = "positive_score",
    output_col: str = "prediction_selected",
) -> DataFrame:
    """Apply a locked validation threshold to another prediction frame."""
    if not 0 <= threshold <= 1:
        raise ValueError("threshold must be in [0, 1]")
    return predictions.withColumn(
        output_col,
        (F.col(score_col) >= F.lit(float(threshold))).cast("double"),
    )