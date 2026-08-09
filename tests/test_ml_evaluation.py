"""Tests for chronological rare-event model evaluation."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from cinescope.ml.evaluation import (
    evaluate_binary_predictions,
    positive_class_metrics,
    select_threshold,
    temporal_split,
    with_balanced_class_weights,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("cinescope-ml-evaluation-tests")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_temporal_split_has_disjoint_year_ranges(spark):
    """Keep later films out of training and validation cohorts."""
    df = spark.createDataFrame(
        [("a", 2010), ("b", 2015), ("c", 2020), ("d", 2025), ("e", None)],
        ["tconst", "start_year"],
    )
    train, validation, test = temporal_split(
        df,
        train_end=2014,
        validation_end=2019,
        test_end=2024,
    )

    assert [row.start_year for row in train.collect()] == [2010]
    assert [row.start_year for row in validation.collect()] == [2015]
    assert [row.start_year for row in test.collect()] == [2020]


def test_balanced_class_weights_equalize_total_weight(spark):
    """Give rare positives the same aggregate training weight as negatives."""
    df = spark.createDataFrame([(0,), (0,), (0,), (1,)], ["label"])
    weighted, weights = with_balanced_class_weights(df, label_col="label")
    totals = {
        row.label: row["sum(class_weight)"]
        for row in weighted.groupBy("label").sum("class_weight").collect()
    }

    assert totals[0] == pytest.approx(totals[1])
    assert weights["positive"] > weights["negative"]


def test_positive_f1_is_computed_from_positive_precision_and_recall():
    """Avoid Spark's majority-dominated frequency-weighted F1."""
    metrics = positive_class_metrics(tp=5, fp=5, fn=15, tn=75)

    assert metrics["precision"] == pytest.approx(0.5)
    assert metrics["recall"] == pytest.approx(0.25)
    assert metrics["positive_f1"] == pytest.approx(1 / 3)


def test_threshold_selection_uses_validation_positive_f1():
    """Select the best validation operating point deterministically."""
    selected = select_threshold(
        [
            {"threshold": 0.2, "positive_f1": 0.4, "recall": 0.8, "precision": 0.27},
            {"threshold": 0.4, "positive_f1": 0.6, "recall": 0.6, "precision": 0.6},
            {"threshold": 0.6, "positive_f1": 0.5, "recall": 0.4, "precision": 0.67},
        ]
    )

    assert selected["threshold"] == pytest.approx(0.4)


def test_binary_evaluator_accepts_scalar_positive_scores(spark):
    """Spark 3.5 supports a double probability as rawPredictionCol."""
    predictions = spark.createDataFrame(
        [
            (0.0, 0.0, 0.1),
            (0.0, 0.0, 0.2),
            (1.0, 1.0, 0.8),
            (1.0, 1.0, 0.9),
        ],
        ["label", "prediction", "positive_score"],
    )

    metrics = evaluate_binary_predictions(predictions, label_col="label")

    assert metrics["roc_auc"] == pytest.approx(1.0)
    assert metrics["pr_auc"] == pytest.approx(1.0)
    assert metrics["positive_f1"] == pytest.approx(1.0)