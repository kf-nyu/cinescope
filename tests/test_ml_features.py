"""Tests for the predictive feature contract."""

from __future__ import annotations

import pytest

from cinescope.ml.features import (
    FORBIDDEN_PREDICTOR_COLUMNS,
    PRE_RELEASE_BOOL,
    PRE_RELEASE_NUMERIC,
    build_feature_pipeline,
    validate_feature_artifact_metadata,
    validate_feature_columns,
)


def test_configured_pre_release_features_exclude_forbidden_columns():
    """Keep outcome, full-career, and snapshot-vote fields out of models."""
    configured = set(PRE_RELEASE_NUMERIC) | set(PRE_RELEASE_BOOL)
    assert configured.isdisjoint(FORBIDDEN_PREDICTOR_COLUMNS)


@pytest.mark.parametrize(
    "column",
    [
        "average_rating",
        "was_oscar_nominated",
        "career_average_rating",
        "director_prior_votes_sum",
    ],
)
def test_validate_feature_columns_rejects_forbidden_predictors(column):
    """Fail closed when a forbidden field is passed explicitly."""
    with pytest.raises(ValueError, match=column):
        validate_feature_columns(["runtime_minutes", column])


def test_validate_feature_columns_accepts_point_in_time_predictors():
    """Allow the documented pre-release feature set."""
    validate_feature_columns(
        ["runtime_minutes", "director_prior_rating_mean", "has_known_director"]
    )


def test_feature_pipeline_rejects_label_as_predictor():
    """Prevent direct label leakage even for an unfamiliar label name."""
    with pytest.raises(ValueError, match="Label column"):
        build_feature_pipeline(["target"], label_col="target")


def test_feature_artifact_metadata_rejects_pre_fix_outputs():
    """Refuse Parquet generated with full-career known-person flags."""
    with pytest.raises(RuntimeError, match="stale"):
        validate_feature_artifact_metadata({"feature_semantics_version": 1})

    validate_feature_artifact_metadata({"feature_semantics_version": 2})