"""Path safety tests — local flexible dirs, optional SSD volume, HDFS URIs."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from cinescope.paths import (
    CineScopePaths,
    PathConfigurationError,
    StoragePath,
    apply_hdfs_env_defaults,
)


def _set_env(monkeypatch, **values: str) -> None:
    for key, value in values.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)


def test_missing_data_root_raises(monkeypatch):
    """Fail clearly when CINESCOPE_DATA_ROOT is not configured."""
    monkeypatch.delenv("CINESCOPE_DATA_ROOT", raising=False)
    monkeypatch.setenv("CINESCOPE_STORAGE_BACKEND", "local")
    monkeypatch.setenv("SPARK_LOCAL_DIR", "/tmp/cinescope-spark")
    monkeypatch.setenv("SPARK_WAREHOUSE_DIR", "/tmp/cinescope-wh")
    monkeypatch.setenv("SPARK_CHECKPOINT_DIR", "/tmp/cinescope-ckpt")
    with pytest.raises(PathConfigurationError, match="CINESCOPE_DATA_ROOT"):
        CineScopePaths(validate_mount=False)


def test_allows_plain_local_directory_without_volume(monkeypatch, tmp_path):
    """Allow a partner-style local data root when no SSD volume is set."""
    data_root = tmp_path / "cinescope-data"
    data_root.mkdir()
    monkeypatch.delenv("CINESCOPE_SSD_VOLUME", raising=False)
    _set_env(
        monkeypatch,
        CINESCOPE_STORAGE_BACKEND="local",
        CINESCOPE_DATA_ROOT=str(data_root),
        SPARK_LOCAL_DIR=str(data_root / "spark-temp"),
        SPARK_WAREHOUSE_DIR=str(data_root / "warehouse"),
        SPARK_CHECKPOINT_DIR=str(data_root / "checkpoints"),
    )
    paths = CineScopePaths(validate_mount=False, create_dirs=True)
    assert paths.backend == "local"
    assert paths.ssd_volume is None
    assert paths.raw_imdb_dir.exists()
    assert str(paths.movies_ratings_dir).endswith("bronze/movies_ratings")


def test_rejects_repo_as_data_root(monkeypatch):
    """Refuse to place large data inside the Git repository."""
    repo = Path(__file__).resolve().parents[1]
    monkeypatch.delenv("CINESCOPE_SSD_VOLUME", raising=False)
    _set_env(
        monkeypatch,
        CINESCOPE_STORAGE_BACKEND="local",
        CINESCOPE_DATA_ROOT=str(repo / "data"),
        SPARK_LOCAL_DIR=str(repo / "spark-temp"),
        SPARK_WAREHOUSE_DIR=str(repo / "warehouse"),
        SPARK_CHECKPOINT_DIR=str(repo / "checkpoints"),
    )
    with pytest.raises(PathConfigurationError, match="Git repository"):
        CineScopePaths(validate_mount=False)


def test_volume_requires_data_root_inside(monkeypatch):
    """When a volume is configured, the data root must sit on that volume."""
    _set_env(
        monkeypatch,
        CINESCOPE_STORAGE_BACKEND="local",
        CINESCOPE_SSD_VOLUME="/Volumes/DriveA",
        CINESCOPE_DATA_ROOT="/Volumes/DriveB/cinescope-data",
        SPARK_LOCAL_DIR="/Volumes/DriveB/cinescope-data/spark-temp",
        SPARK_WAREHOUSE_DIR="/Volumes/DriveB/cinescope-data/warehouse",
        SPARK_CHECKPOINT_DIR="/Volumes/DriveB/cinescope-data/checkpoints",
    )
    with pytest.raises(PathConfigurationError, match="CINESCOPE_SSD_VOLUME"):
        CineScopePaths(validate_mount=False)


def test_valid_paths_under_volume_without_mount_check(monkeypatch):
    """Accept a data root nested under the configured volume (mount check off)."""
    _set_env(
        monkeypatch,
        CINESCOPE_STORAGE_BACKEND="local",
        CINESCOPE_SSD_VOLUME="/Volumes/DriveA",
        CINESCOPE_DATA_ROOT="/Volumes/DriveA/cinescope-data",
        SPARK_LOCAL_DIR="/Volumes/DriveA/cinescope-data/spark-temp",
        SPARK_WAREHOUSE_DIR="/Volumes/DriveA/cinescope-data/warehouse",
        SPARK_CHECKPOINT_DIR="/Volumes/DriveA/cinescope-data/checkpoints",
    )
    paths = CineScopePaths(validate_mount=False, create_dirs=False)
    assert paths.raw_imdb_dir.name == "imdb"
    assert paths.bronze_dir.name == "bronze"


def test_hdfs_netid_derives_data_root(monkeypatch):
    """Derive HDFS data/scratch paths from CINESCOPE_NETID alone."""
    monkeypatch.delenv("CINESCOPE_SSD_VOLUME", raising=False)
    monkeypatch.delenv("CINESCOPE_DATA_ROOT", raising=False)
    monkeypatch.delenv("SPARK_WAREHOUSE_DIR", raising=False)
    monkeypatch.delenv("SPARK_CHECKPOINT_DIR", raising=False)
    monkeypatch.delenv("SPARK_LOCAL_DIR", raising=False)
    monkeypatch.delenv("CINESCOPE_HDFS_USER", raising=False)
    _set_env(
        monkeypatch,
        CINESCOPE_STORAGE_BACKEND="hdfs",
        CINESCOPE_NETID="ab1234",
    )
    apply_hdfs_env_defaults()
    assert os.environ["CINESCOPE_HDFS_USER"] == "ab1234_nyu_edu"
    assert os.environ["CINESCOPE_DATA_ROOT"].endswith("/user/ab1234_nyu_edu/cinescope-data")

    paths = CineScopePaths(validate_mount=False, create_dirs=False)
    assert paths.backend == "hdfs"
    assert str(paths.data_root) == "hdfs:///user/ab1234_nyu_edu/cinescope-data"
    assert str(paths.movies_ratings_dir).endswith("bronze/movies_ratings")
    assert paths.spark_local_dir.backend == "local"
    assert "ab1234" in str(paths.spark_local_dir)


def test_storage_path_join():
    """Join path segments for both local and HDFS StoragePath values."""
    root = StoragePath("/tmp/cinescope-data", "local")
    assert str(root / "raw" / "imdb") == "/tmp/cinescope-data/raw/imdb"
    hdfs = StoragePath("hdfs:///user/x/cinescope-data", "hdfs")
    assert str(hdfs / "bronze" / "movies_ratings") == (
        "hdfs:///user/x/cinescope-data/bronze/movies_ratings"
    )
