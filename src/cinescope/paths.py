"""Centralized path handling for local disk or HDFS backends.

Local mode accepts any safe data root (external SSD, ~/cinescope-data, etc.).
HDFS mode targets NYU Dataproc-style URIs (``hdfs:///user/...``).

Large data must never live inside the Git repository.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path


class PathConfigurationError(RuntimeError):
    """Raised when large-data paths are missing or unsafe."""


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise PathConfigurationError(
            f"Missing required environment variable: {name}. "
            "Copy .env.example (local) or .env.hpc.example (Dataproc) to .env."
        )
    return value


def _optional_env(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _is_under(child: Path, parent: Path) -> bool:
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False


def _volume_is_mounted(ssd_volume: Path) -> bool:
    if not ssd_volume.exists() or not ssd_volume.is_dir():
        return False
    result = subprocess.run(
        ["mount"],
        check=False,
        capture_output=True,
        text=True,
    )
    return f"on {ssd_volume} " in result.stdout


def storage_backend() -> str:
    backend = os.environ.get("CINESCOPE_STORAGE_BACKEND", "local").strip().lower()
    if backend not in {"local", "hdfs"}:
        raise PathConfigurationError(
            f"CINESCOPE_STORAGE_BACKEND must be 'local' or 'hdfs' (got {backend!r})."
        )
    return backend


def apply_hdfs_env_defaults() -> None:
    """Fill HDFS path env vars from ``CINESCOPE_NETID`` when unset.

    NYU Dataproc HDFS homes use ``/user/<netid>_nyu_edu/``. Prefer setting
    only ``CINESCOPE_NETID`` in ``.env``; derived paths are applied here so
    Python jobs match the shell helpers in ``scripts/lib/storage.sh``.
    """
    if storage_backend() != "hdfs":
        return

    netid = (os.environ.get("CINESCOPE_NETID") or "").strip()
    hdfs_user = (os.environ.get("CINESCOPE_HDFS_USER") or "").strip()

    if not hdfs_user:
        if netid:
            hdfs_user = f"{netid}_nyu_edu"
        else:
            import getpass

            who = getpass.getuser()
            if who.endswith("_nyu_edu"):
                hdfs_user = who
                netid = who[: -len("_nyu_edu")]
            elif who and who != "root":
                netid = netid or who
                hdfs_user = f"{netid}_nyu_edu"

    if not hdfs_user:
        raise PathConfigurationError(
            "HDFS mode requires CINESCOPE_NETID (short NetID) or CINESCOPE_HDFS_USER "
            "in the environment / .env file."
        )

    os.environ["CINESCOPE_HDFS_USER"] = hdfs_user
    if netid:
        os.environ["CINESCOPE_NETID"] = netid

    root = f"hdfs:///user/{hdfs_user}/cinescope-data"
    scratch_id = netid or hdfs_user

    def _fill(name: str, value: str) -> None:
        if not (os.environ.get(name) or "").strip():
            os.environ[name] = value

    _fill("CINESCOPE_DATA_ROOT", root)
    _fill("SPARK_WAREHOUSE_DIR", f"{root}/warehouse")
    _fill("SPARK_CHECKPOINT_DIR", f"{root}/checkpoints")
    _fill("SPARK_LOCAL_DIR", f"/tmp/cinescope-spark-{scratch_id}")
    _fill("CINESCOPE_DOWNLOAD_STAGING", f"/tmp/cinescope-download-{scratch_id}")


@dataclass(frozen=True)
class StoragePath:
    """Filesystem or HDFS location with ``/`` joining and existence helpers."""

    value: str
    backend: str

    def __truediv__(self, other: str | Path) -> "StoragePath":
        left = self.value.rstrip("/")
        right = str(other).strip("/")
        if not right:
            return self
        if self.backend == "local":
            return StoragePath(str((Path(self.value) / right).expanduser()), self.backend)
        return StoragePath(f"{left}/{right}", self.backend)

    def __str__(self) -> str:
        return self.value

    def __fspath__(self) -> str:
        if self.backend != "local":
            raise TypeError("HDFS StoragePath is not a local filesystem path")
        return self.value

    @property
    def name(self) -> str:
        return self.value.rstrip("/").split("/")[-1]

    def as_local_path(self) -> Path:
        if self.backend != "local":
            raise PathConfigurationError(f"Not a local path: {self.value}")
        return Path(self.value).expanduser()

    def exists(self) -> bool:
        if self.backend == "hdfs":
            return (
                subprocess.run(
                    ["hdfs", "dfs", "-test", "-e", self.value],
                    check=False,
                    capture_output=True,
                ).returncode
                == 0
            )
        return self.as_local_path().exists()

    def is_file(self) -> bool:
        if self.backend == "hdfs":
            return (
                subprocess.run(
                    ["hdfs", "dfs", "-test", "-f", self.value],
                    check=False,
                    capture_output=True,
                ).returncode
                == 0
            )
        return self.as_local_path().is_file()

    def is_dir(self) -> bool:
        if self.backend == "hdfs":
            return (
                subprocess.run(
                    ["hdfs", "dfs", "-test", "-d", self.value],
                    check=False,
                    capture_output=True,
                ).returncode
                == 0
            )
        return self.as_local_path().is_dir()

    def mkdir(self, *, parents: bool = True, exist_ok: bool = True) -> None:
        if self.backend == "hdfs":
            cmd = ["hdfs", "dfs", "-mkdir"]
            if parents:
                cmd.append("-p")
            cmd.append(self.value)
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            return
        self.as_local_path().mkdir(parents=parents, exist_ok=exist_ok)

    def resolve(self) -> "StoragePath":
        if self.backend == "local":
            return StoragePath(str(self.as_local_path().resolve()), self.backend)
        return self

    def size_bytes(self) -> int:
        """File or directory size in bytes (HDFS via ``hdfs dfs -du -s``)."""
        if self.backend == "hdfs":
            result = subprocess.run(
                ["hdfs", "dfs", "-du", "-s", self.value],
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0 or not result.stdout.strip():
                return 0
            try:
                return int(result.stdout.strip().split()[0])
            except (ValueError, IndexError):
                return 0
        path = self.as_local_path()
        if path.is_file():
            return path.stat().st_size
        if not path.exists():
            return 0
        total = 0
        for root, _dirs, files in os.walk(path):
            for name in files:
                total += (Path(root) / name).stat().st_size
        return total


def _normalize_hdfs_uri(uri: str) -> str:
    value = uri.strip().rstrip("/")
    if value.startswith("hdfs://"):
        return value
    if value.startswith("/"):
        # /user/... → hdfs:///user/... (default FS authority)
        return f"hdfs://{value}"
    raise PathConfigurationError(
        f"HDFS path must look like hdfs:///user/<netid>/cinescope-data (got {uri!r})."
    )


def _assert_not_repo_or_root(path: Path, label: str) -> None:
    repo = _repo_root()
    if path == Path("/") or str(path) == "/":
        raise PathConfigurationError(f"{label} must not be the filesystem root.")
    if _is_under(path, repo) or path == repo:
        raise PathConfigurationError(
            f"{label} must not resolve into the Git repository (got {path})."
        )


def _assert_safe_local_path(
    path: Path,
    label: str,
    *,
    volume: Path | None,
) -> None:
    _assert_not_repo_or_root(path, label)
    if volume is not None and not _is_under(path, volume):
        raise PathConfigurationError(
            f"{label} must be inside CINESCOPE_SSD_VOLUME={volume} (got {path})."
        )


class CineScopePaths:
    """Resolved and validated filesystem / HDFS locations for CineScope."""

    def __init__(self, *, create_dirs: bool = False, validate_mount: bool = True):
        self.backend = storage_backend()
        self.ssd_volume: Path | None = None
        volume_env = _optional_env("CINESCOPE_SSD_VOLUME")

        data_root_raw = _require_env("CINESCOPE_DATA_ROOT")
        spark_local_raw = _require_env("SPARK_LOCAL_DIR")
        spark_warehouse_raw = _require_env("SPARK_WAREHOUSE_DIR")
        spark_checkpoint_raw = _require_env("SPARK_CHECKPOINT_DIR")

        if self.backend == "hdfs":
            self.data_root = StoragePath(_normalize_hdfs_uri(data_root_raw), "hdfs")
            # spark.local.dir must be a local filesystem path on executors.
            local_scratch = Path(spark_local_raw).expanduser().resolve()
            _assert_not_repo_or_root(local_scratch, "SPARK_LOCAL_DIR")
            self.spark_local_dir = StoragePath(str(local_scratch), "local")
            self.spark_warehouse_dir = StoragePath(
                _normalize_hdfs_uri(spark_warehouse_raw), "hdfs"
            )
            self.spark_checkpoint_dir = StoragePath(
                _normalize_hdfs_uri(spark_checkpoint_raw), "hdfs"
            )
        else:
            if volume_env:
                self.ssd_volume = Path(volume_env).expanduser().resolve()
                if validate_mount and not _volume_is_mounted(self.ssd_volume):
                    raise PathConfigurationError(
                        f"Configured volume is not mounted at {self.ssd_volume}. "
                        "Reconnect the drive, or unset CINESCOPE_SSD_VOLUME to use a "
                        "plain local directory."
                    )

            data_root = Path(data_root_raw).expanduser().resolve()
            spark_local = Path(spark_local_raw).expanduser().resolve()
            spark_warehouse = Path(spark_warehouse_raw).expanduser().resolve()
            spark_checkpoint = Path(spark_checkpoint_raw).expanduser().resolve()

            for label, path in (
                ("CINESCOPE_DATA_ROOT", data_root),
                ("SPARK_LOCAL_DIR", spark_local),
                ("SPARK_WAREHOUSE_DIR", spark_warehouse),
                ("SPARK_CHECKPOINT_DIR", spark_checkpoint),
            ):
                _assert_safe_local_path(path, label, volume=self.ssd_volume)

            self.data_root = StoragePath(str(data_root), "local")
            self.spark_local_dir = StoragePath(str(spark_local), "local")
            self.spark_warehouse_dir = StoragePath(str(spark_warehouse), "local")
            self.spark_checkpoint_dir = StoragePath(str(spark_checkpoint), "local")

        self.raw_imdb_dir = self.data_root / "raw" / "imdb"
        self.raw_oscars_dir = self.data_root / "raw" / "oscars"
        self.bronze_dir = self.data_root / "bronze"
        self.silver_dir = self.data_root / "silver"
        self.gold_dir = self.data_root / "gold"
        self.samples_dir = self.data_root / "samples"
        self.model_dir = self.data_root / "models"
        self.export_dir = self.data_root / "exports"
        self.movies_ratings_dir = self.bronze_dir / "movies_ratings"
        self.cast_crew_features_dir = self.silver_dir / "cast_crew_features"
        self.movies_enriched_dir = self.silver_dir / "movies_enriched"
        self.oscars_nominations_dir = self.bronze_dir / "oscars_nominations"
        self.movie_oscar_features_dir = self.silver_dir / "movie_oscar_features"
        self.movies_awards_enriched_dir = self.silver_dir / "movies_awards_enriched"

        if create_dirs:
            self.ensure_directories()

    def ensure_directories(self) -> None:
        if (
            self.backend == "local"
            and self.ssd_volume is not None
            and not _volume_is_mounted(self.ssd_volume)
        ):
            raise PathConfigurationError(
                f"Refusing to create directories: volume not mounted at {self.ssd_volume}."
            )
        for path in (
            self.raw_imdb_dir,
            self.raw_oscars_dir,
            self.bronze_dir,
            self.silver_dir,
            self.gold_dir,
            self.samples_dir,
            self.spark_local_dir,
            self.spark_checkpoint_dir,
            self.spark_warehouse_dir,
            self.model_dir,
            self.export_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


def load_paths(*, create_dirs: bool = False, validate_mount: bool = True) -> CineScopePaths:
    """Load dotenv (if present) and return validated storage paths."""
    try:
        from dotenv import load_dotenv

        load_dotenv(_repo_root() / ".env")
    except ImportError:
        pass
    apply_hdfs_env_defaults()
    return CineScopePaths(create_dirs=create_dirs, validate_mount=validate_mount)


# Convenience aliases populated lazily via get_paths() for job modules.
RAW_IMDB_DIR: StoragePath | None = None
BRONZE_DIR: StoragePath | None = None
SILVER_DIR: StoragePath | None = None
GOLD_DIR: StoragePath | None = None
SPARK_LOCAL_DIR: StoragePath | None = None
SPARK_WAREHOUSE_DIR: StoragePath | None = None
SPARK_CHECKPOINT_DIR: StoragePath | None = None
MODEL_DIR: StoragePath | None = None
EXPORT_DIR: StoragePath | None = None


def get_paths(*, create_dirs: bool = False, validate_mount: bool = True) -> CineScopePaths:
    """Resolve paths and expose module-level aliases."""
    global RAW_IMDB_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR
    global SPARK_LOCAL_DIR, SPARK_WAREHOUSE_DIR, SPARK_CHECKPOINT_DIR
    global MODEL_DIR, EXPORT_DIR

    paths = load_paths(create_dirs=create_dirs, validate_mount=validate_mount)
    RAW_IMDB_DIR = paths.raw_imdb_dir
    BRONZE_DIR = paths.bronze_dir
    SILVER_DIR = paths.silver_dir
    GOLD_DIR = paths.gold_dir
    SPARK_LOCAL_DIR = paths.spark_local_dir
    SPARK_WAREHOUSE_DIR = paths.spark_warehouse_dir
    SPARK_CHECKPOINT_DIR = paths.spark_checkpoint_dir
    MODEL_DIR = paths.model_dir
    EXPORT_DIR = paths.export_dir
    return paths
