"""Pytest configuration for CineScope Spark tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path


# Keep Spark workers on the same interpreter as the test driver (.venv Python 3.11).
_python = sys.executable
os.environ.setdefault("PYSPARK_PYTHON", _python)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", _python)

# Ensure src/ is importable when pytest is invoked without PYTHONPATH.
_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
