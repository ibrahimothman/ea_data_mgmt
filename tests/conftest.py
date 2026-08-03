"""Shared test fixtures.

These tests cover the pure-Python functions only: no Spark session, no
Delta tables, no notebook. They run with plain pytest.
"""

import sys
from pathlib import Path

import pytest

# Make the package importable without installing it.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from ea_pipeline.config import DatasetContract  # noqa: E402


@pytest.fixture
def strict_contract():
    """A contract that rejects unexpected columns."""
    return DatasetContract(
        required=frozenset({"project_id", "project_name", "project_status"}),
        optional=frozenset({"start_date", "end_date"}),
        reject_unexpected=True,
    )


@pytest.fixture
def lenient_contract():
    """A contract that allows unexpected columns - matches production."""
    return DatasetContract(
        required=frozenset({"project_id", "project_name", "project_status"}),
        optional=frozenset({"start_date", "end_date"}),
        reject_unexpected=False,
    )


@pytest.fixture
def write_csv(tmp_path):
    """
    Write a CSV and return its path.

    tmp_path gives a fresh directory per test, so files never leak
    between tests.
    """
    def _write(content, name="test.csv", encoding="utf-8"):
        path = tmp_path / name
        path.write_text(content, encoding=encoding, newline="")
        return str(path)

    return _write
