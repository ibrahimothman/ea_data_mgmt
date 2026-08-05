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

from ea_pipeline.config import DatasetContract, ColumnSpec  # noqa: E402


@pytest.fixture
def strict_contract():
    """A contract that rejects unexpected columns."""
    return DatasetContract(
        key_column="project_id",
        columns=(
            ColumnSpec("project_id", "string", required=True),
            ColumnSpec("project_name", "string", required=True),
            ColumnSpec("project_status", "string", required=True),
            ColumnSpec("start_date", "date", required=False),
            ColumnSpec("end_date", "date", required=False),
        ),
        reject_unexpected=True,
    )


@pytest.fixture
def lenient_contract():
    """A contract that allows unexpected columns - matches production."""
    return DatasetContract(
        key_column="project_id",
        columns=(
            ColumnSpec("project_id", "string", required=True),
            ColumnSpec("project_name", "string", required=True),
            ColumnSpec("project_status", "string", required=True),
            ColumnSpec("start_date", "date", required=False),
            ColumnSpec("end_date", "date", required=False),
        ),
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
