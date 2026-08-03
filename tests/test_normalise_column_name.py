"""Tests for column name normalisation.

This function is why validation stopped rejecting legitimate Excel
exports: "Project ID" must match a contract expecting "project_id".
"""

import pytest

from ea_pipeline.files import normalise_column_name


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("project_id", "project_id"),
        ("Project ID", "project_id"),
        ("PROJECT ID", "project_id"),
        ("  project_id  ", "project_id"),
        ("project-id", "project_id"),
        ("project.id", "project_id"),
        ("project/id", "project_id"),
        ("Project - ID", "project_id"),
        ("Project   ID", "project_id"),
        ("_project_id_", "project_id"),
    ],
)
def test_normalises_to_expected(raw, expected):
    assert normalise_column_name(raw) == expected


def test_excel_header_matches_contract_name():
    """The specific bug this function was written to fix."""
    assert normalise_column_name("Project ID") == "project_id"


def test_distinct_names_stay_distinct():
    assert normalise_column_name("start_date") != normalise_column_name("end_date")


def test_empty_string():
    assert normalise_column_name("") == ""


def test_only_separators_collapses_to_empty():
    """Guards the blank-column-name check in read_csv_structure."""
    assert normalise_column_name("   ") == ""
    assert normalise_column_name("---") == ""
