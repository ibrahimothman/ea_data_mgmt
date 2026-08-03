"""Tests for reading a CSV header and row count.

Every failure here must be a ContractViolation, because those map to
REJECTED rather than FAILED - the file is wrong, and retrying the same
file will not help.
"""

import pytest

from ea_pipeline.errors import ContractViolation
from ea_pipeline.files import read_csv_structure

CLEAN = (
    "project_id,project_name,project_status\n"
    "P001,Website,Active\n"
    "P002,Mobile App,Active\n"
)


def test_reads_header_and_counts_rows(write_csv):
    structure = read_csv_structure(write_csv(CLEAN))

    assert structure.header == ["project_id", "project_name", "project_status"]
    assert structure.row_count == 2      # excludes the header


def test_header_is_normalised(write_csv):
    content = "Project ID,Project Name\nP001,Website\n"
    structure = read_csv_structure(write_csv(content))

    assert structure.header == ["project_id", "project_name"]


def test_completely_empty_file(write_csv):
    with pytest.raises(ContractViolation, match="completely empty"):
        read_csv_structure(write_csv(""))


def test_header_but_no_data_rows(write_csv):
    """A common real mistake: exported with a filter still applied."""
    content = "project_id,project_name,project_status\n"

    with pytest.raises(ContractViolation, match="no data rows"):
        read_csv_structure(write_csv(content))


def test_blank_column_name(write_csv):
    content = "project_id,,project_status\nP001,x,Active\n"

    with pytest.raises(ContractViolation, match="blank column name"):
        read_csv_structure(write_csv(content))


def test_semicolon_delimiter_is_detected(write_csv):
    """
    Without this check the error would list every required column as
    missing, which is true but useless to the analyst.
    """
    content = "project_id;project_name;project_status\nP001;Website;Active\n"

    with pytest.raises(ContractViolation, match="comma-separated"):
        read_csv_structure(write_csv(content))


def test_tab_delimiter_is_detected(write_csv):
    content = "project_id\tproject_name\nP001\tWebsite\n"

    with pytest.raises(ContractViolation, match="comma-separated"):
        read_csv_structure(write_csv(content))


def test_non_utf8_file(write_csv):
    """Older Excel exports cp1252, which must not surface as FAILED."""
    content = "project_id,project_name\nP001,Cafe\u0301\n"

    with pytest.raises(ContractViolation, match="not valid UTF-8"):
        read_csv_structure(write_csv(content, encoding="cp1252"))


def test_utf8_bom_is_tolerated(write_csv):
    """Excel CSV UTF-8 adds a BOM; it must not corrupt the first name."""
    content = "project_id,project_name\nP001,Website\n"
    path = write_csv(content, encoding="utf-8-sig")

    structure = read_csv_structure(path)

    assert structure.header[0] == "project_id"


def test_quoted_line_break_counts_as_one_row(write_csv):
    """
    A line break inside a quoted value is one row, not two. This count
    must agree with Spark multiLine reader.
    """
    content = (
        "project_id,project_name\n"
        'P001,"Website\nRebuild"\n'
        "P002,Mobile App\n"
    )

    structure = read_csv_structure(write_csv(content))

    assert structure.row_count == 2      # not 3


def test_duplicate_columns_are_preserved(write_csv):
    """
    The header is returned as a list, not a set, so validate_columns can
    still detect the repeat.
    """
    content = "project_id,project_name,project_id\nP001,Website,P001\n"

    structure = read_csv_structure(write_csv(content))

    assert structure.header.count("project_id") == 2
