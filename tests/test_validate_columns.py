"""Tests for comparing a header against a dataset contract.

validate_columns takes a list of names and a contract and returns a
judgement - no file access, no table lookup - which is what makes these
tests possible without Spark.
"""

import pytest

from ea_pipeline.config import DatasetContract
from ea_pipeline.validate import validate_columns


def test_exact_required_columns_pass(strict_contract):
    result = validate_columns(
        ["project_id", "project_name", "project_status"], strict_contract
    )

    assert result.is_valid
    assert result.missing_columns == []
    assert result.unexpected_columns == []
    assert result.duplicate_columns == []


def test_required_plus_optional_passes(strict_contract):
    result = validate_columns(
        ["project_id", "project_name", "project_status", "start_date"],
        strict_contract,
    )

    assert result.is_valid


def test_column_order_does_not_matter(strict_contract):
    result = validate_columns(
        ["project_status", "project_id", "project_name"], strict_contract
    )

    assert result.is_valid


def test_missing_required_column_fails(strict_contract):
    result = validate_columns(["project_id", "project_name"], strict_contract)

    assert not result.is_valid
    assert result.missing_columns == ["project_status"]


def test_missing_columns_are_sorted(strict_contract):
    result = validate_columns(["project_status"], strict_contract)

    assert result.missing_columns == ["project_id", "project_name"]


def test_duplicate_column_fails(strict_contract):
    result = validate_columns(
        ["project_id", "project_name", "project_status", "project_id"],
        strict_contract,
    )

    assert not result.is_valid
    assert result.duplicate_columns == ["project_id"]


def test_duplicates_detected_even_when_otherwise_valid(lenient_contract):
    """A set would have lost the repeat entirely."""
    result = validate_columns(
        ["project_id", "project_id", "project_name", "project_status"],
        lenient_contract,
    )

    assert not result.is_valid
    assert result.duplicate_columns == ["project_id"]


def test_unexpected_column_rejected_when_strict(strict_contract):
    result = validate_columns(
        ["project_id", "project_name", "project_status", "surprise"],
        strict_contract,
    )

    assert not result.is_valid
    assert result.unexpected_columns == ["surprise"]


def test_unexpected_column_allowed_when_lenient(lenient_contract):
    """
    Source systems add columns routinely; blocking on that would halt
    the pipeline for a harmless change.
    """
    result = validate_columns(
        ["project_id", "project_name", "project_status", "surprise"],
        lenient_contract,
    )

    assert result.is_valid
    assert result.unexpected_columns == ["surprise"]      # still recorded


def test_lenient_contract_still_fails_on_missing(lenient_contract):
    """reject_unexpected must not weaken the required-column check."""
    result = validate_columns(["project_id", "surprise"], lenient_contract)

    assert not result.is_valid
    assert "project_name" in result.missing_columns


def test_actual_columns_preserves_input_order(strict_contract):
    columns = ["project_status", "project_id", "project_name"]
    result = validate_columns(columns, strict_contract)

    assert result.actual_columns == columns


def test_rejection_flag_is_carried_through(lenient_contract):
    """The message builder needs this to word extras as a warning."""
    result = validate_columns(["project_id"], lenient_contract)

    assert result.unexpected_columns_rejected is False


def test_empty_header_reports_all_required_missing(strict_contract):
    result = validate_columns([], strict_contract)

    assert not result.is_valid
    assert len(result.missing_columns) == 3


def test_contract_rejects_overlapping_columns():
    """A column in both required and optional is a config bug."""
    with pytest.raises(ValueError, match="both required and optional"):
        DatasetContract(
            required=frozenset({"project_id"}),
            optional=frozenset({"project_id"}),
        )


def test_contract_rejects_empty_required():
    with pytest.raises(ValueError, match="at least one required"):
        DatasetContract(required=frozenset())
