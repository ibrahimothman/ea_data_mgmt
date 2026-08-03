"""Tests for the result dataclasses.

The point is shape consistency: every exit path from a stage must return
the same fields, so callers never branch on which path ran.
"""

from ea_pipeline.models import BronzeOutcome, ColumnCheckResult, ValidationOutcome
from ea_pipeline.states import UploadStatus


def test_rejected_outcome_has_same_fields_as_success():
    """
    The bug this prevents: a caller reading outcome.column_check and
    getting AttributeError only on the rejection path.
    """
    rejected = ValidationOutcome.rejected("abc", "projects", "empty file")

    assert rejected.column_check.missing_columns == []
    assert rejected.column_check.unexpected_columns == []
    assert rejected.column_check.duplicate_columns == []
    assert rejected.source_row_count == 0


def test_rejected_outcome_status():
    outcome = ValidationOutcome.rejected("abc", "projects", "empty file")

    assert outcome.status == UploadStatus.REJECTED
    assert not outcome.is_valid


def test_validated_outcome_is_valid():
    outcome = ValidationOutcome(
        upload_id="abc",
        dataset_name="projects",
        status=UploadStatus.VALIDATED,
        validation_message="ok",
        column_check=ColumnCheckResult(is_valid=True),
        source_row_count=10,
    )

    assert outcome.is_valid


def test_outcomes_are_immutable():
    outcome = ValidationOutcome.rejected("abc", "projects", "empty")

    try:
        outcome.status = UploadStatus.VALIDATED
        raise AssertionError("frozen dataclass should not allow assignment")
    except AttributeError:
        pass


def test_column_check_defaults_are_independent():
    """
    field(default_factory=list) rather than a shared mutable default -
    two instances must not share one list.
    """
    first = ColumnCheckResult(is_valid=True)
    second = ColumnCheckResult(is_valid=True)

    first.missing_columns.append("leaked")

    assert second.missing_columns == []


def test_manifest_update_returns_plain_python():
    """
    models.py must stay Spark-free: the values here are plain Python,
    and manifest.py converts them to column expressions.
    """
    outcome = ValidationOutcome.rejected("abc", "projects", "empty file")
    update = outcome.as_manifest_update()

    assert isinstance(update["status"], str)
    assert isinstance(update["missing_columns"], list)


def test_bronze_rejected_has_zero_counts():
    outcome = BronzeOutcome.rejected(
        "abc", "projects", "ea_dev.bronze.projects", "file changed"
    )

    assert outcome.row_count == 0
    assert outcome.corrupt_row_count == 0
    assert not outcome.succeeded


def test_bronze_success_flag():
    outcome = BronzeOutcome(
        upload_id="abc",
        dataset_name="projects",
        bronze_table="ea_dev.bronze.projects",
        status=UploadStatus.PROCESSED,
        message="ok",
        row_count=10,
    )

    assert outcome.succeeded
