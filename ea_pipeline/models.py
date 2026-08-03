"""Result objects for each pipeline stage.

Each stage returns a frozen dataclass rather than a dict, so every exit
path from a function produces the same shape. The `rejected` classmethods
are the single place that decides what the empty fields look like when a
stage failed before it could fill them.
"""

from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone

from ea_pipeline.states import UploadStatus


@dataclass(frozen=True)
class ColumnCheckResult:
    """Outcome of comparing a file header against a dataset contract."""

    is_valid: bool
    actual_columns: list[str] = field(default_factory=list)
    missing_columns: list[str] = field(default_factory=list)
    unexpected_columns: list[str] = field(default_factory=list)
    duplicate_columns: list[str] = field(default_factory=list)
    unexpected_columns_rejected: bool = True


@dataclass(frozen=True)
class ValidationOutcome:
    """Result of validating one registered upload."""

    upload_id: str
    dataset_name: str
    status: UploadStatus
    validation_message: str
    column_check: ColumnCheckResult
    source_row_count: int = 0

    @property
    def is_valid(self) -> bool:
        return self.status == UploadStatus.VALIDATED

    @classmethod
    def rejected(
        cls,
        upload_id: str,
        dataset_name: str,
        message: str,
        column_check: ColumnCheckResult | None = None,
        source_row_count: int = 0,
    ) -> "ValidationOutcome":
        """
        Build a rejection outcome.

        Single place that decides the shape when validation never got
        far enough to fill the column fields.
        """
        return cls(
            upload_id=upload_id,
            dataset_name=dataset_name,
            status=UploadStatus.REJECTED,
            validation_message=message,
            column_check=column_check or ColumnCheckResult(is_valid=False),
            source_row_count=source_row_count,
        )

    def as_manifest_update(self) -> dict:
        """The columns this outcome writes to the manifest."""
        return {
            "status": self.status,
            "validation_message": self.validation_message,
            "source_row_count": self.source_row_count,
            "missing_columns": self.column_check.missing_columns,
            "unexpected_columns": self.column_check.unexpected_columns,
            "duplicate_columns": self.column_check.duplicate_columns,
        }


@dataclass(frozen=True)
class BronzeOutcome:
    """Result of loading one upload into bronze."""

    upload_id: str
    dataset_name: str
    bronze_table: str
    status: UploadStatus           
    message: str
    row_count: int = 0
    corrupt_row_count: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status == UploadStatus.PROCESSED

    @classmethod
    def rejected(
        cls,
        upload_id: str,
        dataset_name: str,
        bronze_table: str,
        message: str,
    ) -> "BronzeOutcome":
        """Build a rejection outcome with zeroed counts."""
        return cls(
            upload_id=upload_id,
            dataset_name=dataset_name,
            bronze_table=bronze_table,
            status=UploadStatus.BRONZE_REJECTED,
            message=message,
        )

    def as_manifest_update(self) -> dict:
        """The columns this outcome writes to the manifest."""
        values = {
            "status": self.status,
            "validation_message": self.message,
            "bronze_row_count": self.row_count,
            "bronze_corrupt_row_count": self.corrupt_row_count
        }

        # Only stamp the completion time on an actual success — otherwise
        # "bronze_processed_at IS NOT NULL" would be a lie.
        if self.succeeded:
            values["bronze_processed_at"] = datetime.now(timezone.utc)

        return values