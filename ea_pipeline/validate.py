"""Validate a registered upload against its dataset contract.

The second stage. Reads the file's header and row count, compares the
header against the contract, and records the outcome on the manifest.
"""

import logging
import os
from collections import Counter

from pyspark.sql import functions as F

from ea_pipeline.config import DATASET_CONTRACTS
from ea_pipeline.errors import ContractViolation
from ea_pipeline.files import calculate_file_hash, read_csv_structure
from ea_pipeline.manifest import claim_upload, load_manifest_record, update_manifest
from ea_pipeline.models import ColumnCheckResult, ValidationOutcome, truncate
from ea_pipeline.states import CLAIMABLE_FOR_VALIDATION, UploadStatus

logger = logging.getLogger(__name__)


def validate_columns(columns: list[str], contract) -> ColumnCheckResult:
    """
    Compare a list of column names against a contract.

    Pure: no file access, no table lookup. Takes the names and the rules,
    returns a judgement — which is what makes it testable without Spark.
    """
    required_columns = contract.required
    optional_columns = contract.optional
    allowed_columns = required_columns | optional_columns

    actual_columns = set(columns)

    missing_columns = sorted(required_columns - actual_columns)
    unexpected_columns = sorted(actual_columns - allowed_columns)

    # Duplicates must be counted on the LIST — converting to a set
    # discards the repeats entirely.
    column_counts = Counter(columns)
    duplicate_columns = sorted(
        column for column, count in column_counts.items() if count > 1
    )

    # Extra columns only block the upload if the contract says so.
    is_valid = not (
        missing_columns
        or duplicate_columns
        or (unexpected_columns and contract.reject_unexpected)
    )

    return ColumnCheckResult(
        is_valid=is_valid,
        actual_columns=columns,
        missing_columns=missing_columns,
        unexpected_columns=unexpected_columns,
        duplicate_columns=duplicate_columns,
        unexpected_columns_rejected=contract.reject_unexpected,
    )


def _build_message(result: ColumnCheckResult) -> str:
    """Human-readable summary of what the column check found."""
    if result.is_valid:
        notes = ["File passed structural contract validation."]

        # Extra columns are reported even when they are allowed.
        if result.unexpected_columns:
            notes.append(
                "Extra columns present but allowed: "
                + ", ".join(result.unexpected_columns)
            )

        return "; ".join(notes)

    problems = []

    if result.missing_columns:
        problems.append(
            "Missing required columns: " + ", ".join(result.missing_columns)
        )

    if result.duplicate_columns:
        problems.append(
            "Duplicate columns: " + ", ".join(result.duplicate_columns)
        )

    if result.unexpected_columns and result.unexpected_columns_rejected:
        problems.append(
            "Unexpected columns: " + ", ".join(result.unexpected_columns)
        )

    return "; ".join(problems)


def _print_outcome(outcome: ValidationOutcome) -> None:
    print(f"Upload ID: {outcome.upload_id}")
    print(f"Dataset: {outcome.dataset_name}")
    print(f"Status: {outcome.status}")
    print(f"Rows: {outcome.source_row_count}")
    print(f"Message: {outcome.validation_message}")


def validate_registered_upload(
    upload_id: str,
    verbose: bool = True,
) -> ValidationOutcome:
    """
    Validate a registered upload against its dataset contract and record
    the outcome on the manifest.
    """

    # 1. Load the manifest record — before any state change.
    record = load_manifest_record(
        upload_id,
        ["upload_id", "dataset_name", "stored_file_path", "file_hash", "status"],
    )

    dataset_name = record["dataset_name"].strip().lower()
    file_path = record["stored_file_path"]
    registered_hash = record["file_hash"]

    # 2. Resolve the contract. A missing contract is a configuration
    #    problem, not a file problem, so it raises to the caller rather
    #    than being recorded as a rejection — and it happens before the
    #    claim, so the row is not left in VALIDATING.
    if dataset_name not in DATASET_CONTRACTS:
        raise ValueError(f"No contract exists for dataset: {dataset_name}")

    contract = DATASET_CONTRACTS[dataset_name]

    # 3. Claim the row (atomic status check + change)
    claim_upload(
        upload_id, CLAIMABLE_FOR_VALIDATION, UploadStatus.VALIDATING, "validated"
    )

    # 4. Do the validation work.
    #    Only this part is wrapped: a failure here IS a validation result.
    try:
        # 4a. Is the file still the one we registered?
        if not os.path.isfile(file_path):
            raise ContractViolation(
                f"The registered file no longer exists at {file_path}."
            )

        if calculate_file_hash(file_path) != registered_hash:
            raise ContractViolation(
                f"The file at {file_path} has changed since it was registered. "
                "Register the new version as a new upload."
            )

        # 4b. Read the file's structure, then judge it
        structure = read_csv_structure(file_path)
        result = validate_columns(structure.header, contract)

        status = UploadStatus.VALIDATED if result.is_valid else UploadStatus.REJECTED
        message = _build_message(result)

        outcome = ValidationOutcome(
            upload_id=upload_id,
            dataset_name=dataset_name,
            status=status,
            validation_message=message,
            column_check=result,
            source_row_count=structure.row_count,
        )

    except ContractViolation as violation:
        # The file is wrong. Terminal — a new file is needed.
        logger.warning("Upload %s rejected: %s", upload_id, violation)

        outcome = ValidationOutcome.rejected(
            upload_id=upload_id,
            dataset_name=dataset_name,
            message=str(violation),
        )
        update_manifest(upload_id, outcome.as_manifest_update())

        if verbose:
            _print_outcome(outcome)

        return outcome

    except Exception as error:
        # Something unexpected broke. Retryable.
        logger.exception("Validation failed for upload %s", upload_id)

        update_manifest(upload_id, {
            "status": F.lit(UploadStatus.FAILED),
            "validation_message": F.lit(
                truncate(f"Contract validation failed: {error}")
            ),
        })
        raise

    # 5. Write the outcome — deliberately OUTSIDE the try, so a failure
    #    writing the result is not misrecorded as a validation failure.
    update_manifest(upload_id, outcome.as_manifest_update())

    logger.info(
        "Upload %s validated: dataset=%s status=%s rows=%d",
        upload_id, dataset_name, outcome.status, outcome.source_row_count,
    )

    if verbose:
        _print_outcome(outcome)

    return outcome