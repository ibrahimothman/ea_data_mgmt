"""Load a validated upload into its dataset's bronze table.

The third stage. Bronze preserves exactly what arrived: every column is
a string, nothing is dropped, and the load is re-runnable. Casting and
cleaning belong in silver, where failures can be quarantined with a
reason instead of silently becoming NULL.
"""

import logging
import os

from pyspark.sql import functions as F

from ea_pipeline.config import (
    BRONZE_TABLES, CORRUPT_RECORD_COLUMN, MAX_CORRUPT_ROW_RATIO, spark,
)
from ea_pipeline.errors import BronzeIngestionError
from ea_pipeline.files import calculate_file_hash, normalise_column_name
from ea_pipeline.manifest import claim_upload, load_manifest_record, update_manifest
from ea_pipeline.models import BronzeOutcome
from ea_pipeline.states import CLAIMABLE_FOR_BRONZE, UploadStatus

logger = logging.getLogger(__name__)


def _verify_file_unchanged(file_path: str, registered_hash: str) -> None:
    """
    Confirm the file is still the one that was validated.

    Without this, a file replaced between validation and bronze would be
    loaded under an upload_id that claims VALIDATED — putting data into
    bronze that nothing has ever checked.
    """
    if not os.path.isfile(file_path):
        raise BronzeIngestionError(
            f"The registered file no longer exists at {file_path}."
        )

    if calculate_file_hash(file_path) != registered_hash:
        raise BronzeIngestionError(
            f"The file at {file_path} has changed since it was validated. "
            "Register the new version as a new upload."
        )


def _read_source_csv(file_path: str):
    """
    Read the CSV as raw text, one column per source column.

    Every option here matters:
      inferSchema=false  keeps "N/A" as "N/A" instead of a silent NULL
      PERMISSIVE         keeps unparseable rows instead of dropping them
      multiLine          a quoted line break is one row, not two
      escape             matches Python's csv defaults, so both readers
                         count the same file the same way
    """
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(file_path)
    )


def _prepare_dataframe(df, upload_id: str, source_file_name: str):
    """
    Normalise column names and attach lineage.

    Returns:
        (dataframe, original_columns)
    """
    # ONE Analyze call. Under Spark Connect this is a network round trip,
    # so it is read once into a plain list and reused.
    original_columns = df.columns

    # Rebuild every column in a single select rather than chaining
    # renames — one plan node instead of one per column.
    renamed = []

    for original in original_columns:
        if original == CORRUPT_RECORD_COLUMN:
            renamed.append(F.col(f"`{original}`"))   # Spark's name, leave it
        else:
            renamed.append(
                F.col(f"`{original}`").alias(normalise_column_name(original))
            )

    df = df.select(renamed)

    # current_timestamp() is evaluated once per query, so every row in
    # one load shares the same ingested_at.
    df = df.withColumns({
        "upload_id": F.lit(upload_id),
        "source_file_name": F.lit(source_file_name),
        "ingested_at": F.current_timestamp(),
    })

    return df, original_columns


def _count_rows(df, original_columns: list[str]) -> tuple[int, int]:
    """
    Count total rows and unparseable rows in a single action.

    _corrupt_record is only populated as a side effect of parsing, so a
    query touching only that column can have the parse optimised away.
    One agg over all rows forces the parse and avoids the problem.

    Spark only adds the column when it actually hits a bad row, so its
    presence is checked rather than assumed.
    """
    if CORRUPT_RECORD_COLUMN not in original_columns:
        return df.count(), 0

    counts = df.agg(
        F.count("*").alias("total"),
        F.count(F.col(CORRUPT_RECORD_COLUMN)).alias("corrupt"),   # non-null only
    ).first()

    return counts["total"], counts["corrupt"]


def _check_corrupt_ratio(row_count: int, corrupt_row_count: int) -> None:
    """
    Reject the file if too much of it failed to parse.

    A file this broken is usually the wrong file entirely, or exported
    with the wrong delimiter — not a file with a few bad rows in it.
    """
    if row_count == 0:
        raise BronzeIngestionError("The file produced no rows when read.")

    ratio = corrupt_row_count / row_count

    if ratio > MAX_CORRUPT_ROW_RATIO:
        raise BronzeIngestionError(
            f"{corrupt_row_count} of {row_count} rows ({ratio:.1%}) could not "
            f"be parsed, which exceeds the {MAX_CORRUPT_ROW_RATIO:.0%} limit. "
            "Check the file's delimiter and encoding."
        )


def _clear_previous_load(bronze_table: str, upload_id: str) -> None:
    """
    Remove any rows a previous attempt wrote.

    This is what makes bronze re-runnable. If an earlier run died after
    writing but before recording PROCESSED, those rows are still there,
    and a retry would append them again.

    On a first attempt this deletes nothing.
    """
    spark.sql(
        f"DELETE FROM {bronze_table} WHERE upload_id = :upload_id",
        args={"upload_id": upload_id},
    )


def _write_to_bronze(df, bronze_table: str) -> None:
    """
    Append to the dataset's bronze table.

    mergeSchema is on because contracts allow unexpected columns, so a
    file may legitimately contain a column bronze has never seen. Since
    every column is a string, there are no type conflicts to reconcile.
    """
    (
        df.write
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )


def _print_outcome(outcome: BronzeOutcome) -> None:
    print(f"Upload ID: {outcome.upload_id}")
    print(f"Dataset: {outcome.dataset_name}")
    print(f"Table: {outcome.bronze_table}")
    print(f"Status: {outcome.status}")
    print(f"Rows: {outcome.row_count}")

    if outcome.corrupt_row_count:
        print(f"Corrupt rows: {outcome.corrupt_row_count}")

    print(f"Message: {outcome.message}")


def load_upload_to_bronze(upload_id: str, verbose: bool = True) -> BronzeOutcome:
    """
    Load one VALIDATED upload into its dataset's bronze table.

    Safe to re-run: rows written by a previous attempt are removed before
    writing, so the result is the same whether this runs once or three
    times.
    """

    # 1. Load the manifest record — before any state change.
    record = load_manifest_record(
        upload_id,
        [
            "upload_id", "dataset_name", "original_file_name",
            "stored_file_path", "file_hash", "source_row_count", "status",
        ],
    )

    dataset_name = record["dataset_name"]
    file_path = record["stored_file_path"]
    file_name = record["original_file_name"]
    registered_hash = record["file_hash"]
    source_row_count = record["source_row_count"]

    # 2. Resolve the target table. Configuration problems raise to the
    #    caller rather than being recorded as a load failure.
    if dataset_name not in BRONZE_TABLES:
        raise ValueError(f"No bronze table configured for dataset: {dataset_name}")

    bronze_table = BRONZE_TABLES[dataset_name]

    if not spark.catalog.tableExists(bronze_table):
        raise RuntimeError(
            f"Bronze table '{bronze_table}' does not exist. Run the setup DDL first."
        )

    # 3. Claim the row. Only VALIDATED is eligible — DUPLICATE rows never
    #    reach that status, so they are skipped automatically.
    claim_upload(
        upload_id, CLAIMABLE_FOR_BRONZE, UploadStatus.PROCESSING, "loaded to bronze"
    )

    # 4. Do the work.
    try:
        _verify_file_unchanged(file_path, registered_hash)

        df = _read_source_csv(file_path)
        df, original_columns = _prepare_dataframe(df, upload_id, file_name)

        row_count, corrupt_row_count = _count_rows(df, original_columns)
        _check_corrupt_ratio(row_count, corrupt_row_count)

        _clear_previous_load(bronze_table, upload_id)
        _write_to_bronze(df, bronze_table)

    except BronzeIngestionError as rejection:
        # The file cannot be loaded as it stands. Terminal.
        logger.warning("Upload %s rejected at bronze: %s", upload_id, rejection)

        outcome = BronzeOutcome.rejected(
            upload_id=upload_id,
            dataset_name=dataset_name,
            bronze_table=bronze_table,
            message=str(rejection),
        )
        update_manifest(upload_id, outcome.as_manifest_update())

        if verbose:
            _print_outcome(outcome)

        return outcome

    except Exception as error:
        # Something unexpected broke. Retryable.
        logger.exception("Bronze load failed for upload %s", upload_id)

        update_manifest(upload_id, {
            "status": F.lit(UploadStatus.BRONZE_FAILED.value),
            "validation_message": F.lit(f"Bronze load failed: {error}"),
        })
        raise

    # 5. Reconcile — a warning, not a failure. Python's csv reader and
    #    Spark's can legitimately differ by a row on odd files.
    message = f"Loaded {row_count} rows into {bronze_table}."

    if corrupt_row_count:
        message += f" {corrupt_row_count} rows could not be parsed."

    if source_row_count is not None and source_row_count != row_count:
        logger.warning(
            "Row count mismatch for upload %s: validation counted %s, bronze wrote %s",
            upload_id, source_row_count, row_count,
        )
        message += (
            f" Note: validation counted {source_row_count} rows "
            f"but bronze wrote {row_count}."
        )

    # 6. Record success — outside the try, so a failure writing the
    #    outcome is not misrecorded as a load failure.
    outcome = BronzeOutcome(
        upload_id=upload_id,
        dataset_name=dataset_name,
        bronze_table=bronze_table,
        status=UploadStatus.PROCESSED,
        message=message,
        row_count=row_count,
        corrupt_row_count=corrupt_row_count,
    )

    update_manifest(upload_id, outcome.as_manifest_update())

    logger.info(
        "Upload %s loaded to bronze: table=%s rows=%d corrupt=%d",
        upload_id, bronze_table, row_count, corrupt_row_count,
    )

    if verbose:
        _print_outcome(outcome)

    return outcome