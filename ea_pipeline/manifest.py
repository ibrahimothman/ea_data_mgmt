"""Shared operations on the upload manifest table.

Every read and write of the manifest goes through here, so the table
name and the claim pattern exist in one place.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from ea_pipeline.config import MANIFEST_TABLE, STALE_CLAIM_MINUTES, spark
from ea_pipeline.states import UploadStatus


def load_manifest_record(upload_id: str, columns: list[str]):
    """
    Fetch one manifest row.

    Called before any status change, so an unknown upload_id fails with
    nothing modified.

    Raises:
        ValueError: if the upload_id does not exist.
    """
    record = (
        spark.table(MANIFEST_TABLE)
        .where(F.col("upload_id") == upload_id)
        .select(*columns)
        .first()
    )

    if record is None:
        raise ValueError(f"Upload ID was not found: {upload_id}")

    return record


def get_status(upload_id: str) -> str | None:
    """Current status of an upload, or None if it does not exist."""
    record = (
        spark.table(MANIFEST_TABLE)
        .where(F.col("upload_id") == upload_id)
        .select("status")
        .first()
    )
    return record["status"] if record else None


def update_manifest(upload_id: str, values: dict) -> None:
    """
    Update one manifest row.

    Uses the Delta API rather than building SQL text, so there is
    nothing to escape and no way for a future edit to introduce an
    unescaped value.

    updated_at is set here so it can never be forgotten at a call site.
    """
    values = {**values, "updated_at": F.current_timestamp()}

    DeltaTable.forName(spark, MANIFEST_TABLE).update(
        condition=F.col("upload_id") == upload_id,
        set=values,
    )


def claim_upload(
    upload_id: str,
    from_statuses: list[str],
    to_status: UploadStatus,
    stage_label: str,
) -> None:
    """
    Atomically move an upload into a working status.

    The status check and the status change happen in ONE statement, so
    two runs cannot both decide the upload is theirs to process.

    A row stuck in `to_status` for longer than STALE_CLAIM_MINUTES is
    treated as abandoned and may be reclaimed — otherwise an interrupted
    run would lock the record forever.

    Args:
        from_statuses: statuses eligible to be claimed.
        to_status:     the working status to move into.
        stage_label:   used in the error message, e.g. "validated".

    Raises:
        ValueError: if the row could not be claimed.
    """
    eligible = ", ".join(f"'{status}'" for status in from_statuses)

    claim = spark.sql(
        f"""
        UPDATE {MANIFEST_TABLE}
        SET status = :to_status,
            updated_at = current_timestamp()
        WHERE upload_id = :upload_id
          AND (
                status IN ({eligible})
                OR (
                     status = :to_status
                     AND updated_at < current_timestamp()
                                      - INTERVAL {STALE_CLAIM_MINUTES} MINUTES
                   )
              )
        """,
        args={"upload_id": upload_id, "to_status": to_status},
    )

    if claim.first()["num_affected_rows"] == 0:
        current = get_status(upload_id)
        raise ValueError(
            f"Upload {upload_id} has status '{current}' and cannot be "
            f"{stage_label}. Eligible statuses: {', '.join(from_statuses)}."
        )