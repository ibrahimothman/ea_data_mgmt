"""Batch driver: discover new files and move uploads through the stages.

Runs on a schedule. Every stage is a status query, so a failed upload is
retried on the next run without any special handling — FAILED and
BRONZE_FAILED are both claimable statuses.
"""

import logging
import os
import time

from pyspark.sql import functions as F

from ea_pipeline.bronze import load_upload_to_bronze
from ea_pipeline.config import DATASET_DIRECTORIES, MANIFEST_TABLE, spark, MIN_FILE_AGE_SECONDS, STALE_CLAIM_MINUTES
from ea_pipeline.register import register_uploaded_file
from ea_pipeline.states import CLAIMABLE_FOR_BRONZE, CLAIMABLE_FOR_VALIDATION, UploadStatus
from ea_pipeline.validate import validate_registered_upload

logger = logging.getLogger(__name__)


def register_landing_files() -> list[str]:
    """
    Register every CSV in the landing directories.

    Scans all files, not only new ones — registration's duplicate check
    is what distinguishes them. An already-registered path with unchanged
    contents returns its existing upload_id and writes nothing, so this
    is safe to run every 15 minutes.

    Returns:
        upload_ids of every file seen, new or already known.
    """
    found = []
    now = time.time()

    for dataset_name, directory in DATASET_DIRECTORIES.items():
        if not os.path.isdir(directory):
            logger.warning("Landing directory missing: %s", directory)
            continue

        for file_name in sorted(os.listdir(directory)):
            path = os.path.join(directory, file_name)

            if not os.path.isfile(path):
                continue

            if not file_name.lower().endswith(".csv"):
                continue

            # Skip files that may still be uploading.
            if now - os.path.getmtime(path) < MIN_FILE_AGE_SECONDS:
                logger.info("Skipping recently modified file: %s", path)
                continue

            try:
                upload_id = register_uploaded_file(
                    file_path=path,
                    dataset_name=dataset_name,
                    verbose=False,
                )
                found.append(upload_id)

            except Exception:
                # One bad file must not stop the rest of the run.
                logger.exception("Failed to register %s", path)

    return found


def _uploads_with_status(statuses) -> list[str]:
    """upload_ids currently in any of these statuses, oldest first."""
    rows = (
        spark.table(MANIFEST_TABLE)
        .where(F.col("status").isin([str(status) for status in statuses]))
        .orderBy("created_at")
        .select("upload_id")
        .collect()
    )
    return [row["upload_id"] for row in rows]
    

def run_upload_pipeline() -> dict:
    """
    One batch run: discover, validate, load.

    Each upload is handled independently — a failure is recorded on its
    manifest row and the run continues. The next run retries it, because
    FAILED and BRONZE_FAILED are claimable statuses.
    """
    summary = {
        "registered": 0, "validated": 0, "rejected": 0,
        "loaded": 0, "errors": 0,
    }

    # 1. Discover
    summary["files_seen"] = len(register_landing_files())

    # 2. Validate everything waiting. Includes FAILED rows from earlier
    #    runs — that is how retries happen.
    for upload_id in _uploads_with_status(CLAIMABLE_FOR_VALIDATION):
        try:
            outcome = validate_registered_upload(upload_id, verbose=False)
            if outcome.is_valid:
                summary["validated"] += 1
            else:
                summary["rejected"] += 1
        except Exception:
            logger.exception("Validation errored for %s", upload_id)
            summary["errors"] += 1

    # 3. Load everything validated.
    for upload_id in _uploads_with_status(CLAIMABLE_FOR_BRONZE):
        try:
            outcome = load_upload_to_bronze(upload_id, verbose=False)
            if outcome.succeeded:
                summary["loaded"] += 1
            else:
                summary["rejected"] += 1
        except Exception:
            logger.exception("Bronze load errored for %s", upload_id)
            summary["errors"] += 1

    logger.info("Pipeline run complete: %s", summary)
    return summary


def release_stale_claims() -> None:
    """
    Reset uploads abandoned mid-stage.

    claim_upload can reclaim a stale row, but only when someone retries
    that specific upload_id. This sweeps up rows nobody retries, so an
    interrupted run shows as FAILED in monitoring instead of sitting in
    a working status forever.
    """
    for working, failed in [
        (UploadStatus.VALIDATING, UploadStatus.FAILED),
        (UploadStatus.PROCESSING, UploadStatus.BRONZE_FAILED),
    ]:
        spark.sql(
            f"""
            UPDATE {MANIFEST_TABLE}
            SET status = :failed,
                validation_message = 'Interrupted before completion.',
                updated_at = current_timestamp()
            WHERE status = :working
              AND updated_at < current_timestamp()
                               - INTERVAL {STALE_CLAIM_MINUTES} MINUTES
            """,
            args={"failed": str(failed), "working": str(working)},
        )    
