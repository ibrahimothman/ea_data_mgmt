"""Register an uploaded landing file in the upload manifest.

The first stage. Deliberately does not open the file — it records facts
about it and checks it is where it should be. Reading and parsing are
validation's job, so a corrupt file still gets a manifest row and a
visible rejection rather than failing before any record exists.
"""

import logging
import os
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, LongType, StringType, StructField, StructType, TimestampType,
)

from ea_pipeline.config import DATASET_DIRECTORIES, MANIFEST_TABLE, spark
from ea_pipeline.files import calculate_file_hash

logger = logging.getLogger(__name__)


# "2024-Q3" or "2024-10"
REPORTING_PERIOD_PATTERN = re.compile(r"\d{4}-(Q[1-4]|0[1-9]|1[0-2])")

# Leave empty to accept any source system (still normalised).
KNOWN_SOURCE_SYSTEMS: set[str] = set()


MANIFEST_SCHEMA = StructType([
    StructField("upload_id", StringType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("original_file_name", StringType(), False),
    StructField("stored_file_path", StringType(), False),
    StructField("file_size_bytes", LongType(), True),
    StructField("file_hash", StringType(), True),
    StructField("uploaded_by", StringType(), True),
    StructField("uploaded_at", TimestampType(), True),
    StructField("source_system", StringType(), True),
    StructField("reporting_period", StringType(), True),
    StructField("status", StringType(), False),
    StructField("validation_message", StringType(), True),
    StructField("missing_columns", ArrayType(StringType()), True),
    StructField("unexpected_columns", ArrayType(StringType()), True),
    StructField("duplicate_columns", ArrayType(StringType()), True),
    StructField("duplicate_of_upload_id", StringType(), True),
    StructField("pipeline_run_id", StringType(), True),
    StructField("bronze_processed_at", TimestampType(), True),
    StructField("created_at", TimestampType(), False),
    StructField("updated_at", TimestampType(), False),
    StructField("source_row_count", LongType(), True),
    StructField("bronze_row_count", LongType(), True),
    StructField("bronze_corrupt_row_count", LongType(), True),
])


def _resolve_and_check_directory(file_path: str, dataset_name: str) -> str:
    """
    Confirm the file really sits inside its dataset's directory.

    realpath collapses ".." and follows symlinks; commonpath compares
    folder by folder rather than letter by letter, so "projects_old"
    does not count as being inside "projects".

    Returns:
        The resolved path, which is what gets stored — so "./x.csv" and
        "x.csv" are not registered as two different files.
    """
    expected_directory = DATASET_DIRECTORIES[dataset_name]

    resolved_path = os.path.realpath(file_path)
    resolved_directory = os.path.realpath(expected_directory)

    try:
        is_inside = (
            os.path.commonpath([resolved_path, resolved_directory])
            == resolved_directory
        )
    except ValueError:
        # Raised when the paths have nothing in common at all
        is_inside = False

    if not is_inside:
        raise ValueError(
            f"The file for dataset '{dataset_name}' must be inside "
            f"'{expected_directory}'. Got: {resolved_path}"
        )

    return resolved_path


def _clean_metadata(
    source_system: str | None,
    reporting_period: str | None,
) -> tuple[str | None, str | None]:
    """
    Normalise the optional metadata.

    Without this the columns fill up with "2024-Q3", "Q3 2024", "2024Q3"
    all meaning the same thing, and any later GROUP BY becomes painful.
    """
    if reporting_period is not None:
        reporting_period = reporting_period.strip().upper()

        if not REPORTING_PERIOD_PATTERN.fullmatch(reporting_period):
            raise ValueError(
                f"reporting_period must look like '2024-Q3' or '2024-10'. "
                f"Got: {reporting_period}"
            )

    if source_system is not None:
        source_system = source_system.strip().upper()

        if KNOWN_SOURCE_SYSTEMS and source_system not in KNOWN_SOURCE_SYSTEMS:
            raise ValueError(
                f"Unknown source_system '{source_system}'. "
                f"Expected one of: {sorted(KNOWN_SOURCE_SYSTEMS)}"
            )

    return source_system, reporting_period


def register_uploaded_file(
    file_path: str,
    dataset_name: str,
    source_system: str | None = None,
    reporting_period: str | None = None,
    verbose: bool = True,
) -> str:
    """
    Register an uploaded landing file in the upload manifest.

    Safe to run twice: if the identical file (same path, same contents)
    is already registered, the existing upload_id is returned rather
    than raising.

    Returns:
        The upload_id — newly created, or the existing one.
    """

    # 1. Is this a dataset we know?
    dataset_name = dataset_name.strip().lower()

    if dataset_name not in DATASET_DIRECTORIES:
        raise ValueError(
            f"Unknown dataset '{dataset_name}'. "
            f"Expected one of: {list(DATASET_DIRECTORIES)}"
        )

    # 2. Does the manifest table exist?
    #    Without this a typo in the name would make Spark quietly create
    #    a new empty table and the row would disappear into it.
    if not spark.catalog.tableExists(MANIFEST_TABLE):
        raise RuntimeError(
            f"Manifest table '{MANIFEST_TABLE}' does not exist. "
            "Run the table setup script first."
        )

    # 3. Is the file inside the correct directory?
    resolved_path = _resolve_and_check_directory(file_path, dataset_name)

    # 4. Does the path exist, and is it a file?
    if not os.path.exists(resolved_path):
        raise FileNotFoundError(f"File does not exist: {resolved_path}")

    if not os.path.isfile(resolved_path):
        raise ValueError(f"The supplied path is not a file: {resolved_path}")

    # 5. Clean the optional metadata
    source_system, reporting_period = _clean_metadata(
        source_system, reporting_period
    )

    # 6. Collect the technical details.
    #    The hash is calculated before the duplicate check, because that
    #    check needs it to tell "same file" from "changed file".
    original_file_name = os.path.basename(resolved_path)
    file_size_bytes = os.path.getsize(resolved_path)
    file_hash = calculate_file_hash(resolved_path)

    # 7. Has this path been registered before?
    existing = (
        spark.table(MANIFEST_TABLE)
        .filter(F.col("stored_file_path") == resolved_path)
        .select("upload_id", "file_hash")
        .first()
    )

    if existing is not None:

        # Same path, same contents — someone just ran this twice.
        if existing["file_hash"] == file_hash:
            logger.info(
                "File already registered, returning existing upload_id %s",
                existing["upload_id"],
            )
            if verbose:
                print("File was already registered (identical contents)")
                print(f"Upload ID: {existing['upload_id']}")

            return existing["upload_id"]

        # Same path, different contents — the file was overwritten after
        # registration. Stop and make a human look at it.
        raise ValueError(
            f"This path is already registered but the contents have changed: "
            f"{resolved_path}. The file was overwritten after registration. "
            "Investigate before registering again."
        )

    # 8. Is this the same data under a different name?
    #    Registered anyway so there is a record, but flagged so bronze
    #    can skip it.
    duplicate_of = (
        spark.table(MANIFEST_TABLE)
        .filter(F.col("file_hash") == file_hash)
        .filter(F.col("dataset_name") == dataset_name)
        .select("upload_id")
        .first()
    )

    duplicate_of_upload_id = duplicate_of["upload_id"] if duplicate_of else None
    status = "DUPLICATE" if duplicate_of_upload_id else "RECEIVED"

    # 9. Build the manifest row
    upload_id = str(uuid.uuid4())

    # now(timezone.utc) is timezone-aware. The old utcnow() gave the
    # right numbers with no label attached.
    current_time = datetime.now(timezone.utc)

    uploaded_by = spark.sql(
        "SELECT current_user() AS current_user"
    ).first()["current_user"]

    manifest_record = [{
        "upload_id": upload_id,
        "dataset_name": dataset_name,
        "original_file_name": original_file_name,
        "stored_file_path": resolved_path,
        "file_size_bytes": file_size_bytes,
        "file_hash": file_hash,
        "uploaded_by": uploaded_by,
        "uploaded_at": current_time,
        "source_system": source_system,
        "reporting_period": reporting_period,
        "status": status,
        "validation_message": None,
        "missing_columns": None,
        "unexpected_columns": None,
        "duplicate_columns": None,
        "duplicate_of_upload_id": duplicate_of_upload_id,
        "pipeline_run_id": None,
        "bronze_processed_at": None,
        "created_at": current_time,
        "updated_at": current_time,
        "source_row_count": None,
        "bronze_row_count": None,
        "bronze_corrupt_row_count": None,
    }]

    # The explicit schema matters: without it Spark cannot infer the
    # type of all those None values.
    manifest_df = spark.createDataFrame(manifest_record, schema=MANIFEST_SCHEMA)

    # 10. Write the row
    manifest_df.write.mode("append").saveAsTable(MANIFEST_TABLE)

    logger.info(
        "Registered upload %s: dataset=%s file=%s size=%d status=%s",
        upload_id, dataset_name, original_file_name, file_size_bytes, status,
    )

    if verbose:
        print("File successfully registered")
        print(f"Upload ID: {upload_id}")
        print(f"Dataset: {dataset_name}")
        print(f"Filename: {original_file_name}")
        print(f"Size: {file_size_bytes} bytes")
        print(f"Hash: {file_hash}")
        print(f"Status: {status}")

        if duplicate_of_upload_id:
            print(f"Same contents as earlier upload: {duplicate_of_upload_id}")

    return upload_id