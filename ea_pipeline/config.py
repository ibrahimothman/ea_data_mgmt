from pyspark.sql import SparkSession
from dataclasses import dataclass

spark = SparkSession.builder.getOrCreate()

MANIFEST_TABLE = "ea_dev.operations.upload_manifest"

DATASET_DIRECTORIES = {
    "projects":     "/Volumes/ea_dev/landing/uploads/projects",
    "contracts":    "/Volumes/ea_dev/landing/uploads/contracts",
    "applications": "/Volumes/ea_dev/landing/uploads/applications",
}

@dataclass(frozen=True)
class DatasetContract:
    """Column rules for one dataset."""

    required: frozenset[str]
    optional: frozenset[str] = frozenset()
    reject_unexpected: bool = True

    def __post_init__(self):
        overlap = self.required & self.optional
        if overlap:
            raise ValueError(
                f"Columns listed as both required and optional: {sorted(overlap)}"
            )
        if not self.required:
            raise ValueError("A contract must have at least one required column.")


DATASET_CONTRACTS = {
    "projects": DatasetContract(
        required=frozenset({"project_id", "project_name", "project_status"}),
        optional=frozenset({"start_date", "end_date", "project_manager"}),
        reject_unexpected=False,
    ),
    "contracts": DatasetContract(
        required=frozenset({"contract_id", "contract_name", "supplier_name"}),
        optional=frozenset({
            "contract_value", "start_date", "end_date",
            "application_id", "project_id",
        }),
        reject_unexpected=False,
    ),
    "applications": DatasetContract(
        required=frozenset({"application_id", "application_name", "lifecycle_status"}),
        optional=frozenset({"business_owner", "technology_owner", "criticality"}),
        reject_unexpected=False,
    ),
}

BRONZE_TABLES = {
    "projects":     "ea_dev.bronze.projects",
    "contracts":    "ea_dev.bronze.contracts",
    "applications": "ea_dev.bronze.applications",
}

STALE_CLAIM_MINUTES = 1
MAX_MESSAGE_LENGTH = 1000
MAX_CORRUPT_ROW_RATIO = 0.05
CORRUPT_RECORD_COLUMN = "_corrupt_record"
