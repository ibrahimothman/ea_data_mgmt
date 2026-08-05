from pyspark.sql import SparkSession
from dataclasses import dataclass

spark = SparkSession.builder.getOrCreate()

MANIFEST_TABLE = "ea_dev.operations.upload_manifest"

DATASET_DIRECTORIES = {
    "projects":     "/Volumes/ea_dev/landing/uploads/projects",
    "contracts":    "/Volumes/ea_dev/landing/uploads/contracts",
    "applications": "/Volumes/ea_dev/landing/uploads/applications",
    "application_project_map": "/Volumes/ea_dev/landing/uploads/application_project_map"
}

@dataclass(frozen=True)
class ColumnSpec:
    """How one column moves from bronze to silver."""
    name: str
    data_type: str          # "string" | "integer" | "decimal(18,2)" | "date"
    required: bool = False


@dataclass(frozen=True)
class DatasetContract:
    key_columns: tuple[str, ...]
    columns: tuple[ColumnSpec, ...]
    reject_unexpected: bool = False

    @property
    def required(self) -> frozenset[str]:
        """Column names validation requires — derived, not duplicated."""
        return frozenset(c.name for c in self.columns if c.required)

    @property
    def optional(self) -> frozenset[str]:
        return frozenset(c.name for c in self.columns if not c.required)


DATASET_CONTRACTS = {
    "projects": DatasetContract(
        key_columns=("project_code",),
        columns=(
            ColumnSpec("project_code", "string", required=True),
            ColumnSpec("project_name", "string", required=True),
            ColumnSpec("status", "string", required=True),
            ColumnSpec("planned_start_date", "date", required=True),
            ColumnSpec("planned_end_date", "date", required=True),
            ColumnSpec("actual_start_date", "date", required=True),
            ColumnSpec("actual_end_date", "date", required=True),
        ),
        reject_unexpected=False,
    ),
    "contracts": DatasetContract(
        key_columns=("contract_code",),
        columns=(
            ColumnSpec("contract_code",   "string", required=True),
            ColumnSpec("contract_name",   "string", required=True),
            ColumnSpec("vendor_name",   "string",  required=True),
            ColumnSpec("total_contract_value",  "decimal(18,2)", required=True),
            ColumnSpec("start_date",      "date", required=True),
            ColumnSpec("end_date",        "date", required=True)
        ),
    ),
    "applications": DatasetContract(
        key_columns=("application_code",),
        columns=(
            ColumnSpec("application_code", "string", True),
            ColumnSpec("application_name", "string", True),
            ColumnSpec("lifecycle_status", "string", True),
        ),
        reject_unexpected=False,
    ),
    "application_project_map": DatasetContract(
        key_columns=("application_code", "project_code"),
        columns=(
            ColumnSpec("application_code", "string", required=True),
            ColumnSpec("project_code", "string", required=True),
            ColumnSpec("asserted_by", "string"),
            ColumnSpec("asserted_at", "date"),
            ColumnSpec("notes", "string"),
        ),
    )
}

BRONZE_TABLES = {
    "projects":     "ea_dev.bronze.projects",
    "contracts":    "ea_dev.bronze.contracts",
    "applications": "ea_dev.bronze.applications",
    "application_project_map": "ea_dev.bronze.application_project_map",
}

SILVER_TABLES = {
    "projects":  "ea_dev.silver.projects",
    "contracts": "ea_dev.silver.contracts",
    "applications":  "ea_dev.silver.applications",
    "application_project_map": "ea_dev.silver.application_project_map",
}

LINK_TABLES = {
    "application_project": "ea_dev.silver.link_application_project",
}

STALE_CLAIM_MINUTES = 30
MAX_MESSAGE_LENGTH = 1000
MAX_CORRUPT_ROW_RATIO = 0.05
CORRUPT_RECORD_COLUMN = "_corrupt_record"

# A file still being written is visible on disk but incomplete. Hashing
# it would record a truncated file, and the next stage would then see a
# hash mismatch and report the file as "changed" — which is technically
# true but completely misleading. Recently-touched files wait for the
# next run instead.
MIN_FILE_AGE_SECONDS = 60