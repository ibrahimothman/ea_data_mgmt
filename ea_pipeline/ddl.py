"""Generate DDL from the dataset contracts.

Bronze tables declare only the lineage columns — the data columns arrive on
first write via mergeSchema, because bronze must accept whatever a file
contains. Silver tables declare the full typed shape, because silver is the
curated view and its columns are decided by the contract.

Keeping DDL derived means the tables and the transforms cannot drift.
"""

from ea_pipeline.schema_loader import (
    BRONZE_TABLES,
    DATASET_CONTRACTS,
    SILVER_TABLES,
    LINK_FINDINGS_TABLE,
    TABLE_PROPERTIES,
    DatasetContract,
)

CORRUPT_RECORD_COLUMN = "_corrupt_record"


def _properties_clause() -> str:
    entries = ",\n".join(
        f"    '{key}' = '{value}'" for key, value in TABLE_PROPERTIES.items()
    )
    return f"TBLPROPERTIES (\n{entries}\n)"


def _escape(text: str) -> str:
    return text.replace("'", "''")


def bronze_ddl(dataset_name: str) -> str:
    """
    CREATE TABLE for one bronze table.

    Only lineage columns are declared. Every source column arrives as a
    string via mergeSchema on first write.
    """
    contract = DATASET_CONTRACTS[dataset_name]

    return (
        f"CREATE TABLE IF NOT EXISTS {BRONZE_TABLES[dataset_name]} (\n"
        "    upload_id STRING COMMENT 'Joins to operations.upload_manifest',\n"
        "    source_file_name STRING COMMENT 'Original file name as uploaded',\n"
        "    ingested_at TIMESTAMP COMMENT 'When this row was written to bronze',\n"
        f"    {CORRUPT_RECORD_COLUMN} STRING "
        "COMMENT 'Raw text of rows Spark could not parse'\n"
        ")\n"
        "USING DELTA\n"
        f"{_properties_clause()}\n"
        f"COMMENT '{_escape(contract.display_name)} — raw landing data. "
        "All source columns are STRING. Append-only.'"
    )


def silver_ddl(dataset_name: str) -> str:
    """
    CREATE TABLE for one silver table.

    Typed columns carry a _raw copy and an _is_valid flag, so a failed cast
    stays distinguishable from a genuinely empty cell.
    """
    contract: DatasetContract = DATASET_CONTRACTS[dataset_name]
    lines = []

    for spec in contract.columns:
        comment = _escape(spec.description)

        if spec.enum_values:
            comment += f" Allowed: {', '.join(spec.enum_values)}."

        lines.append(f"    {spec.name} {spec.data_type} COMMENT '{comment}'")

        if spec.is_typed:
            lines.append(
                f"    {spec.name}_raw STRING "
                f"COMMENT 'Original text of {spec.name} before casting'"
            )
            lines.append(
                f"    {spec.name}_is_valid BOOLEAN "
                f"COMMENT 'False when {spec.name} was present but could not be cast'"
            )

    lines += [
        "    upload_id STRING COMMENT 'Joins to operations.upload_manifest'",
        "    ingested_at TIMESTAMP COMMENT 'When the source row reached bronze'",
        "    silver_processed_at TIMESTAMP COMMENT 'When this row was built'",
    ]

    return (
        f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES[dataset_name]} (\n"
        + ",\n".join(lines)
        + "\n)\nUSING DELTA\n"
        + f"{_properties_clause()}\n"
        + f"COMMENT '{_escape(contract.display_name)} — typed and deduplicated. "
        f"One row per {', '.join(contract.key_columns)}.'"
    )

def link_findings_ddl() -> str:
    """
    CREATE TABLE for the link integrity findings.

    Unlike the silver tables this shape is fixed rather than derived from
    the portfolio schema — it describes problems, not portfolio data.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {LINK_FINDINGS_TABLE} (\n"
        "    link_dataset STRING "
        "COMMENT 'Relationship dataset the broken reference was found in',\n"
        "    link_id STRING "
        "COMMENT 'Surrogate key of the offending relationship row',\n"
        "    fk_column STRING "
        "COMMENT 'Foreign key column that did not resolve',\n"
        "    fk_value STRING "
        "COMMENT 'The value that had no matching record',\n"
        "    target_dataset STRING "
        "COMMENT 'Entity dataset the value should have matched',\n"
        "    target_column STRING "
        "COMMENT 'Key column in the target dataset',\n"
        "    upload_id STRING "
        "COMMENT 'Upload the offending row came from — joins to the manifest',\n"
        "    checked_at TIMESTAMP "
        "COMMENT 'When this check ran'\n"
        ")\n"
        "USING DELTA\n"
        f"{_properties_clause()}\n"
        "COMMENT 'Foreign keys in the relationship datasets that do not "
        "resolve to an existing record. Rebuilt in full on each silver run. "
        "Empty means every reference is valid.'"
    )
def all_ddl() -> list[tuple[str, str]]:
    """Every CREATE TABLE statement, as (label, sql) pairs."""
    statements = []

    for dataset_name in DATASET_CONTRACTS:
        statements.append((f"bronze.{dataset_name}", bronze_ddl(dataset_name)))

    for dataset_name in DATASET_CONTRACTS:
        statements.append((f"silver.{dataset_name}", silver_ddl(dataset_name)))

    statements.append((f"silver.{LINK_FINDINGS_TABLE}", link_findings_ddl()))    

    return statements


def print_all_ddl() -> None:
    """Print every statement for review before running it."""
    for label, sql in all_ddl():
        print(f"-- {label}")
        print(sql)
        print(";\n")