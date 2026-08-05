"""Generate silver table DDL from the dataset contracts.

Run once per dataset when setting up, and again when a contract gains a
column. Keeping the DDL derived from the contract means the table and
the transform cannot drift apart.

Not called by the pipeline — this is a setup tool.
"""

from ea_pipeline.config import DATASET_CONTRACTS, SILVER_TABLES


def silver_ddl(dataset_name: str) -> str:
    """Build the CREATE TABLE statement for one dataset's silver table."""
    contract = DATASET_CONTRACTS[dataset_name]
    lines = []

    for spec in contract.columns:
        lines.append(f"    {spec.name} {spec.data_type}")

        # Typed columns keep the raw text and a validity flag, so a
        # failed cast stays distinguishable from a genuinely empty cell.
        if spec.data_type != "string":
            lines.append(f"    {spec.name}_raw STRING")
            lines.append(f"    {spec.name}_is_valid BOOLEAN")

    lines += [
        "    upload_id STRING",
        "    ingested_at TIMESTAMP",
        "    silver_processed_at TIMESTAMP",
    ]

    return (
        f"CREATE TABLE IF NOT EXISTS {SILVER_TABLES[dataset_name]} (\n"
        + ",\n".join(lines)
        + "\n)\nUSING DELTA\n"
        "TBLPROPERTIES (\n"
        "    'delta.autoOptimize.optimizeWrite' = 'true',\n"
        "    'delta.columnMapping.mode'         = 'name'\n"
        ")"
    )