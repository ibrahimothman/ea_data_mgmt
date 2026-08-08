"""Tear down and recreate the environment from the schema JSON.

For use while the model is still changing. Bronze accumulates columns via
mergeSchema, so a schema change leaves stale columns behind and old rows
whose keys no longer exist — dropping and recreating is cleaner than
patching. Silver and gold are rebuilt from bronze anyway, so they are
simply dropped.

Everything physical is read from the schema, so adding a dataset to the
JSON is enough for it to be created here.

DESTRUCTIVE. Every function that deletes requires confirm=True.
"""

import logging
import os
import shutil

from ea_pipeline.config import spark
from ea_pipeline.ddl import bronze_ddl, silver_ddl, link_findings_ddl
from ea_pipeline.schema_loader import (
    BRONZE_SCHEMA,
    BRONZE_TABLES,
    DATASET_CONTRACTS,
    DATASET_DIRECTORIES,
    GOLD_SCHEMA,
    MANIFEST_TABLE,
    OPERATIONS_SCHEMA,
    SILVER_SCHEMA,
    SILVER_TABLES,
)

logger = logging.getLogger(__name__)

CONFIRMATION = "RESET"


def _require_confirmation(confirm: str) -> None:
    if confirm != CONFIRMATION:
        raise ValueError(
            f"This deletes data. Pass confirm='{CONFIRMATION}' to proceed."
        )


# --- teardown -----------------------------------------------------------


def clear_landing_files(confirm: str = "") -> list[str]:
    """
    Delete every file in every landing directory.

    Directories themselves are kept — recreating them is harmless but
    removing them would break anything holding a path.
    """
    _require_confirmation(confirm)
    removed = []

    for dataset_name, directory in DATASET_DIRECTORIES.items():
        if not os.path.isdir(directory):
            logger.warning("Landing directory missing: %s", directory)
            continue

        for file_name in os.listdir(directory):
            path = os.path.join(directory, file_name)

            if os.path.isfile(path):
                os.remove(path)
                removed.append(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
                removed.append(path)

    logger.info("Removed %d landing files", len(removed))
    return removed


def drop_tables(confirm: str = "") -> list[str]:
    """
    Drop every bronze and silver table, and every table in the gold schema.

    Gold is dropped by listing the schema rather than by name, because gold
    tables are not declared in the schema.
    """
    _require_confirmation(confirm)
    dropped = []

    for table in list(BRONZE_TABLES.values()) + list(SILVER_TABLES.values()):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        dropped.append(table)

    spark.sql(f"DROP TABLE IF EXISTS {FINDINGS_TABLE}")
    dropped.append(FINDINGS_TABLE)    

    if spark.catalog.databaseExists(GOLD_SCHEMA):
        for row in spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}").collect():
            table = f"{GOLD_SCHEMA}.{row['tableName']}"
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            dropped.append(table)

    logger.info("Dropped %d tables", len(dropped))
    return dropped


def clear_manifest(confirm: str = "", drop: bool = False) -> None:
    """
    Empty the manifest.

    Deleting the rows is enough in most cases — the manifest schema does not
    change when the schema does. Pass drop=True after changing
    MANIFEST_SCHEMA itself.

    This must run whenever landing files are removed, or registration would
    still hold rows pointing at files that no longer exist.
    """
    _require_confirmation(confirm)

    if not spark.catalog.tableExists(MANIFEST_TABLE):
        return

    if drop:
        spark.sql(f"DROP TABLE IF EXISTS {MANIFEST_TABLE}")
        logger.info("Dropped %s", MANIFEST_TABLE)
    else:
        spark.sql(f"DELETE FROM {MANIFEST_TABLE}")
        logger.info("Emptied %s", MANIFEST_TABLE)


# --- rebuild ------------------------------------------------------------


def create_schemas() -> None:
    """Create the catalog schemas if they do not exist."""
    for schema in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, OPERATIONS_SCHEMA):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    logger.info("Schemas ready")


def create_landing_directories() -> list[str]:
    """Create a landing directory per dataset."""
    created = []

    for directory in DATASET_DIRECTORIES.values():
        os.makedirs(directory, exist_ok=True)
        created.append(directory)

    return created


def create_manifest_table() -> None:
    """Create the manifest table from MANIFEST_SCHEMA if it is missing."""
    if spark.catalog.tableExists(MANIFEST_TABLE):
        return

    # Imported here rather than at module level to keep the import graph
    # one-way: register depends on config, not the other way round.
    from ea_pipeline.register import MANIFEST_SCHEMA

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} ({MANIFEST_SCHEMA.toDDL()}) "
        "USING DELTA"
    )
    logger.info("Created %s", MANIFEST_TABLE)


def create_tables() -> list[str]:
    """Create every bronze and silver table from the schema."""
    created = []

    for dataset_name in DATASET_CONTRACTS:
        spark.sql(bronze_ddl(dataset_name))
        created.append(BRONZE_TABLES[dataset_name])

        spark.sql(silver_ddl(dataset_name))
        created.append(SILVER_TABLES[dataset_name])

        spark.sql(link_findings_ddl())
        created.append(FINDINGS_TABLE)

    logger.info("Created %d tables", len(created))
    return created


# --- the whole thing ----------------------------------------------------


def reset_environment(confirm: str = "", drop_manifest: bool = False) -> dict:
    """
    Full teardown and rebuild.

    Order matters: files and manifest rows go first, so nothing is left
    referencing a table that is about to be dropped.
    """
    _require_confirmation(confirm)

    summary = {}

    summary["landing_files_removed"] = len(clear_landing_files(confirm))
    clear_manifest(confirm, drop=drop_manifest)
    summary["tables_dropped"] = len(drop_tables(confirm))

    create_schemas()
    summary["directories"] = len(create_landing_directories())
    create_manifest_table()
    summary["tables_created"] = len(create_tables())

    summary["datasets"] = sorted(DATASET_CONTRACTS)

    logger.info("Reset complete: %s", summary)
    return summary