"""Build silver tables from bronze.

Silver is the curated, typed view of each dataset: only the columns the
contract declares, cast to real types, one row per key.

Rebuilt in full on each run rather than incrementally. At current volumes
that is cheaper than tracking what changed, and it means silver is always
a pure function of bronze — no accumulated state to drift.
"""

import logging

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ea_pipeline.config import spark
from ea_pipeline.schema_loader import BRONZE_TABLES, DATASET_CONTRACTS, SILVER_TABLES, DATETIME_FORMAT

logger = logging.getLogger(__name__)

DATE_FORMAT = "dd/MM/yyyy"


def bronze_has_new_data(dataset_name: str) -> bool:
    """
    True if bronze has rows newer than the last silver build.

    Avoids rebuilding four tables every hour when nothing was uploaded.
    """
    silver = SILVER_TABLES[dataset_name]

    if not spark.catalog.tableExists(silver):
        return True

    last_built = (
        spark.table(silver)
        .agg(F.max("silver_processed_at").alias("t"))
        .first()["t"]
    )

    if last_built is None:
        return True

    newest_bronze = (
        spark.table(BRONZE_TABLES[dataset_name])
        .agg(F.max("ingested_at").alias("t"))
        .first()["t"]
    )

    return newest_bronze is not None and newest_bronze > last_built

def _cast_expressions(contract) -> list:
    """
    Build the select expressions for one dataset.

    Typed columns produce three outputs: the cast value, the original
    text, and a validity flag. Without the flag a failed cast and a
    genuinely empty cell are both NULL and indistinguishable — so
    "someone typed N/A" would be invisible.
    """
    expressions = []

    for spec in contract.columns:
        raw = F.col(spec.name)

        if spec.data_type == "string":
            expressions.append(F.trim(raw).alias(spec.name))
            continue

        if spec.data_type == "date":
            cast = F.try_to_date(F.trim(raw), F.lit(DATE_FORMAT))
        elif spec.data_type == "timestamp":
            cast = F.try_to_timestamp(F.trim(raw), F.lit(DATETIME_FORMAT))
        else:
            cast = F.trim(raw).cast(spec.data_type)

        expressions.append(cast.alias(spec.name))
        expressions.append(raw.alias(f"{spec.name}_raw"))

        # Valid if the source was empty (nothing to convert) or the cast
        # succeeded. Only a non-empty value producing NULL is a failure.
        expressions.append(
            (raw.isNull() | cast.isNotNull()).alias(f"{spec.name}_is_valid")
        )

    return expressions


def build_silver_table(dataset_name: str) -> dict:
    """
    Rebuild one silver table from its bronze source.

    Returns a summary of what was kept and dropped.
    """
    contract = DATASET_CONTRACTS[dataset_name]
    bronze_table = BRONZE_TABLES[dataset_name]
    silver_table = SILVER_TABLES[dataset_name]

    bronze = spark.table(bronze_table)
    bronze_columns = set(bronze.columns)
    bronze_count = bronze.count()

    # A missing REQUIRED column means something is wrong — usually a
    # contract edit that does not match what files actually contain.
    required_missing = [
        spec.name for spec in contract.columns
        if spec.required and spec.name not in bronze_columns
    ]

    if required_missing:
        raise ValueError(
            f"The contract for '{dataset_name}' requires columns that do not "
            f"exist in {bronze_table}: {required_missing}. "
            f"Bronze currently has: {sorted(bronze_columns)}"
        )

    # A missing OPTIONAL column is fine — nobody has filled it in yet.
    # Add it as typed nulls so silver's shape does not depend on what
    # happens to have been uploaded.
    for spec in contract.columns:
        if spec.name not in bronze_columns:
            bronze = bronze.withColumn(spec.name, F.lit(None).cast("string"))

    # 1. Drop rows Spark could not parse. They are preserved in bronze;
    #    silver is the clean view.
    parsed = bronze.where(F.col("_corrupt_record").isNull())

    # 2. Drop rows with no key — nothing downstream can join to them.
    for key in contract.key_columns:
        parsed = parsed.where(
            F.col(key).isNotNull() & (F.trim(F.col(key)) != "")
        )

    keyed_count = parsed.count()

    # 3. Select and cast the contract's columns, plus lineage.
    typed = parsed.select(
        *_cast_expressions(contract),
        F.col("upload_id"),
        F.col("ingested_at"),
    )

    # 4. One row per key, most recently ingested wins.
    #    row_number rather than dropDuplicates, because that picks an
    #    arbitrary row rather than the newest.
    #    sort by upload_id as a tiebreaker in case 2 rows share the same ingested_at
    window = Window.partitionBy(
        *[F.col(key) for key in contract.key_columns]
    ).orderBy(F.col("ingested_at").desc(), F.col("upload_id").desc())

    deduplicated = (
        typed
        .withColumn("_row", F.row_number().over(window))
        .where(F.col("_row") == 1)
        .drop("_row")
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    # 5. Overwrite. Silver is a pure function of bronze, so a full
    #    replace cannot leave stale rows behind.
    (
        deduplicated.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )

    silver_count = spark.table(silver_table).count()

    summary = {
        "dataset": dataset_name,
        "bronze_rows": bronze_count,
        "dropped_corrupt_or_keyless": bronze_count - keyed_count,
        "deduplicated_away": keyed_count - silver_count,
        "silver_rows": silver_count,
    }

    logger.info("Built %s: %s", silver_table, summary)
    return summary


def build_all_silver_tables(force: bool = False) -> list[dict]:
    summaries = []

    for dataset_name in DATASET_CONTRACTS:
        if not force and not bronze_has_new_data(dataset_name):
            logger.info("Skipping %s — no new bronze data", dataset_name)
            summaries.append({"dataset": dataset_name, "skipped": True})
            continue

        try:
            summaries.append(build_silver_table(dataset_name))

        except Exception as error:
            logger.exception("Silver build failed for %s", dataset_name)
            summaries.append({
                "dataset": dataset_name,
                "error": str(error)[:500],
            })

    return summaries