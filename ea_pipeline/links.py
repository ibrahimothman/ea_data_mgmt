"""Check foreign keys on the relationship datasets.

Silver types and deduplicates each dataset independently and cannot know
whether a referenced id exists elsewhere. Referential integrity is
therefore checked here, after all silver tables are built.

Findings are recorded rather than acted on: a link pointing at a
non-existent record is a data quality problem for someone to fix, and
removing it would mean nobody ever learns.
"""

import logging

from pyspark.sql import functions as F

from ea_pipeline.config import spark
from ea_pipeline.schema_loader import SILVER_SCHEMA, SILVER_TABLES, LINK_FINDINGS_TABLE

logger = logging.getLogger(__name__)


# Every foreign key in the model: which link column must exist as which
# key in which entity table. Derived from the schema's foreign_keys, but
# stated here so the check is readable in one place.
FOREIGN_KEYS = [
    ("project_application",  "project_id",     "project_initiative", "project_id"),
    ("project_application",  "application_id", "application",        "application_id"),
    ("application_contract", "application_id", "application",        "application_id"),
    ("application_contract", "contract_id",    "contract",           "contract_id"),
    ("project_contract",     "project_id",     "project_initiative", "project_id"),
    ("project_contract",     "contract_id",    "contract",           "contract_id"),
]

# Surrogate primary key of each link dataset, so a finding names the row.
LINK_KEYS = {
    "project_application":  "project_application_id",
    "application_contract": "application_contract_id",
    "project_contract":     "project_contract_id",
}


def _find_orphans(
    link_dataset: str,
    fk_column: str,
    target_dataset: str,
    target_column: str,
):
    """
    Rows in a link whose foreign key has no matching record.

    A left anti join keeps only the rows that did NOT match, which is
    exactly the set of broken references.
    """
    link = spark.table(SILVER_TABLES[link_dataset]).alias("l")
    target = (
        spark.table(SILVER_TABLES[target_dataset])
        .select(F.col(target_column).alias("_target_key"))
        .alias("t")
    )

    orphans = link.join(
        target,
        F.col(f"l.{fk_column}") == F.col("t._target_key"),
        "left_anti",
    )

    return orphans.select(
        F.lit(link_dataset).alias("link_dataset"),
        F.col(LINK_KEYS[link_dataset]).alias("link_id"),
        F.lit(fk_column).alias("fk_column"),
        F.col(fk_column).alias("fk_value"),
        F.lit(target_dataset).alias("target_dataset"),
        F.lit(target_column).alias("target_column"),
        F.col("upload_id"),
    )


def build_link_findings() -> dict:
    """
    Check every foreign key and write the findings.

    Rebuilt in full each run, like silver — it is a pure function of the
    silver tables.
    """
    findings = None

    for link_dataset, fk_column, target_dataset, target_column in FOREIGN_KEYS:
        orphans = _find_orphans(
            link_dataset, fk_column, target_dataset, target_column
        )
        findings = orphans if findings is None else findings.union(orphans)

    findings = findings.withColumn("checked_at", F.current_timestamp())

    (
        findings.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(LINK_FINDINGS_TABLE)
    )

    # Counts per link and column, so the summary is useful without a query.
    counts = {
        f"{row['link_dataset']}.{row['fk_column']}": row["n"]
        for row in (
            spark.table(LINK_FINDINGS_TABLE)
            .groupBy("link_dataset", "fk_column")
            .agg(F.count("*").alias("n"))
            .collect()
        )
    }

    total = spark.table(LINK_FINDINGS_TABLE).count()
    summary = {"table": LINK_FINDINGS_TABLE, "total_findings": total, **counts}

    logger.info("Link integrity: %s", summary)
    return summary


def build_all_link_tables() -> list[dict]:
    try:
        return [build_link_findings()]
    except Exception as error:
        logger.exception("Link check failed")
        return [{"link": "link_integrity", "error": str(error)[:500]}]