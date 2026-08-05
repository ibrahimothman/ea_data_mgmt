"""Build link tables between silver entities.

Links are where referential integrity is checked. Silver transforms one
dataset at a time and cannot know whether a code exists elsewhere, so an
orphaned reference only becomes visible when the tables are joined.

Orphans are flagged, not dropped: a mapping pointing at a non-existent
project is a data quality finding someone should fix, and silently
removing it means nobody ever learns.
"""

import logging

from pyspark.sql import functions as F

from ea_pipeline.config import LINK_TABLES, SILVER_TABLES, spark

logger = logging.getLogger(__name__)


def build_application_project_link() -> dict:
    """
    Build link_application_project from the owner-asserted mapping.

    Every mapping row is kept. link_status records whether both ends
    resolve to a real record.
    """
    mapping = spark.table(SILVER_TABLES["application_project_map"])
    applications = spark.table(SILVER_TABLES["applications"])
    projects = spark.table(SILVER_TABLES["projects"])

    # Left joins: keep every mapping row, whether or not it resolves.
    # A null on the right means the code does not exist.
    resolved = (
        mapping.alias("m")
        .join(
            applications.select(
                F.col("application_code").alias("_app_exists")
            ).alias("a"),
            F.col("m.application_code") == F.col("a._app_exists"),
            "left",
        )
        .join(
            projects.select(
                F.col("project_code").alias("_prj_exists")
            ).alias("p"),
            F.col("m.project_code") == F.col("p._prj_exists"),
            "left",
        )
    )

    linked = resolved.select(
        F.col("m.application_code"),
        F.col("m.project_code"),
        F.col("m.asserted_by"),
        F.col("m.asserted_at"),
        F.col("m.notes"),

        # Both ends must resolve for the link to be usable downstream.
        F.when(
            F.col("_app_exists").isNull() & F.col("_prj_exists").isNull(),
            F.lit("ORPHAN_BOTH"),
        ).when(
            F.col("_app_exists").isNull(), F.lit("ORPHAN_APPLICATION")
        ).when(
            F.col("_prj_exists").isNull(), F.lit("ORPHAN_PROJECT")
        ).otherwise(
            F.lit("VALID")
        ).alias("link_status"),

        F.col("m.upload_id"),
        F.current_timestamp().alias("link_built_at"),
    )

    (
        linked.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(LINK_TABLES["application_project"])
    )

    # Counts by status, so orphans are visible without a separate query.
    counts = {
        row["link_status"]: row["n"]
        for row in linked.groupBy("link_status").agg(
            F.count("*").alias("n")
        ).collect()
    }

    summary = {"table": LINK_TABLES["application_project"], **counts}
    logger.info("Built link table: %s", summary)

    return summary


def build_all_link_tables() -> list[dict]:
    """
    Build every link table.

    Runs after silver, since links join silver tables. Each is
    independent — a failure is recorded and the rest continue.
    """
    builders = {
        "application_project": build_application_project_link,
    }

    summaries = []

    for name, builder in builders.items():
        try:
            summaries.append(builder())
        except Exception as error:
            logger.exception("Link build failed for %s", name)
            summaries.append({"link": name, "error": str(error)[:500]})

    return summaries 