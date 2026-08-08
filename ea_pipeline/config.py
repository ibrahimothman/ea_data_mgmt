"""Runtime configuration.

Everything about the data model — dataset names, columns, types, table
names, landing directories — comes from schema/portfolio_schema.json via
schema_loader. This module holds only what the schema does not: the Spark
session and the operational thresholds.
"""

from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()


# --- operational thresholds ---------------------------------------------

# How long an in-progress claim may sit before it is treated as abandoned.
STALE_CLAIM_MINUTES = 30

# Cap on messages written to the manifest, so a stack trace cannot fill a cell.
MAX_MESSAGE_LENGTH = 1000

# Share of unparseable rows above which a file is rejected. A file this
# broken is usually the wrong file, or exported with the wrong delimiter.
MAX_CORRUPT_ROW_RATIO = 0.05

# How long a file must be untouched before it is processed, so a file still
# being uploaded is not hashed half-written.
MIN_FILE_AGE_SECONDS = 60

# Column Spark fills with the raw text of rows it could not parse.
CORRUPT_RECORD_COLUMN = "_corrupt_record"