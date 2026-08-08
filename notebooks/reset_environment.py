# Databricks notebook source
# Reset the environment from schema/portfolio_schema.json.
#
# DESTRUCTIVE. Deletes every landing file, every manifest row, and every
# bronze, silver and gold table, then recreates them from the schema.
#
# Use this while the model is still changing. Bronze accumulates columns
# via mergeSchema, so a schema change leaves stale columns and orphaned
# rows behind — recreating is cleaner than patching.

from ea_pipeline.ddl import print_all_ddl
from ea_pipeline.reset import reset_environment
from ea_pipeline.schema_loader import DATASET_CONTRACTS, SCHEMA

# COMMAND ----------

# What the schema currently describes. Check this before running anything.

print(f"{SCHEMA['schema_name']} v{SCHEMA['schema_version']}\n")

for name, contract in DATASET_CONTRACTS.items():
    print(f"{name:24} {contract.table_type:12} key={contract.key_columns}")
    print(f"{'':24} {len(contract.columns)} columns, "
          f"{len(contract.required)} required")

# COMMAND ----------

# The DDL that will be run. Read it before executing the reset.

print_all_ddl()

# COMMAND ----------

# The destructive step. Requires the literal confirmation string.

summary = reset_environment(confirm="RESET")
print(summary)

# COMMAND ----------

# Verify.

for name in DATASET_CONTRACTS:
    bronze = spark.sql(f"DESCRIBE TABLE ea_dev.bronze.{name}").count()
    silver = spark.sql(f"DESCRIBE TABLE ea_dev.silver.{name}").count()
    print(f"{name:24} bronze={bronze:3} cols   silver={silver:3} cols")