# EA Data Management — Silver Layer

Turns the raw, all-string bronze tables into a curated, typed model: one row per entity, real data types, and verified relationships between them.

Silver is a **pure function of bronze**. Every table is rebuilt from scratch on each run, so the result depends only on what bronze currently holds. There is no accumulated state to drift, no idempotency question, and no stale rows.

---

## Contents

- [How it works](#how-it-works)
- [Tables](#tables)
- [The dataset contract](#the-dataset-contract)
- [Transformation rules](#transformation-rules)
- [Link tables](#link-tables)
- [Running it](#running-it)
- [Monitoring](#monitoring)
- [Changing a contract](#changing-a-contract)
- [Design decisions](#design-decisions)
- [Known limitations](#known-limitations)

---

## How it works

```
bronze (all strings, every upload ever)
   │
   ├─ drop unparseable rows
   ├─ drop rows with no key
   ├─ select only the contract's columns
   ├─ cast to real types, keeping raw value + validity flag
   ├─ deduplicate — one row per key, newest upload wins
   ▼
silver (typed, current state)
   │
   ├─ join entities together
   ├─ flag references that do not resolve
   ▼
link tables (verified relationships)
```

Two stages in one job. Links run after silver because they join silver tables.

### Latency

A file uploaded at 10:01 reaches bronze by 10:15 (upload pipeline, every 15 minutes) and appears in silver and links by 11:00 (silver pipeline, hourly).

---

## Tables

### Entities

| Table | Source | Key |
|---|---|---|
| `ea_dev.silver.applications` | `bronze.applications` | `application_code` |
| `ea_dev.silver.projects` | `bronze.projects` | `project_code` |
| `ea_dev.silver.contracts` | `bronze.contracts` | `contract_code` |
| `ea_dev.silver.application_project_map` | `bronze.application_project_map` | `application_code` + `project_code` |

### Links

| Table | Built from | Purpose |
|---|---|---|
| `ea_dev.silver.link_application_project` | the map + applications + projects | verified application ↔ project relationships |

### Column shape

Every silver table contains:

- the contract's declared columns, cast to their declared types
- for each **typed** (non-string) column: `<name>_raw` and `<name>_is_valid`
- `upload_id` and `ingested_at` — lineage carried from bronze
- `silver_processed_at` — when this row was built

Columns present in bronze but absent from the contract do **not** reach silver. They remain available in bronze.

---

## The dataset contract

One definition per dataset serves three purposes: validation checks the column names, silver reads the types, and the DDL generator reads both.

```python
"contracts": DatasetContract(
    key_columns=("contract_code",),
    columns=(
        ColumnSpec("contract_code",  "string",        required=True),
        ColumnSpec("contract_name",  "string",        required=True),
        ColumnSpec("supplier_name",  "string",        required=True),
        ColumnSpec("contract_value", "decimal(18,2)"),
        ColumnSpec("start_date",     "date"),
        ColumnSpec("end_date",       "date"),
    ),
    reject_unexpected=False,
)
```

`required` and `optional` are **derived properties** computed from the specs, so the upload pipeline's validation works unchanged.

`key_columns` is a tuple because links are many-to-many — `application_project_map` needs both columns as its key, or deduplication would keep one project per application and silently discard the rest.

### Supported types

| `data_type` | Notes |
|---|---|
| `string` | trimmed only, no flag columns |
| `decimal(18,2)` | for money — never `double`, see below |
| `date` | parsed with `d/M/yyyy` |
| `integer`, `bigint` | standard casts |

---

## Transformation rules

### Rows dropped

| Reason | Why |
|---|---|
| `_corrupt_record` is not null | Spark could not parse the row. Preserved in bronze; silver is the clean view. |
| any key column is null or empty | Nothing downstream can join to it or deduplicate it. |

### Casting

Typed columns produce three outputs:

```
contract_value | contract_value_raw | contract_value_is_valid
50000.00       | "50000.00"         | true
null           | "N/A"              | false      ← failed to cast
null           | null               | true       ← genuinely empty
```

Rows 2 and 3 are both `null`, but they mean different things. The flag is what distinguishes "someone typed N/A" from "the cell was blank" — without it, the evidence is destroyed at cast time.

The flag is `raw IS NULL OR cast IS NOT NULL`: valid if there was nothing to convert, or the conversion worked. Only a non-empty value producing null is a failure.

Casts use `try_to_date` and equivalent, so a bad value returns null rather than raising. **One bad cell must never fail the build** — the whole flag design depends on it.

### Deduplication

Bronze accumulates every upload. Silver keeps one row per key, most recently ingested:

```python
window = Window.partitionBy(*key_columns).orderBy(F.col("ingested_at").desc())
.withColumn("_row", F.row_number().over(window)).where(F.col("_row") == 1)
```

`row_number()` rather than `dropDuplicates()`, because `dropDuplicates` keeps an arbitrary row — you cannot control which, and it can differ between runs on identical data. The window makes "latest upload wins" a stated rule.

---

## Link tables

Silver transforms one dataset at a time and cannot know whether a code exists elsewhere. Referential integrity is therefore checked when tables are joined, not at upload — validation only reads the header.

`link_application_project` left-joins the mapping to both entity tables and records the result:

| `link_status` | Meaning |
|---|---|
| `VALID` | both codes resolve to a real record |
| `ORPHAN_APPLICATION` | `application_code` does not exist |
| `ORPHAN_PROJECT` | `project_code` does not exist |
| `ORPHAN_BOTH` | neither exists — usually the wrong file or wrong code format |

**Orphans are flagged, not dropped.** An inner join would silently remove them and nobody would learn that an owner mapped to a non-existent project. Downstream consumers filter on `link_status = 'VALID'`; the quality report counts the rest.

`ORPHAN_BOTH` is separated because it usually indicates a structural problem rather than one mistyped row.

---

## Running it

### Scheduled

Job `ea_silver_rebuild`, notebook task pointing at `notebooks/run_silver`, hourly.

- **Maximum concurrent runs: 1**
- **Retries: 0**

### Manually

```python
from ea_pipeline.silver import build_all_silver_tables
from ea_pipeline.links import build_all_link_tables

print(build_all_silver_tables())
print(build_all_link_tables())
```

### A single table

```python
from ea_pipeline.silver import build_silver_table

print(build_silver_table("contracts"))
```

### Forcing a rebuild

Silver skips a dataset when bronze has no rows newer than the last build. After a contract change there is no new bronze data, so the skip must be overridden:

```python
build_silver_table("contracts", force=True)
```

### Summaries

```python
{'dataset': 'applications', 'bronze_rows': 150,
 'dropped_corrupt_or_keyless': 2, 'deduplicated_away': 48,
 'silver_rows': 100}

{'table': 'ea_dev.silver.link_application_project',
 'VALID': 42, 'ORPHAN_PROJECT': 7}
```

A large `deduplicated_away` means more repeat uploads than expected — worth understanding, since it may mean the same file is being sent repeatedly.

---

## Monitoring

### Cast failures

```sql
SELECT contract_code, contract_value_raw
FROM ea_dev.silver.contracts
WHERE NOT contract_value_is_valid;
```

Returns the rows where a value could not be converted, with the original text. This is the list to send back to whoever produced the file.

### Orphaned links

```sql
SELECT application_code, project_code, link_status, asserted_by
FROM ea_dev.silver.link_application_project
WHERE link_status != 'VALID';
```

Common causes: case mismatch, whitespace, or a project genuinely missing from the projects extract.

### Mapping coverage

```sql
SELECT
    count(*) AS applications,
    count(DISTINCT l.application_code) AS mapped,
    round(100.0 * count(DISTINCT l.application_code) / count(*), 1) AS pct
FROM ea_dev.silver.applications a
LEFT JOIN ea_dev.silver.link_application_project l
    ON a.application_code = l.application_code
   AND l.link_status = 'VALID';
```

The honest measure of how much the model can be trusted. An unmapped application has no cost attributed to it and will appear at zero in any ranking.

### Ambiguous dates

```sql
SELECT count(*) AS possibly_inverted
FROM ea_dev.silver.contracts
WHERE day(start_date) <= 12;
```

`3/4/2026` is 3 April under `d/M/yyyy` but 4 March if exported from a US-locale Excel. Same string, no error, wrong answer. No format setting detects this — only the count of ambiguous values gives a hint.

### Unexpected columns arriving

```sql
SELECT dataset_name, unexpected_columns, count(*)
FROM ea_dev.operations.upload_manifest
WHERE size(unexpected_columns) > 0
GROUP BY dataset_name, unexpected_columns;
```

What sources are sending that is not being modelled — a prompt for "should this be in the contract?"

---

## Changing a contract

Re-running the DDL generator is **almost never** what is needed. Silver writes with `overwriteSchema`, so the table follows the contract on every rebuild.

| Change | What to do |
|---|---|
| **Add a column** | Confirm bronze has it, then rebuild with `force=True` |
| **Remove a column** | Rebuild. Check nothing downstream reads it first — it disappears silently |
| **Change a type** | Rebuild, then check `_is_valid` counts. A tighter type may start failing values that previously passed |
| **Change required/optional** | Affects future uploads only. To reassess existing ones, reset them to `RECEIVED` |
| **Change `key_columns`** | Rebuild with `force=True` and verify the row count moved as expected |

### Renaming a key column is a migration, not a config change

Editing the contract does not rename anything in bronze. Old rows keep the old column, which is null under the new name, so **every historical row is dropped** for having no key.

Two real options:

- **Upload a complete extract** with the new names. Silver rebuilds from it entirely.
- **Rename in bronze**, if it is the same value under a new name:
  ```sql
  ALTER TABLE ea_dev.bronze.projects RENAME COLUMN project_id TO project_code;
  ```
  Metadata-only and instant, thanks to `delta.columnMapping.mode = 'name'`.

A guard in `build_silver_table` catches the contract naming a required column bronze does not have, and names the dataset and available columns rather than producing a bare Spark resolution error.

---

## Design decisions

### Full rebuild, not incremental

Silver is derived entirely from bronze, so rebuilding produces the identical result every time. That removes the idempotency question, prevents stale rows, and makes schema changes free.

The cost is recomputing everything each run — irrelevant at current volumes, and the thing to revisit when it is not.

**Consequence:** bronze retention is effectively silver retention. If bronze were ever cleaned up, silver would silently shrink on the next run.

### `decimal`, never `double`

`double` is binary floating point, so `0.1 + 0.2` is not `0.3` and sums drift by fractions of a penny over thousands of rows. Someone will eventually reconcile against finance and find a discrepancy nobody can explain.

### Missing optional columns become typed nulls

If no uploaded file has ever contained an optional column, bronze does not have it. Rather than failing, silver adds it as a string column of nulls.

This keeps silver's shape stable — the table always has exactly the contract's columns, regardless of what happens to have been uploaded. A table that gains and loses columns depending on upload history is awkward for anything reading it.

### Each dataset is isolated

`build_all_silver_tables` catches per dataset and continues, so one bad contract cannot block the whole layer. The runner raises at the end if anything failed, after printing every summary.

### Plain Python, not DLT

DLT genuinely fits this layer — declarative transforms, built-in expectations, automatic ordering, and `AUTO CDC` would replace the deduplication window with two lines.

Deferred because learning a framework while also deciding what silver should contain is two unknowns at once, and the second is the interesting one.

**Revisit when:** gold is built, or a fifth dataset arrives. By then the transformations and quality rules will be known, so evaluating DLT becomes concrete rather than theoretical.

### Links are a separate step

Silver is a generic "one dataset in, one table out" function driven by contracts and run in a loop. Adding cross-table joins to it would break that shape. Links are their own module and their own stage, which also generalises to the link tables still to come.

### Links rebuild unconditionally

A link table depends on three silver tables, and any of them changing invalidates it. Working out whether a rebuild is needed costs more than doing it.

---

## Known limitations

### No history — current state only

Silver keeps one row per key. Earlier versions are overwritten.

This means questions like *"how many applications were active at year end"*, *"when did APP001 get retired"*, or *"what did this report show in June"* cannot be answered.

Bronze holds the raw material, but not usably: it is untyped, it records ingestion dates rather than validity dates, and deletions are invisible — a record dropped from an extract simply stops receiving new rows, with nothing marking the removal.

**The fix is SCD Type 2** — keeping every version with `valid_from` / `valid_to` / `is_current`. This is what DLT's `AUTO CDC` provides.

**Trigger:** anyone asks an "as at" or "when did X change" question.

Worth knowing early: retrofitting history only recovers what bronze happens to hold, so the sooner the need is known, the more history survives.

### Deletions do not propagate

A partial extract does not replace previous state — silver ends up with the union. An entity removed from the source keeps its last known row in silver indefinitely.

Usually the desired behaviour for incremental extracts, but wrong if a source sends partial files expecting them to be authoritative.

### Ambiguous dates cannot be detected

`3/4/2026` parses successfully under both `d/M/yyyy` and `M/d/yyyy`, giving different dates. No error, no flag. Only the count of values where the day is 12 or lower gives any signal.

### Referential integrity is after the fact

An owner cannot be stopped from mapping to a non-existent project at upload time, because validation only reads the header. Orphans are found at link build and flagged, which means a wrong mapping can sit in the system for up to an hour before it is visible.

### Contract and bronze can drift

Editing a contract does not revalidate existing uploads or change bronze. The guard catches missing required columns at build time, but a contract can still describe a shape no file has ever had.