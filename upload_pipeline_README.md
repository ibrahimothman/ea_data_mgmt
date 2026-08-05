# EA Data Management — Upload Pipeline

Ingests manually uploaded CSV files into the bronze layer, with a manifest recording what happened to every file.

Built for **human-uploaded files**, not machine feeds. The questions it answers are "who sent this", "is it the same file as last week", "why was it rejected" — not "how fast can we ingest". That shapes most of the design decisions below.

---

## Contents

- [How it works](#how-it-works)
- [Status state machine](#status-state-machine)
- [Repository layout](#repository-layout)
- [Configuration](#configuration)
- [Running it](#running-it)
- [Monitoring](#monitoring)
- [Design decisions](#design-decisions)
- [Deferred improvements](#deferred-improvements)
- [Development](#development)

---

## How it works

A scheduled job runs every 15 minutes and moves files through three stages.

```
landing volume  →  register  →  validate  →  bronze
                       ↓            ↓           ↓
                   manifest ────────────────────┘
```

**Register** — records a file in the manifest: name, path, size, SHA-256 hash, who uploaded it. Deliberately does not open the file, so a corrupt file still gets a manifest row and a visible rejection rather than failing before any record exists.

**Validate** — reads the header, compares it against the dataset's column contract, counts the data rows. Records what was missing, unexpected, or duplicated.

**Bronze** — reads the CSV as all-strings, attaches lineage columns, writes to the dataset's bronze table.

Every stage re-checks the file's hash before working on it, so a file replaced mid-pipeline is caught rather than silently processed.

### Datasets

| Dataset | Landing directory | Bronze table |
|---|---|---|
| `projects` | `/Volumes/ea_dev/landing/uploads/projects` | `ea_dev.bronze.projects` |
| `contracts` | `/Volumes/ea_dev/landing/uploads/contracts` | `ea_dev.bronze.contracts` |
| `applications` | `/Volumes/ea_dev/landing/uploads/applications` | `ea_dev.bronze.applications` |

The dataset is inferred from which directory a file is in.

---

## Status state machine

Every upload has exactly one status in `ea_dev.operations.upload_manifest`.

```
                    ┌──────────────┐
                    │   RECEIVED   │ ◄── registration
                    └──────┬───────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
      ┌───────────────┐          ┌─────────────┐
      │  VALIDATING   │          │  DUPLICATE  │ (terminal)
      └───────┬───────┘          └─────────────┘
              │
    ┌─────────┼─────────┐
    ▼         ▼         ▼
┌─────────┐ ┌──────────┐ ┌────────┐
│VALIDATED│ │ REJECTED │ │ FAILED │
└────┬────┘ └──────────┘ └───┬────┘
     │        (terminal)     │ retryable
     │                       └──► back to VALIDATING
     ▼
┌────────────┐
│ PROCESSING │
└─────┬──────┘
      │
  ┌───┴────────────┬──────────────────┐
  ▼                ▼                  ▼
┌───────────┐ ┌─────────────────┐ ┌───────────────┐
│ PROCESSED │ │ BRONZE_REJECTED │ │ BRONZE_FAILED │
└───────────┘ └─────────────────┘ └───────┬───────┘
 (terminal)      (terminal)               │ retryable
                                          └──► back to PROCESSING
```

### What each status means

| Status | Meaning | Next |
|---|---|---|
| `RECEIVED` | registered, not yet validated | validation |
| `DUPLICATE` | same content as an earlier upload | nothing — terminal |
| `VALIDATING` | validation in progress | — |
| `VALIDATED` | passed the contract | bronze |
| `REJECTED` | file is wrong — needs a new file | nothing — terminal |
| `FAILED` | something broke — retryable | validation retries it |
| `PROCESSING` | bronze load in progress | — |
| `PROCESSED` | loaded successfully | nothing — terminal |
| `BRONZE_REJECTED` | can't be loaded as it stands | nothing — terminal |
| `BRONZE_FAILED` | something broke — retryable | bronze retries it |

**REJECTED vs FAILED** is the important distinction. `REJECTED` means the analyst must send a different file — retrying achieves nothing. `FAILED` means the pipeline hit a problem, so the next scheduled run tries again automatically.

### Claiming and stale claims

A stage moves an upload into a working status (`VALIDATING`, `PROCESSING`) with a single conditional `UPDATE`, so two runs cannot both claim the same row. If a run dies mid-stage the row would be stuck forever, so a claim older than `STALE_CLAIM_MINUTES` can be reclaimed, and `release_stale_claims()` resets abandoned rows to the matching retryable status.

---

## Repository layout

```
ea_pipeline/
    config.py         Spark session, table names, directories, thresholds, contracts
    states.py         UploadStatus enum and the claimable-status lists
    errors.py         ContractViolation, BronzeIngestionError
    models.py         Result dataclasses (pure Python, no Spark)
    files.py          Hashing, CSV structure reading, column normalisation
    manifest.py       All manifest reads and writes
    register.py       Stage 1
    validate.py       Stage 2
    bronze.py         Stage 3
    orchestrate.py    Batch driver — discovery and stage sequencing

notebooks/
    run_pipeline.py       Scheduled entry point
    integration_tests.py  Manual tests — need a live workspace

tests/                Pure-function unit tests, run by CI
```

Dependencies flow one way: `config`/`states`/`errors`/`models` depend on nothing, `files`/`manifest` sit above them, the three stages above that, and `orchestrate` on top. The stages do not import each other — the pipeline order lives in `orchestrate`.

---

## Configuration

All in `ea_pipeline/config.py`.

| Constant | Default | What it controls |
|---|---|---|
| `STALE_CLAIM_MINUTES` | 30 | when an in-progress claim is treated as abandoned |
| `MAX_MESSAGE_LENGTH` | 1000 | cap on messages written to the manifest |
| `MAX_CORRUPT_ROW_RATIO` | 0.05 | share of unparseable rows that rejects a file |
| `MIN_FILE_AGE_SECONDS` | 60 | how long a file must be untouched before processing |
| `DATASET_CONTRACTS` | — | required/optional columns per dataset |

### Column contracts

```python
"projects": DatasetContract(
    required=frozenset({"project_id", "project_name", "project_status"}),
    optional=frozenset({"start_date", "end_date", "project_manager"}),
    reject_unexpected=False,
)
```

`reject_unexpected=False` on all datasets: extra columns are recorded and mentioned in the validation message, but do not block the upload. Source systems add columns routinely and blocking on that would halt the pipeline for a harmless change.

Column names are normalised before comparison — spaces, hyphens, dots and slashes become underscores — so `Project ID` from an Excel export matches a contract expecting `project_id`.

---

## Running it

### Scheduled

Job `ea_upload_pipeline`, notebook task pointing at `notebooks/run_pipeline`, every 15 minutes.

Required settings:

- **Maximum concurrent runs: 1** — overlapping runs would let the reaper mark another run's in-progress work as failed
- **Retries: 0** — the manifest handles retries via the claimable statuses; a job-level retry would re-run the whole batch

### Manually

```python
from ea_pipeline.orchestrate import release_stale_claims, run_pipeline

release_stale_claims()
print(run_pipeline())
```

Returns a summary like:

```python
{'files_seen': 8, 'registered': 3, 'validated': 2,
 'rejected': 1, 'loaded': 2, 'errors': 0}
```

### A single upload

```python
from ea_pipeline import (
    register_uploaded_file, validate_registered_upload, load_upload_to_bronze
)

upload_id = register_uploaded_file(path, "projects")
validate_registered_upload(upload_id)
load_upload_to_bronze(upload_id)
```

---

## Monitoring

### Health summary

```sql
SELECT status, count(*) AS n, max(updated_at) AS latest
FROM ea_dev.operations.upload_manifest
GROUP BY status
ORDER BY n DESC;
```

Terminal statuses (`PROCESSED`, `REJECTED`, `DUPLICATE`) accumulating is normal. Non-terminal statuses should stay near zero — a pile in `VALIDATED` means bronze isn't running. Every `latest` being hours old means the job has stopped, which no alert will tell you.

### Anything not finished

```sql
SELECT original_file_name, status, validation_message, uploaded_by, updated_at
FROM ea_dev.operations.upload_manifest
WHERE status NOT IN ('PROCESSED', 'DUPLICATE')
ORDER BY updated_at DESC;
```

The working list — and what to show an analyst asking about their file.

### Reconciliation

```sql
SELECT original_file_name, source_row_count, bronze_row_count,
       bronze_corrupt_row_count
FROM ea_dev.operations.upload_manifest
WHERE status = 'PROCESSED'
  AND source_row_count != bronze_row_count;
```

`source_row_count` comes from Python's CSV reader at validation; `bronze_row_count` from Spark's at load. A gap of one or two is a reader quirk on an odd file. A large gap means rows were lost.

### Alerting

The job fails when `summary["errors"] > 0`, which triggers the job's failure notification. **Rejections are silent** — see the deferred list.

---

## Design decisions

Decisions that look wrong without their reasoning.

### Bronze stores everything as STRING

Typed reads destroy the evidence you most need. A `contract_value` of `"N/A"` in a `DOUBLE` column becomes `NULL` — indistinguishable from a genuinely empty cell. As a string it survives to silver, which casts it, fails, and quarantines the row with a reason.

It also makes `mergeSchema` trivially safe: string meets string, so there are no type conflicts to reconcile.

**Do not add typed columns to bronze.** Cast in silver.

### An explicit read schema is required

Spark only populates `_corrupt_record` when it has a schema to check rows against. Without one, every column is a string and nothing can be malformed — so the corrupt-row threshold silently never fires. The schema is built per-file from the validated header, all `StringType`: it fixes the column count, it does not impose types.

This was found by integration test T9, which passed a file that was 50% malformed.

### Bronze deletes before writing

`_clear_previous_load` removes rows for the upload before writing. This is what makes the load re-runnable: if an earlier attempt died after writing but before recording `PROCESSED`, a retry would otherwise append the rows again and silently double the data.

### NULL vs empty array

`missing_columns`, `unexpected_columns` and `duplicate_columns`:

- `NULL` — not validated yet (written by registration)
- `[]` — validated, nothing found (written by validation)

A useful distinction. Do not "fix" the inconsistency. Downstream queries should use `size(missing_columns) > 0`, not `IS NOT NULL`.

### Content hashing, not just paths

The duplicate check compares SHA-256 hashes, not filenames. The common real duplicate is the same file re-sent under a new name (`projects_october_FINAL.csv`), which path-matching would miss entirely.

The hash is also re-checked at every stage, so a file replaced between validation and bronze is caught rather than loaded as if it had passed validation.

### Column mapping on bronze tables

`delta.columnMapping.mode = 'name'` lets bronze add, drop and rename columns as a metadata operation, and allows column names Parquet would otherwise reject.

**One-way door.** It cannot be disabled, and it raises the reader version so tools outside Databricks may not be able to read these tables. Accepted because bronze is an internal layer that grows columns by design.

### Discovery is stateless

`register_landing_files` scans every CSV in landing on every run and registers all of them. It keeps no record of what it has seen — registration's own duplicate check distinguishes new from known, and returns the existing `upload_id` without writing anything.

One source of truth instead of two that can disagree. The cost is a repeated scan, which motivates the archive item below.

### Databricks Jobs, not Auto Loader or DLT

Auto Loader deduplicates by *path* and tracks only processed/not-processed. It has no equivalent of content hashing, change detection, the status lifecycle, `uploaded_by`, or rejecting a file before it reaches bronze. This pipeline is case management for human-uploaded files, not high-throughput ingestion.

**DLT is the right tool for bronze → silver → gold** and should be considered when that work starts. It does not fit a manifest-driven state machine with per-file hashing and explicit claims.

### The 60-second file age check

A file being uploaded is visible on disk before it is complete. Hashing it mid-write records a truncated file, and the next stage then reports the file as "changed" — technically true, completely misleading.

`MIN_FILE_AGE_SECONDS = 60` skips recently-touched files until the next run. A `.done` marker file is the correct solution but needs cooperation from the uploader, which a person dragging a file into a UI will not give. Worth insisting on if an SFTP or automated feed is added.

---

## Deferred improvements

Not bugs. Each is correct for current usage, with a trigger for revisiting.

### Small, do before relying on this

| # | Item | Trigger | Effort |
|---|---|---|---|
| 1 | **`pipeline_run_id` never set.** Nothing links a manifest row to the run that produced it. Read the job run id from the Databricks context in the claim step. | Before the first serious incident | Very small |
| 2 | **No alerting on rejections.** The job succeeds when files are rejected, so nobody is told. Add a SQL alert on rejections in the last hour. | Before anyone relies on this | Small |
| 3 | **Archive processed files.** Landing grows forever and discovery rescans it every run. Move to an archive directory after `PROCESSED`, updating `stored_file_path`. Use dated subfolders to avoid name collisions. | Scan slows, or storage cost | Small |

### Medium

| # | Item | Trigger | Effort |
|---|---|---|---|
| 4 | **No cap on uploads per run.** `_uploads_with_status` collects everything pending. A large backlog could make one run overrun its 15-minute slot. Add `MAX_UPLOADS_PER_RUN` with `.limit()`. | Pending counts reach the hundreds | Two lines |
| 5 | **No retry limit.** `FAILED` is claimable, so an upload failing for a permanent reason retries every 15 minutes forever. Add a `retry_count` column and stop claiming past a threshold. | Same upload_id in errors run after run | One column |
| 6 | **Registration returns a bare string.** Callers wanting the hash or size must re-query the table. Return a `RegistrationOutcome` dataclass like the other stages. | A caller needs the metadata | Small, touches callers |
| 7 | **`upload_id` lookups slow as tables grow.** Random UUIDs defeat min/max file skipping, and both the manifest and bronze filter on them. `OPTIMIZE ... ZORDER BY (upload_id)` on a schedule. Do not partition — one folder per upload is the worst case. | Millions of rows, or loads feel slow | Very small |
| 8 | **Two Delta commits per stage.** Claim, then outcome. The claim is what prevents the race, so it cannot be removed. A batch version would claim all rows in one `UPDATE` and write outcomes in one `MERGE`. | Hundreds of files per run | Medium |

### Assumptions worth rechecking

| # | Item | Trigger |
|---|---|---|
| 9 | **`STALE_CLAIM_MINUTES = 30`** assumes no stage ever legitimately runs longer. Two runs could then process the same upload. | Any stage gets slower |
| 10 | **`MIN_FILE_AGE_SECONDS = 60`** assumes uploads never stall longer than that. | Slower uploads, or "file has changed" errors on untouched files |
| 11 | **Column normalisation is permissive** — `project.id`, `project-id` and `Project ID` all collapse to `project_id`. Two genuinely different source columns could become a false duplicate. | A puzzling duplicate is reported |
| 12 | **`reject_unexpected=False`** everywhere. Flip per dataset if an extra column ever causes real downstream harm. | A name collision in bronze, or data silently ignored |

---

## Development

### Tests

Pure-function unit tests run in CI on every push:

```bash
pytest tests/ -v
ruff check ea_pipeline/ tests/
```

They cover `normalise_column_name`, `read_csv_structure`, `validate_columns` and the result dataclasses — no Spark, no Databricks.

**Integration tests** (`notebooks/integration_tests.py`) need a live workspace and are **not** run by CI. Run them by hand after changing anything in the Spark paths. They cover idempotency, stale claim recovery, the corrupt-row threshold, schema evolution, multiline reconciliation and the status gate.

### Adding a dataset

1. Create the landing directory under `/Volumes/ea_dev/landing/uploads/`
2. Create the bronze table with the lineage columns and the two `TBLPROPERTIES` (see any existing bronze DDL)
3. Add entries to `DATASET_DIRECTORIES`, `BRONZE_TABLES` and `DATASET_CONTRACTS`

### Conventions

- Branch per change: `feature/`, `fix/`, `refactor/`, `docs/`
- `main` always works — merge only when tests pass
- Commit messages explain *why*; the diff shows *what*