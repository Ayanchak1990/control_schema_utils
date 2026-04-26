# Pipeline Implementation Standards
## Databricks + Delta Lake + SQL Server JDBC

This document defines what every pipeline in this framework must guarantee,
regardless of who built it. These are non-negotiable behavioural contracts.

---

## 1. Ingestion Pattern

**Rule:** Every pipeline must declare its load type in the control table. No pipeline decides its own mode at runtime.

- First run is always `full` — controlled via `last_watermark_value IS NULL`
- All subsequent runs are `incremental` — watermark-driven
- Load type flips from `full` to `incremental` only after a confirmed successful write
- Manual reset (set `last_watermark_value = NULL`) is the only way to trigger a re-full-load

---

## 2. Watermark Strategy

**Rule:** Watermark window must always be bounded — a defined start and end — captured before the query executes.

- `new_watermark` is captured from the source before the read, not derived from the data after
- Query window: `watermark_column > last_watermark AND watermark_column <= new_watermark`
- A configurable overlap (default: 30 minutes subtracted from `last_watermark`) must be applied to catch late-arriving records
- Watermark is only written back to the control table after a confirmed successful write — never before

---

## 3. Late Arriving Data

**Rule:** Every incremental pipeline must account for late-arriving records. Strict watermark-only logic is not sufficient.

- Apply a lookback overlap on every incremental run (configurable per table in control schema)
- For tables with known late data SLAs (e.g. source updates records up to 48 hours after creation), set `late_arrival_window` accordingly in the control table
- MERGE handles deduplication — overlapping records are safe to reprocess

---

## 4. Write Strategy

**Rule:** Write behaviour is determined by load type, not by the engineer's choice.

| Load Type | Write Mode | Reason |
|---|---|---|
| Full | Overwrite | Clean slate, schema refresh |
| Incremental | MERGE (upsert) | Idempotent, handles late data |

- Plain `append` is never used for incremental loads
- MERGE keys must be defined in the control table (`merge_keys` column) — never hardcoded in pipeline logic
- `overwriteSchema = true` is only permitted on full loads

---

## 5. Schema Evolution

**Rule:** Schema changes from the source must never cause silent failures or silent data corruption.

- A schema comparison step runs before every write
- **Additive changes** (new columns): allowed, Delta schema auto-evolution enabled, alert raised
- **Breaking changes** (type change, column removal): pipeline fails immediately with a clear error — no partial writes
- Schema version is logged in the audit table on every run
- No pipeline may proceed to write if a breaking schema change is detected

---

## 6. Data Validation

**Rule:** Data must be validated before it reaches the Delta table. Loading bad data is worse than a failed load.

Minimum validations required for every pipeline:

- Critical columns (defined in control table) must not exceed configured null threshold
- Duplicate check on merge keys before write — log count of duplicates found
- Row count check — if source returns 0 rows on an incremental run, raise a warning (not a failure) and skip write

**Rejection handling:**

- Records failing validation are written to a quarantine table, not dropped silently
- Quarantine table captures: pipeline_id, source_table, rejection_reason, rejected_at, raw_record
- Main pipeline continues after quarantine — rejection does not block the load unless threshold is breached

---

## 7. Error Handling & Retry

**Rule:** Retry behaviour must be config-driven. No pipeline retries indefinitely or fails without a documented reason.

- Max retry attempts: configurable per pipeline in control table (default: 3)
- Backoff strategy: exponential (1min → 2min → 4min)
- Retriable errors: network timeouts, JDBC connection drops, transient Spark errors
- Terminal errors: schema mismatch, authentication failure, merge key not found
- Terminal errors do not retry — they fail immediately and write error context to control table
- After max retries exhausted: status = `FAILED`, full error message persisted, pipeline moves to next table

---

## 8. Delete Handling

**Rule:** Every pipeline must have a defined delete strategy. Undefined = stale data accumulates forever.

Choose one per table (defined in control table via `delete_strategy` column):

| Strategy | When to Use |
|---|---|
| `soft_delete_flag` | Source has a `is_deleted` or `deleted_at` column |
| `full_reconciliation` | Small tables where full reload is acceptable |
| `ignore` | Source never deletes (append-only sources) |
| `cdc` | Source supports CDC (future) |

- Default is `ignore` — but this must be an explicit decision, not an omission
- `ignore` tables must be documented with justification in control table (`delete_notes` column)

---

## 9. Parallelism & Concurrency

**Rule:** Parallelism must be controlled — never unbounded.

- Max parallel tables per job run: configurable at job level (default: 5)
- Max JDBC connections per run: controlled via `numPartitions` in control table per table
- Tables are processed in priority order — `priority` column in control table (1 = highest)
- No pipeline may open more than its allocated JDBC connections regardless of cluster size

---

## 10. Audit & Lineage

**Rule:** Every run must leave a complete, queryable audit trail.

The following must be written to the audit table on every run (success or failure):

- `pipeline_id`
- `source_view`
- `target_table`
- `load_type`
- `watermark_from`
- `watermark_to`
- `source_row_count`
- `written_row_count`
- `rejected_row_count`
- `schema_version`
- `run_start`
- `run_end`
- `status`
- `error_message`

Additionally, every Delta table must have these audit columns on every record:

- `_ingestion_ts` — when the record was loaded
- `_source_ts` — watermark column value from source
- `_pipeline_id` — which pipeline wrote it

---

## 11. Delta Table Standards

**Rule:** All Delta tables in this framework follow a consistent structure.

- Partitioning strategy defined in control table per table — never ad hoc
- `OPTIMIZE` and `ZORDER` strategy documented per table for query patterns
- `VACUUM` retention: minimum 7 days (never below Delta default)
- Small file problem: `OPTIMIZE` scheduled after every full load, and after incremental loads exceeding 1M rows
- Table properties (`delta.autoOptimize.optimizeWrite`, `delta.autoOptimize.autoCompact`) enabled by default

---

## 12. Backfill & Reprocessing

**Rule:** Reprocessing history must be a controlled, documented operation — never an ad hoc fix.

- Backfill is triggered via control table — set `backfill_from` and `backfill_to` date range
- Backfill runs in bounded time windows — never open-ended
- Backfill uses MERGE — same idempotency guarantees as incremental
- Backfill must be run on a separate job cluster — never on the same cluster as live incremental runs
- Backfill status tracked separately in audit table (`load_type = 'backfill'`)

---

## Control Table — Required Columns

These columns are mandatory. No pipeline may be registered without them.

| Column | Type | Purpose |
|---|---|---|
| `pipeline_id` | VARCHAR | Unique identifier |
| `source_schema` | VARCHAR | SQL Server schema |
| `source_view` | VARCHAR | View name |
| `target_schema` | VARCHAR | Delta schema |
| `target_table` | VARCHAR | Delta table name |
| `watermark_column` | VARCHAR | Column used for incremental tracking |
| `last_watermark_value` | TIMESTAMP | NULL = full load on next run |
| `load_type` | VARCHAR | `full` / `incremental` / `backfill` |
| `merge_keys` | VARCHAR | Comma-separated primary keys |
| `delete_strategy` | VARCHAR | `soft_delete_flag` / `full_reconciliation` / `ignore` |
| `late_arrival_window_mins` | INT | Lookback overlap in minutes |
| `batch_size` | INT | JDBC fetch size |
| `num_partitions` | INT | JDBC parallel partitions |
| `priority` | INT | Execution priority (1 = highest) |
| `max_retries` | INT | Max retry attempts |
| `null_threshold_pct` | FLOAT | Max allowed null % on critical columns |
| `critical_columns` | VARCHAR | Comma-separated columns to validate |
| `is_active` | BOOLEAN | Enable / disable pipeline |
| `status` | VARCHAR | `PENDING` / `RUNNING` / `SUCCESS` / `FAILED` |
| `last_run_start` | TIMESTAMP | Audit |
| `last_run_end` | TIMESTAMP | Audit |
| `last_run_record_count` | INT | Audit |
| `error_message` | TEXT | Last failure reason |
| `delete_notes` | TEXT | Justification if delete_strategy = ignore |
