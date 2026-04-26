# Code Review — Anomalies Against Coding & Implementation Standards

Reviewed against `context/coding_standards.md` and `context/implementation_standards.md`.

---

## Critical

### 1. Watermark is wall-clock time, not a bounded source window
**File:** `quper_control_schema_utils/watermark_manager.py` — `update_watermark`

`last_run_ts` is set to `current_timestamp()` (Spark wall clock). The implementation standard (§2) requires:
- `new_watermark` captured from the source **before** the query executes
- Query window bounded as `watermark_column > last_watermark AND watermark_column <= new_watermark`

Using wall clock instead of a source-derived watermark breaks late-arrival data handling and can silently miss records when Spark job runtime diverges from data timestamp boundaries. The `update_watermark` signature needs a `new_watermark` parameter passed in by the caller, captured before the JDBC read.

---

### 2. `register_schema` is not idempotent — plain INSERT, no MERGE
**File:** `quper_control_schema_utils/schema_registry.py` — `register_schema`

Uses `INSERT INTO` with no existence check. On a retry the function inserts duplicate column rows for the same `(object_id, pipeline_id, column_name)`. The only guard is a doc comment on the caller side. The coding standard is explicit: every operation must be safe to re-run. This must be a MERGE on `(object_id, pipeline_id, column_name)` — matched rows update `data_type`, `is_nullable`, `updated_at`; unmatched rows insert.

---

### 3. `evolve_table` silently swallows ALTER TABLE failures
**File:** `quper_control_schema_utils/drift_logger.py` — `evolve_table`, lines 109–117

An exception from `ALTER TABLE ADD COLUMN` is caught, logged as `WARNING`, and then execution continues to the next column. If the column cannot be added, the subsequent DataFrame write will either fail with a confusing schema error or silently omit the column. The coding standard says "never swallow exceptions silently — always capture, log, and re-raise or handle explicitly." A schema evolution failure is a terminal error (same class as schema mismatch) and must raise `SchemaDriftHaltError`.

---

### 4. `get_pipeline_config` returns empty dict silently when no config rows exist
**File:** `quper_control_schema_utils/config_reader.py` — `get_pipeline_config`

The docstring declares:
> Raises: PipelineConfigError: If the query fails **or no config found**.

But when the query succeeds and returns 0 rows the function returns `{}` without raising. Callers relying on the documented contract will receive a silent empty dict and fail later with an opaque `KeyError` rather than a meaningful `PipelineConfigError`. A row-count check after the query must raise if `len(rows) == 0`.

---

## High

### 5. `detect_drift` client method accepts a redundant `pipeline_id` parameter
**File:** `quper_control_schema_utils/client.py` — `detect_drift`, line 389

`detect_drift` is the only method on `ControlSchemaClient` that accepts `pipeline_id` as a parameter, even though `self.pipeline_id` is already bound on the client. Every other method uses `self.pipeline_id` internally. A caller can inadvertently pass a different `pipeline_id`, causing drift events to be logged under the wrong pipeline — a silent data integrity bug. The parameter should be removed and `self.pipeline_id` used directly, consistent with every other method.

---

### 6. `Status` constants are missing `PENDING`
**File:** `quper_control_schema_utils/models.py` — `Status` class

The implementation standard and control table DDL define the pipeline lifecycle as:
```
PENDING → RUNNING → SUCCESS / FAILED
```
`Status` only defines `RUNNING`, `SUCCESS`, `FAILED`, `PARTIAL`. `PENDING` is absent, making it impossible to represent the pre-run state using the library's own constants. `Status.PENDING = "PENDING"` must be added.

---

### 7. `detect_drift` hardcodes `action_taken = EVOLVED` before config is consulted
**File:** `quper_control_schema_utils/schema_registry.py` — `detect_drift`, line 218

NEW_COLUMN drifts are stamped `action_taken=DriftAction.EVOLVED` at detection time, before the caller has read the `schema_drift_action` pipeline config key. If the pipeline is configured to `WARN` rather than `EVOLVE`, `log_drift` will still persist `EVOLVED` as the recorded action even though no `ALTER TABLE` was executed — an audit trail lie. The detector should stamp `action_taken=DriftAction.WARNED` for NEW_COLUMN by default; the caller sets it to `EVOLVED` only after successfully calling `evolve_table`.

---

## Medium

### 8. `logger.info` emitted inside the DQ rule evaluation loop
**File:** `quper_control_schema_utils/dq_runner.py` — `run_dq_checks`, lines 144–147

An `INFO` log line is emitted for every DQ rule on every run. The coding standard is explicit: *"No log spam — don't log inside loops unless it's a failure."* With many rules across many objects this creates noise that buries real signals. Only `WARNING` / `ERROR` logs should be emitted per-rule (on failure); a single `INFO` summary at the end of all checks is sufficient for passing runs.

---

## Summary

| # | File | Issue | Severity |
|---|------|-------|----------|
| 1 | `watermark_manager.py` | Wall-clock watermark instead of bounded source window | Critical |
| 2 | `schema_registry.py:register_schema` | Plain INSERT — not idempotent on retry | Critical |
| 3 | `drift_logger.py:evolve_table` | ALTER TABLE failure silently swallowed | Critical |
| 4 | `config_reader.py:get_pipeline_config` | Silent empty dict return breaks documented contract | Critical |
| 5 | `client.py:detect_drift` | Redundant `pipeline_id` param risks silent pipeline mismatch | High |
| 6 | `models.py:Status` | `PENDING` constant missing | High |
| 7 | `schema_registry.py:detect_drift` | `EVOLVED` action stamped before config is checked | High |
| 8 | `dq_runner.py` | INFO log spam inside rule evaluation loop | Medium |
