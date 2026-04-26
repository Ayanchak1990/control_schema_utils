# quper_control_schema_utils

A shared Python library that handles all read/write operations against a Databricks Unity Catalog control schema (Delta tables). Any ingestion pipeline that uses a compatible control schema can import this library and get full observability, audit logging, watermark management, data quality checks, and schema registry functionality — with zero boilerplate.

---
## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [ControlSchemaClient](#controlschemaclient)
  - [Initialisation](#initialisation)
  - [Config Readers](#config-readers)
  - [Watermark Management](#watermark-management)
  - [Audit Logging](#audit-logging)
  - [Error Logging](#error-logging)
  - [Metrics Logging](#metrics-logging)
  - [Schema Registry](#schema-registry)
  - [Schema Drift](#schema-drift)
  - [Data Quality](#data-quality)
- [Data Models](#data-models)
- [Exceptions](#exceptions)
- [Constants](#constants)
- [Control Schema Tables](#control-schema-tables)
- [Build](#build)
- [Tests](#tests)

---

## Overview

`quper_control_schema_utils` wraps a set of 12 Delta control tables that govern how Databricks ingestion pipelines behave. Instead of writing bespoke Spark SQL in every pipeline notebook, you instantiate a single `ControlSchemaClient` and call its methods. The client internally delegates to focused modules:

| Module | Responsibility |
|---|---|
| `config_reader` | Reads source objects, pipeline config, metadata columns, DQ rules, connection config |
| `watermark_manager` | Gets and updates the high-watermark for incremental loads |
| `audit_logger` | Inserts run-start / run-end rows into the audit table |
| `error_logger` | Inserts structured error rows |
| `metrics_logger` | Inserts per-object ingestion metrics |
| `schema_registry` | Registers schemas and detects column-level drift |
| `drift_logger` | Logs drift events and evolves target tables via `ALTER TABLE` |
| `dq_runner` | Evaluates DQ rules against a DataFrame and logs results |

---

## Installation

**From wheel file:**

```bash
pip install quper_control_schema_utils-0.1.0-py3-none-any.whl
```

**Via Databricks job YAML:**

```yaml
libraries:
  - whl: /Volumes/dev/shared/libs/quper_control_schema_utils-0.1.0-py3-none-any.whl
```

---

## Quick Start

```python
from quper_control_schema_utils import ControlSchemaClient, DQFailureError, SchemaDriftHaltError
import traceback

client = ControlSchemaClient(
    spark=spark,
    catalog="dev",
    control_schema="deltek_cdm",
    pipeline_id="raw_employee_pipeline",
    environment="dev"
)

run_id = client.start_run(triggered_by="schedule")

try:
    objects = client.get_source_objects()
    config  = client.get_pipeline_config()

    success_count = 0
    failed_count  = 0

    for obj in objects:
        try:
            watermark  = client.get_watermark(obj.object_id)
            meta_cols  = client.get_metadata_columns(obj.load_type)
            dq_rules   = client.get_dq_rules(obj.object_id)
            connection = client.get_connection_config()

            # --- your extraction + transform + write logic here ---

            # Schema drift detection
            registered = client.get_registered_schema(obj.object_id)
            if not registered:
                client.register_schema(obj.object_id, df.schema, obj.primary_key.split(","))
            else:
                drifts = client.detect_drift(
                    obj.object_id, obj.pipeline_id, obj.source_object,
                    run_id, registered, df.schema
                )
                if drifts:
                    client.log_drift(drifts)
                    if any(d.action_taken == "HALTED" for d in drifts):
                        raise SchemaDriftHaltError(f"TYPE_CHANGE detected in {obj.source_object}")
                    client.evolve_table(
                        obj.target_catalog, obj.target_schema, obj.target_table, drifts
                    )

            # DQ checks
            dq_results = client.run_dq_checks(run_id, obj.object_id, df, dq_rules)
            client.log_dq_results(dq_results)

            # Metrics + watermark
            client.log_metrics(metrics)
            client.update_watermark(obj.object_id, run_id, metrics.rows_inserted)
            success_count += 1

        except Exception as e:
            client.log_error(
                run_id, obj.object_id, obj.source_object,
                "UNKNOWN", str(e), traceback.format_exc(), retry_attempt=1
            )
            failed_count += 1

    status = "SUCCESS" if failed_count == 0 else ("FAILED" if success_count == 0 else "PARTIAL")
    client.end_run(run_id, status, len(objects), success_count, failed_count)

except Exception as e:
    client.end_run(run_id, "FAILED", 0, 0, 0)
    raise
```

---

## ControlSchemaClient

The single entry point for all control schema operations.

### Initialisation

```python
ControlSchemaClient(
    spark: SparkSession,
    catalog: str,
    control_schema: str,
    pipeline_id: str,
    environment: str
)
```

| Parameter | Type | Description |
|---|---|---|
| `spark` | `SparkSession` | Active Spark session |
| `catalog` | `str` | Unity Catalog name, e.g. `"dev"` |
| `control_schema` | `str` | Control schema name, e.g. `"deltek_cdm"` |
| `pipeline_id` | `str` | Pipeline identifier, e.g. `"raw_employee_pipeline"` |
| `environment` | `str` | Deployment environment: `"dev"`, `"qa"`, or `"prod"` |

---

### Config Readers

#### `get_source_objects() -> List[SourceObject]`

Returns all active source objects for the pipeline, ordered by `load_order ASC`. Each object represents one table/entity to be ingested.

```python
objects = client.get_source_objects()
for obj in objects:
    print(obj.source_object, obj.load_type, obj.primary_key)
```

Raises `TableNotFoundError` if `source_object_config` does not exist, `PipelineConfigError` for other failures.

---

#### `get_pipeline_config() -> Dict[str, str]`

Returns all active pipeline config entries as a flat `{config_key: config_value}` dictionary. Only rows with `active_flag=true` are returned.

```python
config = client.get_pipeline_config()
batch_size = int(config.get("batch_size", "1000"))
```

Raises `TableNotFoundError` if `pipeline_config` does not exist, `PipelineConfigError` otherwise.

---

#### `get_metadata_columns(load_type: str) -> List[MetadataColumn]`

Returns metadata columns to inject into the DataFrame for the given `load_type`. Lookup order:
1. Rows matching `pipeline_id` with `active_flag=true`
2. Falls back to `pipeline_id = 'default'` if none found

Filtered by `applies_to IN ('all', load_type)` and ordered by `column_order ASC`.

```python
meta_cols = client.get_metadata_columns("full_load")
for col in meta_cols:
    print(col.column_name, col.data_type, col.computation)
```

Raises `TableNotFoundError` if `metadata_column_config` does not exist.

---

#### `get_dq_rules(object_id: str) -> List[DQRule]`

Returns all active DQ rules configured for the given `object_id` and the current `pipeline_id`.

```python
rules = client.get_dq_rules("emp_001")
```

Raises `TableNotFoundError` if `dq_rule_config` does not exist.

---

#### `get_connection_config() -> ConnectionConfig`

Returns the active JDBC connection config for the pipeline. Contains Databricks secret scope references for credentials — never raw passwords.

```python
conn = client.get_connection_config()
print(conn.jdbc_url_secret_key, conn.driver_class)
```

Raises `TableNotFoundError` if `connection_config` does not exist, `ConnectionConfigError` if no active config is found.

---

### Watermark Management

#### `get_watermark(object_id: str) -> Optional[WatermarkEntry]`

Returns the most recent watermark for an object. Returns `None` on first run (no row yet), which signals a full load should be performed. A `WatermarkEntry` with `last_run_ts=None` also means first run.

```python
wm = client.get_watermark(obj.object_id)
if wm is None or wm.last_run_ts is None:
    # perform full load
else:
    # perform incremental load from wm.last_run_ts
```

Raises `WatermarkError` on unexpected query failure.

---

#### `update_watermark(object_id: str, run_id: str, rows_loaded: int) -> None`

Performs a `MERGE INTO watermark_control` on `(object_id, pipeline_id)`. Sets `last_run_ts` to the current timestamp, `last_run_id` to `run_id`, and `rows_last_loaded` to the supplied count.

**Only call this after a confirmed successful write to the target table.**

```python
client.update_watermark(obj.object_id, run_id, metrics.rows_inserted)
```

Raises `WatermarkError` if the MERGE fails.

---

### Audit Logging

#### `start_run(triggered_by: str = "manual", job_id: Optional[str] = None, job_run_id: Optional[str] = None) -> str`

Inserts a row into `job_run_audit` with `status='RUNNING'` and returns the generated `run_id` (UUID string). Call this once at the very start of the pipeline.

```python
run_id = client.start_run(triggered_by="schedule", job_id="12345")
```

| Parameter | Type | Description |
|---|---|---|
| `triggered_by` | `str` | `"schedule"`, `"manual"`, or `"api"` |
| `job_id` | `Optional[str]` | Databricks job ID |
| `job_run_id` | `Optional[str]` | Databricks job run ID |

Returns `run_id` (UUID string). Raises `AuditLogError` if the INSERT fails.

---

#### `end_run(run_id: str, status: str, total_objects: int, success_objects: int, failed_objects: int) -> None`

Updates the audit row for `run_id` with the final `status`, `end_time`, `duration_seconds`, and object counts. This method swallows exceptions internally — it will never raise, even if the MERGE fails.

```python
client.end_run(run_id, "SUCCESS", total_objects=5, success_objects=5, failed_objects=0)
# or for mixed results:
client.end_run(run_id, "PARTIAL", total_objects=5, success_objects=3, failed_objects=2)
```

| `status` value | Meaning |
|---|---|
| `"SUCCESS"` | All objects processed successfully |
| `"PARTIAL"` | Some succeeded, some failed |
| `"FAILED"` | All objects failed |

---

### Error Logging

#### `log_error(run_id, object_id, source_object, error_type, error_message, stack_trace, retry_attempt) -> None`

Inserts a structured error row into `job_run_error`. This method swallows exceptions — logging a failure must never crash the pipeline.

```python
client.log_error(
    run_id=run_id,
    object_id=obj.object_id,
    source_object=obj.source_object,
    error_type="JDBC_ERROR",
    error_message=str(e),
    stack_trace=traceback.format_exc(),
    retry_attempt=1
)
```

| `error_type` values | |
|---|---|
| `"JDBC_ERROR"` | JDBC connection or query failure |
| `"SCHEMA_MISMATCH"` | Schema incompatibility |
| `"MERGE_ERROR"` | Delta MERGE failure |
| `"VALIDATION_ERROR"` | Data validation failure |
| `"DQ_ERROR"` | Data quality rule failure |
| `"UNKNOWN"` | Unclassified error |

---

### Metrics Logging

#### `log_metrics(metrics: IngestionMetrics) -> None`

Inserts one row into `ingestion_metrics` for the given object. Must never raise.

```python
from quper_control_schema_utils.models import IngestionMetrics
import uuid
from datetime import datetime

metrics = IngestionMetrics(
    metric_id=str(uuid.uuid4()),
    run_id=run_id,
    pipeline_id="raw_employee_pipeline",
    object_id=obj.object_id,
    source_object=obj.source_object,
    target_table=obj.target_table,
    load_type=obj.load_type,
    write_mode=obj.write_mode,
    rows_read=10000,
    rows_inserted=9800,
    rows_updated=150,
    rows_deleted=50,
    rows_rejected=0,
    duration_seconds=45,
    status="SUCCESS",
    metric_ts=datetime.utcnow(),
    environment="dev"
)
client.log_metrics(metrics)
```

---

### Schema Registry

#### `get_registered_schema(object_id: str) -> List[SchemaColumn]`

Returns all active columns registered for the object. Returns an empty list on the first run (no schema registered yet).

```python
registered = client.get_registered_schema(obj.object_id)
if not registered:
    # first run — register the schema
```

Raises `SchemaRegistryError` on unexpected failure.

---

#### `register_schema(object_id: str, df_schema: StructType, primary_key_cols: List[str]) -> None`

Inserts one row per column from the PySpark `StructType` into `object_schema_registry`. Only call when `get_registered_schema` returns an empty list.

```python
client.register_schema(
    object_id=obj.object_id,
    df_schema=df.schema,
    primary_key_cols=obj.primary_key.split(",")
)
```

Raises `SchemaRegistryError` if the INSERT fails.

---

#### `detect_drift(object_id, pipeline_id, source_object, run_id, registered, live_schema) -> List[SchemaDrift]`

Pure comparison — no Spark I/O. Compares the previously registered columns against the current live `StructType` and returns a list of `SchemaDrift` objects describing what changed.

```python
drifts = client.detect_drift(
    object_id=obj.object_id,
    pipeline_id=obj.pipeline_id,
    source_object=obj.source_object,
    run_id=run_id,
    registered=registered,
    live_schema=df.schema
)
```

Detected drift types:

| Drift Type | Meaning |
|---|---|
| `NEW_COLUMN` | Column present in live schema but not in registry |
| `DROPPED_COLUMN` | Column in registry but missing from live schema |
| `TYPE_CHANGE` | Column exists in both but data type changed |
| `NULLABLE_CHANGE` | Column exists in both but nullability changed |

---

### Schema Drift

#### `log_drift(drifts: List[SchemaDrift]) -> None`

Inserts all drift rows into `schema_drift_log`. Must never raise.

```python
if drifts:
    client.log_drift(drifts)
```

---

#### `evolve_table(target_catalog, target_schema, target_table, new_columns) -> None`

Issues `ALTER TABLE ... ADD COLUMN` for each `NEW_COLUMN` drift. Only call this when the `schema_drift_action` pipeline config is set to `"EVOLVE"`. If any `TYPE_CHANGE` drift is present in `new_columns`, raises `SchemaDriftHaltError` immediately.

```python
client.evolve_table(
    target_catalog=obj.target_catalog,
    target_schema=obj.target_schema,
    target_table=obj.target_table,
    new_columns=drifts
)
```

Raises `SchemaDriftHaltError` if `TYPE_CHANGE` drifts are present.

---

### Data Quality

#### `run_dq_checks(run_id, object_id, df, rules) -> List[DQResult]`

Evaluates all supplied DQ rules against the DataFrame. After running all rules, if any rule with `severity="ERROR"` has `status="FAILED"`, raises `DQFailureError`.

```python
dq_results = client.run_dq_checks(
    run_id=run_id,
    object_id=obj.object_id,
    df=df,
    rules=dq_rules
)
```

Raises `DQFailureError` if any `ERROR`-severity rule fails.

---

#### `log_dq_results(results: List[DQResult]) -> None`

Inserts all `DQResult` rows into `dq_check_results`. Must never raise.

```python
client.log_dq_results(dq_results)
```

---

## Data Models

All models are plain Python `dataclass` instances. No Spark dependency — they are transport objects only.

### `SourceObject`
Maps to `source_object_config`. Describes one ingestion unit (a source table/entity).

| Field | Type | Description |
|---|---|---|
| `object_id` | `str` | Unique object identifier |
| `pipeline_id` | `str` | Parent pipeline |
| `source_system` | `str` | Source system name |
| `source_schema` | `str` | Source schema/database |
| `source_object` | `str` | Source table name |
| `target_catalog` | `str` | Target Unity Catalog |
| `target_schema` | `str` | Target schema |
| `target_table` | `str` | Target Delta table |
| `staging_table` | `Optional[str]` | Staging table if used |
| `load_type` | `str` | e.g. `full_load`, `hash_incremental` |
| `write_mode` | `str` | e.g. `overwrite`, `merge` |
| `primary_key` | `str` | Comma-separated PK columns |
| `watermark_column` | `Optional[str]` | Column used for incremental loads |
| `hash_columns` | `Optional[str]` | Columns used for hash-based change detection |
| `active_flag` | `bool` | Whether this object is active |
| `load_order` | `int` | Processing sequence |

### `WatermarkEntry`
Maps to `watermark_control`. Tracks incremental load state per object.

| Field | Type | Description |
|---|---|---|
| `object_id` | `str` | Object identifier |
| `pipeline_id` | `str` | Pipeline identifier |
| `last_run_ts` | `Optional[datetime]` | Timestamp of last successful run |
| `last_run_id` | `Optional[str]` | Run ID of last successful run |
| `rows_last_loaded` | `Optional[int]` | Row count from last run |
| `updated_at` | `datetime` | Last update timestamp |

### `IngestionMetrics`
Maps to `ingestion_metrics`. One row per object per run.

| Field | Type | Description |
|---|---|---|
| `metric_id` | `str` | UUID |
| `run_id` | `str` | Parent run ID |
| `rows_read` | `Optional[int]` | Rows read from source |
| `rows_inserted` | `Optional[int]` | New rows written |
| `rows_updated` | `Optional[int]` | Updated rows |
| `rows_deleted` | `Optional[int]` | Deleted rows |
| `rows_rejected` | `Optional[int]` | Rejected/skipped rows |
| `duration_seconds` | `Optional[int]` | Object processing time |
| `status` | `str` | `SUCCESS` / `FAILED` |

### `SchemaColumn`
Maps to `object_schema_registry`. One row per registered column.

| Field | Type | Description |
|---|---|---|
| `column_name` | `str` | Column name |
| `data_type` | `str` | Spark data type string |
| `is_nullable` | `bool` | Nullability |
| `is_primary_key` | `bool` | Whether this is a PK column |
| `column_order` | `int` | Column position |
| `active_flag` | `bool` | Whether currently active |

### `SchemaDrift`
Maps to `schema_drift_log`. One row per detected drift event.

| Field | Type | Description |
|---|---|---|
| `drift_type` | `str` | `NEW_COLUMN`, `DROPPED_COLUMN`, `TYPE_CHANGE`, `NULLABLE_CHANGE` |
| `column_name` | `str` | Affected column |
| `old_value` | `Optional[str]` | Previous type/nullability |
| `new_value` | `Optional[str]` | New type/nullability |
| `action_taken` | `str` | `EVOLVED`, `WARNED`, or `HALTED` |

### `DQRule`
Maps to `dq_rule_config`. Defines a single data quality check.

| Field | Type | Description |
|---|---|---|
| `rule_id` | `str` | Rule identifier |
| `rule_name` | `str` | Human-readable name |
| `rule_type` | `str` | Rule category (e.g. `not_null`, `unique`) |
| `column_name` | `Optional[str]` | Column the rule applies to |
| `rule_expression` | `str` | SQL expression for evaluation |
| `expected_value` | `Optional[str]` | Expected result value |
| `severity` | `str` | `ERROR` (halts on fail) or `WARNING` (continues) |

### `DQResult`
Maps to `dq_check_results`. Result of a single rule evaluation.

| Field | Type | Description |
|---|---|---|
| `rows_checked` | `Optional[int]` | Total rows evaluated |
| `rows_passed` | `Optional[int]` | Rows that passed |
| `rows_failed` | `Optional[int]` | Rows that failed |
| `pass_rate` | `Optional[float]` | Fraction of rows passing (0.0–1.0) |
| `status` | `str` | `PASSED`, `FAILED`, or `WARNING` |
| `action_taken` | `str` | `CONTINUED` or `HALTED` |

### `MetadataColumn`
Maps to `metadata_column_config`. Defines columns to inject into ingested DataFrames.

| Field | Type | Description |
|---|---|---|
| `column_name` | `str` | Column name to inject |
| `data_type` | `str` | Target data type |
| `applies_to` | `str` | `"all"` or a specific `load_type` |
| `computation` | `str` | Expression or constant to compute the column |
| `column_order` | `int` | Injection position |
| `is_merge_key` | `bool` | Whether this column is part of the merge key |

### `ConnectionConfig`
Maps to `connection_config`. Holds JDBC connection references via Databricks secret scope.

| Field | Type | Description |
|---|---|---|
| `source_system` | `str` | Source system name |
| `connection_name` | `str` | Connection label |
| `secret_scope` | `str` | Databricks secret scope name |
| `jdbc_url_secret_key` | `str` | Secret key for JDBC URL |
| `jdbc_user_secret_key` | `str` | Secret key for username |
| `jdbc_password_secret_key` | `str` | Secret key for password |
| `driver_class` | `str` | JDBC driver class |
| `extra_jdbc_options` | `Optional[str]` | Additional JDBC options |

---

## Exceptions

All exceptions inherit from `ControlSchemaError`, so you can catch the entire family with a single `except` clause.

```python
from quper_control_schema_utils.exceptions import ControlSchemaError
```

| Exception | Raised When |
|---|---|
| `ControlSchemaError` | Base class for all library exceptions |
| `TableNotFoundError` | A required control schema Delta table does not exist |
| `PipelineConfigError` | Required pipeline config is missing or a query fails |
| `WatermarkError` | Watermark read or MERGE fails |
| `SchemaRegistryError` | Schema registry read or INSERT fails |
| `DQFailureError` | One or more DQ rules with `severity=ERROR` fail |
| `SchemaDriftHaltError` | A `TYPE_CHANGE` drift is detected — requires manual review |
| `AuditLogError` | Audit log INSERT fails critically |
| `ConnectionConfigError` | Connection config is missing or inactive for the pipeline |

**Catching all library errors:**

```python
from quper_control_schema_utils.exceptions import ControlSchemaError

try:
    objects = client.get_source_objects()
except ControlSchemaError as e:
    # handle any library error
    raise
```

---

## Constants

Use these constants instead of magic strings to avoid typos.

```python
from quper_control_schema_utils.models import (
    Status, DriftType, DriftAction, DQStatus, DQAction, ErrorType, Severity, TableName
)
```

| Class | Values |
|---|---|
| `Status` | `RUNNING`, `SUCCESS`, `FAILED`, `PARTIAL` |
| `DriftType` | `NEW_COLUMN`, `DROPPED_COLUMN`, `TYPE_CHANGE`, `NULLABLE_CHANGE` |
| `DriftAction` | `EVOLVED`, `WARNED`, `HALTED` |
| `DQStatus` | `PASSED`, `FAILED`, `WARNING` |
| `DQAction` | `CONTINUED`, `HALTED` |
| `ErrorType` | `JDBC_ERROR`, `SCHEMA_MISMATCH`, `MERGE_ERROR`, `VALIDATION_ERROR`, `DQ_ERROR`, `UNKNOWN` |
| `Severity` | `ERROR`, `WARNING` |
| `TableName` | `SOURCE_OBJECT_CONFIG`, `PIPELINE_CONFIG`, `METADATA_COLUMN_CONFIG`, `DQ_RULE_CONFIG`, `CONNECTION_CONFIG`, `WATERMARK_CONTROL`, `OBJECT_SCHEMA_REGISTRY`, `JOB_RUN_AUDIT`, `JOB_RUN_ERROR`, `INGESTION_METRICS`, `SCHEMA_DRIFT_LOG`, `DQ_CHECK_RESULTS` |

---

## Control Schema Tables

The library expects these 12 Delta tables to exist under `{catalog}.{control_schema}`:

| Table | Purpose |
|---|---|
| `source_object_config` | Defines each source entity and its ingestion settings |
| `pipeline_config` | Key-value configuration for each pipeline |
| `metadata_column_config` | Columns to inject into ingested DataFrames |
| `dq_rule_config` | Data quality rule definitions |
| `connection_config` | JDBC connection settings (via Databricks secrets) |
| `watermark_control` | High-watermark state for incremental loads |
| `object_schema_registry` | Column-level schema snapshot per object |
| `job_run_audit` | One row per pipeline run with start/end times and status |
| `job_run_error` | Structured error log per object per run |
| `ingestion_metrics` | Row counts and duration per object per run |
| `schema_drift_log` | History of all detected schema changes |
| `dq_check_results` | Detailed results of each DQ rule evaluation |

---

## Build

```bash
pip install build
python -m build --wheel
```

Output: `dist/quper_control_schema_utils-0.1.0-py3-none-any.whl`

---

## Tests

```bash
python -m pytest tests/ -v
```

Test modules:

| File | Covers |
|---|---|
| `tests/test_models.py` | Dataclass instantiation and constant values |
| `tests/test_config_reader.py` | Config reader functions with mocked Spark |
| `tests/test_audit_logger.py` | Audit log start/end behaviour |
| `tests/test_watermark_manager.py` | Watermark get/update logic |
