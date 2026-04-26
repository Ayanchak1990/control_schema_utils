# quper_control_schema_utils — Full Project Reference

---

## 1. Project Overview

**Package:** `quper_control_schema_utils` **v0.1.0** — Author: Quper

A client-agnostic Python utility library distributed as a `.whl` file that handles all read/write operations against a Databricks Unity Catalog control schema (12 Delta tables). Any ingestion pipeline that targets a compatible control schema imports `ControlSchemaClient` and gets audit logging, watermark management, DQ evaluation, schema drift detection, metrics, and error tracking with zero boilerplate.

**Role:** Shared utility library (control schema layer). Not a pipeline. Not a Databricks bundle. It sits between the control schema Delta tables and any consumer ingestion pipeline. Consumer pipelines import and call it; this library never knows what they are ingesting or from where.

**Tech stack:**
- Python 3.9+
- PySpark — provided by Databricks cluster, not a pip dependency
- Delta Lake — provided by Databricks cluster, not a pip dependency
- Build toolchain: `setuptools>=61.0` + `wheel`, built with `python -m build --wheel`
- No external pip dependencies beyond Python stdlib

---

## 2. Folder Structure

```
quper_control_schema_utils.py/          ← repo root (the folder is named *.py — not a file)
│
├── quper_control_schema_utils/         ← installable Python package
│   ├── __init__.py                     ← public API: re-exports ControlSchemaClient, all models, all exceptions
│   ├── _internal.py                    ← private helper: _escape_sql() for SQL string sanitisation
│   ├── client.py                       ← ControlSchemaClient — single entry point wiring all modules
│   ├── models.py                       ← all dataclasses + Status/DriftType/DQStatus/etc. constants
│   ├── exceptions.py                   ← full exception hierarchy rooted at ControlSchemaError
│   ├── config_reader.py                ← reads source_object_config, pipeline_config, metadata_column_config, dq_rule_config, connection_config
│   ├── audit_logger.py                 ← log_run_start (INSERT) and log_run_end (MERGE) on job_run_audit
│   ├── error_logger.py                 ← log_error (INSERT) into job_run_error — never raises
│   ├── metrics_logger.py               ← log_metrics (INSERT) into ingestion_metrics — never raises
│   ├── watermark_manager.py            ← get_watermark (SELECT) and update_watermark (MERGE) on watermark_control
│   ├── schema_registry.py              ← get_registered_schema, register_schema, detect_drift (pure)
│   ├── drift_logger.py                 ← log_drift (INSERT into schema_drift_log), evolve_table (ALTER TABLE)
│   └── dq_runner.py                    ← run_dq_checks (DataFrame ops), log_dq_results (INSERT into dq_check_results)
│
├── tests/
│   ├── __init__.py                     ← empty init
│   ├── test_models.py                  ← unit tests: all 12 dataclass constructors and field assertions
│   ├── test_config_reader.py           ← unit tests: all config_reader functions with mock SparkSession
│   ├── test_audit_logger.py            ← unit tests: log_run_start (raises AuditLogError) and log_run_end (swallows)
│   └── test_watermark_manager.py       ← unit tests: get_watermark (None on miss) and update_watermark (MERGE)
│
├── context/
│   ├── coding_standards.md             ← production engineering rules (not shipped in wheel)
│   ├── implementation_standards.md     ← pipeline implementation contracts (not shipped in wheel)
│   └── prompt.md                       ← original Claude build prompt with authoritative DDL schemas
│
├── .claude/
│   └── settings.local.json             ← Claude Code local settings
│
├── pyproject.toml                      ← build-system: setuptools>=61.0, wheel
├── setup.cfg                           ← package metadata: name, version, author, python_requires>=3.9
├── setup.py                            ← one-liner: setup() — required by setuptools legacy mode
├── MANIFEST.in                         ← includes README.md, LICENSE, all .py and py.typed files
├── README.md                           ← usage guide and API walkthrough
├── .gitignore                          ← excludes __pycache__, dist/, build/, *.egg-info/, *.whl, .env
└── proj_details.md                     ← this file
```

---

## 3. Configuration

### Environment Variables

None. The library uses no environment variables.

### Bundle / DAB Variables

None. There is no Databricks Asset Bundle, no `databricks.yml`, no bundle configuration in this repo.

### Runtime Configuration (constructor arguments)

All configuration is passed at construction time by the consumer pipeline:

| Argument | Type | Example values | Description |
|---|---|---|---|
| `spark` | `SparkSession` | — | Active SparkSession from the cluster |
| `catalog` | `str` | `"dev"`, `"qa"`, `"prod"` | Unity Catalog name |
| `control_schema` | `str` | `"deltek_cdm"` | Control schema name inside the catalog |
| `pipeline_id` | `str` | `"raw_employee_pipeline"` | Identifies which pipeline's rows to read |
| `environment` | `str` | `"dev"`, `"qa"`, `"prod"` | Written into all audit/metrics/error rows |

### Hardcoded Values

| Value | Location | Notes |
|---|---|---|
| `pipeline_id='default'` | `config_reader.py:get_metadata_columns()` | Fallback lookup when no pipeline-specific metadata columns exist |
| `retry_attempt=1` | `client.py:log_error()` docstring example | The client always passes `retry_attempt=1` — retry logic is not implemented in the library |
| SQL keyword `LIMIT 1` | `config_reader.py:get_connection_config()` | Returns only the first active connection config row |
| `active_flag = true` | All config_reader queries | All reads filter to active rows only — inactive rows are never read |

---

## 4. Database Objects

### Schema

The library reads from and writes to a single control schema. The schema name and catalog are passed at runtime — nothing is hardcoded. The library assumes the schema already exists and all 12 tables are pre-created by the consumer.

**Canonical schema reference:** `{catalog}.{control_schema}` (e.g. `dev.deltek_cdm`)

---

### Tables

#### `source_object_config` — **Config (seeded)**
Defines one row per source object (view/table) that a pipeline must ingest.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `object_id` | STRING | NOT NULL | Unique identifier for this source object |
| `pipeline_id` | STRING | NOT NULL | Parent pipeline identifier |
| `source_system` | STRING | NOT NULL | Source system name (e.g. `sqlserver`) |
| `source_schema` | STRING | NOT NULL | Schema on the source system (e.g. `dbo`) |
| `source_object` | STRING | NOT NULL | View or table name on the source |
| `target_catalog` | STRING | NOT NULL | Unity Catalog for the target table |
| `target_schema` | STRING | NOT NULL | Target Delta schema |
| `target_table` | STRING | NOT NULL | Target Delta table name |
| `staging_table` | STRING | nullable | Staging table name if used; NULL otherwise |
| `load_type` | STRING | NOT NULL | `full_load` or `hash_incremental` |
| `write_mode` | STRING | NOT NULL | `overwrite` or `merge` |
| `primary_key` | STRING | NOT NULL | Comma-separated primary key column names |
| `watermark_column` | STRING | nullable | Watermark column for incremental loads |
| `hash_columns` | STRING | nullable | Comma-separated columns for hash-based change detection |
| `active_flag` | BOOLEAN | NOT NULL | Whether this object is active (read filter: `= true`) |
| `load_order` | INT | NOT NULL | Execution order (1 = first) |
| `created_at` | TIMESTAMP | NOT NULL | Audit |
| `updated_at` | TIMESTAMP | NOT NULL | Audit |
| `created_by` | STRING | NOT NULL | Audit |
| `updated_by` | STRING | NOT NULL | Audit |

---

#### `watermark_control` — **Runtime (written by pipeline)**
Tracks the high-watermark for each source object. Composite PK: `(object_id, pipeline_id)`.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `object_id` | STRING | NOT NULL | Source object identifier |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `last_run_ts` | TIMESTAMP | nullable | NULL = never run = trigger full load on next run |
| `last_run_id` | STRING | nullable | `run_id` of the last successful run |
| `rows_last_loaded` | BIGINT | nullable | Row count from last confirmed write |
| `updated_at` | TIMESTAMP | NOT NULL | Last updated timestamp |

---

#### `pipeline_config` — **Config (seeded)**
Key-value config store per pipeline. Consumed as `Dict[str, str]`.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `config_id` | STRING | NOT NULL | Row identifier |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `config_key` | STRING | NOT NULL | Config key (e.g. `max_retries`, `batch_size`) |
| `config_value` | STRING | NOT NULL | Config value (always string; consumer casts) |
| `description` | STRING | nullable | Human-readable description |
| `active_flag` | BOOLEAN | NOT NULL | Read filter: `= true` |
| `created_at` | TIMESTAMP | NOT NULL | Audit |
| `updated_at` | TIMESTAMP | NOT NULL | Audit |

---

#### `job_run_audit` — **Runtime (written by pipeline)**
One row per pipeline run. PARTITIONED BY (`environment`).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `run_id` | STRING | NOT NULL | UUID generated by `log_run_start` |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `job_id` | STRING | nullable | Databricks job ID (optional) |
| `job_run_id` | STRING | nullable | Databricks job run ID (optional) |
| `triggered_by` | STRING | NOT NULL | `schedule`, `manual`, or `api` |
| `start_time` | TIMESTAMP | NOT NULL | Set to `current_timestamp()` on INSERT |
| `end_time` | TIMESTAMP | nullable | NULL while RUNNING; set on MERGE at end |
| `duration_seconds` | BIGINT | nullable | Computed as `unix_timestamp(end) - unix_timestamp(start)` |
| `status` | STRING | NOT NULL | `RUNNING` → `SUCCESS` / `FAILED` / `PARTIAL` |
| `total_objects` | INT | nullable | NULL while running; set on MERGE |
| `success_objects` | INT | nullable | NULL while running; set on MERGE |
| `failed_objects` | INT | nullable | NULL while running; set on MERGE |
| `environment` | STRING | NOT NULL | Partition column |
| `created_at` | TIMESTAMP | NOT NULL | Set to `current_timestamp()` on INSERT |

---

#### `job_run_error` — **Runtime (written by pipeline)**
One row per error event. PARTITIONED BY (`environment`).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `error_id` | STRING | NOT NULL | UUID generated on each error |
| `run_id` | STRING | NOT NULL | Parent run identifier |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `object_id` | STRING | NOT NULL | Source object that failed |
| `source_object` | STRING | NOT NULL | Source object name |
| `error_type` | STRING | NOT NULL | `JDBC_ERROR`, `SCHEMA_MISMATCH`, `MERGE_ERROR`, `VALIDATION_ERROR`, `DQ_ERROR`, `UNKNOWN` |
| `error_message` | STRING | nullable | Human-readable error description |
| `stack_trace` | STRING | nullable | Full Python traceback string |
| `retry_attempt` | INT | NOT NULL | Retry attempt number (currently always `1`) |
| `error_ts` | TIMESTAMP | NOT NULL | Set to `current_timestamp()` on INSERT |
| `environment` | STRING | NOT NULL | Partition column |

---

#### `ingestion_metrics` — **Runtime (written by pipeline)**
One row per source object per run. PARTITIONED BY (`environment`).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `metric_id` | STRING | NOT NULL | UUID |
| `run_id` | STRING | NOT NULL | Parent run identifier |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `object_id` | STRING | NOT NULL | Source object identifier |
| `source_object` | STRING | NOT NULL | Source object name |
| `target_table` | STRING | NOT NULL | Target Delta table name |
| `load_type` | STRING | NOT NULL | `full_load` or `hash_incremental` |
| `write_mode` | STRING | NOT NULL | `overwrite` or `merge` |
| `rows_read` | BIGINT | nullable | Rows read from source |
| `rows_inserted` | BIGINT | nullable | Rows inserted into target |
| `rows_updated` | BIGINT | nullable | Rows updated in target |
| `rows_deleted` | BIGINT | nullable | Rows deleted from target |
| `rows_rejected` | BIGINT | nullable | Rows that failed DQ |
| `duration_seconds` | BIGINT | nullable | Wall-clock seconds for this object |
| `status` | STRING | NOT NULL | `SUCCESS` or `FAILED` |
| `metric_ts` | TIMESTAMP | NOT NULL | Set to `current_timestamp()` on INSERT |
| `environment` | STRING | NOT NULL | Partition column |

---

#### `object_schema_registry` — **Runtime (written once then updated)**
Column-level schema snapshot for each source object. One row per column.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `schema_id` | STRING | NOT NULL | UUID per column row |
| `object_id` | STRING | NOT NULL | Source object identifier |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `column_name` | STRING | NOT NULL | Column name |
| `data_type` | STRING | NOT NULL | PySpark data type string (e.g. `StringType()`) |
| `is_nullable` | BOOLEAN | NOT NULL | Whether the column is nullable |
| `is_primary_key` | BOOLEAN | NOT NULL | Whether the column is part of the primary key |
| `column_order` | INT | NOT NULL | 1-based column position |
| `registered_at` | TIMESTAMP | NOT NULL | When first registered |
| `updated_at` | TIMESTAMP | NOT NULL | Last updated |
| `active_flag` | BOOLEAN | NOT NULL | Read filter: `= true` |

---

#### `schema_drift_log` — **Runtime (written by pipeline)**
One row per detected drift event.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `drift_id` | STRING | NOT NULL | UUID |
| `run_id` | STRING | NOT NULL | Parent run identifier |
| `object_id` | STRING | NOT NULL | Source object identifier |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `source_object` | STRING | NOT NULL | Source object name |
| `column_name` | STRING | NOT NULL | Column where drift was detected |
| `drift_type` | STRING | NOT NULL | `NEW_COLUMN`, `DROPPED_COLUMN`, `TYPE_CHANGE`, `NULLABLE_CHANGE` |
| `old_value` | STRING | nullable | Previous data type or nullable value |
| `new_value` | STRING | nullable | New data type or nullable value |
| `action_taken` | STRING | NOT NULL | `EVOLVED`, `HALTED`, `WARNED`, `IGNORED` |
| `detected_at` | TIMESTAMP | NOT NULL | Set to `datetime.now(UTC)` in Python |
| `environment` | STRING | NOT NULL | Deployment environment |

---

#### `dq_rule_config` — **Config (seeded)**
DQ rules evaluated per source object per run.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `rule_id` | STRING | NOT NULL | UUID |
| `object_id` | STRING | NOT NULL | Source object this rule applies to |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `rule_name` | STRING | NOT NULL | Human-readable rule name |
| `rule_type` | STRING | NOT NULL | `NOT_NULL`, `UNIQUE`, `ROW_COUNT`, `VALUE_RANGE`, `REGEX`, `REFERENTIAL` |
| `column_name` | STRING | nullable | Target column (NULL for table-level rules) |
| `rule_expression` | STRING | NOT NULL | Expression evaluated by the runner |
| `expected_value` | STRING | nullable | Expected threshold value (used by `ROW_COUNT`) |
| `severity` | STRING | NOT NULL | `ERROR` (halt on fail) or `WARN` (continue on fail) |
| `active_flag` | BOOLEAN | NOT NULL | Read filter: `= true` |
| `created_at` | TIMESTAMP | NOT NULL | Audit |
| `updated_at` | TIMESTAMP | NOT NULL | Audit |

---

#### `dq_check_results` — **Runtime (written by pipeline)**
One row per rule per run. PARTITIONED BY (`environment`).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `result_id` | STRING | NOT NULL | UUID |
| `run_id` | STRING | NOT NULL | Parent run identifier |
| `object_id` | STRING | NOT NULL | Source object identifier |
| `rule_id` | STRING | NOT NULL | DQ rule identifier |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `rule_name` | STRING | NOT NULL | Rule name |
| `rule_type` | STRING | NOT NULL | Rule type |
| `column_name` | STRING | nullable | Column name |
| `rows_checked` | BIGINT | nullable | Total rows evaluated |
| `rows_passed` | BIGINT | nullable | Rows that passed |
| `rows_failed` | BIGINT | nullable | Rows that failed |
| `pass_rate` | DOUBLE | nullable | `rows_passed / rows_checked * 100.0` |
| `status` | STRING | NOT NULL | `PASSED`, `FAILED`, or `WARNING` |
| `action_taken` | STRING | NOT NULL | `CONTINUED` or `HALTED` |
| `checked_at` | TIMESTAMP | NOT NULL | Set to `current_timestamp()` on INSERT |
| `environment` | STRING | NOT NULL | Partition column |

---

#### `metadata_column_config` — **Config (seeded)**
Defines columns injected into every DataFrame before writing (e.g. `_ingestion_ts`).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `config_id` | STRING | NOT NULL | Row identifier |
| `pipeline_id` | STRING | NOT NULL | `default` = shared across all pipelines; pipeline name = override |
| `column_name` | STRING | NOT NULL | Column to inject (e.g. `_ingestion_ts`) |
| `data_type` | STRING | NOT NULL | SQL data type string |
| `applies_to` | STRING | NOT NULL | `all`, `hash_incremental`, or `full_load` |
| `computation` | STRING | NOT NULL | Expression to compute the column (e.g. `current_timestamp()`) |
| `column_order` | INT | NOT NULL | Injection order |
| `is_merge_key` | BOOLEAN | NOT NULL | Whether this column participates in MERGE key |
| `active_flag` | BOOLEAN | NOT NULL | Read filter: `= true` |
| `created_at` | TIMESTAMP | NOT NULL | Audit |

---

#### `connection_config` — **Config (seeded)**
JDBC connection details for a pipeline's source system. Credentials are stored as Databricks secret scope keys, never as raw values.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `connection_id` | STRING | NOT NULL | UUID |
| `pipeline_id` | STRING | NOT NULL | Pipeline identifier |
| `source_system` | STRING | NOT NULL | Source system name (e.g. `sqlserver`) |
| `connection_name` | STRING | NOT NULL | Human-readable connection name (e.g. `deltek_prod`) |
| `secret_scope` | STRING | NOT NULL | Databricks secret scope name |
| `jdbc_url_secret_key` | STRING | NOT NULL | Key within scope for the JDBC URL |
| `jdbc_user_secret_key` | STRING | NOT NULL | Key within scope for the username |
| `jdbc_password_secret_key` | STRING | NOT NULL | Key within scope for the password |
| `driver_class` | STRING | NOT NULL | JDBC driver class (e.g. `com.microsoft.sqlserver.jdbc.SQLServerDriver`) |
| `extra_jdbc_options` | STRING | nullable | Additional JDBC options string; NULL if none |
| `active_flag` | BOOLEAN | NOT NULL | Read filter: `LIMIT 1` on first active row |
| `created_at` | TIMESTAMP | NOT NULL | Audit |
| `updated_at` | TIMESTAMP | NOT NULL | Audit |

---

## 5. Data Models / Dataclasses

All defined in `models.py`. Plain Python `@dataclass` — no Spark dependency.

---

### `SourceObject`
Maps to `source_object_config`.

| Field | Type | Description |
|---|---|---|
| `object_id` | `str` | Unique object identifier |
| `pipeline_id` | `str` | Parent pipeline |
| `source_system` | `str` | Source system name |
| `source_type` | `str` | Source type (**added post-DDL — not in original schema; see Known Limitations**) |
| `source_schema` | `str` | Source schema |
| `source_object` | `str` | Source view/table name |
| `target_catalog` | `str` | Target Unity Catalog |
| `target_schema` | `str` | Target schema |
| `target_table` | `str` | Target Delta table |
| `staging_table` | `Optional[str]` | Staging table or None |
| `load_type` | `str` | `full_load` or `hash_incremental` |
| `write_mode` | `str` | `overwrite` or `merge` |
| `primary_key` | `str` | Comma-separated PKs |
| `watermark_column` | `Optional[str]` | Incremental watermark column |
| `hash_columns` | `Optional[str]` | Hash-based change detection columns |
| `active_flag` | `bool` | Active flag |
| `load_order` | `int` | Execution order |
| `created_at` | `datetime` | Audit timestamp |
| `updated_at` | `datetime` | Audit timestamp |
| `created_by` | `str` | Audit user |
| `updated_by` | `str` | Audit user |

---

### `WatermarkEntry`
Maps to `watermark_control`.

| Field | Type | Description |
|---|---|---|
| `object_id` | `str` | Source object identifier |
| `pipeline_id` | `str` | Pipeline identifier |
| `last_run_ts` | `Optional[datetime]` | None = first run / full load |
| `last_run_id` | `Optional[str]` | run_id from last successful run |
| `rows_last_loaded` | `Optional[int]` | Row count from last confirmed write |
| `updated_at` | `datetime` | Last updated |

---

### `PipelineConfigEntry`
Maps to `pipeline_config`. Note: `created_at` and `updated_at` present in DDL but absent from this dataclass.

| Field | Type | Description |
|---|---|---|
| `config_id` | `str` | Row identifier |
| `pipeline_id` | `str` | Pipeline identifier |
| `config_key` | `str` | Config key |
| `config_value` | `str` | Config value (always string) |
| `description` | `Optional[str]` | Human-readable description |
| `active_flag` | `bool` | Active flag |

---

### `RunAudit`
Maps to `job_run_audit`.

| Field | Type | Description |
|---|---|---|
| `run_id` | `str` | UUID |
| `pipeline_id` | `str` | Pipeline identifier |
| `job_id` | `Optional[str]` | Databricks job ID |
| `job_run_id` | `Optional[str]` | Databricks job run ID |
| `triggered_by` | `str` | `schedule`, `manual`, or `api` |
| `start_time` | `datetime` | Run start |
| `end_time` | `Optional[datetime]` | None while RUNNING |
| `duration_seconds` | `Optional[int]` | None while RUNNING |
| `status` | `str` | `RUNNING`, `SUCCESS`, `FAILED`, `PARTIAL` |
| `total_objects` | `Optional[int]` | None while RUNNING |
| `success_objects` | `Optional[int]` | None while RUNNING |
| `failed_objects` | `Optional[int]` | None while RUNNING |
| `environment` | `str` | Deployment environment |
| `created_at` | `datetime` | Row creation time |

---

### `RunError`
Maps to `job_run_error`.

| Field | Type | Description |
|---|---|---|
| `error_id` | `str` | UUID |
| `run_id` | `str` | Parent run identifier |
| `pipeline_id` | `str` | Pipeline identifier |
| `object_id` | `str` | Source object that failed |
| `source_object` | `str` | Source object name |
| `error_type` | `str` | Error classification |
| `error_message` | `Optional[str]` | Error description |
| `stack_trace` | `Optional[str]` | Full traceback |
| `retry_attempt` | `int` | Attempt number |
| `error_ts` | `datetime` | Error timestamp |
| `environment` | `str` | Deployment environment |

---

### `IngestionMetrics`
Maps to `ingestion_metrics`.

| Field | Type | Description |
|---|---|---|
| `metric_id` | `str` | UUID |
| `run_id` | `str` | Parent run identifier |
| `pipeline_id` | `str` | Pipeline identifier |
| `object_id` | `str` | Source object identifier |
| `source_object` | `str` | Source object name |
| `target_table` | `str` | Target Delta table |
| `load_type` | `str` | Load type |
| `write_mode` | `str` | Write mode |
| `rows_read` | `Optional[int]` | Rows read from source |
| `rows_inserted` | `Optional[int]` | Rows inserted |
| `rows_updated` | `Optional[int]` | Rows updated |
| `rows_deleted` | `Optional[int]` | Rows deleted |
| `rows_rejected` | `Optional[int]` | Rows rejected by DQ |
| `duration_seconds` | `Optional[int]` | Duration in seconds |
| `status` | `str` | `SUCCESS` or `FAILED` |
| `metric_ts` | `datetime` | Metric timestamp |
| `environment` | `str` | Deployment environment |

---

### `SchemaColumn`
Maps to `object_schema_registry`.

| Field | Type | Description |
|---|---|---|
| `schema_id` | `str` | UUID |
| `object_id` | `str` | Source object identifier |
| `pipeline_id` | `str` | Pipeline identifier |
| `column_name` | `str` | Column name |
| `data_type` | `str` | PySpark data type string |
| `is_nullable` | `bool` | Nullable flag |
| `is_primary_key` | `bool` | Primary key flag |
| `column_order` | `int` | 1-based column position |
| `registered_at` | `datetime` | First registration timestamp |
| `updated_at` | `datetime` | Last updated |
| `active_flag` | `bool` | Active flag |

---

### `SchemaDrift`
Maps to `schema_drift_log`.

| Field | Type | Description |
|---|---|---|
| `drift_id` | `str` | UUID |
| `run_id` | `str` | Parent run identifier |
| `object_id` | `str` | Source object identifier |
| `pipeline_id` | `str` | Pipeline identifier |
| `source_object` | `str` | Source object name |
| `column_name` | `str` | Column where drift occurred |
| `drift_type` | `str` | `NEW_COLUMN`, `DROPPED_COLUMN`, `TYPE_CHANGE`, `NULLABLE_CHANGE` |
| `old_value` | `Optional[str]` | Previous value |
| `new_value` | `Optional[str]` | New value |
| `action_taken` | `str` | `EVOLVED`, `HALTED`, `WARNED` |
| `detected_at` | `datetime` | Detection timestamp |
| `environment` | `str` | Deployment environment |

---

### `DQRule`
Maps to `dq_rule_config`. Note: `created_at` and `updated_at` present in DDL but absent from this dataclass.

| Field | Type | Description |
|---|---|---|
| `rule_id` | `str` | UUID |
| `object_id` | `str` | Source object this rule applies to |
| `pipeline_id` | `str` | Pipeline identifier |
| `rule_name` | `str` | Human-readable rule name |
| `rule_type` | `str` | `NOT_NULL`, `UNIQUE`, `ROW_COUNT`, `VALUE_RANGE`, `REGEX`, `REFERENTIAL` |
| `column_name` | `Optional[str]` | Target column (None for table-level rules) |
| `rule_expression` | `str` | Expression evaluated by the runner |
| `expected_value` | `Optional[str]` | Threshold value (used by `ROW_COUNT`) |
| `severity` | `str` | `ERROR` or `WARNING` |
| `active_flag` | `bool` | Active flag |

---

### `DQResult`
Maps to `dq_check_results`.

| Field | Type | Description |
|---|---|---|
| `result_id` | `str` | UUID |
| `run_id` | `str` | Parent run identifier |
| `object_id` | `str` | Source object identifier |
| `rule_id` | `str` | DQ rule identifier |
| `pipeline_id` | `str` | Pipeline identifier |
| `rule_name` | `str` | Rule name |
| `rule_type` | `str` | Rule type |
| `column_name` | `Optional[str]` | Column name |
| `rows_checked` | `Optional[int]` | Total rows evaluated |
| `rows_passed` | `Optional[int]` | Rows that passed |
| `rows_failed` | `Optional[int]` | Rows that failed |
| `pass_rate` | `Optional[float]` | `rows_passed / rows_checked * 100.0`, rounded to 2dp |
| `status` | `str` | `PASSED`, `FAILED`, or `WARNING` |
| `action_taken` | `str` | `CONTINUED` or `HALTED` |
| `checked_at` | `datetime` | Check timestamp |
| `environment` | `str` | Deployment environment |

---

### `MetadataColumn`
Maps to `metadata_column_config`. Note: `created_at` present in DDL but absent from this dataclass.

| Field | Type | Description |
|---|---|---|
| `config_id` | `str` | Row identifier |
| `pipeline_id` | `str` | `default` or specific pipeline |
| `column_name` | `str` | Column to inject |
| `data_type` | `str` | SQL data type |
| `applies_to` | `str` | `all`, `hash_incremental`, or `full_load` |
| `computation` | `str` | Expression (e.g. `current_timestamp()`) |
| `column_order` | `int` | Injection order |
| `is_merge_key` | `bool` | Participates in MERGE key |
| `active_flag` | `bool` | Active flag |

---

### `ConnectionConfig`
Maps to `connection_config`. Note: `created_at` and `updated_at` present in DDL but absent from this dataclass.

| Field | Type | Description |
|---|---|---|
| `connection_id` | `str` | UUID |
| `pipeline_id` | `str` | Pipeline identifier |
| `source_system` | `str` | Source system name |
| `connection_name` | `str` | Human-readable name |
| `secret_scope` | `str` | Databricks secret scope name |
| `jdbc_url_secret_key` | `str` | Key for JDBC URL in secret scope |
| `jdbc_user_secret_key` | `str` | Key for username in secret scope |
| `jdbc_password_secret_key` | `str` | Key for password in secret scope |
| `driver_class` | `str` | JDBC driver class name |
| `extra_jdbc_options` | `Optional[str]` | Additional JDBC options or None |
| `active_flag` | `bool` | Active flag |

---

### Constant Classes

All are plain classes with typed class attributes — not `enum.Enum` subclasses.

**`Status`**
| Attribute | Value |
|---|---|
| `RUNNING` | `"RUNNING"` |
| `SUCCESS` | `"SUCCESS"` |
| `FAILED` | `"FAILED"` |
| `PARTIAL` | `"PARTIAL"` |

**`DriftType`**
| Attribute | Value |
|---|---|
| `NEW_COLUMN` | `"NEW_COLUMN"` |
| `DROPPED_COLUMN` | `"DROPPED_COLUMN"` |
| `TYPE_CHANGE` | `"TYPE_CHANGE"` |
| `NULLABLE_CHANGE` | `"NULLABLE_CHANGE"` |

**`DriftAction`**
| Attribute | Value |
|---|---|
| `EVOLVED` | `"EVOLVED"` |
| `WARNED` | `"WARNED"` |
| `HALTED` | `"HALTED"` |

**`DQStatus`**
| Attribute | Value |
|---|---|
| `PASSED` | `"PASSED"` |
| `FAILED` | `"FAILED"` |
| `WARNING` | `"WARNING"` |

**`DQAction`**
| Attribute | Value |
|---|---|
| `CONTINUED` | `"CONTINUED"` |
| `HALTED` | `"HALTED"` |

**`ErrorType`**
| Attribute | Value |
|---|---|
| `JDBC_ERROR` | `"JDBC_ERROR"` |
| `SCHEMA_MISMATCH` | `"SCHEMA_MISMATCH"` |
| `MERGE_ERROR` | `"MERGE_ERROR"` |
| `VALIDATION_ERROR` | `"VALIDATION_ERROR"` |
| `DQ_ERROR` | `"DQ_ERROR"` |
| `UNKNOWN` | `"UNKNOWN"` |

**`Severity`**
| Attribute | Value |
|---|---|
| `ERROR` | `"ERROR"` |
| `WARNING` | `"WARNING"` |

**`TableName`**
| Attribute | Value |
|---|---|
| `SOURCE_OBJECT_CONFIG` | `"source_object_config"` |
| `PIPELINE_CONFIG` | `"pipeline_config"` |
| `METADATA_COLUMN_CONFIG` | `"metadata_column_config"` |
| `DQ_RULE_CONFIG` | `"dq_rule_config"` |
| `CONNECTION_CONFIG` | `"connection_config"` |
| `WATERMARK_CONTROL` | `"watermark_control"` |
| `OBJECT_SCHEMA_REGISTRY` | `"object_schema_registry"` |
| `JOB_RUN_AUDIT` | `"job_run_audit"` |
| `JOB_RUN_ERROR` | `"job_run_error"` |
| `INGESTION_METRICS` | `"ingestion_metrics"` |
| `SCHEMA_DRIFT_LOG` | `"schema_drift_log"` |
| `DQ_CHECK_RESULTS` | `"dq_check_results"` |

---

## 6. Functions and Methods

### `_internal.py`

#### `_escape_sql(value: str) -> str`
**Parameters:** `value` — raw string value  
**Returns:** string with all single quotes doubled (`'` → `''`)  
**Raises:** nothing  
**Purpose:** Prevents SQL injection in dynamically constructed `spark.sql()` strings. Used by every module that builds SQL. Not exported via `__init__.py`.

---

### `audit_logger.py`

#### `log_run_start(spark, catalog, control_schema, pipeline_id, environment, triggered_by, job_id, job_run_id) -> str`
**Parameters:**
- `spark: Any` — active SparkSession
- `catalog: str` — Unity Catalog name
- `control_schema: str` — control schema name
- `pipeline_id: str` — pipeline identifier
- `environment: str` — deployment environment
- `triggered_by: str` — `schedule`, `manual`, or `api`
- `job_id: Optional[str]` — Databricks job ID or None
- `job_run_id: Optional[str]` — Databricks job run ID or None

**Returns:** `str` — generated UUID `run_id`  
**Raises:** `AuditLogError` if the INSERT fails  
**Purpose:** INSERT one row into `job_run_audit` with `status='RUNNING'` and `start_time=current_timestamp()`. Generates `run_id = str(uuid.uuid4())`.

---

#### `log_run_end(spark, catalog, control_schema, run_id, status, total_objects, success_objects, failed_objects) -> None`
**Parameters:**
- `run_id: str` — the run to update
- `status: str` — `SUCCESS`, `FAILED`, or `PARTIAL`
- `total_objects: int`, `success_objects: int`, `failed_objects: int` — counts

**Returns:** `None`  
**Raises:** never — swallows all exceptions, logs WARNING  
**Purpose:** MERGE INTO `job_run_audit` on `run_id`. Sets `end_time=current_timestamp()`, computes `duration_seconds`, updates `status` and object counts.

---

### `config_reader.py`

All functions are module-level (not methods). Used internally by `ControlSchemaClient`.

#### `get_source_objects(spark, catalog, control_schema, pipeline_id) -> List[SourceObject]`
**Returns:** list of `SourceObject` ordered by `load_order ASC`, filtered by `active_flag=true`  
**Raises:** `TableNotFoundError` if `TABLE_OR_VIEW_NOT_FOUND` in exception; `PipelineConfigError` for any other failure

---

#### `get_pipeline_config(spark, catalog, control_schema, pipeline_id) -> Dict[str, str]`
**Returns:** `{config_key: config_value}` for all `active_flag=true` rows  
**Raises:** `TableNotFoundError`, `PipelineConfigError`

---

#### `get_metadata_columns(spark, catalog, control_schema, pipeline_id, load_type) -> List[MetadataColumn]`
**Returns:** list ordered by `column_order ASC`, filtered by `applies_to IN ('all', load_type)`. Falls back to `pipeline_id='default'` rows if pipeline-specific rows return empty.  
**Raises:** `TableNotFoundError`, `PipelineConfigError`

---

#### `get_dq_rules(spark, catalog, control_schema, pipeline_id, object_id) -> List[DQRule]`
**Returns:** all active DQ rules for the given `(pipeline_id, object_id)` pair  
**Raises:** `TableNotFoundError`, `PipelineConfigError`

---

#### `get_connection_config(spark, catalog, control_schema, pipeline_id) -> ConnectionConfig`
**Returns:** first active connection config row (`LIMIT 1`)  
**Raises:** `ConnectionConfigError` if no active row found; `TableNotFoundError` if table missing; `ConnectionConfigError` for any other failure

---

### `error_logger.py`

#### `log_error(spark, catalog, control_schema, run_id, pipeline_id, object_id, source_object, error_type, error_message, stack_trace, retry_attempt, environment) -> None`
**Returns:** `None`  
**Raises:** never — entire body is in `try/except`, logs WARNING on failure  
**Purpose:** INSERT one row into `job_run_error`. Generates `error_id = str(uuid.uuid4())`. Sanitises all string fields via `_escape_sql`.

---

### `metrics_logger.py`

#### `_sql_val(value: Any, is_string: bool = False) -> str`
**Returns:** `"NULL"` if None; `f"'{escaped}'"` if `is_string=True`; `str(value)` otherwise  
**Raises:** nothing  
**Purpose:** Private helper to convert Python values to SQL literals for the INSERT statement.

---

#### `log_metrics(spark, catalog, control_schema, metrics: IngestionMetrics) -> None`
**Returns:** `None`  
**Raises:** never — entire body in `try/except`, logs WARNING on failure  
**Purpose:** INSERT one row into `ingestion_metrics` using all fields from the `IngestionMetrics` dataclass.

---

### `watermark_manager.py`

#### `get_watermark(spark, catalog, control_schema, object_id, pipeline_id) -> Optional[WatermarkEntry]`
**Returns:** `WatermarkEntry` if row found; `None` if no row exists (first run)  
**Raises:** `WatermarkError` if query fails  
**Purpose:** SELECT from `watermark_control` WHERE `object_id=? AND pipeline_id=?`. `None` return signals full load to the consumer.

---

#### `update_watermark(spark, catalog, control_schema, object_id, pipeline_id, run_id, rows_loaded) -> None`
**Parameters:** `rows_loaded: int` — row count from the confirmed write  
**Returns:** `None`  
**Raises:** `WatermarkError` if MERGE fails  
**Purpose:** MERGE INTO `watermark_control` ON `(object_id, pipeline_id)`. WHEN MATCHED → update `last_run_ts`, `last_run_id`, `rows_last_loaded`, `updated_at`. WHEN NOT MATCHED → INSERT full row. Must only be called after a confirmed successful write.

---

### `schema_registry.py`

#### `get_registered_schema(spark, catalog, control_schema, object_id, pipeline_id) -> List[SchemaColumn]`
**Returns:** list of `SchemaColumn` ordered by `column_order ASC`, `active_flag=true` only. Empty list on first run.  
**Raises:** `SchemaRegistryError`

---

#### `register_schema(spark, catalog, control_schema, object_id, pipeline_id, df_schema, primary_key_cols) -> None`
**Parameters:** `df_schema: Any` — PySpark `StructType`; `primary_key_cols: List[str]`  
**Returns:** `None`  
**Raises:** `SchemaRegistryError` if INSERT fails  
**Purpose:** INSERT one row per column into `object_schema_registry`. Derives `data_type` from `str(field.dataType)`, `is_nullable` from `field.nullable`, `is_primary_key` from membership in `primary_key_cols`. Only call when `get_registered_schema` returns empty list.

---

#### `detect_drift(object_id, pipeline_id, source_object, run_id, registered, live_schema, environment) -> List[SchemaDrift]`
**Parameters:** `registered: List[SchemaColumn]`; `live_schema: Any` — PySpark `StructType`  
**Returns:** list of `SchemaDrift` objects (empty if no drift)  
**Raises:** nothing — pure Python comparison, no Spark  
**Drift rules:**
| Condition | `drift_type` | `action_taken` |
|---|---|---|
| In live, not in registered | `NEW_COLUMN` | `EVOLVED` |
| In registered, not in live | `DROPPED_COLUMN` | `WARNED` |
| Both, `data_type` differs | `TYPE_CHANGE` | `HALTED` |
| Both, `is_nullable` differs | `NULLABLE_CHANGE` | `WARNED` |

---

### `drift_logger.py`

#### `log_drift(spark, catalog, control_schema, drifts: List[SchemaDrift]) -> None`
**Returns:** `None`  
**Raises:** never — swallows all exceptions, logs WARNING  
**Purpose:** Bulk INSERT all `SchemaDrift` rows into `schema_drift_log`. No-ops if `drifts` is empty.

---

#### `evolve_table(spark, target_catalog, target_schema, target_table, new_columns: List[SchemaDrift]) -> None`
**Returns:** `None`  
**Raises:** `SchemaDriftHaltError` if any row in `new_columns` has `drift_type == TYPE_CHANGE` — raises before executing any ALTER  
**Purpose:** For each row where `drift_type == NEW_COLUMN` and `new_value` is not None: execute `ALTER TABLE {catalog}.{schema}.{table} ADD COLUMN \`{column_name}\` {data_type}`. Individual column failures are logged as WARNING but do not halt the loop.

---

### `dq_runner.py`

#### `run_dq_checks(spark, run_id, pipeline_id, object_id, df, rules, environment) -> List[DQResult]`
**Parameters:** `df: Any` — PySpark DataFrame; `rules: List[DQRule]`  
**Returns:** list of `DQResult` (one per rule)  
**Raises:** `DQFailureError` if any `severity=ERROR` rule has `status=FAILED`; propagates any exception thrown during rule evaluation (no swallowing)  
**Rule evaluation (DataFrame ops — no `spark.sql()`):**
| `rule_type` | Evaluation |
|---|---|
| `NOT_NULL` | `df.filter(col(rule.column_name).isNull()).count()` |
| `RANGE` | `df.filter(~expr(rule.rule_expression)).count()` |
| `ROW_COUNT` | `df.count()` compared against `int(rule.expected_value)` |
| anything else | raises `DQFailureError` immediately |

**Status logic:**
- `failed_count == 0` → `PASSED`, `CONTINUED`
- `failed_count > 0` and `severity == ERROR` → `FAILED`, `HALTED`
- `failed_count > 0` and `severity != ERROR` → `WARNING`, `CONTINUED`

---

#### `log_dq_results(spark, catalog, control_schema, results: List[DQResult]) -> None`
**Returns:** `None`  
**Raises:** never — swallows all exceptions, logs WARNING  
**Purpose:** Bulk INSERT all `DQResult` rows into `dq_check_results`. No-ops if `results` is empty.

---

### `client.py` — `ControlSchemaClient`

Single class. Constructor stores five attributes; all methods delegate to the module functions above.

#### `__init__(spark, catalog, control_schema, pipeline_id, environment) -> None`
Stores: `self.spark`, `self.catalog`, `self.control_schema`, `self.pipeline_id`, `self.environment`. No I/O.

#### Config readers (delegate to `config_reader.*`)
| Method | Returns | Raises |
|---|---|---|
| `get_source_objects()` | `List[SourceObject]` | `TableNotFoundError`, `PipelineConfigError` |
| `get_pipeline_config()` | `Dict[str, str]` | `TableNotFoundError`, `PipelineConfigError` |
| `get_metadata_columns(load_type: str)` | `List[MetadataColumn]` | `TableNotFoundError`, `PipelineConfigError` |
| `get_dq_rules(object_id: str)` | `List[DQRule]` | `TableNotFoundError`, `PipelineConfigError` |
| `get_connection_config()` | `ConnectionConfig` | `TableNotFoundError`, `ConnectionConfigError` |

#### Watermark (delegate to `watermark_manager.*`)
| Method | Returns | Raises |
|---|---|---|
| `get_watermark(object_id: str)` | `Optional[WatermarkEntry]` | `WatermarkError` |
| `update_watermark(object_id, run_id, rows_loaded)` | `None` | `WatermarkError` |

#### Audit (delegate to `audit_logger.*`)
| Method | Returns | Raises |
|---|---|---|
| `start_run(triggered_by='manual', job_id=None, job_run_id=None)` | `str` (run_id) | `AuditLogError` |
| `end_run(run_id, status, total_objects, success_objects, failed_objects)` | `None` | never |

#### Error / Metrics (delegate to `error_logger.*`, `metrics_logger.*`)
| Method | Returns | Raises |
|---|---|---|
| `log_error(run_id, object_id, source_object, error_type, error_message, stack_trace, retry_attempt)` | `None` | never |
| `log_metrics(metrics: IngestionMetrics)` | `None` | never |

#### Schema registry (delegate to `schema_registry.*`)
| Method | Returns | Raises |
|---|---|---|
| `get_registered_schema(object_id)` | `List[SchemaColumn]` | `SchemaRegistryError` |
| `register_schema(object_id, df_schema, primary_key_cols)` | `None` | `SchemaRegistryError` |
| `detect_drift(object_id, pipeline_id, source_object, run_id, registered, live_schema)` | `List[SchemaDrift]` | never (pure) |

#### Drift (delegate to `drift_logger.*`)
| Method | Returns | Raises |
|---|---|---|
| `log_drift(drifts)` | `None` | never |
| `evolve_table(target_catalog, target_schema, target_table, new_columns)` | `None` | `SchemaDriftHaltError` |

#### DQ (delegate to `dq_runner.*`)
| Method | Returns | Raises |
|---|---|---|
| `run_dq_checks(run_id, object_id, df, rules)` | `List[DQResult]` | `DQFailureError` |
| `log_dq_results(results)` | `None` | never |

---

## 7. Job / Task Definition

No Databricks Asset Bundle, no `databricks.yml`, no job definition, and no task graph exist in this repository. This is a pure Python library distributed as a `.whl` file. Job definitions live in the consumer pipeline projects that import this library.

**Build task (manual, not automated):**
```bash
python -m build --wheel
# Output: dist/quper_control_schema_utils-0.1.0-py3-none-any.whl
```

**Installation on Databricks cluster:**
```yaml
# In consumer bundle's databricks.yml:
libraries:
  - whl: /Volumes/dev/shared/libs/quper_control_schema_utils-0.1.0-py3-none-any.whl
```

---

## 8. Seed Data

No seed data scripts, migration files, or DDL files exist in this repository. The library assumes all 12 control schema tables are pre-created and pre-seeded by the consumer project before any pipeline run.

The canonical table schemas (the only data definitions in this repo) are in `context/prompt.md` — they are documentation only, not executable DDL.

Test files use mock `SparkSession` and contain inline sample data for unit testing only:

| Table mocked | Sample values used in tests |
|---|---|
| `source_object_config` | `object_id=obj-001`, `pipeline_id=pipe-001`, `source_system=sqlserver`, `source_object=vw_employees`, `load_type=full_load`, `write_mode=overwrite`, `primary_key=employee_id` |
| `watermark_control` | `object_id=obj-001`, `last_run_ts=None` (first run) or `now`, `rows_last_loaded=500` |
| `pipeline_config` | `config_key=max_retries` → `3`, `config_key=batch_size` → `10000` |
| `metadata_column_config` | `column_name=_ingestion_ts`, `computation=current_timestamp()`, `applies_to=all` |
| `dq_rule_config` | `rule_type=NOT_NULL`, `column_name=employee_id`, `severity=ERROR` |
| `connection_config` | `connection_name=deltek_prod`, `secret_scope=deltek-secrets`, `driver_class=com.microsoft.sqlserver.jdbc.SQLServerDriver` |

---

## 9. Exception Hierarchy

```
Exception
└── ControlSchemaError                  ← base; catch all library errors with one clause
    ├── TableNotFoundError              ← raised when TABLE_OR_VIEW_NOT_FOUND in Spark exception or AnalysisException
    │                                      raised by: get_source_objects, get_pipeline_config,
    │                                                 get_metadata_columns, get_dq_rules, get_connection_config,
    │                                                 get_watermark, get_registered_schema, register_schema
    │
    ├── PipelineConfigError             ← raised when any config_reader query fails for a non-table-missing reason
    │                                      raised by: get_source_objects, get_pipeline_config,
    │                                                 get_metadata_columns, get_dq_rules
    │
    ├── WatermarkError                  ← raised when watermark SELECT or MERGE fails
    │                                      raised by: get_watermark, update_watermark
    │
    ├── SchemaRegistryError             ← raised when schema registry SELECT or INSERT fails
    │                                      raised by: get_registered_schema, register_schema
    │
    ├── DQFailureError                  ← raised when any ERROR-severity DQ rule fails, or an unknown rule_type
    │                                      is encountered, or df.count() / df.filter() throws during evaluation
    │                                      raised by: run_dq_checks
    │
    ├── SchemaDriftHaltError            ← raised when TYPE_CHANGE drift is detected before evolve_table executes
    │                                      raised by: evolve_table
    │
    ├── AuditLogError                   ← raised when the run-start INSERT into job_run_audit fails
    │                                      raised by: log_run_start
    │
    └── ConnectionConfigError           ← raised when no active connection config row found,
                                           or when get_connection_config query fails for a non-table-missing reason
                                           raised by: get_connection_config
```

**Never-raise contract** (swallow + log WARNING): `log_run_end`, `log_error`, `log_metrics`, `log_drift`, `log_dq_results`

---

## 10. Known Limitations

### Dataclass / DDL Divergences

| Issue | Details |
|---|---|
| `source_type: str` added to `SourceObject` | Not present in the canonical DDL in `context/prompt.md`. Adding it means the field must exist in `source_object_config` or every `get_source_objects()` call will fail with a `KeyError` at `r["source_type"]`. Tests in `test_models.py` and `test_config_reader.py` do not include this field and will fail at runtime if run against the updated dataclass. |
| `PipelineConfigEntry` missing columns | `created_at` and `updated_at` exist in the DDL but are not in the dataclass. `SELECT *` in `get_pipeline_config` doesn't use this dataclass — it returns a plain `dict` — so no immediate breakage, but the dataclass is not a complete mirror of the table. |
| `DQRule` missing columns | `created_at` and `updated_at` exist in DDL but are not in the dataclass. Fields are silently ignored when constructing from `SELECT *` rows. |
| `MetadataColumn` missing column | `created_at` exists in DDL but is not in the dataclass. |
| `ConnectionConfig` missing columns | `created_at` and `updated_at` exist in DDL but are not in the dataclass. |
| `SchemaDrift.action_taken` gap | DDL allows `IGNORED` as a value for `action_taken`. `DriftAction` constant class has only `EVOLVED`, `WARNED`, `HALTED`. No code path ever produces `IGNORED`. |

### DQ Rule Type Coverage

`dq_runner.py` handles only `NOT_NULL`, `RANGE`, and `ROW_COUNT`. The DDL defines six rule types: `NOT_NULL`, `UNIQUE`, `ROW_COUNT`, `VALUE_RANGE`, `REGEX`, `REFERENTIAL`. Any rule with `rule_type` set to `UNIQUE`, `VALUE_RANGE`, `REGEX`, or `REFERENTIAL` will immediately raise `DQFailureError` with "Unknown rule_type" — the pipeline halts for that object.

### Severity String Mismatch

The `Severity` constant class defines `WARNING = "WARNING"`. The DDL for `dq_rule_config` specifies `severity` as `'ERROR' or 'WARN'` (not `'WARNING'`). The `run_dq_checks` function compares `rule.severity == Severity.ERROR` — a rule seeded with `severity='WARN'` will not match `"ERROR"`, so it will be treated as non-halting. The `DQStatus.WARNING` status is set correctly, but the mismatch means the string in the database must be `WARNING` not `WARN` for the severity check to work.

### Retry Attempt Is Always 1

`log_error` accepts `retry_attempt: int` but the client always passes `retry_attempt=1`. No retry logic exists in this library — retry behaviour is the consumer pipeline's responsibility.

### `detect_drift` Takes `pipeline_id` As Explicit Argument

`client.detect_drift()` takes `pipeline_id` as a parameter even though `self.pipeline_id` is available on the client. This means the caller can pass a different pipeline_id than the client was initialised with — the API is inconsistent with the pattern used by all other methods.

### No DDL / Migration Scripts

This repo contains no executable DDL. All 12 control schema tables must be created by the consumer before the library can be used. The only schema reference is documentation in `context/prompt.md`.

### No Retry / Backoff Implementation

The implementation standards (`context/implementation_standards.md`) define a configurable retry strategy with exponential backoff. This library has no retry logic — a single failed Spark call raises immediately.

### `MANIFEST.in` References Missing `LICENSE` File

`MANIFEST.in` includes `include LICENSE` but no `LICENSE` file exists in the repo. This produces a warning during `python -m build --wheel` but does not fail the build.

### `py.typed` Marker File Missing

`setup.cfg` declares `[options.package_data] * = py.typed` and `MANIFEST.in` includes `py.typed`, but no `py.typed` file exists in the package directory. This produces a warning during build and means PEP 561 type checking marker is not actually shipped.

### Logging Uses Python `logging` Module, Not `_log()` Helper

The original prompt (`context/prompt.md`) specified a `_log()` print-based helper for all logging. The actual implementation uses Python's standard `logging` module with `logging.getLogger(__name__)` per module. This is the correct production approach — the discrepancy is between the prompt spec and what was built, not a bug.
