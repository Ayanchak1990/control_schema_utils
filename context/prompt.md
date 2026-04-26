# Claude Code Prompt — quper_control_schema_utils
# Generic, reusable Python library for Databricks pipeline control schema operations
# Version: 0.1.0
# Distribution: .whl file

---

## ROLE

You are a Senior Python Engineer building a production-grade, client-agnostic Python library
called `quper_control_schema_utils`. This library will be distributed as a `.whl` file and
imported by any Databricks ingestion pipeline — across any client, any project.

Think of it like pandas or pyspark — generic, well-structured, versioned, installable.

---

## WHAT THIS LIBRARY IS

A shared utility library that handles ALL read/write operations against a Databricks
control schema (Delta tables). Any pipeline that uses a compatible control schema
can import this library and get all observability, audit, watermark, DQ, and schema
registry functionality for free — zero boilerplate in the pipeline code.

The library does NOT know anything about:
- Which client is using it
- What source views exist
- What the ingestion logic is
- Which catalog or schema to use (passed as config at runtime)

The library ONLY knows about:
- The exact 12 control schema table structures defined below
- How to read from and write to them correctly
- How to handle errors gracefully

---

## TECH STACK

- Python 3.9+
- PySpark (provided by Databricks cluster — NOT a pip dependency)
- Delta Lake (provided by Databricks cluster — NOT a pip dependency)
- No external dependencies beyond Python stdlib + PySpark
- Build tool: setuptools + wheel
- Distribution: .whl file

---

## LIBRARY STRUCTURE TO BUILD

```
quper_control_schema_utils/
├── quper_control_schema_utils/
│   ├── __init__.py
│   ├── client.py
│   ├── config_reader.py
│   ├── audit_logger.py
│   ├── error_logger.py
│   ├── metrics_logger.py
│   ├── watermark_manager.py
│   ├── schema_registry.py
│   ├── drift_logger.py
│   ├── dq_runner.py
│   ├── models.py
│   └── exceptions.py
├── tests/
│   ├── __init__.py
│   ├── test_audit_logger.py
│   ├── test_watermark_manager.py
│   ├── test_config_reader.py
│   └── test_models.py
├── setup.py
├── setup.cfg
├── pyproject.toml
├── MANIFEST.in
├── README.md
└── .gitignore
```

---

## EXACT TABLE SCHEMAS — BUILD ALL MODELS AND FUNCTIONS AGAINST THESE

These are the exact column definitions from the DDL files.
Every dataclass, every SQL query, every INSERT must match these exactly.

---

### source_object_config
```
object_id           STRING        NOT NULL
pipeline_id         STRING        NOT NULL
source_system       STRING        NOT NULL
source_schema       STRING        NOT NULL
source_object       STRING        NOT NULL
target_catalog      STRING        NOT NULL
target_schema       STRING        NOT NULL
target_table        STRING        NOT NULL
staging_table       STRING                    -- nullable
load_type           STRING        NOT NULL    -- 'full_load' or 'hash_incremental'
write_mode          STRING        NOT NULL    -- 'overwrite' or 'merge'
primary_key         STRING        NOT NULL
watermark_column    STRING                    -- nullable
hash_columns        STRING                    -- nullable
active_flag         BOOLEAN       NOT NULL
load_order          INT           NOT NULL
created_at          TIMESTAMP     NOT NULL
updated_at          TIMESTAMP     NOT NULL
created_by          STRING        NOT NULL
updated_by          STRING        NOT NULL
```

### watermark_control
Composite PK: (object_id + pipeline_id)
```
object_id           STRING        NOT NULL
pipeline_id         STRING        NOT NULL
last_run_ts         TIMESTAMP                 -- nullable. NULL = never run = trigger full load
last_run_id         STRING                    -- nullable
rows_last_loaded    BIGINT                    -- nullable
updated_at          TIMESTAMP     NOT NULL
```

### pipeline_config
```
config_id           STRING        NOT NULL
pipeline_id         STRING        NOT NULL
config_key          STRING        NOT NULL
config_value        STRING        NOT NULL
description         STRING                    -- nullable
active_flag         BOOLEAN       NOT NULL
created_at          TIMESTAMP     NOT NULL
updated_at          TIMESTAMP     NOT NULL
```

### job_run_audit
PARTITIONED BY (environment)
```
run_id              STRING        NOT NULL
pipeline_id         STRING        NOT NULL
job_id              STRING                    -- nullable
job_run_id          STRING                    -- nullable
triggered_by        STRING        NOT NULL    -- 'schedule', 'manual', 'api'
start_time          TIMESTAMP     NOT NULL
end_time            TIMESTAMP                 -- nullable. NULL = still running
duration_seconds    BIGINT                    -- nullable. NULL = still running
status              STRING        NOT NULL    -- 'RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL'
total_objects       INT                       -- nullable
success_objects     INT                       -- nullable
failed_objects      INT                       -- nullable
environment         STRING        NOT NULL
created_at          TIMESTAMP     NOT NULL
```

### job_run_error
PARTITIONED BY (environment)
```
error_id            STRING        NOT NULL
run_id              STRING        NOT NULL
pipeline_id         STRING        NOT NULL
object_id           STRING        NOT NULL
source_object       STRING        NOT NULL
error_type          STRING        NOT NULL    -- 'JDBC_ERROR','SCHEMA_MISMATCH','MERGE_ERROR','VALIDATION_ERROR','DQ_ERROR','UNKNOWN'
error_message       STRING                    -- nullable
stack_trace         STRING                    -- nullable
retry_attempt       INT           NOT NULL
error_ts            TIMESTAMP     NOT NULL
environment         STRING        NOT NULL
```

### ingestion_metrics
PARTITIONED BY (environment)
```
metric_id           STRING        NOT NULL
run_id              STRING        NOT NULL
pipeline_id         STRING        NOT NULL
object_id           STRING        NOT NULL
source_object       STRING        NOT NULL
target_table        STRING        NOT NULL
load_type           STRING        NOT NULL
write_mode          STRING        NOT NULL
rows_read           BIGINT                    -- nullable
rows_inserted       BIGINT                    -- nullable
rows_updated        BIGINT                    -- nullable
rows_deleted        BIGINT                    -- nullable
rows_rejected       BIGINT                    -- nullable
duration_seconds    BIGINT                    -- nullable
status              STRING        NOT NULL    -- 'SUCCESS' or 'FAILED'
metric_ts           TIMESTAMP     NOT NULL
environment         STRING        NOT NULL
```

### object_schema_registry
```
schema_id           STRING        NOT NULL
object_id           STRING        NOT NULL
pipeline_id         STRING        NOT NULL
column_name         STRING        NOT NULL
data_type           STRING        NOT NULL
is_nullable         BOOLEAN       NOT NULL
is_primary_key      BOOLEAN       NOT NULL
column_order        INT           NOT NULL
registered_at       TIMESTAMP     NOT NULL
updated_at          TIMESTAMP     NOT NULL
active_flag         BOOLEAN       NOT NULL
```

### schema_drift_log
```
drift_id            STRING        NOT NULL
run_id              STRING        NOT NULL
object_id           STRING        NOT NULL
pipeline_id         STRING        NOT NULL
source_object       STRING        NOT NULL
column_name         STRING        NOT NULL
drift_type          STRING        NOT NULL    -- 'NEW_COLUMN','DROPPED_COLUMN','TYPE_CHANGE','NULLABLE_CHANGE'
old_value           STRING                    -- nullable
new_value           STRING                    -- nullable
action_taken        STRING        NOT NULL    -- 'EVOLVED','HALTED','WARNED','IGNORED'
detected_at         TIMESTAMP     NOT NULL
environment         STRING        NOT NULL
```

### dq_rule_config
```
rule_id             STRING        NOT NULL
object_id           STRING        NOT NULL
pipeline_id         STRING        NOT NULL
rule_name           STRING        NOT NULL
rule_type           STRING        NOT NULL    -- 'NOT_NULL','UNIQUE','ROW_COUNT','VALUE_RANGE','REGEX','REFERENTIAL'
column_name         STRING                    -- nullable
rule_expression     STRING        NOT NULL
expected_value      STRING                    -- nullable
severity            STRING        NOT NULL    -- 'ERROR' or 'WARN'
active_flag         BOOLEAN       NOT NULL
created_at          TIMESTAMP     NOT NULL
updated_at          TIMESTAMP     NOT NULL
```

### dq_check_results
PARTITIONED BY (environment)
```
result_id           STRING        NOT NULL
run_id              STRING        NOT NULL
object_id           STRING        NOT NULL
rule_id             STRING        NOT NULL
pipeline_id         STRING        NOT NULL
rule_name           STRING        NOT NULL
rule_type           STRING        NOT NULL
column_name         STRING                    -- nullable
rows_checked        BIGINT                    -- nullable
rows_passed         BIGINT                    -- nullable
rows_failed         BIGINT                    -- nullable
pass_rate           DOUBLE                    -- nullable
status              STRING        NOT NULL    -- 'PASSED','FAILED','WARNING'
action_taken        STRING        NOT NULL    -- 'CONTINUED' or 'HALTED'
checked_at          TIMESTAMP     NOT NULL
environment         STRING        NOT NULL
```

### metadata_column_config
```
config_id           STRING        NOT NULL
pipeline_id         STRING        NOT NULL    -- 'default' = shared. pipeline name = override.
column_name         STRING        NOT NULL
data_type           STRING        NOT NULL
applies_to          STRING        NOT NULL    -- 'all','hash_incremental','full_load'
computation         STRING        NOT NULL
column_order        INT           NOT NULL
is_merge_key        BOOLEAN       NOT NULL
active_flag         BOOLEAN       NOT NULL
created_at          TIMESTAMP     NOT NULL
```

### connection_config
```
connection_id            STRING    NOT NULL
pipeline_id              STRING    NOT NULL
source_system            STRING    NOT NULL
connection_name          STRING    NOT NULL
secret_scope             STRING    NOT NULL
jdbc_url_secret_key      STRING    NOT NULL
jdbc_user_secret_key     STRING    NOT NULL
jdbc_password_secret_key STRING    NOT NULL
driver_class             STRING    NOT NULL
extra_jdbc_options       STRING               -- nullable
active_flag              BOOLEAN   NOT NULL
created_at               TIMESTAMP NOT NULL
updated_at               TIMESTAMP NOT NULL
```

---

## models.py — DATACLASSES

All dataclasses are plain Python — no Spark dependency. Match exactly to the DDL schemas above.

```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List

@dataclass
class SourceObject:
    object_id: str
    pipeline_id: str
    source_system: str
    source_schema: str
    source_object: str
    target_catalog: str
    target_schema: str
    target_table: str
    staging_table: Optional[str]
    load_type: str
    write_mode: str
    primary_key: str
    watermark_column: Optional[str]
    hash_columns: Optional[str]
    active_flag: bool
    load_order: int
    created_at: datetime
    updated_at: datetime
    created_by: str
    updated_by: str

@dataclass
class WatermarkEntry:
    object_id: str
    pipeline_id: str
    last_run_ts: Optional[datetime]      # None = never run = trigger full load
    last_run_id: Optional[str]
    rows_last_loaded: Optional[int]
    updated_at: datetime

@dataclass
class PipelineConfigEntry:
    config_id: str
    pipeline_id: str
    config_key: str
    config_value: str
    description: Optional[str]
    active_flag: bool

@dataclass
class RunAudit:
    run_id: str
    pipeline_id: str
    job_id: Optional[str]
    job_run_id: Optional[str]
    triggered_by: str
    start_time: datetime
    end_time: Optional[datetime]
    duration_seconds: Optional[int]
    status: str
    total_objects: Optional[int]
    success_objects: Optional[int]
    failed_objects: Optional[int]
    environment: str
    created_at: datetime

@dataclass
class RunError:
    error_id: str
    run_id: str
    pipeline_id: str
    object_id: str
    source_object: str
    error_type: str
    error_message: Optional[str]
    stack_trace: Optional[str]
    retry_attempt: int
    error_ts: datetime
    environment: str

@dataclass
class IngestionMetrics:
    metric_id: str
    run_id: str
    pipeline_id: str
    object_id: str
    source_object: str
    target_table: str
    load_type: str
    write_mode: str
    rows_read: Optional[int]
    rows_inserted: Optional[int]
    rows_updated: Optional[int]
    rows_deleted: Optional[int]
    rows_rejected: Optional[int]
    duration_seconds: Optional[int]
    status: str
    metric_ts: datetime
    environment: str

@dataclass
class SchemaColumn:
    schema_id: str
    object_id: str
    pipeline_id: str
    column_name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool
    column_order: int
    registered_at: datetime
    updated_at: datetime
    active_flag: bool

@dataclass
class SchemaDrift:
    drift_id: str
    run_id: str
    object_id: str
    pipeline_id: str
    source_object: str
    column_name: str
    drift_type: str
    old_value: Optional[str]
    new_value: Optional[str]
    action_taken: str
    detected_at: datetime
    environment: str

@dataclass
class DQRule:
    rule_id: str
    object_id: str
    pipeline_id: str
    rule_name: str
    rule_type: str
    column_name: Optional[str]
    rule_expression: str
    expected_value: Optional[str]
    severity: str
    active_flag: bool

@dataclass
class DQResult:
    result_id: str
    run_id: str
    object_id: str
    rule_id: str
    pipeline_id: str
    rule_name: str
    rule_type: str
    column_name: Optional[str]
    rows_checked: Optional[int]
    rows_passed: Optional[int]
    rows_failed: Optional[int]
    pass_rate: Optional[float]
    status: str
    action_taken: str
    checked_at: datetime
    environment: str

@dataclass
class MetadataColumn:
    config_id: str
    pipeline_id: str
    column_name: str
    data_type: str
    applies_to: str
    computation: str
    column_order: int
    is_merge_key: bool
    active_flag: bool

@dataclass
class ConnectionConfig:
    connection_id: str
    pipeline_id: str
    source_system: str
    connection_name: str
    secret_scope: str
    jdbc_url_secret_key: str
    jdbc_user_secret_key: str
    jdbc_password_secret_key: str
    driver_class: str
    extra_jdbc_options: Optional[str]
    active_flag: bool
```

---

## exceptions.py

```python
class ControlSchemaError(Exception):
    """Base exception for all quper_control_schema_utils errors."""

class TableNotFoundError(ControlSchemaError):
    """Raised when a required control schema table does not exist."""

class PipelineConfigError(ControlSchemaError):
    """Raised when required pipeline config keys are missing or invalid."""

class WatermarkError(ControlSchemaError):
    """Raised when watermark read or write fails."""

class SchemaRegistryError(ControlSchemaError):
    """Raised when schema registry read or write fails."""

class DQFailureError(ControlSchemaError):
    """Raised when one or more DQ rules with severity=ERROR fail."""

class SchemaDriftHaltError(ControlSchemaError):
    """Raised when TYPE_CHANGE drift is detected — pipeline must halt for manual review."""

class AuditLogError(ControlSchemaError):
    """Raised when audit log write fails critically."""

class ConnectionConfigError(ControlSchemaError):
    """Raised when connection config is missing or inactive for a pipeline."""
```

---

## client.py — MAIN ENTRY POINT

Single class a pipeline imports. Wires all modules together.
All table paths are built internally from catalog + control_schema.

```python
class ControlSchemaClient:
    """
    Main entry point for all control schema operations.

    Usage:
        from quper_control_schema_utils import ControlSchemaClient

        client = ControlSchemaClient(
            spark=spark,
            catalog="dev",
            control_schema="deltek_cdm",
            pipeline_id="raw_employee_pipeline",
            environment="dev"
        )

        run_id  = client.start_run(triggered_by="schedule")
        objects = client.get_source_objects()
        config  = client.get_pipeline_config()

        for obj in objects:
            wm       = client.get_watermark(obj.object_id)
            meta     = client.get_metadata_columns(obj.load_type)
            rules    = client.get_dq_rules(obj.object_id)
            conn     = client.get_connection_config()

            # ... pipeline does extraction + metadata injection + write ...

            results = client.run_dq_checks(run_id, obj.object_id, df, rules)
            client.log_dq_results(results)
            client.log_metrics(metrics)
            client.update_watermark(obj.object_id, run_id, rows_loaded)

        client.end_run(run_id, "SUCCESS", total_objects=2,
                       success_objects=2, failed_objects=0)
    """

    def __init__(
        self,
        spark,
        catalog: str,
        control_schema: str,
        pipeline_id: str,
        environment: str
    ):
        """
        Initialise the client.

        Args:
            spark:          Active SparkSession.
            catalog:        Unity Catalog name e.g. 'dev'.
            control_schema: Control schema name e.g. 'deltek_cdm'.
            pipeline_id:    Pipeline identifier e.g. 'raw_employee_pipeline'.
            environment:    Deployment environment: 'dev', 'qa', or 'prod'.
        """

    # ── Config readers ──────────────────────────────────────────────────────
    def get_source_objects(self) -> List[SourceObject]:
        """Return active source objects for this pipeline ordered by load_order ASC."""

    def get_pipeline_config(self) -> Dict[str, str]:
        """
        Return pipeline config as {config_key: config_value} dict.
        Only active_flag=true rows returned.
        """

    def get_metadata_columns(self, load_type: str) -> List[MetadataColumn]:
        """
        Return metadata columns for given load_type.
        Lookup order:
          1. Rows where pipeline_id = self.pipeline_id AND active_flag = true
          2. If none found → rows where pipeline_id = 'default' AND active_flag = true
        Filter by applies_to IN ('all', load_type).
        Order by column_order ASC.
        """

    def get_dq_rules(self, object_id: str) -> List[DQRule]:
        """Return active DQ rules for given object_id and this pipeline."""

    def get_connection_config(self) -> ConnectionConfig:
        """
        Return active connection config for this pipeline.
        Raises ConnectionConfigError if not found or not active.
        """

    # ── Watermark ───────────────────────────────────────────────────────────
    def get_watermark(self, object_id: str) -> Optional[WatermarkEntry]:
        """
        Return watermark for object.
        Returns None if no row exists (first run — triggers full load).
        last_run_ts = None also means first run.
        """

    def update_watermark(
        self,
        object_id: str,
        run_id: str,
        rows_loaded: int
    ) -> None:
        """
        MERGE INTO watermark_control on (object_id + pipeline_id).
        WHEN MATCHED → UPDATE last_run_ts=current UTC, last_run_id, rows_last_loaded, updated_at.
        WHEN NOT MATCHED → INSERT new row.
        Only call this after a confirmed successful write — never before.
        """

    # ── Audit ────────────────────────────────────────────────────────────────
    def start_run(
        self,
        triggered_by: str = "manual",
        job_id: Optional[str] = None,
        job_run_id: Optional[str] = None
    ) -> str:
        """
        INSERT row into job_run_audit with status='RUNNING'.
        Returns run_id (UUID).
        """

    def end_run(
        self,
        run_id: str,
        status: str,
        total_objects: int,
        success_objects: int,
        failed_objects: int
    ) -> None:
        """
        MERGE INTO job_run_audit on run_id.
        SET end_time=current UTC, duration_seconds, status, total/success/failed objects.
        Must never raise — if this fails, log warning and swallow.
        """

    def log_error(
        self,
        run_id: str,
        object_id: str,
        source_object: str,
        error_type: str,
        error_message: str,
        stack_trace: str,
        retry_attempt: int
    ) -> None:
        """
        INSERT row into job_run_error.
        Must never raise — logging a failure must never crash the pipeline.
        """

    # ── Metrics ──────────────────────────────────────────────────────────────
    def log_metrics(self, metrics: IngestionMetrics) -> None:
        """
        INSERT row into ingestion_metrics.
        Must never raise.
        """

    # ── Schema registry ──────────────────────────────────────────────────────
    def get_registered_schema(self, object_id: str) -> List[SchemaColumn]:
        """
        Return registered schema columns for object (active_flag=true only).
        Returns empty list if never registered (first run).
        """

    def register_schema(
        self,
        object_id: str,
        df_schema,
        primary_key_cols: List[str]
    ) -> None:
        """
        INSERT one row per column into object_schema_registry.
        df_schema is a PySpark StructType.
        Only call when get_registered_schema returns empty list.
        """

    def detect_drift(
        self,
        object_id: str,
        pipeline_id: str,
        source_object: str,
        run_id: str,
        registered: List[SchemaColumn],
        live_schema
    ) -> List[SchemaDrift]:
        """
        Pure comparison — no Spark needed.
        Compare registered columns vs live_schema (PySpark StructType) fields.
        Returns list of SchemaDrift objects.
        Drift rules:
          NEW_COLUMN:      in live, not in registered → action_taken = EVOLVED
          DROPPED_COLUMN:  in registered, not in live → action_taken = WARNED
          TYPE_CHANGE:     exists in both, data_type differs → action_taken = HALTED
          NULLABLE_CHANGE: exists in both, is_nullable differs → action_taken = WARNED
        """

    # ── Drift ─────────────────────────────────────────────────────────────────
    def log_drift(self, drifts: List[SchemaDrift]) -> None:
        """
        INSERT all SchemaDrift rows into schema_drift_log.
        Must never raise.
        """

    def evolve_table(
        self,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        new_columns: List[SchemaDrift]
    ) -> None:
        """
        For each NEW_COLUMN drift:
          ALTER TABLE {target_catalog}.{target_schema}.{target_table}
          ADD COLUMN {column_name} {data_type}
        Only call when schema_drift_action config = 'EVOLVE'.
        Raises SchemaDriftHaltError if any TYPE_CHANGE drift is present in new_columns.
        """

    # ── DQ ───────────────────────────────────────────────────────────────────
    def run_dq_checks(
        self,
        run_id: str,
        object_id: str,
        df,
        rules: List[DQRule]
    ) -> List[DQResult]:
        """
        Run all rules in rules list against df.
        For each rule:
          - Register df as temp view
          - Execute rule_expression via spark.sql()
          - Compute rows_checked, rows_passed, rows_failed, pass_rate
          - Set status: PASSED / FAILED / WARNING
          - Set action_taken: CONTINUED / HALTED
        After all rules:
          If any ERROR severity rule FAILED → raise DQFailureError.
        Return list of all DQResult objects.
        """

    def log_dq_results(self, results: List[DQResult]) -> None:
        """
        INSERT all DQResult rows into dq_check_results.
        Must never raise.
        """
```

---

## MODULE REQUIREMENTS

### config_reader.py
All functions are standalone (not methods) — take spark, catalog, control_schema, pipeline_id as args.
Used internally by client.py.

Functions:
- `get_source_objects(spark, catalog, control_schema, pipeline_id) → List[SourceObject]`
- `get_pipeline_config(spark, catalog, control_schema, pipeline_id) → Dict[str, str]`
- `get_metadata_columns(spark, catalog, control_schema, pipeline_id, load_type) → List[MetadataColumn]`
- `get_dq_rules(spark, catalog, control_schema, pipeline_id, object_id) → List[DQRule]`
- `get_connection_config(spark, catalog, control_schema, pipeline_id) → ConnectionConfig`

All functions must:
- Build full table path as f"{catalog}.{control_schema}.{table_name}"
- Use spark.sql() for queries — no DataFrame API
- Map rows to dataclasses exactly matching the DDL column names
- Raise appropriate custom exceptions on failure

### audit_logger.py
Standalone functions used by client.py.

- `log_run_start(spark, catalog, control_schema, pipeline_id, environment, triggered_by, job_id, job_run_id) → str`
  INSERT into job_run_audit. status='RUNNING'. Return run_id.

- `log_run_end(spark, catalog, control_schema, run_id, status, total_objects, success_objects, failed_objects) → None`
  MERGE INTO job_run_audit on run_id.
  SET end_time=current UTC, duration_seconds=(end_time - start_time), status, counts.
  Must never raise.

### error_logger.py
- `log_error(spark, catalog, control_schema, run_id, pipeline_id, object_id, source_object, error_type, error_message, stack_trace, retry_attempt, environment) → None`
  INSERT into job_run_error. Generate error_id = uuid4().
  Must NEVER raise — wrap entirely in try/except. On failure: _log WARNING and return.

### metrics_logger.py
- `log_metrics(spark, catalog, control_schema, metrics: IngestionMetrics) → None`
  INSERT into ingestion_metrics using all fields from the IngestionMetrics dataclass.
  Must NEVER raise.

### watermark_manager.py
- `get_watermark(spark, catalog, control_schema, object_id, pipeline_id) → Optional[WatermarkEntry]`
  Query watermark_control WHERE object_id=? AND pipeline_id=?.
  Return None if no row found.

- `update_watermark(spark, catalog, control_schema, object_id, pipeline_id, run_id, rows_loaded) → None`
  MERGE INTO watermark_control ON (object_id + pipeline_id).
  WHEN MATCHED → UPDATE last_run_ts=current_timestamp(), last_run_id, rows_last_loaded, updated_at.
  WHEN NOT MATCHED → INSERT full row.

### schema_registry.py
- `get_registered_schema(spark, catalog, control_schema, object_id, pipeline_id) → List[SchemaColumn]`
- `register_schema(spark, catalog, control_schema, object_id, pipeline_id, df_schema, primary_key_cols) → None`
- `detect_drift(object_id, pipeline_id, source_object, run_id, registered, live_schema, environment) → List[SchemaDrift]`
  Pure Python — no spark arg needed.

### drift_logger.py
- `log_drift(spark, catalog, control_schema, drifts: List[SchemaDrift]) → None`
  INSERT all drift rows into schema_drift_log. Must never raise.

- `evolve_table(spark, target_catalog, target_schema, target_table, new_columns: List[SchemaDrift]) → None`
  ALTER TABLE for each NEW_COLUMN.
  Raises SchemaDriftHaltError if TYPE_CHANGE present.

### dq_runner.py
- `run_dq_checks(spark, run_id, pipeline_id, object_id, df, rules: List[DQRule], environment) → List[DQResult]`
  Execute each rule. Raise DQFailureError if any ERROR rule fails.
  Return all results regardless.

- `log_dq_results(spark, catalog, control_schema, results: List[DQResult]) → None`
  INSERT all results. Must never raise.

---

## INTERNAL LOGGING HELPER

Every module uses this helper — NO print(), NO external logging libraries:

```python
def _log(level: str, message: str) -> None:
    """
    Args:
        level:   INFO, SUCCESS, WARNING, ERROR
        message: Log message
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] [{level:<7}] {message}", flush=True)
```

---

## __init__.py — PUBLIC API

```python
from quper_control_schema_utils.client import ControlSchemaClient
from quper_control_schema_utils.models import (
    SourceObject, WatermarkEntry, PipelineConfigEntry, RunAudit,
    RunError, IngestionMetrics, SchemaColumn, SchemaDrift,
    DQRule, DQResult, MetadataColumn, ConnectionConfig
)
from quper_control_schema_utils.exceptions import (
    ControlSchemaError, TableNotFoundError, PipelineConfigError,
    WatermarkError, SchemaRegistryError, DQFailureError,
    SchemaDriftHaltError, AuditLogError, ConnectionConfigError
)

__version__ = "0.1.0"
__author__ = "Quper"

__all__ = [
    "ControlSchemaClient",
    "SourceObject", "WatermarkEntry", "PipelineConfigEntry", "RunAudit",
    "RunError", "IngestionMetrics", "SchemaColumn", "SchemaDrift",
    "DQRule", "DQResult", "MetadataColumn", "ConnectionConfig",
    "ControlSchemaError", "TableNotFoundError", "PipelineConfigError",
    "WatermarkError", "SchemaRegistryError", "DQFailureError",
    "SchemaDriftHaltError", "AuditLogError", "ConnectionConfigError",
]
```

---

## SETUP FILES

### pyproject.toml
```toml
[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.backends.legacy:build"
```

### setup.cfg
```ini
[metadata]
name = quper_control_schema_utils
version = 0.1.0
author = Quper
description = Generic control schema utilities for Databricks ingestion pipelines
long_description = file: README.md
long_description_content_type = text/markdown
python_requires = >=3.9

[options]
packages = find:
install_requires =
    # PySpark is provided by Databricks cluster — not listed here

[options.package_data]
* = py.typed
```

### setup.py
```python
from setuptools import setup
if __name__ == "__main__":
    setup()
```

### MANIFEST.in
```
include README.md
include LICENSE
recursive-include quper_control_schema_utils *.py py.typed
```

### .gitignore
```
__pycache__/
*.pyc
*.pyo
*.pyd
dist/
build/
*.egg-info/
.env
.env.*
.pytest_cache/
.coverage
htmlcov/
*.whl
```

---

## README.md — USAGE EXAMPLE

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

            # ... pipeline extraction + metadata injection + write to RAW ...

            # Schema drift
            registered = client.get_registered_schema(obj.object_id)
            if not registered:
                client.register_schema(obj.object_id, df.schema, obj.primary_key.split(","))
            else:
                drifts = client.detect_drift(obj.object_id, obj.pipeline_id,
                                             obj.source_object, run_id,
                                             registered, df.schema)
                if drifts:
                    client.log_drift(drifts)
                    if any(d.action_taken == "HALTED" for d in drifts):
                        raise SchemaDriftHaltError(f"TYPE_CHANGE detected in {obj.source_object}")
                    client.evolve_table(obj.target_catalog, obj.target_schema,
                                        obj.target_table, drifts)

            # DQ checks
            dq_results = client.run_dq_checks(run_id, obj.object_id, df, dq_rules)
            client.log_dq_results(dq_results)

            # Metrics + watermark
            client.log_metrics(metrics)
            client.update_watermark(obj.object_id, run_id, metrics.rows_inserted)
            success_count += 1

        except Exception as e:
            client.log_error(run_id, obj.object_id, obj.source_object,
                             "UNKNOWN", str(e), traceback.format_exc(), retry_attempt=1)
            failed_count += 1

    status = "SUCCESS" if failed_count == 0 else ("FAILED" if success_count == 0 else "PARTIAL")
    client.end_run(run_id, status, len(objects), success_count, failed_count)

except Exception as e:
    client.end_run(run_id, "FAILED", 0, 0, 0)
    raise
```

---

## BUILD INSTRUCTIONS

After all files are built, run:
```bash
pip install build
python -m build --wheel
```

Output:
```
dist/quper_control_schema_utils-0.1.0-py3-none-any.whl
```

Install on Databricks:
```bash
pip install quper_control_schema_utils-0.1.0-py3-none-any.whl
```

Or via Databricks job yml:
```yaml
libraries:
  - whl: /Volumes/dev/shared/libs/quper_control_schema_utils-0.1.0-py3-none-any.whl
```

---

## CODING STANDARDS

- Module-level docstring on every file
- Docstring with Args and Returns on every function
- f-strings only — no .format() or %
- No hardcoded table names — always f"{catalog}.{control_schema}.{table}"
- Timestamps: datetime.utcnow() for Python
- UUIDs: str(uuid.uuid4())
- _log() helper for all logging — no print(), no external libs
- error_logger, metrics_logger, log_dq_results, log_drift, end_run → MUST NEVER RAISE
- Specific exceptions first (AnalysisException, Py4JJavaError), broad Exception last
- All public functions must have type hints
- SQL: UPPERCASE keywords, one clause per line

---

## WHAT TO BUILD — CHECKLIST

[ ] quper_control_schema_utils/__init__.py
[ ] quper_control_schema_utils/models.py
[ ] quper_control_schema_utils/exceptions.py
[ ] quper_control_schema_utils/client.py
[ ] quper_control_schema_utils/config_reader.py
[ ] quper_control_schema_utils/audit_logger.py
[ ] quper_control_schema_utils/error_logger.py
[ ] quper_control_schema_utils/metrics_logger.py
[ ] quper_control_schema_utils/watermark_manager.py
[ ] quper_control_schema_utils/schema_registry.py
[ ] quper_control_schema_utils/drift_logger.py
[ ] quper_control_schema_utils/dq_runner.py
[ ] tests/__init__.py
[ ] tests/test_audit_logger.py
[ ] tests/test_watermark_manager.py
[ ] tests/test_config_reader.py
[ ] tests/test_models.py
[ ] setup.py
[ ] setup.cfg
[ ] pyproject.toml
[ ] MANIFEST.in
[ ] README.md
[ ] .gitignore

---

## WHAT NOT TO BUILD

- Do NOT build any Databricks bundle or DAB config
- Do NOT build any ingestion logic — library handles control schema operations only
- Do NOT hardcode any catalog, schema, pipeline_id, or environment
- Do NOT add pip dependencies — PySpark is cluster-provided
- Do NOT create notebooks
- Do NOT create CLI tools
- Do NOT create CI/CD config
- Do NOT create any files not in the checklist above