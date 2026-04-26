"""
Data models for quper_control_schema_utils.

Plain Python dataclasses matching the exact DDL schemas of the 12 control
schema tables. No Spark dependency — these are transport objects only.

Also contains shared constants for status values, drift types, error types,
severities, and DQ actions to prevent magic strings across the library.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


# ── Constants ──────────────────────────────────────────────────────────────────


class Status:
    """Pipeline and object run status values."""

    RUNNING: str = "RUNNING"
    SUCCESS: str = "SUCCESS"
    FAILED: str = "FAILED"
    PARTIAL: str = "PARTIAL"


class DriftType:
    """Schema drift type constants."""

    NEW_COLUMN: str = "NEW_COLUMN"
    DROPPED_COLUMN: str = "DROPPED_COLUMN"
    TYPE_CHANGE: str = "TYPE_CHANGE"
    NULLABLE_CHANGE: str = "NULLABLE_CHANGE"


class DriftAction:
    """Schema drift action constants."""

    EVOLVED: str = "EVOLVED"
    WARNED: str = "WARNED"
    HALTED: str = "HALTED"


class DQStatus:
    """Data quality check status constants."""

    PASSED: str = "PASSED"
    FAILED: str = "FAILED"
    WARNING: str = "WARNING"


class DQAction:
    """Data quality check action constants."""

    CONTINUED: str = "CONTINUED"
    HALTED: str = "HALTED"


class ErrorType:
    """Error type classification constants."""

    JDBC_ERROR: str = "JDBC_ERROR"
    SCHEMA_MISMATCH: str = "SCHEMA_MISMATCH"
    MERGE_ERROR: str = "MERGE_ERROR"
    VALIDATION_ERROR: str = "VALIDATION_ERROR"
    DQ_ERROR: str = "DQ_ERROR"
    UNKNOWN: str = "UNKNOWN"


class Severity:
    """DQ rule severity constants."""

    ERROR: str = "ERROR"
    WARNING: str = "WARNING"


class TableName:
    """Control schema table name constants."""

    SOURCE_OBJECT_CONFIG: str = "source_object_config"
    PIPELINE_CONFIG: str = "pipeline_config"
    METADATA_COLUMN_CONFIG: str = "metadata_column_config"
    DQ_RULE_CONFIG: str = "dq_rule_config"
    CONNECTION_CONFIG: str = "connection_config"
    WATERMARK_CONTROL: str = "watermark_control"
    OBJECT_SCHEMA_REGISTRY: str = "object_schema_registry"
    JOB_RUN_AUDIT: str = "job_run_audit"
    JOB_RUN_ERROR: str = "job_run_error"
    INGESTION_METRICS: str = "ingestion_metrics"
    SCHEMA_DRIFT_LOG: str = "schema_drift_log"
    DQ_CHECK_RESULTS: str = "dq_check_results"
    DQ_QUARANTINE: str = "dq_quarantine"


# ── Dataclasses ────────────────────────────────────────────────────────────────


@dataclass
class SourceObject:
    """Row from source_object_config table."""

    object_id: str
    pipeline_id: str
    source_system: str
    source_type: str
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
    """Row from watermark_control table."""

    object_id: str
    pipeline_id: str
    last_run_ts: Optional[datetime]
    last_run_id: Optional[str]
    rows_last_loaded: Optional[int]
    updated_at: datetime


@dataclass
class PipelineConfigEntry:
    """Row from pipeline_config table."""

    config_id: str
    pipeline_id: str
    config_key: str
    config_value: str
    description: Optional[str]
    active_flag: bool


@dataclass
class RunAudit:
    """Row from job_run_audit table."""

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
    """Row from job_run_error table."""

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
    """Row from ingestion_metrics table."""

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
    """Row from object_schema_registry table."""

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
    """Row from schema_drift_log table."""

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
    """Row from dq_rule_config table."""

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
    """Row from dq_check_results table."""

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
    """Row from metadata_column_config table."""

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
    """Row from connection_config table."""

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
