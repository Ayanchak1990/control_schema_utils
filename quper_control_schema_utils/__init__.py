"""
quper_control_schema_utils — Generic control schema utilities for Databricks ingestion pipelines.

A shared utility library that handles ALL read/write operations against a
Databricks control schema (Delta tables). Import ControlSchemaClient and
pass your SparkSession + catalog/schema at runtime.
"""

from quper_control_schema_utils.client import ControlSchemaClient
from quper_control_schema_utils.models import (
    SourceObject, WatermarkEntry, PipelineConfigEntry, RunAudit,
    RunError, IngestionMetrics, SchemaColumn, SchemaDrift,
    DQRule, DQResult, MetadataColumn, ConnectionConfig,
    Status, DriftType, DriftAction, DQStatus, DQAction,
    ErrorType, Severity, TableName,
)
from quper_control_schema_utils.exceptions import (
    ControlSchemaError, TableNotFoundError, PipelineConfigError,
    WatermarkError, SchemaRegistryError, DQFailureError,
    SchemaDriftHaltError, AuditLogError, ConnectionConfigError,
)

__version__ = "0.1.0"
__author__ = "Quper"

__all__ = [
    "ControlSchemaClient",
    "SourceObject", "WatermarkEntry", "PipelineConfigEntry", "RunAudit",
    "RunError", "IngestionMetrics", "SchemaColumn", "SchemaDrift",
    "DQRule", "DQResult", "MetadataColumn", "ConnectionConfig",
    "Status", "DriftType", "DriftAction", "DQStatus", "DQAction",
    "ErrorType", "Severity", "TableName",
    "ControlSchemaError", "TableNotFoundError", "PipelineConfigError",
    "WatermarkError", "SchemaRegistryError", "DQFailureError",
    "SchemaDriftHaltError", "AuditLogError", "ConnectionConfigError",
]
