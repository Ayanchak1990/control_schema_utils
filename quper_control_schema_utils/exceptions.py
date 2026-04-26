"""
Custom exceptions for quper_control_schema_utils.

Every exception inherits from ControlSchemaError so callers can catch
the entire family with a single except clause when needed.
"""


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
