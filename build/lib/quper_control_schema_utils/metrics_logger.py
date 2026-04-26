"""
Metrics logger for quper_control_schema_utils.

Inserts ingestion metrics into the ingestion_metrics table.
This module must NEVER raise.
"""

import logging
import traceback
from typing import Any

from quper_control_schema_utils._internal import _escape_sql
from quper_control_schema_utils.models import IngestionMetrics, TableName

logger = logging.getLogger(__name__)


def _sql_val(value: Any, is_string: bool = False) -> str:
    """
    Convert a Python value to a SQL literal.

    Args:
        value:     The value to convert.
        is_string: Whether the value should be quoted as a string.

    Returns:
        SQL literal string.
    """
    if value is None:
        return "NULL"
    if is_string:
        return f"'{_escape_sql(str(value))}'"
    return str(value)


def log_metrics(spark: Any, catalog: str, control_schema: str, metrics: IngestionMetrics) -> None:
    """
    Insert a row into ingestion_metrics using all fields from the dataclass.

    This function will never raise — on failure it logs a WARNING and returns.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        metrics:        IngestionMetrics dataclass instance.

    Returns:
        None.
    """
    logger.info( f"[pipeline={metrics.pipeline_id}, object={metrics.source_object}] Logging metrics to ingestion_metrics")
    try:
        table = f"{catalog}.{control_schema}.{TableName.INGESTION_METRICS}"
        query = f"""
            INSERT INTO {table}
            (metric_id, run_id, pipeline_id, object_id, source_object,
             target_table, load_type, write_mode, rows_read, rows_inserted,
             rows_updated, rows_deleted, rows_rejected, duration_seconds,
             status, metric_ts, environment)
            VALUES
            ('{_escape_sql(metrics.metric_id)}', '{_escape_sql(metrics.run_id)}',
             '{_escape_sql(metrics.pipeline_id)}', '{_escape_sql(metrics.object_id)}',
             '{_escape_sql(metrics.source_object)}', '{_escape_sql(metrics.target_table)}',
             '{_escape_sql(metrics.load_type)}', '{_escape_sql(metrics.write_mode)}',
             {_sql_val(metrics.rows_read)}, {_sql_val(metrics.rows_inserted)},
             {_sql_val(metrics.rows_updated)}, {_sql_val(metrics.rows_deleted)},
             {_sql_val(metrics.rows_rejected)}, {_sql_val(metrics.duration_seconds)},
             '{_escape_sql(metrics.status)}', current_timestamp(),
             '{_escape_sql(metrics.environment)}')
        """
        spark.sql(query)
        logger.info( f"[pipeline={metrics.pipeline_id}, object={metrics.source_object}] Metrics logged: status={metrics.status}")

    except Exception as e:
        logger.warning( f"[pipeline={metrics.pipeline_id}, object={metrics.source_object}] Failed to log metrics: {e}\n{traceback.format_exc()}")
