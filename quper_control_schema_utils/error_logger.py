"""
Error logger for quper_control_schema_utils.

Inserts error rows into the job_run_error table. This module must
NEVER raise — logging a failure must never crash the pipeline.
"""

import logging
import uuid
from typing import Any

from quper_control_schema_utils._internal import _escape_sql, _swallow_error
from quper_control_schema_utils.models import TableName

logger = logging.getLogger(__name__)


def log_error(
    spark: Any,
    catalog: str,
    control_schema: str,
    run_id: str,
    pipeline_id: str,
    object_id: str,
    source_object: str,
    error_type: str,
    error_message: str,
    stack_trace: str,
    retry_attempt: int,
    environment: str,
) -> None:
    """
    Insert a row into job_run_error.

    This function is wrapped entirely in try/except — it will never raise.
    On failure it logs a WARNING and returns.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        run_id:         Parent run identifier.
        pipeline_id:    Pipeline identifier.
        object_id:      Source object identifier.
        source_object:  Source object name.
        error_type:     One of: JDBC_ERROR, SCHEMA_MISMATCH, MERGE_ERROR,
                        VALIDATION_ERROR, DQ_ERROR, UNKNOWN.
        error_message:  Human-readable error description.
        stack_trace:    Full traceback string.
        retry_attempt:  Current retry attempt number.
        environment:    Deployment environment.

    Returns:
        None.
    """
    try:
        table = f"{catalog}.{control_schema}.{TableName.JOB_RUN_ERROR}"
        error_id = str(uuid.uuid4())

        safe_error_id = _escape_sql(error_id)
        safe_run_id = _escape_sql(run_id)
        safe_pid = _escape_sql(pipeline_id)
        safe_oid = _escape_sql(object_id)
        safe_src = _escape_sql(source_object)
        safe_type = _escape_sql(error_type)
        safe_msg = _escape_sql(error_message) if error_message else ""
        safe_trace = _escape_sql(stack_trace) if stack_trace else ""
        safe_env = _escape_sql(environment)

        query = f"""
            INSERT INTO {table}
            (error_id, run_id, pipeline_id, object_id, source_object,
             error_type, error_message, stack_trace, retry_attempt,
             error_ts, environment)
            VALUES
            ('{safe_error_id}', '{safe_run_id}', '{safe_pid}', '{safe_oid}',
             '{safe_src}', '{safe_type}', '{safe_msg}', '{safe_trace}',
             {retry_attempt}, current_timestamp(), '{safe_env}')
        """
        spark.sql(query)
        logger.info( f"[pipeline={pipeline_id}, object={source_object}] Error logged: error_id={error_id}, type={error_type}")

    except Exception as e:
        _swallow_error(logger, f"pipeline={pipeline_id}, object={source_object}", "Failed to log error", e)
