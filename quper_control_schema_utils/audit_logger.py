"""
Audit logger for quper_control_schema_utils.

Handles INSERT and MERGE operations against the job_run_audit table.
"""

import logging
import traceback
import uuid
from typing import Any, Optional

from quper_control_schema_utils._internal import _escape_sql
from quper_control_schema_utils.exceptions import AuditLogError
from quper_control_schema_utils.models import Status, TableName

logger = logging.getLogger(__name__)


def log_run_start(
    spark: Any,
    catalog: str,
    control_schema: str,
    pipeline_id: str,
    environment: str,
    triggered_by: str,
    job_id: Optional[str],
    job_run_id: Optional[str],
) -> str:
    """
    Insert a new row into job_run_audit with status='RUNNING'.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        pipeline_id:    Pipeline identifier.
        environment:    Deployment environment.
        triggered_by:   Trigger type: 'schedule', 'manual', or 'api'.
        job_id:         Optional Databricks job ID.
        job_run_id:     Optional Databricks job run ID.

    Returns:
        The generated run_id (UUID string).

    Raises:
        AuditLogError: If the INSERT fails.
    """
    table = f"{catalog}.{control_schema}.{TableName.JOB_RUN_AUDIT}"
    run_id = str(uuid.uuid4())

    logger.info( f"[pipeline={pipeline_id}, run={run_id}] Starting audit log insert into {table}")

    safe_run_id = _escape_sql(run_id)
    safe_pid = _escape_sql(pipeline_id)
    safe_env = _escape_sql(environment)
    safe_trigger = _escape_sql(triggered_by)
    job_id_val = f"'{_escape_sql(job_id)}'" if job_id else "NULL"
    job_run_id_val = f"'{_escape_sql(job_run_id)}'" if job_run_id else "NULL"

    try:
        query = f"""
            INSERT INTO {table}
            (run_id, pipeline_id, job_id, job_run_id, triggered_by,
             start_time, end_time, duration_seconds, status,
             total_objects, success_objects, failed_objects,
             environment, created_at)
            VALUES
            ('{safe_run_id}', '{safe_pid}', {job_id_val}, {job_run_id_val},
             '{safe_trigger}', current_timestamp(), NULL, NULL, '{Status.RUNNING}',
             NULL, NULL, NULL, '{safe_env}', current_timestamp())
        """
        spark.sql(query)
        logger.info( f"[pipeline={pipeline_id}, run={run_id}] Run started")
        return run_id

    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}, run={run_id}] Failed to log run start: {e}\n{traceback.format_exc()}")
        raise AuditLogError(f"Failed to log run start for pipeline={pipeline_id}, run={run_id}: {e}") from e


def log_run_end(
    spark: Any,
    catalog: str,
    control_schema: str,
    run_id: str,
    status: str,
    total_objects: int,
    success_objects: int,
    failed_objects: int,
) -> None:
    """
    Merge into job_run_audit to set end_time, duration, status, and object counts.

    This function must never raise — on failure it logs a warning and returns.

    Args:
        spark:           Active SparkSession.
        catalog:         Unity Catalog name.
        control_schema:  Control schema name.
        run_id:          The run_id to update.
        status:          Final status: 'SUCCESS', 'FAILED', or 'PARTIAL'.
        total_objects:   Total number of objects processed.
        success_objects: Number of successfully processed objects.
        failed_objects:  Number of failed objects.

    Returns:
        None.
    """
    table = f"{catalog}.{control_schema}.{TableName.JOB_RUN_AUDIT}"
    safe_run_id = _escape_sql(run_id)
    safe_status = _escape_sql(status)

    logger.info( f"[run={run_id}] Updating audit log in {table} with status={status}")
    try:
        query = f"""
            MERGE INTO {table} AS target
            USING (SELECT '{safe_run_id}' AS run_id) AS source
            ON target.run_id = source.run_id
            WHEN MATCHED THEN UPDATE SET
                end_time = current_timestamp(),
                duration_seconds = BIGINT(
                    unix_timestamp(current_timestamp()) - unix_timestamp(target.start_time)
                ),
                status = '{safe_status}',
                total_objects = {total_objects},
                success_objects = {success_objects},
                failed_objects = {failed_objects}
        """
        spark.sql(query)
        logger.info( f"[run={run_id}] Run ended with status={status}")

    except Exception as e:
        logger.warning( f"[run={run_id}] Failed to log run end: {e}\n{traceback.format_exc()}")
