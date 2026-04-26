"""
Watermark manager for quper_control_schema_utils.

Handles read and write operations against the watermark_control table.
"""

import logging
import traceback
from typing import Any, Optional

from quper_control_schema_utils._internal import _escape_sql
from quper_control_schema_utils.exceptions import WatermarkError
from quper_control_schema_utils.models import TableName, WatermarkEntry

logger = logging.getLogger(__name__)


def get_watermark(
    spark: Any,
    catalog: str,
    control_schema: str,
    object_id: str,
    pipeline_id: str,
) -> Optional[WatermarkEntry]:
    """
    Return watermark for an object.

    Returns None if no row exists (first run — triggers full load).
    A row with last_run_ts = None also means first run.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        object_id:      Source object identifier.
        pipeline_id:    Pipeline identifier.

    Returns:
        WatermarkEntry or None if not found.

    Raises:
        WatermarkError: If the query fails unexpectedly.
    """
    table = f"{catalog}.{control_schema}.{TableName.WATERMARK_CONTROL}"
    safe_oid = _escape_sql(object_id)
    safe_pid = _escape_sql(pipeline_id)
    logger.info( f"[pipeline={pipeline_id}, object={object_id}] Reading watermark from {table}")
    try:
        query = f"""
            SELECT *
            FROM {table}
            WHERE object_id = '{safe_oid}'
              AND pipeline_id = '{safe_pid}'
        """
        rows = spark.sql(query).collect()

        if not rows:
            logger.info( f"[pipeline={pipeline_id}, object={object_id}] No watermark found — first run")
            return None

        r = rows[0]
        logger.info( f"[pipeline={pipeline_id}, object={object_id}] Watermark loaded: last_run_ts={r['last_run_ts']}")

        return WatermarkEntry(
            object_id=r["object_id"],
            pipeline_id=r["pipeline_id"],
            last_run_ts=r["last_run_ts"],
            last_run_id=r["last_run_id"],
            rows_last_loaded=r["rows_last_loaded"],
            updated_at=r["updated_at"],
            watermark_col = r["watermark_col"],
            watermark_col_hash = r["watermark_col_hash"]

        )

    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}, object={object_id}] Failed to read watermark: {e}\n{traceback.format_exc()}")
        raise WatermarkError(f"Failed to read watermark for object={object_id}, pipeline={pipeline_id}: {e}") from e


def update_watermark(
    spark: Any,
    catalog: str,
    control_schema: str,
    object_id: str,
    pipeline_id: str,
    run_id: str,
    rows_loaded: int,
    new_watermark=None,
) -> None:
    """
    Merge into watermark_control on (object_id + pipeline_id).

    WHEN MATCHED: update last_run_ts, last_run_id, rows_last_loaded, updated_at.
    WHEN NOT MATCHED: insert full row.

    Only call this after a confirmed successful write — never before.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        object_id:      Source object identifier.
        pipeline_id:    Pipeline identifier.
        run_id:         Current run identifier.
        rows_loaded:    Number of rows loaded in this run.
        new_watermark:  MAX(watermark_column) captured from source data before the read.
                        When provided, stored as last_run_ts so the next incremental
                        window starts from the actual data upper bound rather than
                        wall-clock time.  Falls back to current_timestamp() if None.

    Returns:
        None.

    Raises:
        WatermarkError: If the MERGE fails.
    """
    table = f"{catalog}.{control_schema}.{TableName.WATERMARK_CONTROL}"
    safe_oid = _escape_sql(object_id)
    safe_pid = _escape_sql(pipeline_id)
    safe_rid = _escape_sql(run_id)

    # Use the actual MAX from source data when available; fall back to wall-clock
    # only as a safety net (e.g. cold-start with no watermark column).
    if new_watermark is not None:
        wm_sql = f"CAST('{new_watermark}' AS TIMESTAMP)"
    else:
        wm_sql = "current_timestamp()"

    logger.info( f"[pipeline={pipeline_id}, object={object_id}] Updating watermark in {table}")
    try:
        query = f"""
            MERGE INTO {table} AS target
            USING (
                SELECT
                    '{safe_oid}' AS object_id,
                    '{safe_pid}' AS pipeline_id
            ) AS source
            ON target.object_id = source.object_id
               AND target.pipeline_id = source.pipeline_id
            WHEN MATCHED THEN UPDATE SET
                last_run_ts = {wm_sql},
                last_run_id = '{safe_rid}',
                rows_last_loaded = {rows_loaded},
                updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT
                (object_id, pipeline_id, last_run_ts, last_run_id,
                 rows_last_loaded, updated_at)
            VALUES
                ('{safe_oid}', '{safe_pid}', {wm_sql},
                 '{safe_rid}', {rows_loaded}, current_timestamp())
        """
        spark.sql(query)
        logger.info( f"[pipeline={pipeline_id}, object={object_id}] Watermark updated: run={run_id}, rows={rows_loaded}, last_run_ts={new_watermark or 'current_timestamp()'}")

    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}, object={object_id}] Failed to update watermark: {e}\n{traceback.format_exc()}")
        raise WatermarkError(f"Failed to update watermark for object={object_id}, pipeline={pipeline_id}: {e}") from e
