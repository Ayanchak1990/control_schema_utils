"""
Watermark manager for quper_control_schema_utils.

Handles read and write operations against the watermark_control table.
"""

import hashlib
import logging
from typing import Any, Optional

from quper_control_schema_utils._internal import _escape_sql, _raise_error
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
            watermark_col_value=r["watermark_col_value"],
            watermark_col_hash = r["watermark_col_hash"]

        )

    except Exception as e:
        _raise_error(logger, f"pipeline={pipeline_id}, object={object_id}", "Failed to read watermark", e, WatermarkError)


def update_watermark(
    spark: Any,
    catalog: str,
    control_schema: str,
    object_id: str,
    pipeline_id: str,
    run_id: str,
    rows_loaded: int,
    new_watermark=None,
    watermark_col: Optional[str] = None,
) -> None:
    """
    Merge into watermark_control on (object_id + pipeline_id).

    WHEN MATCHED: update last_run_ts, last_run_id, rows_last_loaded, updated_at,
                  watermark_col, watermark_col_hash.
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
        new_watermark:  MAX(watermark_column) captured from source data.
                        When provided, stored in watermark_col_value (as a string)
                        and watermark_col_hash = SHA256(str(new_watermark)).
                        Both are stored as NULL when not supplied.
                        last_run_ts is always current_timestamp(), regardless.
        watermark_col:  Name of the watermark column used for this run.
                        Stored as-is; NULL when not supplied.

    Returns:
        None.

    Raises:
        WatermarkError: If the MERGE fails.
    """
    table = f"{catalog}.{control_schema}.{TableName.WATERMARK_CONTROL}"
    safe_oid = _escape_sql(object_id)
    safe_pid = _escape_sql(pipeline_id)
    safe_rid = _escape_sql(run_id)

    # last_run_ts is always wall-clock; new_watermark is stored separately.
    # Hash is computed in Python only when new_watermark is available (no Spark dependency).
    if new_watermark is not None:
        wm_value_sql = f"'{_escape_sql(str(new_watermark))}'"
        wm_hash_sql = f"'{hashlib.sha256(str(new_watermark).encode()).hexdigest()}'"
    else:
        wm_value_sql = "NULL"
        wm_hash_sql = "NULL"

    wm_col_sql = f"'{_escape_sql(watermark_col)}'" if watermark_col is not None else "NULL"

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
                last_run_ts = current_timestamp(),
                last_run_id = '{safe_rid}',
                rows_last_loaded = {rows_loaded},
                updated_at = current_timestamp(),
                watermark_col = {wm_col_sql},
                watermark_col_value = {wm_value_sql},
                watermark_col_hash = {wm_hash_sql}
            WHEN NOT MATCHED THEN INSERT
                (object_id, pipeline_id, last_run_ts, last_run_id,
                 rows_last_loaded, updated_at, watermark_col, watermark_col_value, watermark_col_hash)
            VALUES
                ('{safe_oid}', '{safe_pid}', current_timestamp(),
                 '{safe_rid}', {rows_loaded}, current_timestamp(),
                 {wm_col_sql}, {wm_value_sql}, {wm_hash_sql})
        """
        spark.sql(query)
        logger.info( f"[pipeline={pipeline_id}, object={object_id}] Watermark updated: run={run_id}, rows={rows_loaded}, watermark_col={watermark_col}, watermark_col_value={new_watermark}")

    except Exception as e:
        _raise_error(logger, f"pipeline={pipeline_id}, object={object_id}", "Failed to update watermark", e, WatermarkError)
