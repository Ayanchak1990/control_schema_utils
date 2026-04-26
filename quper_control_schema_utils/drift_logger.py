"""
Drift logger for quper_control_schema_utils.

Handles logging of schema drift events and table evolution (ALTER TABLE).
"""

import logging
import traceback
from typing import Any, List

from quper_control_schema_utils._internal import _escape_sql
from quper_control_schema_utils.exceptions import SchemaDriftHaltError
from quper_control_schema_utils.models import DriftType, SchemaDrift, TableName

logger = logging.getLogger(__name__)


def log_drift(spark: Any, catalog: str, control_schema: str, drifts: List[SchemaDrift]) -> None:
    """
    Insert all SchemaDrift rows into schema_drift_log.

    This function must never raise — on failure it logs a WARNING and returns.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        drifts:         List of SchemaDrift dataclass instances.

    Returns:
        None.
    """
    try:
        if not drifts:
            return

        table = f"{catalog}.{control_schema}.{TableName.SCHEMA_DRIFT_LOG}"
        first = drifts[0]
        logger.info( f"[pipeline={first.pipeline_id}, object={first.object_id}] Logging {len(drifts)} drift event(s) to {table}")

        values = []
        for d in drifts:
            old_val = f"'{_escape_sql(d.old_value)}'" if d.old_value is not None else "NULL"
            new_val = f"'{_escape_sql(d.new_value)}'" if d.new_value is not None else "NULL"
            values.append(
                f"('{_escape_sql(d.drift_id)}', '{_escape_sql(d.run_id)}', "
                f"'{_escape_sql(d.object_id)}', '{_escape_sql(d.pipeline_id)}', "
                f"'{_escape_sql(d.source_object)}', '{_escape_sql(d.column_name)}', "
                f"'{_escape_sql(d.drift_type)}', "
                f"{old_val}, {new_val}, '{_escape_sql(d.action_taken)}', "
                f"current_timestamp(), '{_escape_sql(d.environment)}')"
            )

        values_str = ",\n".join(values)
        query = f"""
            INSERT INTO {table}
            (drift_id, run_id, object_id, pipeline_id, source_object,
             column_name, drift_type, old_value, new_value, action_taken,
             detected_at, environment)
            VALUES
            {values_str}
        """
        spark.sql(query)
        logger.info( f"[pipeline={first.pipeline_id}, object={first.object_id}] Logged {len(drifts)} schema drift event(s)")

    except Exception as e:
        context = f"pipeline={drifts[0].pipeline_id}, object={drifts[0].object_id}" if drifts else "unknown"
        logger.warning( f"[{context}] Failed to log schema drift: {e}\n{traceback.format_exc()}")


def evolve_table(
    spark: Any,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    new_columns: List[SchemaDrift],
) -> None:
    """
    For each NEW_COLUMN drift, execute ALTER TABLE ADD COLUMN.

    Only call when schema_drift_action config = 'EVOLVE'.
    Raises SchemaDriftHaltError if any TYPE_CHANGE drift is present.

    Args:
        spark:          Active SparkSession.
        target_catalog: Target catalog name.
        target_schema:  Target schema name.
        target_table:   Target table name.
        new_columns:    List of SchemaDrift objects to process.

    Returns:
        None.

    Raises:
        SchemaDriftHaltError: If any TYPE_CHANGE drift is found in the list.
    """
    full_table = f"{target_catalog}.{target_schema}.{target_table}"

    type_changes = [d for d in new_columns if d.drift_type == DriftType.TYPE_CHANGE]
    if type_changes:
        cols = ", ".join(d.column_name for d in type_changes)
        raise SchemaDriftHaltError(
            f"TYPE_CHANGE detected on column(s): {cols} in {full_table} — pipeline must halt for manual review"
        )

    logger.info( f"[table={full_table}] Evolving table with {len(new_columns)} new column(s)")
    for d in new_columns:
        if d.drift_type == DriftType.NEW_COLUMN and d.new_value:
            try:
                query = f"""
                    ALTER TABLE {full_table}
                    ADD COLUMN `{d.column_name}` {d.new_value}
                """
                spark.sql(query)
                logger.info( f"[table={full_table}] Added column {d.column_name} {d.new_value}")
            except Exception as e:
                logger.warning( f"[table={full_table}] Failed to add column {d.column_name}: {e}\n{traceback.format_exc()}")
