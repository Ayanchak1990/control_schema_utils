"""
Schema registry for quper_control_schema_utils.

Handles schema registration, retrieval, and drift detection against
the object_schema_registry table.
"""

import logging
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, List

from quper_control_schema_utils._internal import _escape_sql
from quper_control_schema_utils.exceptions import SchemaRegistryError
from quper_control_schema_utils.models import (
    DriftAction,
    DriftType,
    SchemaColumn,
    SchemaDrift,
    TableName,
)

logger = logging.getLogger(__name__)


def get_registered_schema(
    spark: Any,
    catalog: str,
    control_schema: str,
    object_id: str,
    pipeline_id: str,
) -> List[SchemaColumn]:
    """
    Return registered schema columns for an object (active_flag=true only).

    Returns an empty list if never registered (first run).

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        object_id:      Source object identifier.
        pipeline_id:    Pipeline identifier.

    Returns:
        List of SchemaColumn dataclasses.

    Raises:
        SchemaRegistryError: If the query fails unexpectedly.
    """
    table = f"{catalog}.{control_schema}.{TableName.OBJECT_SCHEMA_REGISTRY}"
    safe_oid = _escape_sql(object_id)
    safe_pid = _escape_sql(pipeline_id)
    logger.info( f"[pipeline={pipeline_id}, object={object_id}] Loading registered schema from {table}")
    try:
        query = f"""
            SELECT *
            FROM {table}
            WHERE object_id = '{safe_oid}'
              AND pipeline_id = '{safe_pid}'
              AND active_flag = true
            ORDER BY column_order ASC
        """
        rows = spark.sql(query).collect()
        logger.info( f"[pipeline={pipeline_id}, object={object_id}] Loaded {len(rows)} registered schema columns")

        results = []
        for r in rows:
            results.append(SchemaColumn(
                schema_id=r["schema_id"],
                object_id=r["object_id"],
                pipeline_id=r["pipeline_id"],
                column_name=r["column_name"],
                data_type=r["data_type"],
                is_nullable=r["is_nullable"],
                is_primary_key=r["is_primary_key"],
                column_order=r["column_order"],
                registered_at=r["registered_at"],
                updated_at=r["updated_at"],
                active_flag=r["active_flag"],
            ))
        return results

    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}, object={object_id}] Failed to read schema registry: {e}\n{traceback.format_exc()}")
        raise SchemaRegistryError(
            f"Failed to read schema registry for object={object_id}, pipeline={pipeline_id}: {e}"
        ) from e


def register_schema(
    spark: Any,
    catalog: str,
    control_schema: str,
    object_id: str,
    pipeline_id: str,
    df_schema: Any,
    primary_key_cols: List[str],
) -> None:
    """
    Insert one row per column into object_schema_registry.

    Only call when get_registered_schema returns an empty list.

    Args:
        spark:            Active SparkSession.
        catalog:          Unity Catalog name.
        control_schema:   Control schema name.
        object_id:        Source object identifier.
        pipeline_id:      Pipeline identifier.
        df_schema:        PySpark StructType from the DataFrame.
        primary_key_cols: List of column names that form the primary key.

    Returns:
        None.

    Raises:
        SchemaRegistryError: If the INSERT fails.
    """
    table = f"{catalog}.{control_schema}.{TableName.OBJECT_SCHEMA_REGISTRY}"
    safe_oid = _escape_sql(object_id)
    safe_pid = _escape_sql(pipeline_id)
    logger.info( f"[pipeline={pipeline_id}, object={object_id}] Registering schema ({len(df_schema.fields)} columns) into {table}")
    try:
        values = []
        for idx, field in enumerate(df_schema.fields):
            schema_id = str(uuid.uuid4())
            is_nullable = "true" if field.nullable else "false"
            is_pk = "true" if field.name in primary_key_cols else "false"
            data_type = _escape_sql(str(field.dataType))
            col_name = _escape_sql(field.name)

            values.append(
                f"('{_escape_sql(schema_id)}', '{safe_oid}', '{safe_pid}', "
                f"'{col_name}', '{data_type}', {is_nullable}, {is_pk}, "
                f"{idx + 1}, current_timestamp(), current_timestamp(), true)"
            )

        values_str = ",\n".join(values)
        query = f"""
            INSERT INTO {table}
            (schema_id, object_id, pipeline_id, column_name, data_type,
             is_nullable, is_primary_key, column_order,
             registered_at, updated_at, active_flag)
            VALUES
            {values_str}
        """
        spark.sql(query)
        logger.info( f"[pipeline={pipeline_id}, object={object_id}] Registered {len(df_schema.fields)} columns")

    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}, object={object_id}] Failed to register schema: {e}\n{traceback.format_exc()}")
        raise SchemaRegistryError(
            f"Failed to register schema for object={object_id}, pipeline={pipeline_id}: {e}"
        ) from e


def detect_drift(
    object_id: str,
    pipeline_id: str,
    source_object: str,
    run_id: str,
    registered: List[SchemaColumn],
    live_schema: Any,
    environment: str,
) -> List[SchemaDrift]:
    """
    Pure comparison — no Spark needed.

    Compare registered columns vs live_schema (PySpark StructType) fields.

    Drift rules:
      NEW_COLUMN:      in live, not in registered -> action_taken = EVOLVED
      DROPPED_COLUMN:  in registered, not in live -> action_taken = WARNED
      TYPE_CHANGE:     exists in both, data_type differs -> action_taken = HALTED
      NULLABLE_CHANGE: exists in both, is_nullable differs -> action_taken = WARNED

    Args:
        object_id:     Source object identifier.
        pipeline_id:   Pipeline identifier.
        source_object: Source object name.
        run_id:        Current run identifier.
        registered:    List of SchemaColumn from the registry.
        live_schema:   PySpark StructType from the live DataFrame.
        environment:   Deployment environment.

    Returns:
        List of SchemaDrift objects (empty if no drift detected).
    """
    logger.info( f"[pipeline={pipeline_id}, object={object_id}] Detecting schema drift")
    now = datetime.now(timezone.utc)
    drifts: List[SchemaDrift] = []

    registered_map = {col.column_name: col for col in registered}

    live_map = {}
    for field in live_schema.fields:
        live_map[field.name] = {
            "data_type": str(field.dataType),
            "is_nullable": field.nullable,
        }

    # Check for NEW_COLUMN and changes
    for col_name, live_info in live_map.items():
        if col_name not in registered_map:
            drifts.append(SchemaDrift(
                drift_id=str(uuid.uuid4()),
                run_id=run_id,
                object_id=object_id,
                pipeline_id=pipeline_id,
                source_object=source_object,
                column_name=col_name,
                drift_type=DriftType.NEW_COLUMN,
                old_value=None,
                new_value=live_info["data_type"],
                action_taken=DriftAction.EVOLVED,
                detected_at=now,
                environment=environment,
            ))
        else:
            reg_col = registered_map[col_name]

            if live_info["data_type"] != reg_col.data_type:
                drifts.append(SchemaDrift(
                    drift_id=str(uuid.uuid4()),
                    run_id=run_id,
                    object_id=object_id,
                    pipeline_id=pipeline_id,
                    source_object=source_object,
                    column_name=col_name,
                    drift_type=DriftType.TYPE_CHANGE,
                    old_value=reg_col.data_type,
                    new_value=live_info["data_type"],
                    action_taken=DriftAction.HALTED,
                    detected_at=now,
                    environment=environment,
                ))

            if live_info["is_nullable"] != reg_col.is_nullable:
                old_nullable = str(reg_col.is_nullable)
                new_nullable = str(live_info["is_nullable"])
                drifts.append(SchemaDrift(
                    drift_id=str(uuid.uuid4()),
                    run_id=run_id,
                    object_id=object_id,
                    pipeline_id=pipeline_id,
                    source_object=source_object,
                    column_name=col_name,
                    drift_type=DriftType.NULLABLE_CHANGE,
                    old_value=old_nullable,
                    new_value=new_nullable,
                    action_taken=DriftAction.WARNED,
                    detected_at=now,
                    environment=environment,
                ))

    # Check for DROPPED_COLUMN
    for col_name in registered_map:
        if col_name not in live_map:
            drifts.append(SchemaDrift(
                drift_id=str(uuid.uuid4()),
                run_id=run_id,
                object_id=object_id,
                pipeline_id=pipeline_id,
                source_object=source_object,
                column_name=col_name,
                drift_type=DriftType.DROPPED_COLUMN,
                old_value=registered_map[col_name].data_type,
                new_value=None,
                action_taken=DriftAction.WARNED,
                detected_at=now,
                environment=environment,
            ))

    if drifts:
        logger.warning( f"[pipeline={pipeline_id}, object={object_id}] Detected {len(drifts)} schema drift(s)")
    else:
        logger.info( f"[pipeline={pipeline_id}, object={object_id}] No schema drift detected")

    return drifts
