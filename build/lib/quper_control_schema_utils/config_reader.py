"""
Configuration reader for quper_control_schema_utils.

Standalone functions that read from control schema tables and return
typed dataclass instances. Used internally by ControlSchemaClient.
"""

import logging
import traceback
from typing import Any, Dict, List

from quper_control_schema_utils._internal import _escape_sql
from quper_control_schema_utils.exceptions import (
    ConnectionConfigError,
    PipelineConfigError,
    TableNotFoundError,
)
from quper_control_schema_utils.models import (
    ConnectionConfig,
    DQRule,
    MetadataColumn,
    SourceObject,
    TableName,
)

logger = logging.getLogger(__name__)


def get_source_objects(
    spark: Any,
    catalog: str,
    control_schema: str,
    pipeline_id: str,
) -> List[SourceObject]:
    """
    Return active source objects for a pipeline ordered by load_order ASC.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        pipeline_id:    Pipeline identifier.

    Returns:
        List of SourceObject dataclasses.

    Raises:
        TableNotFoundError:  If the source_object_config table cannot be read.
        PipelineConfigError: If the query fails for any other reason.
    """
    table = f"{catalog}.{control_schema}.{TableName.SOURCE_OBJECT_CONFIG}"
    safe_pid = _escape_sql(pipeline_id)
    logger.info(f"[pipeline={pipeline_id}] Loading source objects from {table}")
    try:
        query = f"""
            SELECT *
            FROM {table}
            WHERE pipeline_id = '{safe_pid}'
              AND active_flag = true
            ORDER BY load_order ASC
        """
        rows = spark.sql(query).collect()
        logger.info(f"[pipeline={pipeline_id}] Loaded {len(rows)} active source objects from {table}")

        results = []
        for r in rows:
            results.append(SourceObject(
                object_id=r["object_id"],
                pipeline_id=r["pipeline_id"],
                source_system=r["source_system"],
                source_type=r["source_type"],
                source_schema=r["source_schema"],
                source_object=r["source_object"],
                target_catalog=r["target_catalog"],
                target_schema=r["target_schema"],
                target_table=r["target_table"],
                staging_table=r["staging_table"],
                load_type=r["load_type"],
                write_mode=r["write_mode"],
                primary_key=r["primary_key"],
                watermark_column=r["watermark_column"],
                hash_columns=r["hash_columns"],
                active_flag=r["active_flag"],
                load_order=r["load_order"],
                created_at=r["created_at"],
                updated_at=r["updated_at"],
                created_by=r["created_by"],
                updated_by=r["updated_by"],
            ))
        return results

    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}] Failed to load source objects from {table}: {e}\n{traceback.format_exc()}")
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "AnalysisException" in str(type(e)):
            raise TableNotFoundError(f"Table {table} not found for pipeline={pipeline_id}: {e}") from e
        raise PipelineConfigError(f"Failed to read source objects for pipeline={pipeline_id}: {e}") from e


def get_pipeline_config(
    spark: Any,
    catalog: str,
    control_schema: str,
    pipeline_id: str,
) -> Dict[str, str]:
    """
    Return pipeline config as {config_key: config_value} dict.

    Only active_flag=true rows are returned.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        pipeline_id:    Pipeline identifier.

    Returns:
        Dict mapping config_key to config_value.

    Raises:
        TableNotFoundError:  If the pipeline_config table cannot be read.
        PipelineConfigError: If the table cannot be read or no config found.
    """
    table = f"{catalog}.{control_schema}.{TableName.PIPELINE_CONFIG}"
    safe_pid = _escape_sql(pipeline_id)
    logger.info( f"[pipeline={pipeline_id}] Loading pipeline config from {table}")
    try:
        query = f"""
            SELECT config_key, config_value
            FROM {table}
            WHERE pipeline_id = '{safe_pid}'
              AND active_flag = true
        """
        rows = spark.sql(query).collect()
        config = {r["config_key"]: r["config_value"] for r in rows}
        logger.info( f"[pipeline={pipeline_id}] Loaded {len(config)} pipeline config entries from {table}")
        return config

    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}] Failed to read pipeline config from {table}: {e}\n{traceback.format_exc()}")
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "AnalysisException" in str(type(e)):
            raise TableNotFoundError(f"Table {table} not found: {e}") from e
        raise PipelineConfigError(f"Failed to read pipeline config for pipeline={pipeline_id}: {e}") from e


def get_metadata_columns(
    spark: Any,
    catalog: str,
    control_schema: str,
    pipeline_id: str,
    load_type: str,
) -> List[MetadataColumn]:
    """
    Return metadata columns for a given load_type.

    Lookup order:
      1. Rows where pipeline_id matches AND active_flag = true.
      2. If none found, rows where pipeline_id = 'default' AND active_flag = true.
    Filtered by applies_to IN ('all', load_type). Ordered by column_order ASC.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        pipeline_id:    Pipeline identifier.
        load_type:      Load type string (e.g. 'full_load', 'hash_incremental').

    Returns:
        List of MetadataColumn dataclasses.

    Raises:
        TableNotFoundError:  If the metadata_column_config table cannot be read.
        PipelineConfigError: If the query fails for any other reason.
    """
    table = f"{catalog}.{control_schema}.{TableName.METADATA_COLUMN_CONFIG}"
    safe_pid = _escape_sql(pipeline_id)
    safe_lt = _escape_sql(load_type)
    logger.info( f"[pipeline={pipeline_id}] Loading metadata columns for load_type={load_type} from {table}")
    try:
        query = f"""
            SELECT *
            FROM {table}
            WHERE pipeline_id = '{safe_pid}'
              AND active_flag = true
              AND applies_to IN ('all', '{safe_lt}')
            ORDER BY column_order ASC
        """
        rows = spark.sql(query).collect()

        if not rows:
            query = f"""
                SELECT *
                FROM {table}
                WHERE pipeline_id = 'default'
                  AND active_flag = true
                  AND applies_to IN ('all', '{safe_lt}')
                ORDER BY column_order ASC
            """
            rows = spark.sql(query).collect()
            logger.info( f"[pipeline={pipeline_id}] Using default metadata columns ({len(rows)} rows)")
        else:
            logger.info( f"[pipeline={pipeline_id}] Loaded {len(rows)} metadata columns")

        results = []
        for r in rows:
            results.append(MetadataColumn(
                config_id=r["config_id"],
                pipeline_id=r["pipeline_id"],
                column_name=r["column_name"],
                data_type=r["data_type"],
                applies_to=r["applies_to"],
                computation=r["computation"],
                column_order=r["column_order"],
                is_merge_key=r["is_merge_key"],
                active_flag=r["active_flag"],
            ))
        return results

    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}] Failed to read metadata columns from {table}: {e}\n{traceback.format_exc()}")
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "AnalysisException" in str(type(e)):
            raise TableNotFoundError(f"Table {table} not found: {e}") from e
        raise PipelineConfigError(f"Failed to read metadata columns for pipeline={pipeline_id}: {e}") from e


def get_dq_rules(
    spark: Any,
    catalog: str,
    control_schema: str,
    pipeline_id: str,
    object_id: str,
) -> List[DQRule]:
    """
    Return active DQ rules for a given object_id and pipeline.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        pipeline_id:    Pipeline identifier.
        object_id:      Source object identifier.

    Returns:
        List of DQRule dataclasses.

    Raises:
        TableNotFoundError:  If the dq_rule_config table cannot be read.
        PipelineConfigError: If the query fails for any other reason.
    """
    table = f"{catalog}.{control_schema}.{TableName.DQ_RULE_CONFIG}"
    safe_pid = _escape_sql(pipeline_id)
    safe_oid = _escape_sql(object_id)
    logger.info( f"[pipeline={pipeline_id}, object={object_id}] Loading DQ rules from {table}")
    try:
        query = f"""
            SELECT *
            FROM {table}
            WHERE pipeline_id = '{safe_pid}'
              AND object_id = '{safe_oid}'
              AND active_flag = true
        """
        rows = spark.sql(query).collect()
        logger.info( f"[pipeline={pipeline_id}, object={object_id}] Loaded {len(rows)} DQ rules")

        results = []
        for r in rows:
            results.append(DQRule(
                rule_id=r["rule_id"],
                object_id=r["object_id"],
                pipeline_id=r["pipeline_id"],
                rule_name=r["rule_name"],
                rule_type=r["rule_type"],
                column_name=r["column_name"],
                rule_expression=r["rule_expression"],
                expected_value=r["expected_value"],
                severity=r["severity"],
                active_flag=r["active_flag"],
            ))
        return results

    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}, object={object_id}] Failed to read DQ rules from {table}: {e}\n{traceback.format_exc()}")
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "AnalysisException" in str(type(e)):
            raise TableNotFoundError(f"Table {table} not found: {e}") from e
        raise PipelineConfigError(f"Failed to read DQ rules for object={object_id}: {e}") from e


def get_connection_config(
    spark: Any,
    catalog: str,
    control_schema: str,
    pipeline_id: str,
) -> ConnectionConfig:
    """
    Return active connection config for a pipeline.

    Args:
        spark:          Active SparkSession.
        catalog:        Unity Catalog name.
        control_schema: Control schema name.
        pipeline_id:    Pipeline identifier.

    Returns:
        ConnectionConfig dataclass.

    Raises:
        TableNotFoundError:    If the connection_config table cannot be read.
        ConnectionConfigError: If no active connection config found or query fails.
    """
    table = f"{catalog}.{control_schema}.{TableName.CONNECTION_CONFIG}"
    safe_pid = _escape_sql(pipeline_id)
    logger.info( f"[pipeline={pipeline_id}] Loading connection config from {table}")
    try:
        query = f"""
            SELECT *
            FROM {table}
            WHERE pipeline_id = '{safe_pid}'
              AND active_flag = true
            LIMIT 1
        """
        rows = spark.sql(query).collect()

        if not rows:
            raise ConnectionConfigError(
                f"No active connection config found for pipeline='{pipeline_id}' in {table}"
            )

        r = rows[0]
        logger.info( f"[pipeline={pipeline_id}] Loaded connection config '{r['connection_name']}'")

        return ConnectionConfig(
            connection_id=r["connection_id"],
            pipeline_id=r["pipeline_id"],
            source_system=r["source_system"],
            connection_name=r["connection_name"],
            secret_scope=r["secret_scope"],
            jdbc_url_secret_key=r["jdbc_url_secret_key"],
            jdbc_user_secret_key=r["jdbc_user_secret_key"],
            jdbc_password_secret_key=r["jdbc_password_secret_key"],
            driver_class=r["driver_class"],
            extra_jdbc_options=r["extra_jdbc_options"],
            active_flag=r["active_flag"],
        )

    except ConnectionConfigError:
        raise
    except Exception as e:
        logger.error( f"[pipeline={pipeline_id}] Failed to read connection config from {table}: {e}\n{traceback.format_exc()}")
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "AnalysisException" in str(type(e)):
            raise TableNotFoundError(f"Table {table} not found: {e}") from e
        raise ConnectionConfigError(f"Failed to read connection config for pipeline={pipeline_id}: {e}") from e
