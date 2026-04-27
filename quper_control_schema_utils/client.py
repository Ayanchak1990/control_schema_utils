"""
Main entry point for quper_control_schema_utils.

ControlSchemaClient wires together all internal modules and exposes a
single, pipeline-friendly API for every control schema operation.
"""

from typing import Any, Dict, List, Optional

from quper_control_schema_utils import audit_logger
from quper_control_schema_utils import config_reader
from quper_control_schema_utils import drift_logger
from quper_control_schema_utils import dq_runner
from quper_control_schema_utils import error_logger
from quper_control_schema_utils import metrics_logger
from quper_control_schema_utils import schema_registry
from quper_control_schema_utils import watermark_manager
from quper_control_schema_utils.models import (
    ConnectionConfig,
    DQResult,
    DQRule,
    IngestionMetrics,
    MetadataColumn,
    SchemaColumn,
    SchemaDrift,
    SourceObject,
    WatermarkEntry,
)


class ControlSchemaClient:
    """
    Main entry point for all control schema operations.

    Usage:
        from quper_control_schema_utils import ControlSchemaClient

        client = ControlSchemaClient(
            spark=spark,
            catalog="dev",
            control_schema="deltek_cdm",
            pipeline_id="raw_employee_pipeline",
            environment="dev"
        )

        run_id  = client.start_run(triggered_by="schedule")
        objects = client.get_source_objects()
        config  = client.get_pipeline_config()

        for obj in objects:
            wm       = client.get_watermark(obj.object_id)
            meta     = client.get_metadata_columns(obj.load_type)
            rules    = client.get_dq_rules(obj.object_id)
            conn     = client.get_connection_config()

            # ... pipeline does extraction + metadata injection + write ...

            results = client.run_dq_checks(run_id, obj.object_id, df, rules)
            client.log_dq_results(results)
            client.log_metrics(metrics)
            client.update_watermark(obj.object_id, run_id, rows_loaded)

        client.end_run(run_id, "SUCCESS", total_objects=2,
                       success_objects=2, failed_objects=0)
    """

    def __init__(
        self,
        spark: Any,
        catalog: str,
        control_schema: str,
        pipeline_id: str,
        environment: str,
    ) -> None:
        """
        Initialise the client.

        Args:
            spark:          Active SparkSession.
            catalog:        Unity Catalog name e.g. 'dev'.
            control_schema: Control schema name e.g. 'deltek_cdm'.
            pipeline_id:    Pipeline identifier e.g. 'raw_employee_pipeline'.
            environment:    Deployment environment: 'dev', 'qa', or 'prod'.

        Returns:
            None.
        """
        self.spark = spark
        self.catalog = catalog
        self.control_schema = control_schema
        self.pipeline_id = pipeline_id
        self.environment = environment

    # ── Config readers ──────────────────────────────────────────────────────

    def get_source_objects(self) -> List[SourceObject]:
        """
        Return active source objects for this pipeline ordered by load_order ASC.

        Returns:
            List of SourceObject dataclasses.

        Raises:
            TableNotFoundError:  If the source_object_config table does not exist.
            PipelineConfigError: If the query fails for any other reason.
        """
        return config_reader.get_source_objects(
            self.spark, self.catalog, self.control_schema, self.pipeline_id
        )

    def get_pipeline_config(self) -> Dict[str, str]:
        """
        Return pipeline config as {config_key: config_value} dict.

        Only active_flag=true rows returned.

        Returns:
            Dict mapping config_key to config_value.

        Raises:
            TableNotFoundError:  If the pipeline_config table does not exist.
            PipelineConfigError: If the query fails or no config found.
        """
        return config_reader.get_pipeline_config(
            self.spark, self.catalog, self.control_schema, self.pipeline_id
        )

    def get_metadata_columns(self, load_type: str) -> List[MetadataColumn]:
        """
        Return metadata columns for given load_type.

        Lookup order:
          1. Rows where pipeline_id = self.pipeline_id AND active_flag = true
          2. If none found -> rows where pipeline_id = 'default' AND active_flag = true
        Filter by applies_to IN ('all', load_type).
        Order by column_order ASC.

        Args:
            load_type: Load type string (e.g. 'full_load', 'hash_incremental').

        Returns:
            List of MetadataColumn dataclasses.

        Raises:
            TableNotFoundError:  If the metadata_column_config table does not exist.
            PipelineConfigError: If the query fails for any other reason.
        """
        return config_reader.get_metadata_columns(
            self.spark, self.catalog, self.control_schema, self.pipeline_id, load_type
        )

    def get_dq_rules(self, object_id: str) -> List[DQRule]:
        """
        Return active DQ rules for given object_id and this pipeline.

        Args:
            object_id: Source object identifier.

        Returns:
            List of DQRule dataclasses.

        Raises:
            TableNotFoundError:  If the dq_rule_config table does not exist.
            PipelineConfigError: If the query fails for any other reason.
        """
        return config_reader.get_dq_rules(
            self.spark, self.catalog, self.control_schema, self.pipeline_id, object_id
        )

    def get_connection_config(self) -> ConnectionConfig:
        """
        Return active connection config for this pipeline.

        Returns:
            ConnectionConfig dataclass.

        Raises:
            TableNotFoundError:    If the connection_config table does not exist.
            ConnectionConfigError: If not found or not active.
        """
        return config_reader.get_connection_config(
            self.spark, self.catalog, self.control_schema, self.pipeline_id
        )

    # ── Watermark ───────────────────────────────────────────────────────────

    def get_watermark(self, object_id: str) -> Optional[WatermarkEntry]:
        """
        Return watermark for object.

        Returns None if no row exists (first run — triggers full load).
        last_run_ts = None also means first run.

        Args:
            object_id: Source object identifier.

        Returns:
            WatermarkEntry or None.

        Raises:
            WatermarkError: If the query fails unexpectedly.
        """
        return watermark_manager.get_watermark(
            self.spark, self.catalog, self.control_schema, object_id, self.pipeline_id
        )

    def update_watermark(
        self,
        object_id: str,
        run_id: str,
        rows_loaded: int,
        watermark_col= None,
        new_watermark=None,
    ) -> None:
        """
        MERGE INTO watermark_control on (object_id + pipeline_id).

        Only call this after a confirmed successful write — never before.

        Args:
            object_id:      Source object identifier.
            run_id:         Current run identifier.
            rows_loaded:    Number of rows loaded in this run.
            watermark_col:  Name of the watermark column used for this run.
            new_watermark:  MAX(watermark_column) captured from source data.
                            Stored in watermark_col_value; NULL when not supplied.

        Returns:
            None.

        Raises:
            WatermarkError: If the MERGE fails.
        """
        watermark_manager.update_watermark(
            self.spark, self.catalog, self.control_schema,
            object_id, self.pipeline_id, run_id, rows_loaded,
            watermark_col=watermark_col, new_watermark=new_watermark,
        )

    # ── Audit ────────────────────────────────────────────────────────────────

    def start_run(
        self,
        triggered_by: str = "unknown",
        triggered_mode: str = "manual",
        job_id: Optional[str] = None,
        job_run_id: Optional[str] = None,
    ) -> str:
        """
        INSERT row into job_run_audit with status='RUNNING'.

        Args:
            triggered_by:   Email of the user who triggered the run, or 'unknown'.
            triggered_mode: 'schedule' if run via Databricks job, 'manual' otherwise.
            job_id:         Optional Databricks job ID.
            job_run_id:     Optional Databricks job run ID.

        Returns:
            run_id (UUID string).

        Raises:
            AuditLogError: If the INSERT fails.
        """
        return audit_logger.log_run_start(
            self.spark, self.catalog, self.control_schema,
            self.pipeline_id, self.environment, triggered_by, triggered_mode, job_id, job_run_id
        )

    def end_run(
        self,
        run_id: str,
        status: str,
        total_objects: int,
        success_objects: int,
        failed_objects: int,
    ) -> None:
        """
        MERGE INTO job_run_audit on run_id to set end_time, duration, status, counts.

        Must never raise — if this fails, log warning and swallow.

        Args:
            run_id:          The run_id to update.
            status:          Final status: 'SUCCESS', 'FAILED', or 'PARTIAL'.
            total_objects:   Total number of objects processed.
            success_objects: Successfully processed objects.
            failed_objects:  Failed objects.

        Returns:
            None.
        """
        audit_logger.log_run_end(
            self.spark, self.catalog, self.control_schema,
            run_id, status, total_objects, success_objects, failed_objects
        )

    def log_error(
        self,
        run_id: str,
        object_id: str,
        source_object: str,
        error_type: str,
        error_message: str,
        stack_trace: str,
        retry_attempt: int,
    ) -> None:
        """
        INSERT row into job_run_error.

        Must never raise — logging a failure must never crash the pipeline.

        Args:
            run_id:         Parent run identifier.
            object_id:      Source object identifier.
            source_object:  Source object name.
            error_type:     Error classification.
            error_message:  Human-readable error description.
            stack_trace:    Full traceback string.
            retry_attempt:  Current retry attempt number.

        Returns:
            None.
        """
        error_logger.log_error(
            self.spark, self.catalog, self.control_schema,
            run_id, self.pipeline_id, object_id, source_object,
            error_type, error_message, stack_trace, retry_attempt, self.environment
        )

    # ── Metrics ──────────────────────────────────────────────────────────────

    def log_metrics(self, metrics: IngestionMetrics) -> None:
        """
        INSERT row into ingestion_metrics. Must never raise.

        Args:
            metrics: IngestionMetrics dataclass instance.

        Returns:
            None.
        """
        metrics_logger.log_metrics(
            self.spark, self.catalog, self.control_schema, metrics
        )

    # ── Schema registry ──────────────────────────────────────────────────────

    def get_registered_schema(self, object_id: str) -> List[SchemaColumn]:
        """
        Return registered schema columns for object (active_flag=true only).

        Returns empty list if never registered (first run).

        Args:
            object_id: Source object identifier.

        Returns:
            List of SchemaColumn dataclasses.

        Raises:
            SchemaRegistryError: If the query fails unexpectedly.
        """
        return schema_registry.get_registered_schema(
            self.spark, self.catalog, self.control_schema, object_id, self.pipeline_id
        )

    def register_schema(
        self,
        object_id: str,
        df_schema: Any,
        primary_key_cols: List[str],
    ) -> None:
        """
        INSERT one row per column into object_schema_registry.

        Only call when get_registered_schema returns empty list.

        Args:
            object_id:        Source object identifier.
            df_schema:        PySpark StructType.
            primary_key_cols: List of primary key column names.

        Returns:
            None.

        Raises:
            SchemaRegistryError: If the INSERT fails.
        """
        schema_registry.register_schema(
            self.spark, self.catalog, self.control_schema,
            object_id, self.pipeline_id, df_schema, primary_key_cols
        )

    def detect_drift(
        self,
        object_id: str,
        pipeline_id: str,
        source_object: str,
        run_id: str,
        registered: List[SchemaColumn],
        live_schema: Any,
    ) -> List[SchemaDrift]:
        """
        Pure comparison — no Spark needed.

        Compare registered columns vs live_schema (PySpark StructType) fields.

        Args:
            object_id:     Source object identifier.
            pipeline_id:   Pipeline identifier.
            source_object: Source object name.
            run_id:        Current run identifier.
            registered:    List of SchemaColumn from the registry.
            live_schema:   PySpark StructType.

        Returns:
            List of SchemaDrift objects.
        """
        return schema_registry.detect_drift(
            object_id, pipeline_id, source_object, run_id,
            registered, live_schema, self.environment
        )

    # ── Drift ─────────────────────────────────────────────────────────────────

    def log_drift(self, drifts: List[SchemaDrift]) -> None:
        """
        INSERT all SchemaDrift rows into schema_drift_log. Must never raise.

        Args:
            drifts: List of SchemaDrift dataclass instances.

        Returns:
            None.
        """
        drift_logger.log_drift(
            self.spark, self.catalog, self.control_schema, drifts
        )

    def evolve_table(
        self,
        target_catalog: str,
        target_schema: str,
        target_table: str,
        new_columns: List[SchemaDrift],
    ) -> None:
        """
        For each NEW_COLUMN drift: ALTER TABLE ADD COLUMN.

        Only call when schema_drift_action config = 'EVOLVE'.

        Args:
            target_catalog: Target catalog name.
            target_schema:  Target schema name.
            target_table:   Target table name.
            new_columns:    List of SchemaDrift objects.

        Returns:
            None.

        Raises:
            SchemaDriftHaltError: If any TYPE_CHANGE drift is present.
        """
        drift_logger.evolve_table(
            self.spark, target_catalog, target_schema, target_table, new_columns
        )

    # ── DQ ───────────────────────────────────────────────────────────────────

    def run_dq_checks(
        self,
        run_id: str,
        object_id: str,
        df: Any,
        rules: List[DQRule],
    ) -> List[DQResult]:
        """
        Run all DQ rules against the DataFrame.

        After all rules: if any ERROR severity rule FAILED, raises DQFailureError.

        Args:
            run_id:    Current run identifier.
            object_id: Source object identifier.
            df:        PySpark DataFrame to validate.
            rules:     List of DQRule dataclass instances.

        Returns:
            List of DQResult dataclass instances.

        Raises:
            DQFailureError: If any ERROR severity rule fails.
        """
        return dq_runner.run_dq_checks(
            self.spark, run_id, self.pipeline_id, object_id,
            df, rules, self.environment
        )

    def log_dq_results(self, results: List[DQResult]) -> None:
        """
        INSERT all DQResult rows into dq_check_results. Must never raise.

        Args:
            results: List of DQResult dataclass instances.

        Returns:
            None.
        """
        dq_runner.log_dq_results(
            self.spark, self.catalog, self.control_schema, results
        )

    def quarantine_failed_records(
        self,
        run_id: str,
        object_id: str,
        source_object: str,
        df: Any,
        rules: Any,
        dq_results: Any,
    ):
        """
        Split df into clean records and quarantine records for WARNING-severity failures.

        Failing rows (NOT_NULL / RANGE rule violations) are written to dq_quarantine
        with rejection metadata.  The returned DataFrame contains only clean records
        and is safe to pass directly to the merge writer.

        Args:
            run_id:        Current run identifier.
            object_id:     Source object identifier.
            source_object: Source object name (e.g. "yellow_tripdata").
            df:            Enriched DataFrame (post-metadata injection).
            rules:         DQRule list evaluated this run.
            dq_results:    DQResult list from run_dq_checks() for this run.

        Returns:
            Tuple of (clean_df, rows_rejected).
        """
        return dq_runner.split_and_quarantine(
            self.spark, self.catalog, self.control_schema,
            run_id, self.pipeline_id, object_id, source_object,
            df, rules, dq_results, self.environment,
        )
