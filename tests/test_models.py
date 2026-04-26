"""Tests for quper_control_schema_utils.models."""

import unittest
from datetime import datetime, timezone
from quper_control_schema_utils.models import (
    SourceObject, WatermarkEntry, PipelineConfigEntry, RunAudit,
    RunError, IngestionMetrics, SchemaColumn, SchemaDrift,
    DQRule, DQResult, MetadataColumn, ConnectionConfig,
)


class TestSourceObject(unittest.TestCase):
    """Test SourceObject dataclass."""

    def test_create_with_all_fields(self):
        now = datetime.now(timezone.utc)
        obj = SourceObject(
            object_id="obj-001",
            pipeline_id="pipe-001",
            source_system="sqlserver",
            source_schema="dbo",
            source_object="vw_employees",
            target_catalog="dev",
            target_schema="raw",
            target_table="employees",
            staging_table=None,
            load_type="full_load",
            write_mode="overwrite",
            primary_key="employee_id",
            watermark_column=None,
            hash_columns=None,
            active_flag=True,
            load_order=1,
            created_at=now,
            updated_at=now,
            created_by="admin",
            updated_by="admin",
        )
        self.assertEqual(obj.object_id, "obj-001")
        self.assertEqual(obj.load_type, "full_load")
        self.assertTrue(obj.active_flag)
        self.assertIsNone(obj.staging_table)

    def test_nullable_fields(self):
        now = datetime.now(timezone.utc)
        obj = SourceObject(
            object_id="obj-002",
            pipeline_id="pipe-001",
            source_system="sqlserver",
            source_schema="dbo",
            source_object="vw_orders",
            target_catalog="dev",
            target_schema="raw",
            target_table="orders",
            staging_table="stg_orders",
            load_type="hash_incremental",
            write_mode="merge",
            primary_key="order_id",
            watermark_column="modified_at",
            hash_columns="col_a,col_b",
            active_flag=True,
            load_order=2,
            created_at=now,
            updated_at=now,
            created_by="admin",
            updated_by="admin",
        )
        self.assertEqual(obj.staging_table, "stg_orders")
        self.assertEqual(obj.watermark_column, "modified_at")
        self.assertEqual(obj.hash_columns, "col_a,col_b")


class TestWatermarkEntry(unittest.TestCase):
    """Test WatermarkEntry dataclass."""

    def test_first_run_watermark(self):
        now = datetime.now(timezone.utc)
        wm = WatermarkEntry(
            object_id="obj-001",
            pipeline_id="pipe-001",
            last_run_ts=None,
            last_run_id=None,
            rows_last_loaded=None,
            updated_at=now,
        )
        self.assertIsNone(wm.last_run_ts)
        self.assertIsNone(wm.last_run_id)

    def test_subsequent_run_watermark(self):
        now = datetime.now(timezone.utc)
        wm = WatermarkEntry(
            object_id="obj-001",
            pipeline_id="pipe-001",
            last_run_ts=now,
            last_run_id="run-abc",
            rows_last_loaded=500,
            updated_at=now,
        )
        self.assertEqual(wm.last_run_ts, now)
        self.assertEqual(wm.rows_last_loaded, 500)


class TestPipelineConfigEntry(unittest.TestCase):
    """Test PipelineConfigEntry dataclass."""

    def test_create(self):
        entry = PipelineConfigEntry(
            config_id="cfg-001",
            pipeline_id="pipe-001",
            config_key="max_retries",
            config_value="3",
            description="Maximum retry attempts",
            active_flag=True,
        )
        self.assertEqual(entry.config_key, "max_retries")
        self.assertEqual(entry.config_value, "3")


class TestRunAudit(unittest.TestCase):
    """Test RunAudit dataclass."""

    def test_running_state(self):
        now = datetime.now(timezone.utc)
        audit = RunAudit(
            run_id="run-001",
            pipeline_id="pipe-001",
            job_id=None,
            job_run_id=None,
            triggered_by="schedule",
            start_time=now,
            end_time=None,
            duration_seconds=None,
            status="RUNNING",
            total_objects=None,
            success_objects=None,
            failed_objects=None,
            environment="dev",
            created_at=now,
        )
        self.assertEqual(audit.status, "RUNNING")
        self.assertIsNone(audit.end_time)


class TestRunError(unittest.TestCase):
    """Test RunError dataclass."""

    def test_create(self):
        now = datetime.now(timezone.utc)
        err = RunError(
            error_id="err-001",
            run_id="run-001",
            pipeline_id="pipe-001",
            object_id="obj-001",
            source_object="vw_employees",
            error_type="JDBC_ERROR",
            error_message="Connection refused",
            stack_trace="Traceback...",
            retry_attempt=1,
            error_ts=now,
            environment="dev",
        )
        self.assertEqual(err.error_type, "JDBC_ERROR")


class TestIngestionMetrics(unittest.TestCase):
    """Test IngestionMetrics dataclass."""

    def test_create(self):
        now = datetime.now(timezone.utc)
        m = IngestionMetrics(
            metric_id="met-001",
            run_id="run-001",
            pipeline_id="pipe-001",
            object_id="obj-001",
            source_object="vw_employees",
            target_table="employees",
            load_type="full_load",
            write_mode="overwrite",
            rows_read=1000,
            rows_inserted=1000,
            rows_updated=0,
            rows_deleted=0,
            rows_rejected=0,
            duration_seconds=45,
            status="SUCCESS",
            metric_ts=now,
            environment="dev",
        )
        self.assertEqual(m.rows_read, 1000)
        self.assertEqual(m.status, "SUCCESS")


class TestSchemaColumn(unittest.TestCase):
    """Test SchemaColumn dataclass."""

    def test_create(self):
        now = datetime.now(timezone.utc)
        col = SchemaColumn(
            schema_id="sch-001",
            object_id="obj-001",
            pipeline_id="pipe-001",
            column_name="employee_id",
            data_type="StringType()",
            is_nullable=False,
            is_primary_key=True,
            column_order=1,
            registered_at=now,
            updated_at=now,
            active_flag=True,
        )
        self.assertTrue(col.is_primary_key)
        self.assertFalse(col.is_nullable)


class TestSchemaDrift(unittest.TestCase):
    """Test SchemaDrift dataclass."""

    def test_new_column_drift(self):
        now = datetime.now(timezone.utc)
        drift = SchemaDrift(
            drift_id="drf-001",
            run_id="run-001",
            object_id="obj-001",
            pipeline_id="pipe-001",
            source_object="vw_employees",
            column_name="new_col",
            drift_type="NEW_COLUMN",
            old_value=None,
            new_value="StringType()",
            action_taken="EVOLVED",
            detected_at=now,
            environment="dev",
        )
        self.assertEqual(drift.drift_type, "NEW_COLUMN")
        self.assertEqual(drift.action_taken, "EVOLVED")


class TestDQRule(unittest.TestCase):
    """Test DQRule dataclass."""

    def test_create(self):
        rule = DQRule(
            rule_id="dq-001",
            object_id="obj-001",
            pipeline_id="pipe-001",
            rule_name="not_null_employee_id",
            rule_type="NOT_NULL",
            column_name="employee_id",
            rule_expression="SELECT * FROM {table} WHERE employee_id IS NULL",
            expected_value=None,
            severity="ERROR",
            active_flag=True,
        )
        self.assertEqual(rule.rule_type, "NOT_NULL")
        self.assertEqual(rule.severity, "ERROR")


class TestDQResult(unittest.TestCase):
    """Test DQResult dataclass."""

    def test_passed_result(self):
        now = datetime.now(timezone.utc)
        result = DQResult(
            result_id="res-001",
            run_id="run-001",
            object_id="obj-001",
            rule_id="dq-001",
            pipeline_id="pipe-001",
            rule_name="not_null_check",
            rule_type="NOT_NULL",
            column_name="employee_id",
            rows_checked=1000,
            rows_passed=1000,
            rows_failed=0,
            pass_rate=100.0,
            status="PASSED",
            action_taken="CONTINUED",
            checked_at=now,
            environment="dev",
        )
        self.assertEqual(result.status, "PASSED")
        self.assertEqual(result.pass_rate, 100.0)


class TestMetadataColumn(unittest.TestCase):
    """Test MetadataColumn dataclass."""

    def test_create(self):
        col = MetadataColumn(
            config_id="mc-001",
            pipeline_id="default",
            column_name="_ingestion_ts",
            data_type="TIMESTAMP",
            applies_to="all",
            computation="current_timestamp()",
            column_order=1,
            is_merge_key=False,
            active_flag=True,
        )
        self.assertEqual(col.applies_to, "all")


class TestConnectionConfig(unittest.TestCase):
    """Test ConnectionConfig dataclass."""

    def test_create(self):
        conn = ConnectionConfig(
            connection_id="conn-001",
            pipeline_id="pipe-001",
            source_system="sqlserver",
            connection_name="deltek_prod",
            secret_scope="deltek-secrets",
            jdbc_url_secret_key="jdbc-url",
            jdbc_user_secret_key="jdbc-user",
            jdbc_password_secret_key="jdbc-password",
            driver_class="com.microsoft.sqlserver.jdbc.SQLServerDriver",
            extra_jdbc_options=None,
            active_flag=True,
        )
        self.assertEqual(conn.source_system, "sqlserver")
        self.assertIsNone(conn.extra_jdbc_options)


if __name__ == "__main__":
    unittest.main()
