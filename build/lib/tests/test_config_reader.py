"""Tests for quper_control_schema_utils.config_reader."""

import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import pytest

from quper_control_schema_utils.config_reader import (
    get_source_objects,
    get_pipeline_config,
    get_metadata_columns,
    get_dq_rules,
    get_connection_config,
)
from quper_control_schema_utils.exceptions import (
    ConnectionConfigError,
    TableNotFoundError,
)


def _mock_row(data: dict):
    """Create a mock Spark Row that supports dict-style access."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: data[key]
    return row


class TestGetSourceObjects(unittest.TestCase):
    """Test get_source_objects function."""

    def test_returns_source_objects(self):
        now = datetime.now(timezone.utc)
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            _mock_row({
                "object_id": "obj-001",
                "pipeline_id": "pipe-001",
                "source_system": "sqlserver",
                "source_schema": "dbo",
                "source_object": "vw_employees",
                "target_catalog": "dev",
                "target_schema": "raw",
                "target_table": "employees",
                "staging_table": None,
                "load_type": "full_load",
                "write_mode": "overwrite",
                "primary_key": "employee_id",
                "watermark_column": None,
                "hash_columns": None,
                "active_flag": True,
                "load_order": 1,
                "created_at": now,
                "updated_at": now,
                "created_by": "admin",
                "updated_by": "admin",
            })
        ]

        results = get_source_objects(mock_spark, "dev", "cdm", "pipe-001")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].object_id, "obj-001")
        self.assertEqual(results[0].load_type, "full_load")

    def test_empty_result(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []
        results = get_source_objects(mock_spark, "dev", "cdm", "pipe-001")
        self.assertEqual(len(results), 0)


class TestGetPipelineConfig(unittest.TestCase):
    """Test get_pipeline_config function."""

    def test_returns_config_dict(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            _mock_row({"config_key": "max_retries", "config_value": "3"}),
            _mock_row({"config_key": "batch_size", "config_value": "10000"}),
        ]

        config = get_pipeline_config(mock_spark, "dev", "cdm", "pipe-001")
        self.assertEqual(config["max_retries"], "3")
        self.assertEqual(config["batch_size"], "10000")
        self.assertEqual(len(config), 2)


class TestGetMetadataColumns(unittest.TestCase):
    """Test get_metadata_columns function."""

    def test_returns_pipeline_specific_columns(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            _mock_row({
                "config_id": "mc-001",
                "pipeline_id": "pipe-001",
                "column_name": "_ingestion_ts",
                "data_type": "TIMESTAMP",
                "applies_to": "all",
                "computation": "current_timestamp()",
                "column_order": 1,
                "is_merge_key": False,
                "active_flag": True,
            })
        ]

        results = get_metadata_columns(mock_spark, "dev", "cdm", "pipe-001", "full_load")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].column_name, "_ingestion_ts")

    def test_falls_back_to_default(self):
        mock_spark = MagicMock()
        # First call returns empty (pipeline-specific), second returns default
        mock_spark.sql.return_value.collect.side_effect = [
            [],
            [_mock_row({
                "config_id": "mc-default",
                "pipeline_id": "default",
                "column_name": "_ingestion_ts",
                "data_type": "TIMESTAMP",
                "applies_to": "all",
                "computation": "current_timestamp()",
                "column_order": 1,
                "is_merge_key": False,
                "active_flag": True,
            })],
        ]

        results = get_metadata_columns(mock_spark, "dev", "cdm", "pipe-001", "full_load")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].pipeline_id, "default")


class TestGetDQRules(unittest.TestCase):
    """Test get_dq_rules function."""

    def test_returns_rules(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            _mock_row({
                "rule_id": "dq-001",
                "object_id": "obj-001",
                "pipeline_id": "pipe-001",
                "rule_name": "not_null_check",
                "rule_type": "NOT_NULL",
                "column_name": "employee_id",
                "rule_expression": "SELECT * FROM {table} WHERE employee_id IS NULL",
                "expected_value": None,
                "severity": "ERROR",
                "active_flag": True,
            })
        ]

        results = get_dq_rules(mock_spark, "dev", "cdm", "pipe-001", "obj-001")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].rule_name, "not_null_check")
        self.assertEqual(results[0].severity, "ERROR")


class TestGetConnectionConfig(unittest.TestCase):
    """Test get_connection_config function."""

    def test_returns_connection(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            _mock_row({
                "connection_id": "conn-001",
                "pipeline_id": "pipe-001",
                "source_system": "sqlserver",
                "connection_name": "deltek_prod",
                "secret_scope": "deltek-secrets",
                "jdbc_url_secret_key": "jdbc-url",
                "jdbc_user_secret_key": "jdbc-user",
                "jdbc_password_secret_key": "jdbc-password",
                "driver_class": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "extra_jdbc_options": None,
                "active_flag": True,
            })
        ]

        result = get_connection_config(mock_spark, "dev", "cdm", "pipe-001")
        self.assertEqual(result.connection_name, "deltek_prod")
        self.assertEqual(result.source_system, "sqlserver")

    def test_raises_on_not_found(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []

        with self.assertRaises(ConnectionConfigError):
            get_connection_config(mock_spark, "dev", "cdm", "pipe-001")


@pytest.mark.parametrize("pipeline_id", [
    "pipe-001",
    "pipe-finance",
    "pipe-hr-nightly",
])
def test_get_source_objects_embeds_pipeline_id_in_sql(pipeline_id):
    """get_source_objects must scope its query to the supplied pipeline_id."""
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = []
    get_source_objects(mock_spark, "dev", "cdm", pipeline_id)
    sql = mock_spark.sql.call_args[0][0]
    assert pipeline_id in sql


@pytest.mark.parametrize("pipeline_id,object_id", [
    ("pipe-001", "obj-001"),
    ("pipe-finance", "obj-invoices"),
    ("pipe-hr", "obj-timecards"),
])
def test_get_dq_rules_embeds_ids_in_sql(pipeline_id, object_id):
    """get_dq_rules must scope its query to both pipeline_id and object_id."""
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = []
    get_dq_rules(mock_spark, "dev", "cdm", pipeline_id, object_id)
    sql = mock_spark.sql.call_args[0][0]
    assert pipeline_id in sql
    assert object_id in sql


@pytest.mark.parametrize("pipeline_id", [
    "pipe-001",
    "pipe-finance",
    "pipe-hr-nightly",
])
def test_get_pipeline_config_embeds_pipeline_id_in_sql(pipeline_id):
    """get_pipeline_config must scope its query to the supplied pipeline_id."""
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = []
    get_pipeline_config(mock_spark, "dev", "cdm", pipeline_id)
    sql = mock_spark.sql.call_args[0][0]
    assert pipeline_id in sql


if __name__ == "__main__":
    unittest.main()
