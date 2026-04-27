"""Tests for quper_control_schema_utils.audit_logger."""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from quper_control_schema_utils.audit_logger import log_run_start, log_run_end
from quper_control_schema_utils.exceptions import AuditLogError


class TestLogRunStart(unittest.TestCase):
    """Test log_run_start function."""

    def test_returns_run_id(self):
        mock_spark = MagicMock()
        run_id = log_run_start(
            mock_spark, "dev", "cdm", "pipe-001", "dev", "schedule", None, None
        )
        self.assertIsInstance(run_id, str)
        self.assertEqual(len(run_id), 36)  # UUID format
        mock_spark.sql.assert_called_once()

    def test_includes_job_ids_when_provided(self):
        mock_spark = MagicMock()
        run_id = log_run_start(
            mock_spark, "dev", "cdm", "pipe-001", "dev", "schedule", "job-123", "jr-456"
        )
        sql_call = mock_spark.sql.call_args[0][0]
        self.assertIn("job-123", sql_call)
        self.assertIn("jr-456", sql_call)

    def test_raises_on_failure(self):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("Connection failed")

        with self.assertRaises(AuditLogError):
            log_run_start(mock_spark, "dev", "cdm", "pipe-001", "dev", "manual", None, None)


class TestLogRunEnd(unittest.TestCase):
    """Test log_run_end function."""

    def test_does_not_raise_on_success(self):
        mock_spark = MagicMock()
        log_run_end(mock_spark, "dev", "cdm", "run-001", "SUCCESS", 5, 5, 0)
        mock_spark.sql.assert_called_once()

    def test_does_not_raise_on_failure(self):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("Connection failed")
        # Must never raise
        log_run_end(mock_spark, "dev", "cdm", "run-001", "FAILED", 5, 0, 5)


@pytest.mark.parametrize("pipeline_id,environment", [
    ("pipe-001", "dev"),
    ("pipe-finance", "qa"),
    ("pipe-hr-nightly", "prod"),
])
def test_log_run_start_embeds_pipeline_id_in_sql(pipeline_id, environment):
    """log_run_start must embed pipeline_id in the INSERT statement."""
    mock_spark = MagicMock()
    run_id = log_run_start(
        mock_spark, "dev", "cdm", pipeline_id, environment, "schedule", None, None
    )
    assert isinstance(run_id, str) and len(run_id) == 36
    sql = mock_spark.sql.call_args[0][0]
    assert pipeline_id in sql


@pytest.mark.parametrize("run_id,status", [
    ("run-001", "SUCCESS"),
    ("run-finance-99", "FAILED"),
    ("run-hr-42", "PARTIAL"),
])
def test_log_run_end_embeds_run_id_and_status_in_sql(run_id, status):
    """log_run_end must embed run_id and status in the MERGE statement."""
    mock_spark = MagicMock()
    log_run_end(mock_spark, "dev", "cdm", run_id, status, 3, 2, 1)
    sql = mock_spark.sql.call_args[0][0]
    assert run_id in sql
    assert status in sql


if __name__ == "__main__":
    unittest.main()
