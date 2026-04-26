"""Tests for quper_control_schema_utils.watermark_manager."""

import unittest
from unittest.mock import MagicMock
from datetime import datetime, timezone

from quper_control_schema_utils.watermark_manager import get_watermark, update_watermark
from quper_control_schema_utils.exceptions import WatermarkError


def _mock_row(data: dict):
    """Create a mock Spark Row that supports dict-style access."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: data[key]
    return row


class TestGetWatermark(unittest.TestCase):
    """Test get_watermark function."""

    def test_returns_none_when_no_row(self):
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []

        result = get_watermark(mock_spark, "dev", "cdm", "obj-001", "pipe-001")
        self.assertIsNone(result)

    def test_returns_watermark_entry(self):
        now = datetime.now(timezone.utc)
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            _mock_row({
                "object_id": "obj-001",
                "pipeline_id": "pipe-001",
                "last_run_ts": now,
                "last_run_id": "run-abc",
                "rows_last_loaded": 500,
                "updated_at": now,
            })
        ]

        result = get_watermark(mock_spark, "dev", "cdm", "obj-001", "pipe-001")
        self.assertIsNotNone(result)
        self.assertEqual(result.object_id, "obj-001")
        self.assertEqual(result.rows_last_loaded, 500)

    def test_raises_on_failure(self):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("Query failed")

        with self.assertRaises(WatermarkError):
            get_watermark(mock_spark, "dev", "cdm", "obj-001", "pipe-001")


class TestUpdateWatermark(unittest.TestCase):
    """Test update_watermark function."""

    def test_executes_merge(self):
        mock_spark = MagicMock()
        update_watermark(mock_spark, "dev", "cdm", "obj-001", "pipe-001", "run-001", 1000)
        mock_spark.sql.assert_called_once()
        sql_call = mock_spark.sql.call_args[0][0]
        self.assertIn("MERGE INTO", sql_call)
        self.assertIn("obj-001", sql_call)

    def test_raises_on_failure(self):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("Merge failed")

        with self.assertRaises(WatermarkError):
            update_watermark(mock_spark, "dev", "cdm", "obj-001", "pipe-001", "run-001", 1000)


if __name__ == "__main__":
    unittest.main()
