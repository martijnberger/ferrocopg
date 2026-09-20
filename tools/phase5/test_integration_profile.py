"""Cursor-lifetime controls must measure the declared API shape."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from integration_profile import main


class IntegrationProfileTests(unittest.TestCase):
    def run_case(self, case):
        conn = MagicMock()
        conn.cursor.return_value.fetchone.return_value = (42,)
        driver = MagicMock()
        driver.connect.return_value.__enter__.return_value = conn
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            with (
                patch(
                    "sys.argv",
                    [
                        "integration_profile.py",
                        "--backend",
                        "c",
                        "--case",
                        case,
                        "--workload",
                        "parameterized",
                        "--revision",
                        "test",
                        "--output",
                        str(output),
                        "--iterations",
                        "2",
                        "--samples",
                        "3",
                    ],
                ),
                patch.dict("os.environ", {"PHASE5_DSN": "test"}),
                patch("integration_profile.driver_for", return_value=driver),
                patch("integration_profile.metadata", return_value={}),
                patch("integration_profile.installed_files", return_value={}),
            ):
                main()
            report = json.loads(output.read_text())
        self.assertFalse(report["acceptance_evidence"])
        self.assertEqual(len(report["samples"]), 3)
        self.assertEqual(conn.cursor.return_value.execute.call_count, 1006)
        conn.cursor.return_value.execute.assert_called_with(
            "select %s::int4 + 1", (41,), prepare=True, binary=True
        )
        return conn

    def test_fresh_cursor_is_created_per_operation(self):
        conn = self.run_case("public-fresh")
        self.assertEqual(conn.cursor.call_count, 1006)

    def test_reused_cursor_is_created_once_and_closed(self):
        conn = self.run_case("public-reused")
        self.assertEqual(conn.cursor.call_count, 1)
        conn.cursor.return_value.close.assert_called_once()
