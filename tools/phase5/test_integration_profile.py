"""Cursor-lifetime controls must measure the declared API shape."""

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from integration_profile import BOOKKEEPING_HELPERS, bookkeeping_control, main


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
        self.assertEqual(report["bookkeeping"], "normal")
        self.assertEqual(report["omitted_helper_bodies"], [])
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

    def test_bookkeeping_control_restores_methods_after_failure(self):
        class Connection:
            autocommit = True
            info = SimpleNamespace(encoding="utf-8")
            _in_transaction = False
            _transaction_failed = False

            def helper(self, query):
                return query

            _refresh_client_encoding = helper
            _refresh_session_timeout = helper
            _update_transaction_state = helper

        conn = Connection()
        with self.assertRaisesRegex(RuntimeError, "execution failed"):
            with bookkeeping_control(conn, "omit-helper-bodies"):
                for name in BOOKKEEPING_HELPERS:
                    self.assertIsNone(getattr(conn, name)("select 42::int4"))
                self.assertIsNone(
                    conn._update_transaction_state(
                        "select 42::int4", transaction_status=ord("I")
                    )
                )
                raise RuntimeError("execution failed")
        for name in BOOKKEEPING_HELPERS:
            self.assertNotIn(name, vars(conn))
            self.assertEqual(getattr(conn, name)("original"), "original")

        for attribute, value in (
            ("autocommit", False),
            ("info", SimpleNamespace(encoding="latin-1")),
            ("_in_transaction", True),
            ("_transaction_failed", True),
            ("_refresh_client_encoding", lambda query: None),
        ):
            with patch.object(conn, attribute, value):
                with self.assertRaises(ValueError):
                    with bookkeeping_control(conn, "omit-helper-bodies"):
                        self.fail("unsafe diagnostic connection accepted")

    def test_bookkeeping_ablation_rejects_c_backend(self):
        with (
            patch(
                "sys.argv",
                [
                    "integration_profile.py",
                    "--backend",
                    "c",
                    "--case",
                    "public-fresh",
                    "--workload",
                    "constant",
                    "--revision",
                    "test",
                    "--output",
                    "unused.json",
                    "--bookkeeping",
                    "omit-helper-bodies",
                ],
            ),
            patch("sys.stderr"),
            patch("integration_profile.driver_for") as driver,
            self.assertRaises(SystemExit),
        ):
            main()
        driver.assert_not_called()
