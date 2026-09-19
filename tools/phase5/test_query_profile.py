"""The diagnostic must exercise the public API and check actual results."""

import argparse
import unittest
from unittest.mock import Mock

from query_profile import PARAMS, PHASES, QUERY, measure, positive


class QueryProfileTests(unittest.TestCase):
    def test_counts_must_be_positive(self):
        self.assertEqual(positive("3"), 3)
        for value in ("0", "-1"):
            with self.assertRaises(argparse.ArgumentTypeError):
                positive(value)

    def test_public_probe_checks_connection_execute_result(self):
        conn = Mock()
        conn.execute.return_value.fetchone.return_value = (42,)
        result = measure(conn, 3, phased=False)
        self.assertEqual(conn.execute.call_count, 3)
        conn.execute.assert_called_with(QUERY, PARAMS)
        conn.cursor.assert_not_called()
        self.assertIsNone(result["phases_us"])
        self.assertGreater(result["wall_us"], 0)
        conn.execute.return_value.fetchone.return_value = (0,)
        with self.assertRaises(AssertionError):
            measure(conn, 1, phased=False)

    def test_phased_probe_checks_cursor_result(self):
        conn = Mock()
        conn.cursor.return_value.fetchone.return_value = (42,)
        result = measure(conn, 3, phased=True)
        self.assertEqual(conn.cursor.call_count, 3)
        self.assertEqual(conn.cursor.return_value.execute.call_count, 3)
        conn.cursor.return_value.execute.assert_called_with(QUERY, PARAMS)
        conn.execute.assert_not_called()
        self.assertEqual(tuple(result["phases_us"]), PHASES)
        self.assertTrue(all(value >= 0 for value in result["phases_us"].values()))
        conn.cursor.return_value.fetchone.return_value = (0,)
        with self.assertRaises(AssertionError):
            measure(conn, 1, phased=True)
