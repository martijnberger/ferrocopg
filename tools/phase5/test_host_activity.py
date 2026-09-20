"""Host telemetry retains contention without asserting perfect idleness."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from host_activity import HostActivity, interval_record


def sample(clock, processes):
    return {
        "monotonic": clock,
        "unix_time": clock + 1000,
        "cpu_times": {"user": clock * 2, "idle": clock * 3},
        "processes": {
            (pid, birth): dict(pid=pid, ppid=1, name="worker", cpu_seconds=cpu)
            for pid, birth, cpu in processes
        },
        "unavailable": 1,
    }


class HostActivityTests(unittest.TestCase):
    def test_interval_retains_active_new_exited_and_reused_pids(self):
        before = sample(10, [(2, 1, 4), (3, 1, 1), (4, 1, 3)])
        after = sample(12, [(2, 1, 5), (3, 11, 0.2), (5, 11, 0.3)])
        record = interval_record(before, after)
        self.assertEqual(record["active_processes"][0]["one_core_percent"], 50)
        self.assertEqual(record["system_cpu_seconds"], {"user": 4, "idle": 6})
        self.assertEqual([r["pid"] for r in record["new_processes"]], [3, 5])
        self.assertEqual([r["pid"] for r in record["disappeared_processes"]], [3, 4])
        self.assertEqual(record["unavailable_processes"], 1)

    def test_invalid_time_and_cpu_are_not_silently_accepted(self):
        before = sample(10, [(2, 1, 4)])
        for after in (sample(10, [(2, 1, 5)]), sample(11, [(2, 1, 3)])):
            with self.assertRaises(ValueError):
                interval_record(before, after)
        with self.assertRaises(ValueError):
            HostActivity(Path("unused"), interval=0)

    def test_observer_records_final_interval_and_overhead(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "activity.jsonl"
            with patch(
                "host_activity.snapshot",
                side_effect=[sample(10, [(2, 1, 1)]), sample(12, [(2, 1, 2)])],
            ):
                with HostActivity(path, interval=100):
                    pass
            records = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual(len(records), 3)
            self.assertEqual(records[0]["mode"], "host-activity")
            self.assertEqual(records[1]["elapsed_seconds"], 2)
            self.assertEqual(records[2]["samples"], 1)
            self.assertEqual(records[2]["status"], "completed")
            self.assertGreaterEqual(records[2]["observer_thread_cpu_seconds"], 0)
            with self.assertRaises(FileExistsError):
                with HostActivity(path):
                    pass

    def test_observer_failure_is_not_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch(
                "host_activity.snapshot",
                side_effect=[sample(10, []), RuntimeError("snapshot failed")],
            ):
                with self.assertRaisesRegex(RuntimeError, "sampling failed"):
                    with HostActivity(Path(tmp) / "activity.jsonl", interval=100):
                        pass
