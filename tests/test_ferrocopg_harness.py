import json
import subprocess
import sys
from pathlib import Path

import pytest

from .fix_ferrocopg import load_manifest, matching_rules

MANIFEST = Path(__file__).with_name("ferrocopg_manifest.toml")
ROOT = Path(__file__).parent.parent
PASS_RATE_SCRIPT = ROOT / "tools" / "ci" / "ferrocopg_pass_rate.py"


@pytest.mark.parametrize(
    ("nodeid", "tag"),
    [
        (
            "tests/test_connection.py::test_context_active_rollback_no_clobber",
            "pgconn",
        ),
        ("tests/pool/test_pool_async.py::test_open", "async"),
        ("tests/crdb/test_cursor_async.py::test_execute", "async"),
        (
            "tests/test_transaction.py::test_context_active_rollback_no_clobber",
            "pgconn",
        ),
        (
            "tests/test_connection_info.py::test_blank_port",
            "pgconn",
        ),
        ("tests/test_waiting.py::test_wait_remote_closed", "pgconn-socket"),
    ],
)
def test_unsafe_harness_rules_skip(nodeid: str, tag: str) -> None:
    rules = matching_rules(nodeid, load_manifest(MANIFEST))

    assert any(rule.tag == tag and rule.action == "skip" for rule in rules)


@pytest.mark.parametrize(
    "nodeid",
    [
        "tests/test_connection.py::test_connect",
        "tests/test_cursor.py::test_execute",
        "tests/test_cursor_raw.py::test_execute",
        "tests/test_cursor_client.py::test_execute",
        "tests/test_cursor_common.py::test_execute[asyncio-ClientCursor]",
        "tests/test_cursor_server.py::test_execute",
    ],
)
def test_sync_adapter_tests_remain_unmanifested(nodeid: str) -> None:
    rules = matching_rules(nodeid, load_manifest(MANIFEST))

    assert not rules


def test_pass_rate_report_splits_sync_and_async(tmp_path: Path) -> None:
    junit = _write_sample_junit(tmp_path)
    manifest = tmp_path / "manifest.toml"
    manifest.write_text("")
    floor = tmp_path / "floor.txt"
    floor.write_text("0.50\n")
    report = tmp_path / "report.json"

    result = subprocess.run(
        [
            sys.executable,
            str(PASS_RATE_SCRIPT),
            str(junit),
            "--manifest",
            str(manifest),
            "--floor",
            str(floor),
            "--sync-floor",
            str(floor),
            "--report",
            str(report),
            "--pytest-status",
            "1",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    data = json.loads(report.read_text())
    assert data["scopes"]["overall"] == {
        "errors": 0,
        "failed": 1,
        "manifested": 1,
        "passed": 2,
        "rate": 0.5,
        "skipped": 1,
        "total": 4,
    }
    assert data["scopes"]["sync"]["passed"] == 1
    assert data["scopes"]["sync"]["total"] == 2
    assert data["scopes"]["async"]["passed"] == 1
    assert data["scopes"]["async"]["total"] == 2
    assert data["families"]["sync"]["connections"]["failed"] == 1
    assert data["families"]["async"]["transactions"]["passed"] == 1
    assert data["families"]["async"]["copy"]["skipped"] == 1


def test_pass_rate_report_preserved_on_abnormal_pytest_exit(tmp_path: Path) -> None:
    junit = _write_sample_junit(tmp_path)
    manifest = tmp_path / "manifest.toml"
    manifest.write_text("")
    floor = tmp_path / "floor.txt"
    floor.write_text("0.0\n")
    report = tmp_path / "report.json"

    result = subprocess.run(
        [
            sys.executable,
            str(PASS_RATE_SCRIPT),
            str(junit),
            "--manifest",
            str(manifest),
            "--floor",
            str(floor),
            "--report",
            str(report),
            "--pytest-status",
            "2",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert "pytest did not complete normally" in result.stderr
    assert json.loads(report.read_text())["pytest_status"] == 2


def test_pass_rate_report_checks_committed_denominator(tmp_path: Path) -> None:
    junit = _write_sample_junit(tmp_path)
    manifest = tmp_path / "manifest.toml"
    manifest.write_text("")
    floor = tmp_path / "floor.txt"
    floor.write_text("0.0\n")
    baseline = tmp_path / "baseline.json"
    expected = {
        "sample": {
            "sync": {"total": 2, "manifested": 1},
            "async": {"total": 2, "manifested": 0},
        }
    }
    baseline.write_text(json.dumps(expected))

    command = [
        sys.executable,
        str(PASS_RATE_SCRIPT),
        str(junit),
        "--manifest",
        str(manifest),
        "--floor",
        str(floor),
        "--baseline",
        str(baseline),
        "--baseline-key",
        "sample",
        "--pytest-status",
        "1",
    ]
    result = subprocess.run(
        command, cwd=ROOT, text=True, capture_output=True, check=False
    )

    assert result.returncode == 0, result.stderr
    assert "denominator satisfied: sample" in result.stdout

    expected["sample"]["sync"]["total"] = 3
    baseline.write_text(json.dumps(expected))
    result = subprocess.run(
        command, cwd=ROOT, text=True, capture_output=True, check=False
    )

    assert result.returncode == 1
    assert "denominator drift for sample" in result.stderr


def test_pass_rate_report_collapses_duplicate_teardown_record(tmp_path: Path) -> None:
    junit = tmp_path / "compat.xml"
    junit.write_text(
        """\
<testsuites><testsuite>
  <testcase classname="tests.test_transaction" name="test_tx">
    <failure message="call failed" />
  </testcase>
  <testcase classname="tests.test_transaction" name="test_tx">
    <error message="teardown failed" />
  </testcase>
</testsuite></testsuites>
"""
    )
    manifest = tmp_path / "manifest.toml"
    manifest.write_text("")
    floor = tmp_path / "floor.txt"
    floor.write_text("0.0\n")
    report = tmp_path / "report.json"

    result = subprocess.run(
        [
            sys.executable,
            str(PASS_RATE_SCRIPT),
            str(junit),
            "--manifest",
            str(manifest),
            "--floor",
            str(floor),
            "--report",
            str(report),
            "--pytest-status",
            "1",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    sync = json.loads(report.read_text())["scopes"]["sync"]
    assert sync["total"] == 1
    assert sync["errors"] == 1
    assert sync["failed"] == 0


def _write_sample_junit(tmp_path: Path) -> Path:
    junit = tmp_path / "compat.xml"
    junit.write_text(
        """\
<testsuites><testsuite>
  <testcase classname="tests.test_connection" name="test_ok" />
  <testcase classname="tests.test_connection" name="test_fail[127.0.0.1]"><failure /></testcase>
  <testcase classname="tests.test_tpc_async.TestTPC" name="test_async_ok" />
  <testcase classname="tests.test_copy_async" name="test_async_skip"><skipped /></testcase>
  <testcase classname="tests.test_cursor" name="test_manifested">
    <skipped message="ferrocopg concrete-cursor: boundary" />
  </testcase>
</testsuite></testsuites>
"""
    )
    return junit
