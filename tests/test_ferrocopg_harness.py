from pathlib import Path

import pytest

from .fix_ferrocopg import load_manifest, matching_rules

MANIFEST = Path(__file__).with_name("ferrocopg_manifest.toml")


@pytest.mark.parametrize(
    ("nodeid", "tag"),
    [
        ("tests/test_cursor.py::test_default_cursor", "concrete-cursor"),
        ("tests/test_cursor_client.py::test_str", "concrete-cursor"),
        ("tests/test_cursor_common.py::test_execute", "concrete-cursor"),
        ("tests/test_cursor_raw.py::test_execute", "concrete-cursor"),
        ("tests/test_cursor_server.py::test_scroll", "concrete-cursor"),
        ("tests/pool/test_pool_async.py::test_open", "async"),
        ("tests/crdb/test_cursor_async.py::test_execute", "async"),
        (
            "tests/test_connection.py::test_connect_timeout",
            "handshake-timeout",
        ),
        (
            "tests/test_connection.py::test_multi_hosts_timeout",
            "handshake-timeout",
        ),
        ("tests/test_concurrency.py::test_break_attempts", "handshake-timeout"),
    ],
)
def test_unsafe_harness_rules_skip(nodeid: str, tag: str) -> None:
    rules = matching_rules(nodeid, load_manifest(MANIFEST))

    assert any(rule.tag == tag and rule.action == "skip" for rule in rules)


def test_sync_adapter_tests_remain_unmanifested() -> None:
    rules = matching_rules(
        "tests/test_connection.py::test_connect", load_manifest(MANIFEST)
    )

    assert not rules
