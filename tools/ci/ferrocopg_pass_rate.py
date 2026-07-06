#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def main() -> int:
    args = parse_args()
    if args.pytest_status not in (0, 1):
        print(
            f"pytest did not complete normally: exit status {args.pytest_status}",
            file=sys.stderr,
        )
        return args.pytest_status

    floor = float(args.floor.read_text().strip())
    manifested = _load_manifest_matcher(args.manifest)
    total = passed = skipped_manifested = 0

    for testcase in ET.parse(args.junit).getroot().iter("testcase"):
        nodeid = _nodeid_from_testcase(testcase)
        if manifested(nodeid) or _has_manifested_skip(testcase):
            skipped_manifested += 1
            continue

        total += 1
        failed = testcase.find("failure") is not None
        errored = testcase.find("error") is not None
        skipped = testcase.find("skipped") is not None
        if not (failed or errored or skipped):
            passed += 1

    rate = 1.0 if total == 0 else passed / total
    print(
        "ferrocopg pass rate: "
        f"{passed}/{total} ({rate:.3f}); "
        f"manifested={skipped_manifested}; floor={floor:.3f}"
    )
    if rate + 1e-9 < floor:
        print(
            "ferrocopg pass rate regressed below the committed floor", file=sys.stderr
        )
        return 1
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("junit", type=Path)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--floor", type=Path, required=True)
    parser.add_argument("--pytest-status", type=int, default=0)
    return parser.parse_args()


def _load_manifest_matcher(manifest: Path):
    sys.path.insert(0, str(manifest.parent.parent))
    from tests.fix_ferrocopg import is_manifested_nodeid

    def manifested(nodeid: str) -> bool:
        return is_manifested_nodeid(nodeid, manifest)

    return manifested


def _nodeid_from_testcase(testcase: ET.Element) -> str:
    classname = testcase.attrib.get("classname", "")
    name = testcase.attrib.get("name", "")
    path = classname.replace(".", "/") + ".py"

    if "." in name:
        cls, test = name.split(".", 1)
        return f"{path}::{cls}::{test}"
    return f"{path}::{name}"


def _has_manifested_skip(testcase: ET.Element) -> bool:
    skipped = testcase.find("skipped")
    if skipped is None:
        return False
    message = skipped.attrib.get("message", "")
    return "ferrocopg " in message


if __name__ == "__main__":
    raise SystemExit(main())
