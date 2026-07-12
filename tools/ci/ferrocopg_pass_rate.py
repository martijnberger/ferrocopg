#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Counts:
    total: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    skipped: int = 0
    manifested: int = 0

    @property
    def rate(self) -> float:
        return 1.0 if self.total == 0 else self.passed / self.total

    def record(self, testcase: ET.Element, *, manifested: bool) -> None:
        if manifested:
            self.manifested += 1
            return

        self.total += 1
        if testcase.find("failure") is not None:
            self.failed += 1
        elif testcase.find("error") is not None:
            self.errors += 1
        elif testcase.find("skipped") is not None:
            self.skipped += 1
        else:
            self.passed += 1

    def json(self) -> dict[str, int | float]:
        data: dict[str, int | float] = asdict(self)
        data["rate"] = self.rate
        return data


def main() -> int:
    args = parse_args()
    if not args.junit.exists():
        print(f"JUnit report not found: {args.junit}", file=sys.stderr)
        return 2

    manifested = _load_manifest_matcher(args.manifest)
    scopes = {"overall": Counts(), "sync": Counts(), "async": Counts()}
    families: dict[str, dict[str, Counts]] = {"sync": {}, "async": {}}

    try:
        testcases = ET.parse(args.junit).getroot().iter("testcase")
        for testcase in testcases:
            nodeid = _nodeid_from_testcase(testcase)
            scope = _test_scope(nodeid)
            is_manifested = manifested(nodeid) or _has_manifested_skip(testcase)
            scopes["overall"].record(testcase, manifested=is_manifested)
            scopes[scope].record(testcase, manifested=is_manifested)
            family = _test_family(nodeid)
            families[scope].setdefault(family, Counts()).record(
                testcase, manifested=is_manifested
            )
    except ET.ParseError as ex:
        print(f"invalid JUnit report {args.junit}: {ex}", file=sys.stderr)
        return 2

    _print_report(scopes, families)
    if args.report:
        _write_report(args.report, scopes, families, args.pytest_status)

    if args.pytest_status not in (0, 1):
        print(
            f"pytest did not complete normally: exit status {args.pytest_status}",
            file=sys.stderr,
        )
        return args.pytest_status

    regressions = 0
    regressions += _check_floor("overall", scopes["overall"], args.floor)
    if args.sync_floor:
        regressions += _check_floor("sync", scopes["sync"], args.sync_floor)
    return 1 if regressions else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("junit", type=Path)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--floor", type=Path, required=True)
    parser.add_argument("--sync-floor", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--pytest-status", type=int, default=0)
    return parser.parse_args()


def _load_manifest_matcher(manifest: Path) -> Callable[[str], bool]:
    sys.path.insert(0, str(ROOT))
    from tests.fix_ferrocopg import is_manifested_nodeid

    def manifested(nodeid: str) -> bool:
        return is_manifested_nodeid(nodeid, manifest)

    return manifested


def _nodeid_from_testcase(testcase: ET.Element) -> str:
    classname = testcase.attrib.get("classname", "")
    name = testcase.attrib.get("name", "")
    parts = classname.split(".")

    for index in range(len(parts), 0, -1):
        candidate = ROOT.joinpath(*parts[:index]).with_suffix(".py")
        if candidate.is_file():
            path = candidate.relative_to(ROOT).as_posix()
            return "::".join((path, *parts[index:], name))

    # Keep producing a useful report for JUnit emitted outside this checkout.
    return f"{classname.replace('.', '/')}.py::{name}"


def _has_manifested_skip(testcase: ET.Element) -> bool:
    skipped = testcase.find("skipped")
    if skipped is None:
        return False
    message = skipped.attrib.get("message", "")
    return "ferrocopg " in message


def _test_scope(nodeid: str) -> str:
    path = nodeid.partition("::")[0]
    return "async" if path.endswith("_async.py") else "sync"


def _test_family(nodeid: str) -> str:
    path = nodeid.partition("::")[0]
    if "/pool/" in path:
        return "pool"
    if "/crdb/" in path:
        return "crdb"
    if "/types/" in path or any(
        name in path for name in ("test_adapt", "test_column", "test_rows", "test_type")
    ):
        return "types-metadata"
    for needle, family in (
        ("transaction", "transactions"),
        ("tpc", "transactions"),
        ("connection", "connections"),
        ("conninfo", "connections"),
        ("prepared", "prepared"),
        ("cursor", "cursors"),
        ("copy", "copy"),
        ("pipeline", "pipeline"),
        ("notify", "notifications"),
        ("waiting", "concurrency-cancel"),
        ("concurrency", "concurrency-cancel"),
    ):
        if needle in path:
            return family
    return "other"


def _print_report(
    scopes: dict[str, Counts], families: dict[str, dict[str, Counts]]
) -> None:
    for scope in ("overall", "sync", "async"):
        counts = scopes[scope]
        print(
            f"ferrocopg {scope} pass rate: "
            f"{counts.passed}/{counts.total} ({counts.rate:.3f}); "
            f"failed={counts.failed}; errors={counts.errors}; "
            f"skipped={counts.skipped}; manifested={counts.manifested}"
        )

    for scope in ("sync", "async"):
        print(f"ferrocopg {scope} feature families:")
        for family, counts in sorted(families[scope].items()):
            print(
                f"  {family}: {counts.passed}/{counts.total} "
                f"({counts.rate:.3f}); failed={counts.failed}; "
                f"errors={counts.errors}; skipped={counts.skipped}; "
                f"manifested={counts.manifested}"
            )


def _write_report(
    path: Path,
    scopes: dict[str, Counts],
    families: dict[str, dict[str, Counts]],
    pytest_status: int,
) -> None:
    data = {
        "pytest_status": pytest_status,
        "scopes": {name: counts.json() for name, counts in scopes.items()},
        "families": {
            scope: {family: counts.json() for family, counts in sorted(items.items())}
            for scope, items in families.items()
        },
    }
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def _check_floor(name: str, counts: Counts, floor_path: Path) -> int:
    floor = float(floor_path.read_text().strip())
    if counts.rate + 1e-9 >= floor:
        print(f"ferrocopg {name} floor satisfied: {counts.rate:.3f} >= {floor:.3f}")
        return 0
    print(
        f"ferrocopg {name} pass rate regressed below the committed floor: "
        f"{counts.rate:.3f} < {floor:.3f}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
