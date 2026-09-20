"""Run three complete comparisons of one installed wheel on one machine."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import platform
import re
import subprocess
import sys
import zipfile
from pathlib import Path
from typing import Any

from run import BACKENDS, BENCHMARKS, PERFORMANCE_LIMITS, PERFORMANCE_POLICY, compare

REQUIRED_RUNS = 3


def identity(wheel: Path) -> dict[str, Any]:
    candidate = importlib.metadata.distribution("ferrocopg")
    with zipfile.ZipFile(wheel) as archive:
        entries = [
            item
            for item in archive.namelist()
            if item.startswith("ferrocopg/") and not item.endswith("/")
        ]
        if not entries or any(
            archive.read(item) != Path(candidate.locate_file(item)).read_bytes()
            for item in entries
        ):
            raise ValueError("installed ferrocopg does not match the supplied wheel")
    packages = {}
    for name in ("ferrocopg", "psycopg", "psycopg-c", "psycopg-pool", "psutil"):
        dist = importlib.metadata.distribution(name)
        digest = hashlib.sha256()
        for entry in sorted(dist.files or (), key=str):
            if str(entry).endswith(".pyc") or "__pycache__" in entry.parts:
                continue
            path = Path(dist.locate_file(entry))
            digest.update(str(entry).encode())
            digest.update(path.read_bytes())
        packages[name] = {"version": dist.version, "files_sha256": digest.hexdigest()}
    return {
        "wheel": wheel.name,
        "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
        "packages": packages,
        "python": sys.version,
        "executable": sys.executable,
        "platform": platform.platform(),
        "host": platform.node(),
    }


def validate_report(report: dict[str, Any], revision: str, status: int) -> list[str]:
    expected = {(b, w) for b in BACKENDS for w in BENCHMARKS}
    results = report["results"]
    if (
        report["mode"] != "benchmark"
        or report["revision"] != revision
        or len(results) != len(expected)
        or {(r["backend"], r["workload"]) for r in results} != expected
        or any(r.get("metadata", {}).get("revision") != revision for r in results)
    ):
        raise ValueError("incomplete benchmark or mismatched revision")
    verdict = compare(results)
    if any(report.get(key) != value for key, value in verdict.items()):
        raise ValueError("benchmark verdict or performance policy is inconsistent")
    if status != int(bool(verdict["failures"])):
        raise ValueError("benchmark exit status disagrees with report")
    config = report["configuration"]
    if config["iterations"] != 100 or config["samples"] != 9 or config["rows"] != 1000:
        raise ValueError("benchmark sizes differ from the frozen protocol")
    if config["warmup"] != 10 or any(
        r.get("iterations") != 100
        or r.get("rows") != 1000
        or len(r.get("seconds", [])) != 9
        for r in results
    ):
        raise ValueError("worker sizes differ from the frozen protocol")
    return verdict["failures"]


def server_identity(report: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "server",
        "server_settings",
        "python",
        "platform",
        "machine",
        "cpu_logical",
        "cpu_physical",
        "memory_bytes",
    )
    environments = [
        {key: row["metadata"][key] for key in keys} for row in report["results"]
    ]
    if any(environment != environments[0] for environment in environments):
        raise ValueError("machine or server changed within a comparison")
    return environments[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch("[0-9a-f]{40}", args.revision):
        parser.error("revision must be the full source commit SHA")
    if not args.wheel.is_file():
        parser.error("wheel must be the release wheel installed in this environment")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("output directory must be empty; preserve previous attempts")
    args.output.mkdir(parents=True, exist_ok=True)
    frozen = identity(args.wheel)
    summary: dict[str, Any] = {
        "schema": 1,
        "revision": args.revision,
        "performance_policy": PERFORMANCE_POLICY,
        "performance_limits": PERFORMANCE_LIMITS,
        "identity": frozen,
        "required_runs": REQUIRED_RUNS,
        "runs": [],
        "failures": [],
        "benchmark_gate_passed": False,
    }

    def save() -> None:
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

    save()
    try:
        for number in range(1, REQUIRED_RUNS + 1):
            output = args.output / f"run-{number}"
            if identity(args.wheel) != frozen:
                raise ValueError(
                    "installed packages, wheel, or machine identity changed"
                )
            process = subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).with_name("run.py")),
                    "benchmark",
                    "--revision",
                    args.revision,
                    "--output",
                    str(output),
                    "--iterations",
                    "100",
                    "--samples",
                    "9",
                    "--rows",
                    "1000",
                    "--warmup",
                    "10",
                ],
                check=False,
            )
            report = json.loads((output / "report.json").read_text())
            failures = validate_report(report, args.revision, process.returncode)
            environment = server_identity(report)
            if summary.setdefault("environment", environment) != environment:
                raise ValueError("machine or server changed between comparisons")
            if identity(args.wheel) != frozen:
                raise ValueError(
                    "installed packages, wheel, or machine identity changed"
                )
            summary["runs"].append(
                {"report": f"run-{number}/report.json", "failures": failures}
            )
            summary["failures"].extend(
                f"run-{number}: {failure}" for failure in failures
            )
            save()
    except Exception as exc:
        summary["failures"].append(str(exc))
    summary["benchmark_gate_passed"] = (
        len(summary["runs"]) == REQUIRED_RUNS and not summary["failures"]
    )
    save()
    return int(not summary["benchmark_gate_passed"])


if __name__ == "__main__":
    raise SystemExit(main())
