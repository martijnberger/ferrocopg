"""Compare release profiles or exact candidates; never release acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import signal
import statistics
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

from run import BACKENDS, compare
from workloads import BENCHMARKS

ROOT = Path(__file__).resolve().parents[2]
PROFILES = {
    # Keep the original Cargo defaults as the control even if the project changes.
    "default": {
        "CARGO_PROFILE_RELEASE_LTO": "false",
        "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "16",
    },
    "thin": {
        "CARGO_PROFILE_RELEASE_LTO": "thin",
        "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "1",
    },
}
WORKLOADS = ("prepared", "parameterized")
ORDER = (("default", "a"), ("thin", "a"), ("thin", "b"), ("default", "b"))
ITERATIONS = 100000
SAMPLES = 9
WARMUP = 10000
INTEGRATION_CASES = ("public-fresh", "public-reused")
INTEGRATION_WORKLOADS = ("constant", "parameterized")
INTEGRATION_ITERATIONS = 5000


def compare_integration(
    output: Path,
    revisions: dict[str, str],
    *,
    iterations: int = INTEGRATION_ITERATIONS,
) -> dict[str, object]:
    """Check all raw cursor controls before reporting either measurement order."""
    if iterations < INTEGRATION_ITERATIONS:
        raise ValueError("integration controls cannot shorten the default sample")
    comparison_order(list(revisions))
    first, second = revisions
    fingerprints = {}
    environment = None
    pairs = {}
    for case in INTEGRATION_CASES:
        for workload in INTEGRATION_WORKLOADS:
            for position in ("a", "b"):
                measurements = {}
                for variant, revision in revisions.items():
                    path = (
                        output
                        / f"integration-{variant}-{case}-{workload}-{position}.json"
                    )
                    result = json.loads(path.read_text())
                    samples = result["samples"]
                    if (
                        result["mode"] != "integration-diagnostic"
                        or result["acceptance_evidence"] is not False
                        or result["backend"] != "rust"
                        or result.get("bookkeeping", "normal") != "normal"
                        or result.get("omitted_helper_bodies", []) != []
                        or result["case"] != case
                        or result["workload"] != workload
                        or result["metadata"]["revision"] != revision
                        or result["iterations"] != iterations
                        or result["warmup"] != 1000
                        or result["binary"] is not True
                        or result["prepared"] is not True
                        or result["profile"] is not None
                        or len(samples) != SAMPLES
                        or any(
                            not math.isfinite(row[key]) or row[key] <= 0
                            for row in samples
                            for key in ("wall_us", "cpu_us")
                        )
                    ):
                        raise ValueError(f"invalid integration measurement: {path}")
                    medians = {
                        key: statistics.median(row[key] for row in samples)
                        for key in ("wall_us", "cpu_us")
                    }
                    if result["medians_us"] != medians:
                        raise ValueError(f"inconsistent integration medians: {path}")
                    files = result["installed_file_sha256"]["ferrocopg"]
                    if (
                        not files
                        or any(
                            not re.fullmatch(r"[0-9a-f]{64}", value)
                            for value in files.values()
                        )
                        or fingerprints.setdefault(variant, files) != files
                    ):
                        raise ValueError(
                            f"changed or missing integration fingerprint: {path}"
                        )
                    identity = {
                        key: result["metadata"][key]
                        for key in (
                            "python",
                            "platform",
                            "machine",
                            "server",
                            "server_settings",
                        )
                    }
                    if environment is None:
                        environment = identity
                    elif environment != identity:
                        raise ValueError(f"changed integration environment: {path}")
                    measurements[variant] = medians
                pairs[f"{case}-{workload}-{position}"] = {
                    "measurements": measurements,
                    f"{second}_over_{first}": {
                        key: measurements[second][key] / measurements[first][key]
                        for key in ("wall_us", "cpu_us")
                    },
                }
    return {
        "release_acceptance": False,
        "variant_revisions": revisions,
        "iterations_per_sample": iterations,
        "pairs": pairs,
    }


def build_environment(
    profile: str, target: Path, profiles: dict[str, dict[str, str]] = PROFILES
) -> dict[str, str]:
    env = dict(os.environ)
    # A caller's codegen overrides must not silently change the control build.
    overrides = [
        key
        for key, value in env.items()
        if value
        and (
            key.startswith("CARGO_PROFILE_RELEASE_")
            or key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_RUSTFLAGS")
            or (key.startswith("CARGO_TARGET_") and key.endswith("_RUSTFLAGS"))
        )
    ]
    if overrides:
        raise ValueError(f"remove inherited codegen overrides: {sorted(overrides)}")
    env.update(profiles[profile], CARGO_TARGET_DIR=str(target))
    return env


def comparison_order(names: Sequence[str]) -> tuple[tuple[str, str], ...]:
    if len(names) != 2 or names[0] == names[1]:
        raise ValueError("a comparison requires two distinct variants")
    first, second = names
    return ((first, "a"), (second, "a"), (second, "b"), (first, "b"))


def query_measurement(path: Path, revision: str, workload: str) -> dict[str, float]:
    result = json.loads(path.read_text())
    if (
        result["metadata"]["revision"] != revision
        or result["mode"] != "benchmark"
        or result["backend"] != "rust"
        or result["workload"] != workload
        or result["failures"]
        or result["iterations"] != ITERATIONS
        or any(
            len(result[field]) != SAMPLES
            or any(not math.isfinite(value) or value <= 0 for value in result[field])
            for field in ("seconds", "cpu_seconds")
        )
    ):
        raise ValueError(f"invalid query measurement: {path}")
    return {
        key: statistics.median(result[field]) / result["iterations"] * 1e6
        for key, field in (("wall_us", "seconds"), ("cpu_us", "cpu_seconds"))
    }


def compare_queries(
    output: Path, revision: str, revisions: dict[str, str] | None = None
) -> dict[str, object]:
    revisions = (
        revisions if revisions is not None else dict.fromkeys(PROFILES, revision)
    )
    comparison_order(list(revisions))
    first, second = revisions
    pairs = {}
    for workload in WORKLOADS:
        for order in ("a", "b"):
            measurements = {
                profile: query_measurement(
                    output / f"{profile}-{workload}-{order}.json",
                    profile_revision,
                    workload,
                )
                for profile, profile_revision in revisions.items()
            }
            pairs[f"{workload}-{order}"] = {
                "measurements": measurements,
                f"{second}_over_{first}": {
                    key: measurements[second][key] / measurements[first][key]
                    for key in ("wall_us", "cpu_us")
                },
            }
    return {
        "revision": revision,
        "mode": "codegen-experiment"
        if list(revisions) == list(PROFILES)
        else "candidate-experiment",
        "variant_revisions": revisions,
        "pairs": pairs,
        "release_acceptance": False,
    }


def complete_measurement(path: Path, revision: str, status: int) -> dict[str, object]:
    report = json.loads(path.read_text())
    results = report["results"]
    expected = {(backend, workload) for backend in BACKENDS for workload in BENCHMARKS}
    if (
        report["revision"] != revision
        or report["mode"] != "benchmark"
        or len(results) != len(expected)
        or {(item["backend"], item["workload"]) for item in results} != expected
        or any(
            item["metadata"]["revision"] != revision or item["failures"]
            for item in results
        )
    ):
        raise ValueError(f"incomplete or failed benchmark workers: {path}")
    verdict = compare(results)
    if (
        report["ratios"] != verdict["ratios"]
        or report["failures"] != verdict["failures"]
        or status != int(bool(verdict["failures"]))
    ):
        raise ValueError(f"inconsistent benchmark verdict: {path}")
    return verdict


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--baseline-source", type=Path)
    parser.add_argument("--baseline-revision")
    args = parser.parse_args()
    if bool(args.baseline_source) != bool(args.baseline_revision):
        parser.error("baseline source and revision must be supplied together")
    profiles = PROFILES
    sources = dict.fromkeys(profiles, ROOT)
    revisions = dict.fromkeys(profiles, args.revision)
    if args.baseline_source:
        if not all(
            re.fullmatch(r"[0-9a-f]{40}", rev)
            for rev in (args.revision, args.baseline_revision)
        ):
            parser.error("candidate comparisons require full commit SHA-1 identities")
        baseline = args.baseline_source.resolve()
        if baseline == ROOT or args.baseline_revision == args.revision:
            parser.error("baseline must be a separate checkout at a distinct revision")
        profiles = {"baseline": {}, "candidate": {}}
        sources = {"baseline": baseline, "candidate": ROOT}
        revisions = {"baseline": args.baseline_revision, "candidate": args.revision}
    order = comparison_order(list(profiles))
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest: dict[str, object] = {
        "revision": args.revision,
        "mode": "candidate-experiment"
        if args.baseline_source
        else "codegen-experiment",
        "release_acceptance": False,
        "status": "in_progress",
        "platform": platform.platform(),
        "python": sys.version,
        "profiles": profiles,
        "variant_sources": {
            profile: {
                "root": str(source),
                "revision": revisions[profile],
                "cargo_manifest": (source / "Cargo.toml").read_text(),
                "cargo_lock_sha256": hashlib.sha256(
                    (source / "Cargo.lock").read_bytes()
                ).hexdigest(),
            }
            for profile, source in sources.items()
        },
        "query_order": order,
        "query_configuration": {
            "warmup": WARMUP,
            "iterations": ITERATIONS,
            "samples": SAMPLES,
        },
        "integration_configuration": {
            "cases": INTEGRATION_CASES,
            "workloads": INTEGRATION_WORKLOADS,
            "iterations": INTEGRATION_ITERATIONS,
            "samples": SAMPLES,
            "warmup": 1000,
        }
        if args.baseline_source
        else None,
        "cargo_manifest": (ROOT / "Cargo.toml").read_text(),
        "cargo_lock_sha256": hashlib.sha256(
            (ROOT / "Cargo.lock").read_bytes()
        ).hexdigest(),
        "wheels": {},
    }

    def save_manifest() -> None:
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    def run(
        name: str,
        command: list[str],
        *,
        cwd: Path = ROOT,
        env: dict[str, str] | None = None,
        allow_gate_failure: bool = False,
    ) -> int:
        print(name, flush=True)
        with (output / f"{name}.log").open("w") as log:
            with subprocess.Popen(
                command,
                cwd=cwd,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            ) as process:
                try:
                    status = process.wait(timeout=1800)
                except BaseException:
                    # Stop benchmark workers too, not only their parent harness.
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                    raise
        if status not in ((0, 1) if allow_gate_failure else (0,)):
            raise RuntimeError(f"{name} failed with status {status}; see its log")
        return status

    save_manifest()
    try:
        with tempfile.TemporaryDirectory(prefix="phase5-codegen-") as directory:
            build = Path(directory)
            python = str(build / "env" / "bin" / "python")
            run(
                "environment",
                ["uv", "venv", str(build / "env"), "--python", sys.executable],
            )
            run(
                "comparators",
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    python,
                    "-r",
                    str(ROOT / "tools/phase5/requirements.txt"),
                ],
            )
            run("rust-version", ["rustc", "--version", "--verbose"])
            wheels = {}
            for profile, source in sources.items():
                run(
                    f"dependencies-{profile}",
                    ["cargo", "fetch", "--locked"],
                    cwd=source,
                )
                stage = build / f"stage-{profile}"
                run(
                    f"stage-{profile}",
                    [
                        sys.executable,
                        str(source / "tools/stage_ferrocopg.py"),
                        str(stage),
                    ],
                    cwd=source,
                )
                wheel_dir = output / f"{profile}-wheel"
                run(
                    f"build-{profile}",
                    [
                        str(ROOT / ".venv/bin/maturin"),
                        "build",
                        "--release",
                        "--offline",
                        "--locked",
                        "-i",
                        python,
                        "--out",
                        str(wheel_dir),
                    ],
                    cwd=stage,
                    env=build_environment(
                        profile, build / f"target-{profile}", profiles
                    ),
                )
                candidates = list(wheel_dir.glob("*.whl"))
                if len(candidates) != 1:
                    raise ValueError(f"expected one wheel for {profile}")
                wheels[profile] = candidates[0]
            manifest["wheels"] = {
                profile: {
                    "revision": revisions[profile],
                    "path": str(wheel.relative_to(output)),
                    "sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
                }
                for profile, wheel in wheels.items()
            }
            save_manifest()

            def install(profile: str, label: str) -> None:
                run(
                    f"install-{label}",
                    [
                        "uv",
                        "pip",
                        "install",
                        "--python",
                        python,
                        "--force-reinstall",
                        "--no-deps",
                        str(wheels[profile]),
                    ],
                )

            # Finish all builds and validation before collecting any timings.
            for profile in profiles:
                install(profile, f"check-{profile}")
                run(
                    f"check-{profile}",
                    [python, "-m", "unittest", "discover", "-s", "tools/phase5", "-v"],
                )
            run("processes-before-timing", ["ps", "-eo", "pid,ppid,pcpu,comm"])
            for profile, position in order:
                install(profile, f"{profile}-{position}")
                for workload in WORKLOADS:
                    label = f"{profile}-{workload}-{position}"
                    run(
                        label,
                        [
                            python,
                            str(ROOT / "tools/phase5/run.py"),
                            "benchmark",
                            "--worker",
                            "--backend",
                            "rust",
                            "--workload",
                            workload,
                            "--warmup",
                            str(WARMUP),
                            "--iterations",
                            str(ITERATIONS),
                            "--samples",
                            str(SAMPLES),
                            "--revision",
                            revisions[profile],
                            "--output",
                            str(output / f"{label}.json"),
                        ],
                    )
                if args.baseline_source:
                    for case in INTEGRATION_CASES:
                        for workload in INTEGRATION_WORKLOADS:
                            label = (
                                f"integration-{profile}-{case}-{workload}-{position}"
                            )
                            run(
                                label,
                                [
                                    python,
                                    str(ROOT / "tools/phase5/integration_profile.py"),
                                    "--case",
                                    case,
                                    "--workload",
                                    workload,
                                    "--iterations",
                                    str(INTEGRATION_ITERATIONS),
                                    "--samples",
                                    str(SAMPLES),
                                    "--revision",
                                    revisions[profile],
                                    "--output",
                                    str(output / f"{label}.json"),
                                ],
                            )
            summary = compare_queries(output, args.revision, revisions)
            if args.baseline_source:
                summary["integration"] = compare_integration(output, revisions)
            gates = {}
            for profile in profiles:
                install(profile, f"complete-{profile}")
                report_dir = output / f"complete-{profile}"
                status = run(
                    f"complete-{profile}",
                    [
                        python,
                        str(ROOT / "tools/phase5/run.py"),
                        "benchmark",
                        "--iterations",
                        "100",
                        "--samples",
                        "9",
                        "--revision",
                        revisions[profile],
                        "--output",
                        str(report_dir),
                    ],
                    allow_gate_failure=True,
                )
                gates[profile] = complete_measurement(
                    report_dir / "report.json", revisions[profile], status
                )
            run("processes-after-timing", ["ps", "-eo", "pid,ppid,pcpu,comm"])
            run("installed-packages", ["uv", "pip", "freeze", "--python", python])
            summary["complete_benchmarks"] = gates
            (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        manifest["status"] = "completed"
        return 0
    except BaseException as exc:
        manifest["status"] = "failed"
        manifest["error"] = str(exc)
        raise
    finally:
        save_manifest()


if __name__ == "__main__":
    raise SystemExit(main())
