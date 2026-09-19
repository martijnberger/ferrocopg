"""Compare portable release profiles on one runner; not release acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import signal
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

from run import BACKENDS, compare
from workloads import BENCHMARKS

ROOT = Path(__file__).resolve().parents[2]
PROFILES = {
    "default": {},
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


def build_environment(profile: str, target: Path) -> dict[str, str]:
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
    env.update(PROFILES[profile], CARGO_TARGET_DIR=str(target))
    return env


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


def compare_queries(output: Path, revision: str) -> dict[str, object]:
    pairs = {}
    for workload in WORKLOADS:
        for order in ("a", "b"):
            measurements = {
                profile: query_measurement(
                    output / f"{profile}-{workload}-{order}.json", revision, workload
                )
                for profile in PROFILES
            }
            pairs[f"{workload}-{order}"] = {
                "measurements": measurements,
                "thin_over_default": {
                    key: measurements["thin"][key] / measurements["default"][key]
                    for key in ("wall_us", "cpu_us")
                },
            }
    return {
        "revision": revision,
        "mode": "codegen-experiment",
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
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest: dict[str, object] = {
        "revision": args.revision,
        "mode": "codegen-experiment",
        "release_acceptance": False,
        "status": "in_progress",
        "platform": platform.platform(),
        "python": sys.version,
        "profiles": PROFILES,
        "query_order": ORDER,
        "query_configuration": {
            "warmup": WARMUP,
            "iterations": ITERATIONS,
            "samples": SAMPLES,
        },
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
            run("dependencies", ["cargo", "fetch", "--locked"])
            run(
                "stage",
                [
                    sys.executable,
                    str(ROOT / "tools/stage_ferrocopg.py"),
                    str(build / "stage"),
                ],
            )
            wheels = {}
            for profile in PROFILES:
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
                    cwd=build / "stage",
                    env=build_environment(profile, build / f"target-{profile}"),
                )
                candidates = list(wheel_dir.glob("*.whl"))
                if len(candidates) != 1:
                    raise ValueError(f"expected one wheel for {profile}")
                wheels[profile] = candidates[0]
            manifest["wheels"] = {
                profile: {
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
            for profile in PROFILES:
                install(profile, f"check-{profile}")
                run(
                    f"check-{profile}",
                    [python, "-m", "unittest", "discover", "-s", "tools/phase5", "-v"],
                )
            for profile, order in ORDER:
                install(profile, f"{profile}-{order}")
                for workload in WORKLOADS:
                    label = f"{profile}-{workload}-{order}"
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
                            args.revision,
                            "--output",
                            str(output / f"{label}.json"),
                        ],
                    )
            run("processes-before-timing", ["ps", "-eo", "pid,ppid,pcpu,comm"])
            summary = compare_queries(output, args.revision)
            gates = {}
            for profile in PROFILES:
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
                        args.revision,
                        "--output",
                        str(report_dir),
                    ],
                    allow_gate_failure=True,
                )
                gates[profile] = complete_measurement(
                    report_dir / "report.json", args.revision, status
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
