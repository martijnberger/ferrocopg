"""Run six matched native/public scalar diagnostics, never release acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import statistics
import sys
from pathlib import Path

from attribution import environment_identity, run_command
from host_activity import HostActivity
from layer_probe import CASES, LAYERS, PARAM_QUERY, PUBLIC_LAYERS, QUERY, ROOT, validate
from repeat_benchmark import identity

MATRIX = tuple((case, fmt) for case in CASES for fmt in ("binary", "text"))


def protocol(case, fmt):
    result = {
        "query": QUERY if case == "constant" else PARAM_QUERY,
        "prepared": case != "unprepared",
        "result_format": fmt,
        "in_flight": 1,
        "iterations": 10000,
        "samples": 9,
        "warmup": 1000,
        "tls": False,
    }
    if case != "constant" or fmt != "binary":
        result.update(
            case=case,
            parameter_oids=[21] if case != "constant" else [],
            parameter_format="binary" if case != "constant" else None,
        )
    return result


def source_identity(native_rust, native_libpq):
    paths = (
        native_rust,
        native_libpq,
        ROOT / "Cargo.lock",
        ROOT / "crates/ferrocopg-postgres/examples/layer_probe.rs",
        ROOT / "tools/phase5/libpq_probe.c",
        ROOT / "tools/phase5/layer_probe.py",
    )
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}


def validate_case(output, revision, case, fmt, sources):
    summary = json.loads((output / "summary.json").read_text())
    if (
        summary["mode"] != "layer-diagnostic"
        or summary["acceptance_evidence"] is not False
        or summary["revision"] != revision
        or summary["failures"]
        or summary["protocol"] != protocol(case, fmt)
        or summary["sha256"] != sources
        or set(summary["orders"]) != {"forward", "reverse"}
    ):
        raise ValueError("invalid matched-layer case identity or protocol")
    environment, fingerprints = None, {}
    for order in ("forward", "reverse"):
        if set(summary["orders"][order]) != set(LAYERS):
            raise ValueError("missing matched-layer measurement")
        for layer in LAYERS:
            raw = json.loads((output / f"{order}-{layer}.json").read_text())
            validate(raw, layer, 10000, 9, case, fmt)
            if summary["orders"][order][layer] != {
                "wall_us": raw["wall_us"],
                "median_us": statistics.median(raw["wall_us"]),
            }:
                raise ValueError("matched-layer median differs from raw samples")
            if layer in PUBLIC_LAYERS:
                if raw["metadata"]["revision"] != revision:
                    raise ValueError("matched-layer worker revision changed")
                current = environment_identity(raw)
                if environment is None:
                    environment = current
                elif environment != current:
                    raise ValueError("matched-layer environment changed")
                backend = "rust" if layer == "binding" else layer
                files = raw["installed_file_sha256"]
                if not files or any(
                    not entries
                    or any(
                        not re.fullmatch(r"[0-9a-f]{64}", v) for v in entries.values()
                    )
                    for entries in files.values()
                ):
                    raise ValueError("invalid installed-code fingerprint")
                if fingerprints.setdefault(backend, files) != files:
                    raise ValueError("matched-layer installed code changed")
    return environment, fingerprints


def coordinate(args):
    frozen = identity(args.wheel)
    sources = source_identity(args.native_rust, args.native_libpq)
    args.output.mkdir()
    manifest = {
        "mode": "matched-layer-matrix",
        "acceptance_evidence": False,
        "revision": args.revision,
        "identity": frozen,
        "sources": sources,
        "status": "running",
        "cases": [],
        "failures": [],
        "limitations": [
            "independent paths are not additive or entirely recoverable costs",
            "one scalar query in flight; no claim about TLS or pipeline throughput",
            "native/binding paths do not implement all public adaptation and factory behavior",
            "host activity is sampled every two seconds with observer overhead; not a proof of perfect idleness",
        ],
    }

    def save():
        (args.output / "manifest.json").write_text(
            json.dumps(manifest, indent=2) + "\n"
        )

    def unchanged():
        if (
            identity(args.wheel) != frozen
            or source_identity(args.native_rust, args.native_libpq) != sources
        ):
            raise ValueError(
                "wheel, comparators, host, probe binaries, or source changed"
            )

    environment, fingerprints = None, None
    save()
    try:
        for case, fmt in MATRIX:
            unchanged()
            name = f"{case}-{fmt}"
            output = args.output / name
            print(name, flush=True)
            with HostActivity(args.output / f"{name}-host-activity.jsonl"):
                run_command(
                    [
                        sys.executable,
                        str(ROOT / "tools/phase5/layer_probe.py"),
                        "--revision",
                        args.revision,
                        "--native-rust",
                        str(args.native_rust),
                        "--native-libpq",
                        str(args.native_libpq),
                        "--case",
                        case,
                        "--result-format",
                        fmt,
                        "--iterations",
                        "10000",
                        "--samples",
                        "9",
                        "--output",
                        str(output),
                    ],
                    args.output / f"{name}.log",
                    timeout=1800,
                )
            unchanged()
            current, files = validate_case(output, args.revision, case, fmt, sources)
            if environment is None:
                environment, fingerprints = current, files
            elif current != environment or files != fingerprints:
                raise ValueError(
                    "environment or installed code changed across matrix cases"
                )
            summary = output / "summary.json"
            manifest["cases"].append(
                {
                    "case": case,
                    "result_format": fmt,
                    "summary": str(summary.relative_to(args.output)),
                    "sha256": hashlib.sha256(summary.read_bytes()).hexdigest(),
                }
            )
            save()
        if len(manifest["cases"]) != 6:
            raise ValueError("incomplete matched-layer matrix")
        unchanged()
        manifest["environment"] = environment
        manifest["installed_file_sha256"] = fingerprints
        manifest["artifact_sha256"] = {
            str(p.relative_to(args.output)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(args.output.rglob("*"))
            if p.is_file() and p.name != "manifest.json"
        }
        manifest["status"] = "completed"
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["failures"].append(str(exc))
    save()
    return int(bool(manifest["failures"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--native-rust", type=Path, required=True)
    parser.add_argument("--native-libpq", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision) or args.output.exists():
        parser.error("full revision and unused output directory required")
    return coordinate(args)


if __name__ == "__main__":
    raise SystemExit(main())
