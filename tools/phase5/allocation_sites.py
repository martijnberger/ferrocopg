"""Preserve allocator totals while exposing recognized profiling-induced sites."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

# Memray 1.20.0 allocator IDs. Unmapping/free events are not allocation events.
ALLOCATIONS = frozenset((2, 3, 4, *range(6, 15)))
DEALLOCATIONS = frozenset((1, 5, 15))
CLASSES = (
    "profile_frame_materialization",
    "monitoring_setup",
    "other_instrumented_allocations",
    "unresolved_native_site",
)


def classify(stack):
    names = [frame[0].split(".llvm.")[0] for frame in stack]
    if not names or names[0] in ("", "<unknown>", "???"):
        return "unresolved_native_site"
    if (
        names[0] == "_PyFrame_MakeAndSetFrameObject"
        and "PyEval_GetFrame" in names[1:4]
        and "call_profile_func" in names[1:5]
    ):
        return "profile_frame_materialization"
    # Linux DWARF preserves allocator frames that the macOS trace can omit.
    if names[:8] == [
        "_PyObject_MallocWithType",
        "gc_alloc",
        "_PyObject_GC_NewVar",
        "_PyFrame_New_NoTrack",
        "_PyFrame_MakeAndSetFrameObject",
        "_PyFrame_GetFrameObject",
        "PyEval_GetFrame",
        "call_profile_func",
    ]:
        return "profile_frame_materialization"
    if names[0] == "force_instrument_lock_held":
        return "monitoring_setup"
    # Instrumented interpreter opcodes also surround ordinary application work.
    return "other_instrumented_allocations"


def summarize(records, expected):
    sites = {}
    classes = {name: {"allocations": 0, "bytes": 0} for name in CLASSES}
    allocators = defaultdict(int)
    for record in records:
        if record.allocator in DEALLOCATIONS:
            continue
        if record.allocator not in ALLOCATIONS:
            raise ValueError("unknown Memray allocator ID")
        if record.n_allocations < 1 or record.size < 0:
            raise ValueError("invalid allocation record")
        native = tuple(tuple(frame) for frame in record.native_stack_trace()[:8])
        python = tuple(tuple(frame) for frame in record.stack_trace()[:1])
        category = classify(native)
        key = (int(record.allocator), native, python)
        site = sites.setdefault(
            key,
            {
                "allocator": int(record.allocator),
                "native_stack": native,
                "python_site": python,
                "classification": category,
                "allocations": 0,
                "bytes": 0,
            },
        )
        for target in (site, classes[category]):
            target["allocations"] += record.n_allocations
            target["bytes"] += record.size
        allocators[int(record.allocator)] += record.n_allocations
    result = {
        "mode": "allocation-sites",
        "acceptance_evidence": False,
        "classes": classes,
        "total_num_allocations": sum(row["allocations"] for row in classes.values()),
        "total_bytes_allocated": sum(row["bytes"] for row in classes.values()),
        "allocator_counts": dict(sorted(allocators.items())),
        "sites": sorted(
            sites.values(), key=lambda row: (-row["allocations"], -row["bytes"])
        ),
        "limitations": [
            "recognized profiling paths are retained, not subtracted from canonical totals",
            "classification is symbol-dependent; unrecognized profiling overhead can remain",
            "other allocations are still instrumented, not an estimate of unprofiled allocations",
            "allocator layers are not distinct objects; bytes are allocation volume, not retained memory",
        ],
    }
    validate(result, expected)
    return result


def validate(result, expected):
    if (
        result["mode"] != "allocation-sites"
        or result["acceptance_evidence"] is not False
        or set(result["classes"]) != set(CLASSES)
        or not result["sites"]
        or any(
            result[key] != expected[key]
            for key in ("total_num_allocations", "total_bytes_allocated")
        )
    ):
        raise ValueError(
            "allocation-site totals disagree with canonical Memray statistics"
        )
    for site in result["sites"]:
        if (
            site["classification"] != classify(site["native_stack"])
            or site["allocator"] not in ALLOCATIONS
            or type(site["allocations"]) is not int
            or site["allocations"] < 1
            or type(site["bytes"]) is not int
            or site["bytes"] < 0
        ):
            raise ValueError("invalid allocation site or classification")
    for metric, total in (
        ("allocations", "total_num_allocations"),
        ("bytes", "total_bytes_allocated"),
    ):
        if sum(row[metric] for row in result["sites"]) != result[total]:
            raise ValueError("allocation sites do not sum to total")
        for name in CLASSES:
            measured = sum(
                row[metric] for row in result["sites"] if row["classification"] == name
            )
            if result["classes"][name][metric] != measured:
                raise ValueError("allocation classification totals are inconsistent")
    if sum(result["allocator_counts"].values()) != result["total_num_allocations"]:
        raise ValueError("allocator event counts are inconsistent")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--stats", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("preserve existing analysis")
    import memray

    if memray.__version__ != "1.20.0":
        raise ValueError("use the pinned Memray version")
    reader = memray.FileReader(args.capture)
    if (
        not reader.metadata.has_native_traces
        or not reader.metadata.trace_python_allocators
        or reader.metadata.file_format != memray.FileFormat.ALL_ALLOCATIONS
    ):
        raise ValueError("full Python/native allocation capture required")
    result = summarize(
        reader.get_allocation_records(), json.loads(args.stats.read_text())
    )
    result.update(
        memray_version=memray.__version__,
        capture=args.capture.name,
        capture_sha256=hashlib.sha256(args.capture.read_bytes()).hexdigest(),
        stats_sha256=hashlib.sha256(args.stats.read_bytes()).hexdigest(),
        analyzer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    )
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
