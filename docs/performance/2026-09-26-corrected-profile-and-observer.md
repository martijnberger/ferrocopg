# Corrected integration attribution and host observation

The `17c77760` release wheel now has eight fresh profiles covering Rust/C,
fresh/reused public cursors, and constant/parameterized binary prepared queries.
Each performs 1,000 warmups followed by 10,000 profiled operations with normal
bookkeeping and checked results. All workers run sequentially. This is local
attribution, not performance acceptance or proof of host idleness.

Wheel SHA-256:
`703a53605a39ab1571995d8013071fca6e65ea5dc0d9cc8645bca396ce982525`.
Independent auditing verifies every Rust worker's installed code against that
wheel, unchanged C package fingerprints across its controls, expected operation
counts, source identity, and each pstats file hash.

## Remaining work

Python-visible function calls per operation, rounded (the enclosing profiler
adds one call to each total):

| Cursor/query | Rust | Official C |
| --- | ---: | ---: |
| Fresh/constant | 154 | 55 |
| Fresh/parameterized | 190 | 63 |
| Reused/constant | 133 | 39 |
| Reused/parameterized | 169 | 46 |

For fresh parameterized Rust, the largest Python regions outside the native
execution call remain `_convert_query_params` (0.148 profiled seconds for
10,000 operations) and fetch-side work (0.143 seconds). Within fetch, native
result-loader setup accounts for 0.096 seconds; these nested regions must not
be summed. Native `execute_query` accounts for 0.225 cumulative profiled seconds
and includes database I/O, not just integration work.

The profile confirms the native owner removed neither Python query conversion
nor per-execution dumper/loader construction. Reused cursors still execute
those paths per query. This supports investigating a coarser adaptation/decoding
boundary, not another adapter-map cache or generic Transformer port. Arbitrary
constructors, mutable callbacks, registration snapshots, value-dependent types,
and recursive/reentrant contexts remain required. No new cache or production
specialization is justified solely by these profiles.

cProfile observes many more Python calls in Rust's shell than in C's opaque
implementation, so it perturbs them differently. Neither the profiled totals
nor their difference is an unprofiled savings budget or native-client floor.
The earlier dedicated both-order comparison remains the evidence for the
combined integration's performance benefit.

## Acceptance tooling

`repeat_benchmark.py` now records `run-N-host-activity.jsonl` throughout every
complete comparison using the existing two-second host observer. Initial and
final intervals, process births/exits, inaccessible-process counts, system CPU
deltas and observer thread CPU are retained. Package checks and builds remain
outside the observed comparison. The summary links each completed run's file
and explicitly requires review, with no automatic idle verdict.

Missing or failed observation prevents a numerical benchmark pass. No retries,
thresholds, workloads, sample counts or comparator limits changed. All reports,
including numerical failures, remain preserved. New monitored runs must not be
mixed with old unmonitored reports to construct a final three-run result.

The observer does not prove perfect idleness: sampling can miss short-lived
activity, process names cannot prove ownership, permissions can hide processes,
and the observer itself adds CPU work. Review the intervals and observer cost
alongside the workload samples; a green workflow is not a completed manual audit.

## Verification and observer effect

All 165 Phase 5 tooling/installed checks pass. New controls cover unavailable and
failed observers, completed telemetry for every run, explicit review status, and
preservation of existing failure/identity/completeness behavior. A real isolated
worker smoke run captures two intervals and about 0.040 CPU seconds of observer
work. It also reports 298 inaccessible processes, rather than claiming idleness.

A bounded control compares observation off/on, then on/off on the same frozen
wheel, using three samples of 25,000 fresh parameterized operations per worker:

| Order | Observed/plain wall | Observed/plain client CPU | Observer CPU as % of one core |
| --- | ---: | ---: | ---: |
| Off then on | 0.9866 | 0.9875 | 1.191% |
| On then off | 1.0009 | 1.0067 | 1.232% |

This short local control does not show a repeatable slowdown, but does not prove
zero overhead or calibrate every workload. Initial coordinator-snapshot CPU is
outside the observer-thread metric and is not included in the percentages.

Initial tooling invocations exposed missing psutil in the source environment
and denied host inspection in the sandbox. These failed attempts are retained.
Unit controls now mock host samples deterministically; the real smoke/control
uses the isolated environment and authorized host access. Production observation
still fails closed rather than pretending those failures mean an idle machine.

[Profile audit](2026-09-26-corrected-integration-profile.json) and
[raw profiles, reports, scripts and test logs](2026-09-26-corrected-profile-and-observer.tar.gz),
archive SHA-256:
`28ebc12af1a872f969626ab40a71a31eea838345c21090474222974abaab90ec`.

No backend runtime changed in this checkpoint. Full exact-candidate validation,
remaining fallback ownership and all-workload GIL/poll/wakeup attribution remain
open. The failed local compatibility gate is not waived.

Report and analysis produced with AI assistance.
