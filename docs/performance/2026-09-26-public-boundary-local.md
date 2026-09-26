# Public Native Boundary: Local Comparison

Diagnostic evidence, not release acceptance. The combined public integration
reduces scalar wall and CPU time in both measurement orders, but does not reach
the <= 1.10 C research objective. Do not attribute the combined gain to any one
subchange or extrapolate it to all workloads.

## Frozen Inputs and Method

- Before: `d957b508785c9c304ad14f82b15fb3f598412cf3`, with native ownership
  primitives present but public queries still using the old integration.
- After: `12f139ce63a0c78d8148913673ef39392794e85f`, published on main. Public
  preparation ownership, direct adapted sequences, and native result navigation
  are integrated. Production files did not change during measurement.
- Before wheel SHA-256:
  `23634114127a3746ee1e6ae2b94625b6bad9dad45cda0a11a580f18eb1badd42`.
- After wheel SHA-256:
  `1b8b361aa0bebcc57df7affad47c83bba575ac0a2e1981d2cedfff32123dae22`.
- Each installed implementation file was checked against its wheel. Independent
  post-measurement validation also checks every reported installed fingerprint.
- CPython 3.14.6, macOS arm64, local PostgreSQL 15.19, TCP with TLS disabled.
  Both environments use official psycopg/psycopg-c 3.3.2, psycopg-pool 3.3.0,
  and psutil 7.2.2. Full machine/server metadata is in each raw report.
- Unmodified `integration_profile.py` from the after revision: prepared binary
  scalar queries, fresh/reused public cursors, constant/parameterized SQL.
  Each worker performs 1,000 warmups and nine samples of 10,000 operations;
  every result is checked. Normal bookkeeping, no profiler or ablation.
- Sequential order: four C controls, four before controls, four after controls,
  four after controls, four before controls, four C controls. All 24 workers
  completed successfully, with no overlapping workers. No samples were discarded.

## Results

Median microseconds per operation. A is before then after; B reverses that order.

| Cursor/query | Order | Before wall | After wall | Before CPU | After CPU | Wall reduction |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Fresh/constant | A | 41.029 | 38.718 | 29.797 | 27.306 | 5.63% |
| Fresh/constant | B | 40.453 | 38.226 | 29.226 | 27.013 | 5.50% |
| Fresh/parameterized | A | 47.292 | 43.188 | 36.102 | 31.781 | 8.68% |
| Fresh/parameterized | B | 47.209 | 43.210 | 36.021 | 31.763 | 8.47% |
| Reused/constant | A | 36.116 | 34.116 | 24.872 | 22.808 | 5.54% |
| Reused/constant | B | 36.811 | 34.307 | 25.417 | 23.120 | 6.80% |
| Reused/parameterized | A | 42.533 | 39.482 | 31.202 | 27.875 | 7.17% |
| Reused/parameterized | B | 42.606 | 39.540 | 31.136 | 27.928 | 7.20% |

Client CPU falls 7.57-11.97%. The C wall controls change by at most 0.152%
between start and end. Against those bracketing controls, after/C wall ratios are
1.236-1.252 (fresh constant), 1.339-1.340 (fresh parameterized), 1.239-1.248
(reused constant), and 1.385-1.388 (reused parameterized). These are contextual
ratios, not randomized acceptance comparisons. Near parity would still require
roughly another 11-21% reduction in the after implementation's wall time for
these cases. No per-query tail-latency data was collected by this diagnostic.

## Measurement and Validation Limits

The machine was not idle. Across 42 sampled intervals covering 84.20 seconds,
aggregate system CPU was 20.56% busy. Observed background activity includes
Ghostty, browser renderers, Node, and Codex. The observer itself consumed
0.950 CPU seconds, about 1.13% of one core; the longest interval was 2.029 seconds.
Up to 299 processes were inaccessible to the observer. No active Cargo/rustc/
maturin/pytest process was observed, but sampling cannot rule out short-lived or
inaccessible activity. Stable C controls do not turn this into an idle-host run.

The after wheel passes 154 installed controls, 32 Rust tests, and 845 focused
source tests (37 skipped). The comparison-harness follow-up adds one tooling
check, yielding 155 installed passes. The before wheel independently passes its
own 148 checks. The harness now validates each wheel with its matching source
suite, records those test hashes, and still uses one common current timing
harness for both variants. This avoids demanding new private implementation
features from the old control; it does not waive final-candidate compatibility.
The earlier failed complete local compatibility run remains failed.

## Disposition and Next Work

Continue evaluating this boundary: both orders support the hypothesis that
removing integration work has a useful effect. It is not yet a retained final
candidate. Obtain the dedicated before/after comparison and all eleven workload
results, preserve failures, complete outcome/error-state ownership, and satisfy
full compatibility, package, cancellation, and resource/soak gates. Do not begin
another speculative cache or transport rewrite from these scalar results.

At this checkpoint, Tests `36230544349` is active/queued for the published after
revision; Phase 5 `36230544336` is in progress and Lint `36230544327` passes.
Do not cancel these runs with an evidence-only main push.

## Raw Evidence

[Complete local archive](2026-09-26-public-boundary-local.tar.gz), SHA-256:
`e4b007e536a703dd867efbb85ee59131a910b9b96a6b0bb9bf285694a9ad6ab5`.
It includes all 24 reports/logs, worker order/status, raw host intervals, frozen
input manifest, independently recomputed audit, coordinator/audit scripts, and
local validation logs. The manifest records all Phase 5 Python source hashes
from `12f139ce`; the reusable harness sources remain available at that revision.
The `/tmp` paths in the coordinator document this run's environments, not a
portable installation recipe or durable wheel hosting.

Report and analysis produced with AI assistance.

## Dedicated Follow-Up

Comparison [36231026807](https://github.com/martijnberger/ferrocopg/actions/runs/36231026807)
is dispatched at `bcd8ded9af06eb4ee10aa43fc18e6c231a2cc441` against the frozen
before revision and is queued. This successor changes documentation and the
comparison harness, not the runtime measured locally. Do not dispatch a duplicate.

The separate main acceptance workflow's benchmark job has completed successfully.
[Independent audit](2026-09-26-public-boundary-three-run-benchmark.json) validates
three complete eleven-workload comparisons, 99 raw workers, and all 89,100
operation latency observations. Every ratio passes the unchanged beta limits.
The greatest Rust/Python ratio is 1.053 (transaction); the greatest Rust/C ratio
is 1.364 (prepared). This is beta numerical evidence, not near parity or a
comparison against the pre-integration implementation.

The uploaded Linux wheel SHA-256 is
`62f736e0525f8995a1f20a9c36426f3665fedd8764aeb3c8549924a07877ee4c` and matches the
frozen summary and artifact checksum. Its 91 Python files match the local wheel;
native extension bytes necessarily differ by platform. The audit records hashes
for all 94 package files so subsequent soak artifacts can be compared exactly.
On-runner checks verified installed package/wheel identity around each run;
archived evidence cannot independently reconstruct the complete remote environment.

[Raw benchmark archive](2026-09-26-public-boundary-three-run-raw.tar.gz), SHA-256:
`7d5d7664d1d126e7a2fe5d811d8fcb9069a33f3607b5c6ae791f107348d2be26`.
It preserves all reports, worker logs, latency arrays, server/process diagnostics,
wheel checksum, and the independent audit script. The native wheel is in GitHub
artifact `10902059653`, not embedded in this text/report archive.

The soak and full compatibility jobs remain live. Process snapshots show no
concurrent build/test process at the measurement boundaries, but do not supply
continuous host-idleness evidence. Final acceptance and the remaining integration
requirements are still open; no failed earlier run has been relabeled or erased.
