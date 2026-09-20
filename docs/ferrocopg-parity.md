# Performance parity investigation

## Decision and scope

For the current workload budget, ownership table, experiment dispositions, and
remaining decision, see [Phase 5 bottlenecks](ferrocopg-bottlenecks.md). This
investigation retains the chronological evidence, including failed prototypes;
historical next-step text is not a request to repeat completed work.

The fork exists to provide a trustworthy Rust backend behind Psycopg's proven
API. Phase 5 now distinguishes beta acceptance from the work needed to approach
performance parity. The approved beta ceilings remain 1.15 times Python and 1.50
times C for every workload. The engineering objective is Python parity and C
parity where practical; "near parity" means no more than 1.10 times C median
duration for research tracking, not a new release gate.

We cannot infer that the underlying Rust library is slow from a Python-level
comparison. The upstream [`postgres` implementation](https://docs.rs/postgres/0.19.13/postgres/#implementation)
wraps `tokio-postgres` with a Tokio runtime. The
[`tokio-postgres` client](https://docs.rs/tokio-postgres/0.7.17/tokio_postgres/#pipelining)
supports pipelining, but that does not imply the same latency for a synchronous
single-query API. Concurrency and single-operation latency are different tests.
Our repository also patches both libraries, so measurements of the vendored
client are not measurements of pristine upstream.

The Python-visible API and supported behavior are the contract; Cython's
internal object graph is not. We may replace private bridge objects and state
duplication, and redesign the Python/native call boundary. Public cursor types,
signatures, metadata, adaptation and row-factory callbacks, exceptions, resource
lifetime, cancellation, and concurrency must remain compatible. Tests for a
private implementation shape should not force us to preserve that shape when
the same observable contract can be tested without it.

## Layer map

| Layer | Work performed | Evidence needed |
| --- | --- | --- |
| Public Python cursor and connection | Cursor creation, factories, parameter adaptation, prepared cache, transaction state, result navigation | Full API versus direct binding; allocation/call profiles; reused versus fresh cursor |
| Compatibility adapters | Cursor/result state synchronization, metadata projection, encoding, SQL classification, loader context | Attribute costs with semantics-preserving controls, not omitted behavior |
| PyO3 binding | Argument/result objects, GIL release/reacquisition, session mutex, signal state and wait callback | Direct native-session binding versus Rust-only session |
| Rust session | Prepared lookup, parameter validation, result metadata and wire rows | Rust session versus direct vendored client |
| Synchronous Rust client | Runtime entry, future polling, notices and wait timers | Vendored client with/without callback, direct Tokio, pristine upstream |
| Protocol and transport | Messages, buffers, socket waits, decoding, server round trips | Native libpq control with matched protocol; syscall/packet trace separately |

Relevant implementation entry points are `Cursor._sync_ferrocopg_cursor()` in
`psycopg/psycopg/cursor_async.py`, `NoTlsCursorAdapter.execute()` and
`NoTlsConnectionAdapter._execute()` in `psycopg/psycopg/_ferrocopg.py`,
`with_session()` in `crates/ferrocopg-python/src/bootstrap.rs`,
`run_statement_refs()` in `crates/ferrocopg-postgres/src/session.rs`, and
`poll_block_on_inner()` in `vendor/postgres/src/connection.rs`.

These show plausible costs, not measured attribution by themselves. In
particular, the public cursor owns a backend cursor adapter and synchronizes
result state after operations. Query execution also performs classification,
encoding/transaction bookkeeping, and loader/factory work. The native result
path builds column descriptions and clones names. None of these costs can be
assumed dominant merely because their code is visible.

## Matched first probe

`tools/phase5/layer_probe.py` compares eight paths in forward and reverse order:
native libpq, vendored `postgres`, vendored `postgres` with a no-op wait callback,
the Rust session, its direct PyO3 binding, official Python, official C, and the
full ferrocopg API. Build both native binaries before any measurement:

```sh
cargo build --release --offline --locked -p ferrocopg-postgres --example layer_probe
cc -O3 -Wall -Wextra -Werror $(pg_config --cppflags) \
  -I"$(pg_config --includedir)" -L"$(pg_config --libdir)" \
  tools/phase5/libpq_probe.c -lpq -o /tmp/phase5-libpq-layer-probe
export PHASE5_DSN='host=127.0.0.1 dbname=postgres user=postgres sslmode=disable'
revision=$(jj log -r @ --no-graph -T commit_id)
/tmp/phase5-env/bin/python tools/phase5/layer_probe.py \
  --native-rust target/release/examples/layer_probe \
  --native-libpq /tmp/phase5-libpq-layer-probe \
  --revision "$revision" --output /tmp/phase5-layer-diagnostic
```

Use the installed-wheel environment from the performance guide. If
`CARGO_TARGET_DIR` is set, adjust the Rust binary path. On macOS, set
`DYLD_LIBRARY_PATH` as described there. The C probe uses POSIX clocks; this
diagnostic is not a Windows portability claim. Its output must be a new path.

Every path prepares `select 42::int4` before measurement, uses binary results,
autocommit, a persistent plaintext connection, and one query in flight. It checks
the returned value on every operation. Warmup is 1,000 operations, followed by
nine samples of 10,000 operations. Full API probes create a cursor for each call,
as normal `conn.execute()` does. The binding probe reads raw bytes rather than
running the public adapter/row-factory machinery; that difference is intentional
and must not be advertised as drop-in API performance.

Reports preserve wall-time samples, executable/source/lockfile hashes, and
installed Python implementation fingerprints. This is a latency diagnostic,
not CPU/allocation profiling and not a replacement for the eleven-workload
release suite. The workload has no bound parameters, TLS, pool contention, COPY,
or explicit transaction commands. It does not isolate pristine upstream or
direct Tokio yet. Differences between separate paths are useful hypotheses,
not an additive profiler breakdown or a proof that all overhead is removable.

## Initial evidence: 2026-09-20

### Matched parameter/result-format probe extension

The layer probe now also accepts `--case prepared` and `--case unprepared`
for `select %s::int + 1`, `(41,)`, with `--result-format text|binary`.
Native paths execute the matching `$1` query with binary `int2` (OID 21)
parameters, using typed prepare or a typed unprepared extended query rather
than silently adding a prepare round trip. The public paths assert their actual
dumped parameter OIDs/formats and result OID/format outside timing. The existing
`constant`/`binary` default remains available; constant text is an additional
control. Explicit `sslmode=disable` is required so libpq/public paths cannot
negotiate TLS while the native probes use plaintext.

Rebuild both native probes before using the new options, then for example:

```sh
/tmp/phase5-env/bin/python tools/phase5/layer_probe.py \
  --native-rust target/release/examples/layer_probe \
  --native-libpq /tmp/phase5-libpq-layer-probe \
  --revision "$revision" --case prepared --result-format text \
  --output /tmp/phase5-prepared-text-layers
```

Run each case/format with a fresh output directory. Each invocation already
performs all eight layers in both orders; keep builds, tests, and profiling
outside timing. The new cases are not yet included in a completed dedicated
runner matrix, and their local smoke timings must not be treated as performance
evidence. The original constant-query dedicated evidence remains unchanged.

[Smoke evidence](performance/2026-09-20-matched-layer-smoke.json) and
[complete reports/source snapshots](performance/2026-09-20-matched-layer-smoke.json.gz)
verify all 96 case/format/layer/order combinations on the unchanged installed
baseline wheel, using only ten measured operations per sample and three samples
after 1,000 warmups. This is correctness/protocol validation on a non-idle Mac,
not a speed comparison. Both native probes build in release mode; all 107
installed/accounting checks pass. Two failed attempts remain preserved: an
early DSN-parser import selected the wrong backend, and official execute did
not retain its optional query cache. Backend selection now precedes parser
import, and parameter validation uses the actual retained dump state; both
issues have regression tests. No production backend code changes in this slice.

### Dedicated matched-layer matrix

`tools/phase5/layer_matrix.py` coordinates all six case/format combinations,
each with eight layers in both orders, nine samples of 10,000 operations, and
1,000 warmups per worker. It validates the exact protocol, all raw sample
medians, installed-code fingerprints, server/environment identity, and native
binary/source hashes. Wheel and comparator identities are checked before and
after every case. Failures retain partial results and cannot produce a completed
manifest. The coordinator and its rejection cases pass in the 112-test Phase 5
suite; the earlier live smoke checks validate the individual probe paths.

The subsequent host-activity extension brings that suite to 116 passing tests.
For each case, a separate coordinator thread samples system CPU counters and
process CPU deltas approximately every two seconds throughout both orders.
The JSONL record retains initial counters, active/new/disappeared processes,
unavailable-process counts, actual interval lengths, and the observer thread's
own CPU time. It records process names/PIDs, not arguments or environments.
Sampling failure fails the diagnostic rather than silently omitting telemetry.
The Mac live smoke produced three intervals over 3.31 seconds with 0.316 seconds
of observer thread CPU, so this observer is not presumed free. Its Linux cost
must be assessed from the new artifact; the smoke is not performance evidence.
CPU activity by processes that start and exit between samples can be missed,
and the initial snapshot's coordinating-thread CPU is outside the thread total.
No threshold or automatic host-idleness verdict is introduced.

After publishing the diagnostic revision, dispatch only this mode:

```sh
gh workflow run phase5.yml --repo martijnberger/ferrocopg \
  --ref martijn/phase5-notice-lock -f layer_matrix=true
```

The workflow builds and tests the release wheel and both native probes before
timing, skips acceptance/codegen jobs, and rejects conflicting diagnostic inputs.
It preserves the wheel, executables, all reports/logs, and manifest in a
`phase5-layer-matrix-<revision>` artifact for 90 days. Audit the exact revision
and raw reports after completion; a dispatch or green workflow alone does not
establish a performance conclusion. This extension has not yet completed a
dedicated run. It does not measure GIL transitions or scheduler wakeups, nor
does its periodic sampling prove perfect host idleness. Review observer cost,
all sampling intervals, and the existing before/after inventory when assessing
the timing evidence; do not silently describe these measurements as observer-free.

### Original constant-query diagnostic

The [recorded diagnostic evidence](performance/2026-09-20-integration-diagnostic.json)
contains all uninstrumented samples in both orders, package/executable/source
fingerprints, and the top separately profiled functions. Production code is the
restored `52b5bcd8` implementation, also present in `7c1a644a`. The installed Rust
wheel SHA-256 is `924f3af85c5b6b31047f901b8bc668a0d1891bfb91ef6bb60c12cfa63a0e11ca`.
Probe source working-copy snapshots were `1c1d7237` and `56e2dc40`; these are not
claims that a new production wheel was built at those revisions.

Environment: macOS ARM64, CPython 3.14.6, PostgreSQL/libpq 15.19, official Psycopg
3.3.2, portable ThinLTO release build. The machine had background UI/browser/node
activity, so this is exploratory evidence requiring idle-runner replication.
All builds and tests finished before timing; profiles ran only after timing.

Matched layer medians, microseconds per operation:

| Path | Forward | Reverse |
| --- | ---: | ---: |
| Native libpq | 19.598 | 19.619 |
| Vendored Rust `postgres` | 18.812 | 18.850 |
| Vendored Rust with wait callback | 18.978 | 19.051 |
| Rust session/result construction | 19.423 | 19.497 |
| Direct PyO3 session binding | 21.679 | 21.689 |
| Full official Python API | 41.448 | 41.726 |
| Full official C API | 31.034 | 30.293 |
| Full ferrocopg API | 40.964 | 41.618 |

For this narrow query, there is no evidence that the Rust client's latency floor
prevents parity: it is slightly faster than native libpq in both orders. The
binding is about 2.2 microseconds above the Rust session; full ferrocopg is about
19.3-19.9 microseconds above the binding. These differences identify where to
investigate, not independent additive costs or wholly removable work. Public
Psycopg necessarily does more than a raw-byte native probe.

### Integration controls

`tools/phase5/integration_profile.py` separately measures explicit cursor
creation/execute/fetch, rather than the layer probe's `conn.execute()` path.
It supports fresh/reused public cursors and fresh/reused backend adapters as
diagnostic bypasses. Each case has 1,000 warmup operations and nine samples of
5,000 operations per order. All queries use prepared statements and binary
results. The parameterized case is `select %s::int4 + 1`, `(41,)`.

For example, run the following independently for each backend/case/workload,
then repeat the sequence in reverse order. Only Rust supports adapter cases.
Run `--profile /tmp/profile.pstats` separately after all timing cases, never
concurrently; it emits profile statistics instead of benchmark samples.

```sh
/tmp/phase5-env/bin/python tools/phase5/integration_profile.py \
  --backend rust --case public-fresh --workload parameterized \
  --revision "$revision" --iterations 5000 --samples 9 \
  --output /tmp/integration-rust-fresh.json
```

| Cursor path | Constant forward/reverse (us) | Parameterized forward/reverse (us) |
| --- | ---: | ---: |
| Rust public, fresh | 41.763 / 41.262 | 47.281 / 46.659 |
| Rust public, reused | 36.333 / 35.513 | 42.474 / 42.032 |
| Rust adapter, fresh | 35.351 / 33.480 | 41.556 / 41.866 |
| Rust adapter, reused | 33.350 / 31.919 | 40.382 / 40.207 |
| C public, fresh | 25.612 / 28.249 | 29.760 / 31.841 |
| C public, reused | 22.243 / 26.571 | 28.723 / 28.302 |

CPU medians also expose client-side work: fresh Rust constant queries consume
29.9-30.2 us versus C's 14.5-15.4 us; parameterized queries consume 35.0-35.8 us
versus C's 17.0-17.7 us. Wall-time variation across orders remains visible and
must not be hidden by selecting one order or mixing these figures with another
experiment's native baseline.

Reusing the public cursor saves approximately 4.6-5.7 us. Bypassing the public
wrapper saves approximately 4.8-7.8 us for fresh cursors, but does not close the
gap. These savings overlap: do not add them. A bare adapter omits Python-visible
public-cursor behavior and is not an acceptable replacement without redesign.

Separate cProfile runs of 10,000 fresh-cursor operations report:

- Rust: 1,850,001 observed calls for constant queries and 2,250,001 for parameters.
  C: 550,001 and 630,001 respectively. C functions conceal internal work, so
  this is evidence of extra Python dispatch, not a comparison of all CPU work.
- Rust constructs `AdaptersMap` 20,000 times versus C's 10,000 in both workloads.
- Rust runs `_sync_ferrocopg_cursor()` and `pgresults()` 20,000 times: after
  execution and fetching, despite one result being fetched per operation.
- Query routing/prepared bookkeeping and `_result_loaders()` /
  `_native_result_transformer()` are prominent Python-side paths. Reused cursors
  still reset query/result transformers on every execution, so cursor reuse alone
  does not eliminate per-query adaptation/loader setup.

Do not use profiled runtimes as benchmark timings or sum cumulative profile
times: instrumentation changes costs and cumulative times overlap. Full `.pstats`
files and worker reports remain under `/tmp/phase5-integration-20260920`; the
linked repository evidence preserves raw timing samples and selected profile
statistics for review.

## Proposed integration shape

The evidence supports **one owner for cursor/result state, with a thin public
Python facade**, rather than a public cursor synchronizing a second cursor-like
adapter. This does not require reproducing Cython internals or exposing native
implementation classes as a new public API.

1. Replace the public-cursor/backend-cursor state mirroring with one execution
   state. Keep the public cursor type and behavior, and project required public
   metadata from stable native result handles without copying bookkeeping on
   every fetch. This is a structural redesign, not the rejected lazy-property
   shortcut. Internal `_results` layout is not itself a product requirement.
2. Give query/loader plans an explicit lifetime and invalidation mechanism.
   Cache only against parameter types, result shape/format, adapter changes,
   encoding, and prepared/session state. Reuse immutable plans; do not reuse
   stale mutable callback contexts or assume built-in adapters are unmodified.
3. Use a coarse native execution/result boundary where measurements justify it.
   Keep PostgreSQL bytes and row offsets owned natively; expose Python values
   and metadata when required. Preserve registered Python adapters/row factories,
   reentrancy, exception ordering, notice delivery, cancellation, GIL/locking
   rules, and results that outlive a cursor or connection.

The next prototype should address step 1, accompanied by a public-contract test
matrix and the same fresh/reused controls. It should be judged on uninstrumented
wall **and CPU** costs in both orders, then all eleven workloads. The bypass
control suggests a useful opportunity, not a promised production speedup.
Step 1 alone is unlikely to close the full gap: in the parameterized controls,
reaching C +10% would require roughly 11.6-14.5 us (25-31%) less total duration.
Plan reuse and query bookkeeping therefore need separate measured follow-ups.

### Public callback ownership baseline

The first contract check found an existing mismatch: synchronous row factories
received `NoTlsCursorAdapter`, not the public cursor. Tests against C confirm
that factories see the exact public cursor and its selected result metadata,
and that selecting a result or replacing a factory initializes the row maker
before fetching. Client, raw, server, and streaming cursor controls cover this
boundary. The compatibility fix uses a weak public-owner reference and preserves
the experimental async facade's explicit ownership transfer. It is not a speed
optimization: until state ownership is consolidated, publishing metadata before
the callback still requires synchronization. Use the corrected behavior as the
control for 5C.1; do not claim gains from skipping or changing callbacks.

### Single-owner prototype: 10f83f98

The first prototype removes the mirrored BaseCursor initialization and the
after-execute/after-fetch synchronization method. One execution state owns the
query, adapter map, result handles, configured format, arraysize, and lifecycle;
normal cursor row position comes directly from its current result. Metadata is
captured at result adoption, not lazily after connection settings may change.
Server cursor default format and active execution format are distinct. Public
factory identity and subclass metadata descriptors remain supported.

Compare it with the corrected callback baseline `8919d7d9`, not the earlier
incorrect callback context. Both installed wheels pass all 78 Phase 5 tests and
contain the same native extension bytes (SHA-256
`81ecf7f6bd0e809c0849b38b4d9fb67026ef3904fca2a7a6e87766784d726dab`).
The final targeted Rust suite passes 748 cases; C cursor controls pass 501.
These checks are not a full final compatibility-matrix verdict.

The full final local run passes 4,748/4,749 supported synchronous cases. Its
only synchronous failure is the unchanged pool `test_check_backoff` timing
assertion, previously reproduced with C too. The strict gate therefore still
fails; no tolerance or manifest was changed. Raw local artifacts are
`/tmp/phase5-single-owner-final-full.xml`, its `-report.json`, and its `.log`.
The supported remote matrix remains pending; experimental native async failures
are classified separately rather than treated as supported-backend regressions.

[Raw integration measurements and profile summaries](performance/2026-09-20-single-owner-integration.json)
preserve 1,000 warmup operations, nine samples of 5,000 operations, both orders,
installed file fingerprints, and machine/server metadata. This Mac was **not
idle**. Ratios below are prototype/baseline; smaller is better, and a/b are
opposite variant orders, not two opportunities to select the better result.

| Prepared binary control | Wall a / b | CPU a / b |
| --- | ---: | ---: |
| Fresh cursor, constant | 0.990 / 0.869 | 0.975 / 0.903 |
| Fresh cursor, parameterized | 0.960 / 0.972 | 0.944 / 0.968 |
| Reused cursor, constant | 0.989 / 0.999 | 0.995 / 0.997 |
| Reused cursor, parameterized | 0.979 / 1.000 | 0.985 / 0.992 |

The fresh parameterized improvement is modest (2.8-4.0% wall); reused controls
are near-flat. The large constant-query order difference limits confidence.
Separate 10,000-operation profiles remove all 30,000 synchronization calls but
add 20,000 result-adoption/publication events. Total calls decrease from
1,940,001 to 1,850,001 for constant queries and 2,340,001 to 2,250,001 for
parameterized queries. Query conversion, execution bookkeeping, and loader
setup remain substantial. Profile durations are not query latency evidence.

**Disposition: retained as the development foundation, not beta acceptance.** Dedicated comparison
[35506310291](https://github.com/martijnberger/ferrocopg/actions/runs/35506310291)
completed for this exact prototype and baseline. It includes both-order
fresh/reused controls, the longer prepared/parameterized controls, and all eleven
workloads. Raw samples, manifests, and complete worker reports are preserved in
[the dedicated comparison evidence](performance/2026-09-20-single-owner-dedicated.json).
The repository copy retains all sample totals and percentile summaries; large
per-operation latency arrays remain in the linked, unmodified CI artifact.
All medians, both-order ratios, wheel hashes, and both complete verdicts were
independently recomputed after download. The baseline and candidate native
extensions are byte-identical (SHA-256
`6275e4a319d7e39ea9be94b6f34f2cf78ce7585ce4803ea6591e39c1062f9f8b`).

| Long query control | Wall a / b | CPU a / b |
| --- | ---: | ---: |
| Prepared | 0.9935 / 0.9826 | 0.9809 / 0.9709 |
| Parameterized | 0.9820 / 0.9856 | 0.9616 / 0.9739 |

Each long control uses 10,000 warmups and nine 100,000-query samples. The smaller
fresh/reused controls remain modest or near-flat: wall ratios span 0.977-0.997,
and two first-order CPU ratios are slightly above one. Do not inflate this into
a broad speed claim. Retention also has a separate architectural reason: one
authoritative result/position state removes synchronization and duplicate
initialization without narrowing the supported public cursor contract.

Both complete reports pass all eleven beta workload limits. Candidate ratios
are 1.034/1.322 for parameterized, 1.018/1.334 for prepared, and 1.017/1.267 for
pool (Python/C). The complete Rust-to-Rust prepared median is 2.3% slower even
though both long paired prepared controls improve; keep both observations.
Only four candidate workloads meet both engineering near-parity targets in this
report. This remains one complete report per variant in experiment mode, not
three acceptance runs. A final three-run gate, full soak,
supported compatibility matrix, and scheduled-run evidence are still required.

### Compatibility follow-up to the frozen prototype

All eight Rust artifacts from Tests `35506169516` explain an exact
inventory drift introduced by the public-callback controls in `8919d7d9`:
two common-cursor tests each expand across three cursor classes, and one server
test expands across two classes. This adds eight supported synchronous cases.
Their async counterparts add two counted server cases and six cases covered by
the existing experimental-async manifest. No exclusions were added. Accordingly,
the committed inventory increases `sync.total` by eight, `async.total` by two,
and `async.manifested` by six for every matrix key; `sync.manifested` stays 249.
The downloaded Python 3.11/PostgreSQL 14-18, Python 3.12/PostgreSQL 16,
Python 3.13/PostgreSQL 17, and Python 3.14/PostgreSQL 18 JUnit reports independently
confirm this exact delta. The [original matrix counts](performance/2026-09-20-single-owner-compatibility.json)
retain each report's outcomes and exact source revision.

Reclassifying these unchanged reports with the corrected inventory passes seven
keys. Python 3.11/PostgreSQL 14 still fails the strict zero-regression gate:
`test_preserves_autocommit[asyncio-pipeline=on-False]` receives two unraisable
warnings from previously created `FerrocopgAsyncCursor.nextset` coroutines.
The existing async server-cursor tests call this method synchronously, as the
public API requires, but the experimental wrapper had declared it `async def`.
The follow-up makes local result navigation synchronous, without a worker or
warning suppression, and extends existing facade/live checks for both `True`
and `None` outcomes. This does not expand supported native-async scope or change
the frozen `10f83f98` performance comparison. The old failing report remains
failed; the follow-up still needs its own compatibility run.

The fix is checkpointed in `775299eb`, with its dynamic delegation return cast
corrected in `87de5ef3`. Configured mypy passes all 239 source files. Two existing
server-cursor navigation cases, two mocked/live facade cases, and all 17 harness
tests pass. The first full follow-up run is preserved at
`/tmp/phase5-compat-successor-775299eb.xml`: its command omitted the virtualenv
from `PATH`, so typing-test subprocesses could not find `mypy`. It also preceded
the return-cast correction. Do not use that run as acceptance or silently
discard it. The corrected `87de5ef3` full run uses an explicit virtualenv `PATH`
and passes 4,746/4,749 supported synchronous cases, with zero sync errors. Its
three timing failures are pool `test_check_backoff` (105.192 ms versus a 105 ms
ceiling), notification `test_notify` (25.443 s versus 0.5 s expected), and
`test_wait_r_no_linux[wait_poll-1-NONE-2]` (9.231 s versus under 2.4 s required).
The full strict gate remains failed. Artifacts are
`/tmp/phase5-compat-successor-typed.xml`, its `.log`, and its `-report.json`.
Isolated C and Rust controls each pass notification/poll and fail pool backoff;
retain `/tmp/phase5-successor-{c,rust}-timing.xml`. These controls do not replace
the full run or prove its extreme delays were driver-independent.

The published successor `8eddc2ec` now passes all 57 jobs in
[Tests 35508454735](https://github.com/martijnberger/ferrocopg/actions/runs/35508454735),
including all eight Rust matrix entries. All downloaded JUnit reports were
independently reclassified with the unchanged strict zero-regression gate,
manifest, floors, and exact-count baselines; each exactly matches its uploaded
JSON report. Supported synchronous passes are 4,483 on Python 3.11/PG14,
4,623 on each of the six intermediate keys, and 4,656 on Python 3.14/PG18,
with zero supported synchronous failures or errors throughout. Experimental
native-async failures remain separately visible. [Recomputed reports](performance/2026-09-20-retained-compatibility.json)
record the exact revision and all scopes. Lint `35508454729` also passes.
This establishes compatibility of the retained runtime, not three performance
passes, a new soak, or scheduled-run evidence. The failed local runs above
remain failed and are not replaced retroactively.

## Next experiments and decision rules

### Execution bookkeeping opportunity (2026-09-20)

The fixed-query diagnostic now supports `--bookkeeping omit-helper-bodies`.
It omits only `_refresh_client_encoding`, `_refresh_session_timeout`, and
`_update_transaction_state`, retaining their calls and all other API work.
Only the Rust public-API control permits it; the connection must be idle,
autocommit, and UTF-8. The script only executes its fixed scalar SELECTs and
restores methods before connection cleanup, including on failure. Omission is
not a compatible backend implementation. Candidate comparison accounting
explicitly rejects reports carrying either the ablation mode or omitted helpers.

The frozen installed control is production revision `8eddc2ec`, wheel SHA-256
`9df38f89e78f5fbc921eb60add9fe5c2e2ba6a84dcd987239138111e37523932`.
All eight controls use 1,000 warmups and nine samples of 50,000 operations,
fresh public cursors, prepared binary results, and both measurement orders.
Installed fingerprints and server settings match throughout. There are no
overlapping builds, tests, or profilers. This Mac is **not idle**: the initial
process snapshot was unavailable due to sandbox permissions; a post-run
snapshot showed Ghostty 21.1%, WindowServer 18.0%, and node 6.5% CPU.

| Workload | Omitted/normal wall a / b | Omitted/normal CPU a / b | Wall savings a / b |
| --- | ---: | ---: | ---: |
| Constant | 0.9731 / 0.9752 | 0.9722 / 0.9645 | 1.089 / 0.999 us |
| Parameterized | 0.9833 / 0.9177 | 0.9777 / 0.9623 | 0.783 / 4.233 us |

The reverse parameterized wall result is much larger than its CPU difference
(1.371 us); do not advertise a repeatable 8.2% gain. Constant-query results
suggest about 1 us of removable helper-body cost in this workload. The control
does not quantify all outcome-related work, the cost of a replacement, or
transaction/pool benefits. It is not a mathematical upper bound. The previous
separate parameterized profile attributes 3.6% of instrumented cumulative time
to these three non-overlapping helpers, consistent with investigating a small
component rather than claiming the whole public API gap is removable here.
[Raw samples, exact identities, and profile counts](performance/2026-09-20-bookkeeping-opportunity.json)
preserve both orders and all caveats. All 84 installed/accounting checks pass.

**Disposition:** do not prioritize a standalone native outcome envelope as the
route to near parity. Retain it as a separate correctness/architecture proposal,
not an implemented or performance-accepted change. `Client::parameter()` in the
vendored Tokio client exposes startup parameters; live `ParameterStatus` updates
are owned by the connection. Query consumers discard `ReadyForQuery` status.
A proper design must retain per-request command/transaction/setting events,
including errors, cancellation, and each intermediate pipeline operation.
Copying a connection's final status onto all results would be incorrect.

The next discriminating experiment proposed removing intermediate **parameter
representations**, not cache mutable callback instances or merely move generic
Python dispatch into Rust. `_convert_query_params()` currently turns the
transformer's separate values/types/formats into Python tuples, then PyO3
extracts another vector before `bound_params()` maps it into native parameters.
Compare a single native execution packet against this path, preserving the
existing Python adaptation callbacks and preparation policy. Capture mutable
buffer contents at the current conversion point, before preparation can run
notices or other callbacks; deferring the copy until execution can change values.
Do not add a cache or assume an extra native builder call is free. Measure the
packet construction and full fresh/reused query path before deciding retention.
The bounded packet implementation below now tests this hypothesis; it is not a
replacement for the complete execution/adaptation layer.

### Native parameter packet disposition (2026-09-20)

Prototype `0390bdc9` creates an immutable Rust-owned parameter packet at the
existing conversion point, then shares its owned buffers with the native
execution call. It removes the intermediate Python tuple list without moving
adaptation callbacks into Rust or caching their instances. Legacy native calls,
non-default query classes, and encoding bridges retain compatible paths.
Tests cover mixed formats/NULLs, non-contiguous buffers, custom `__bytes__`,
bytes subclasses, owner release, prepared/unprepared calls, and mutation before
preparation. The snapshot test explicitly confirmed the candidate used a packet.

The release wheel passes all 87 installed/accounting checks, and 437 focused
upstream cases pass. Its full local classifier remains **failed**: 4,748/4,749
supported sync cases pass with zero errors, but pool `test_check_backoff` exceeds
the unchanged 105 ms ceiling. All other sync families pass; experimental async
remains separate (511/622). Preserve `/tmp/phase5-packet-full.xml`, its log, and
its classifier report. Fresh configured mypy passes 239 files after annotating
the staging helper's variable-length replacement tuple; that annotation changes
no staged runtime behavior.

Both frozen environments have matching dependency versions. The sixteen controls
use 1,000 warmups and nine samples of 50,000 operations in both orders, with no
overlapping builds, tests, installs, or profiling. This Mac was **not idle** and
had substantial desktop/game activity. The longer-sample verifier requires the
declared iteration count in every report; it cannot silently accept default-size
or shorter data. Ratios are candidate/baseline:

| Control | Wall a / b | CPU a / b |
| --- | ---: | ---: |
| Fresh constant | 0.9982 / 0.9855 | 0.9975 / 0.9809 |
| Fresh parameterized | 0.9657 / 1.0441 | 0.9488 / 1.0716 |
| Reused constant | 0.9865 / 1.0038 | 0.9800 / 1.0010 |
| Reused parameterized | 0.9817 / 0.9898 | 0.9814 / 0.9864 |

**Disposition: reject and restore the retained runtime.** The fresh-parameterized
effect changes sign in both wall and CPU results. Reused-parameterized gains of
about 1-2% do not justify the extra packet/ownership machinery without a
repeatable fresh-query benefit. This is not proof that every packet design is
ineffective, nor a stable 4.4% regression claim from a busy machine. No all-eleven
candidate comparison was run because the prototype was not retained.
[Raw measurements, wheel identities, profile, and failed compatibility report](performance/2026-09-20-parameter-packet-rejected.json)
preserve the complete result, not just favorable samples. The prototype remains
in history; its private-only test is removed with it, while the mutable-buffer
snapshot contract and longer-sample accounting remain. Production Rust/Python
backend files again exactly match `8eddc2ec`; the source extension is rebuilt
without the packet. The rejected wheel remains isolated for reproducibility.
All 86 retained installed/accounting checks pass without skips; fresh configured
mypy again passes all 239 files after restoration.

The next priority is the outstanding 5B attribution work on a dedicated runner,
including broader workload profiles and allocation/polling/syscall evidence.
Another small wrapper or generic-transformer rewrite is not justified by these
results. A larger Rust-owned execution/adaptation design needs a measured cost
budget and a separate retention decision, not a claim that these four bounded
experiments have already delivered near parity.

### Acceptance checkpoint

The retained production implementation is frozen for full acceptance at
`7bbc14eba9a573bf528317f0859a8734db589e19`; its runtime matches the green
`8eddc2ec` compatibility checkpoint. [Workflow 35511989284](https://github.com/martijnberger/ferrocopg/actions/runs/35511989284)
has completed successfully, including benchmark and full three-backend soak
jobs. All three benchmark reports and their 99 individual worker files were
downloaded and independently checked against the frozen 100-iteration,
nine-sample, 1,000-row protocol and unchanged beta policy. Every workload passes
both limits in every run. Machine/server identities match throughout, and the
wheel SHA-256 agrees with both the runner's summary and its separate hash file:
`727b049d201963c7f8ea09c59356f3708355dd4ad84fd7d6ca85922ab2567d7b`.
The artifact is `10605224553`. [The readable comparison reports](performance/2026-09-20-retained-three-run-benchmark.json)
retain all nine-sample wall/CPU totals, percentile summaries, identities, and
before/after process snapshots. The [complete compressed reports](performance/2026-09-20-retained-three-run-benchmark.json.gz)
also preserve every per-operation latency array; the readable file records the
archive hash. No sample is discarded to fit the repository's file-size limit. No process
exceeds 5% CPU in the pre-run snapshot; post-run `docker-proxy` is 8%. These are
dedicated-runner snapshots, not continuous host-idleness telemetry.

| Workload | Rust/Python range across three runs | Rust/C range |
| --- | ---: | ---: |
| Parameterized | 1.012-1.074 | 1.162-1.327 |
| Prepared | 1.004-1.030 | 1.353-1.391 |
| Pool | 1.018-1.060 | 1.231-1.310 |

This establishes the three-run **beta benchmark gate**, not near parity. Plain
connection, TLS connection, and binary COPY meet both engineering targets in
all three reports; dictionary rows do so only in run 2. The remaining workloads
still exceed at least one near-parity objective. The separate checkpoint Tests
`35511889731` are now terminal success with all 57 jobs passing, including the
eight Rust compatibility jobs and package job. Lint `35511889747` also passes.
Do not restart completed acceptance. The final combined acceptance audit,
scheduled-run proof, and remaining attribution work are still outstanding.

The [frozen compatibility audit](performance/2026-09-20-frozen-compatibility.json)
independently recomputes every field of all eight uploaded classifier reports
from their raw JUnit XML with unchanged manifests, floors, baselines, and a zero
synchronous regression budget. Supported synchronous passes are 4,483 on
Python 3.11/PostgreSQL 14, 4,623 on the six intermediate configurations, and
4,656 on Python 3.14/PostgreSQL 18, with no synchronous failures or errors.
Experimental native-async failures remain in the original reports rather than
being relabeled passes. The [compressed originals](performance/2026-09-20-frozen-compatibility.json.gz)
retain all eight XML files, original and recomputed reports, classifier logs,
verification inputs, artifact metadata, and the completed package-job log.
The independent package job builds a CPython 3.11 release wheel and verifies
Rust-only queries, coexistence with official Psycopg, and uninstall isolation.
Its wheel SHA-256 is
`a03d93047caf4c3802e18c0ac948500102caa6049486557991143bcc8bc1a4aa`;
the artifact is `10607112769`. Namespace/member hashes are independently checked.
This smoke job resolved official Psycopg 3.3.6, unlike the pinned 3.3.2 benchmark
comparator; its wheel is not the CPython 3.14 performance wheel. This proves the
supported synchronous and package-boundary gates at the frozen revision, not
Phase 6's full platform wheel matrix. The archive also retains the wider
workflow's terminal metadata: all 57 jobs completed successfully.

The full soak artifact is `10607420931`. [The audit summary](performance/2026-09-20-retained-full-soak.json)
records independently recomputed resource budgets, worker identities, coverage,
and cleanup; [the compressed originals](performance/2026-09-20-retained-full-soak.json.gz)
retain the full report, all three worker reports and logs, server configuration,
process snapshots, and wheel hash file. The 33 MB PostgreSQL log remains in the
CI artifact with its SHA-256 recorded in the summary; it is not silently omitted
from the evidence inventory.

| Backend | Measured seconds | Measured cycles per scenario | Resource samples | RSS median growth |
| --- | ---: | ---: | ---: | ---: |
| Rust | 1800.221 | 33,060 | 1,654 | 1.41 MiB |
| Python | 1800.737 | 21,480 | 1,075 | 0.91 MiB |
| C | 1800.579 | 32,040 | 1,603 | 1.20 MiB |

Every worker exercises churn, transactions, cancellation, text/binary COPY,
pipeline, pool, and concurrent pool contention. Measured counts above exclude
the three warmup epochs (60 additional cycles per scenario). The scenario
counts match the snapshot/epoch count exactly. All worker JSON files match their
combined-report rows, and the original growth and late-RSS budgets recompute.
Driver-object, thread, socket, descriptor, and workload-session growth is zero.
Final cleanup retains one observer socket and one main thread, with zero
workload-owned sessions; these are not leaked workload connections. RSS growth
uses the original first/last-three-sample median rule, not endpoint subtraction.

The soak was built separately on its dedicated runner. Its wheel SHA-256 is
`010226ec58f67fdca556fbd8655367724461167da725313164cb2d9cf1841de7`, verified
against the uploaded hash file. All 94 `ferrocopg/` package members are
byte-identical to the benchmark wheel, including native extension SHA-256
`6275e4a319d7e39ea9be94b6f34f2cf78ce7585ce4803ea6591e39c1062f9f8b`.
The different ZIP hashes are retained, not relabeled as one wheel. This passes
the retained candidate's full-duration reliability gate; it does not demonstrate
near parity or scheduled execution.

### Dedicated attribution harness

`tools/phase5/attribution.py` extends the layer probes with all eleven public
benchmark workloads plus the existing pipeline/recovery workload. The optional
`attribution=true` dispatch of `.github/workflows/phase5.yml` uses a separate
dedicated Linux runner and concurrency group. It does not run or replace the
acceptance jobs. Build and install all artifacts, including pinned Memray
`1.20.0`, before measurement; the ordinary acceptance environment is unchanged.

The coordinator first runs the eight layer probes in both orders, then 72
uninstrumented workload workers: three backends, twelve workloads, and both
backend orders. Each worker records nine wall/process-CPU samples after ten
warmups. Prepared/parameterized operations use 10,000 iterations per sample;
other workloads use 100. Subsequent, separate worker processes collect 72 CPU
call profiles, 72 allocation captures, and 72 syscall traces, in both backend
orders. These instrumented
runs never contribute ordinary latency samples or acceptance verdicts.
After those 288 workers, 66 separate protocol-proxy workers cover both backend
orders and all plaintext workloads (TLS connection measurement is excluded).

Measurement boundaries are deliberate:

- CPU call profiles use main-thread CPU time, excluding setup and warmup.
  Native internals are folded into their caller and background-thread CPU is not
  attributed by this profile; ordinary samples separately record process CPU.
- Allocation captures include native and Python allocator events on all threads
  during warmed operations. They exclude workload setup/cleanup. Allocator
  layers are not distinct Python-object counts, and total allocated bytes are
  not retained memory or evidence of a leak. See the
  [Memray API](https://bloomberg.github.io/memray/api.html).
- `strace -f -c` counts include imports, setup, warmup, operations, and cleanup.
  They preserve errors and poll/network/futex counts, but are not per-query
  counts, scheduler wakeup counts, or measured PostgreSQL round trips. The
  separate protocol probe records actual ordered PostgreSQL message flow.
- The pipeline workload includes eight queued queries, error propagation, and
  recovery. Its duration is not a single-query latency measurement.

The coordinator verifies wheel and comparator identities throughout, compares
layer medians against their raw files, checks profiler capture presence and
allocation histogram totals, and preserves failed manifests. Timeouts terminate
the owned process group. Reports, raw profiler files, strace output, executables,
wheel, package inventory, server configuration, and process snapshots are
uploaded with hashes and 90-day retention.

Local validation: eight new accounting tests cover these checks, complete
288-worker plus 66-protocol-worker sequencing with mocked subprocesses, timeout
cleanup, and allocation
window boundaries. Real installed-wheel smoke runs exercised timing, C and Rust
CPU profiles, C/Python allocations, and Rust pipeline/binary-COPY allocations.
Eleven resulting worker/summary JSON files pass the same validators. These are
tooling checks on the non-idle Mac, not comparative performance evidence. The
complete Linux coordinator and strace capture still require remote execution.
The initial harness passed all 94 installed-wheel tests. With protocol tracing,
the complete suite passes 99 tests, including five parser/redaction/accounting
tests and rejection of a malformed protocol worker after all other phases.
Ruff, formatting, codespell, and actionlint also pass.

Allocation postprocessing now additionally runs `allocation_sites.py` on the
capture machine while its native libraries remain available for symbolization.
It preserves every site's counts and reconciles event/byte totals with canonical
Memray statistics, retaining explicit unresolved-site and profiling-path classes.
The coordinator rejects mismatched capture/summary hashes. This processing
happens after ordinary timings and is not itself benchmark evidence.

Do not mark 5B complete from this harness: dedicated-runner results must be
independently analyzed, the protocol captures need causal interpretation rather
than equating cycles with round trips, and the bottleneck table must distinguish
observed costs from recoverable work.
Compatibility run `35511889731` is now terminal success. The diagnostic tooling
is published at `8955d0a66b16d01ea90c683f58ee6f18b9ead0f2` and dispatched once as
[attribution run 35519527974](https://github.com/martijnberger/ferrocopg/actions/runs/35519527974).
The attribution job is terminal success, while acceptance and codegen
jobs were correctly skipped. Do not redispatch completed measurements. Production
Python/Rust sources and Cargo settings remain identical to frozen `7bbc14eb`;
this tooling-only successor also starts Tests `35519509151` and Lint
`35519509127`, both now terminal success (all 57 Tests jobs pass).

### Dedicated Linux results: initial independent audit

[The recomputed report](performance/2026-09-20-dedicated-attribution.json) and
[raw text/report archive](performance/2026-09-20-dedicated-attribution.json.gz)
preserve both measurement orders without selecting favorable samples. Artifact
`10607809668` contains 1,246 hashed result files: all 288 workload workers, 66
protocol workers, and 16 native/public layer measurements validate. Worker
identities, environment and installed-code hashes agree; medians, all cProfile
function records, allocation totals/site accounting, syscall summaries, and
protocol-operation summaries independently recompute. The wheel SHA-256 is
`67b794d451200e66402860d2aad64855190de485ab5ce03dba22758de0568220`.
All 102 installed/harness tests passed before timing. The archive retains the
original reports and verification source; binary captures/profiles, probes,
wheel, and full PostgreSQL log remain in the linked CI artifact with recorded
hashes. This diagnostic is not another acceptance run.

Runner: Linux x86-64, four logical/two physical CPUs, Python 3.14.7,
PostgreSQL 18.6, official Psycopg/C 3.3.2 and pool 3.3.0. No process exceeds 5%
CPU in the pre-run snapshot; post-run docker-proxy is 11.8%. These snapshots do
not prove continuous idleness. Absolute timings must not be compared directly
with the earlier macOS probe.

| Matched layer (us/query) | Forward | Reverse |
| --- | ---: | ---: |
| Native libpq | 81.81 | 81.56 |
| Vendored Rust client | 83.96 | 82.77 |
| Rust session | 84.01 | 84.47 |
| Direct PyO3 binding | 88.74 | 89.40 |
| Full official Python | 140.27 | 141.02 |
| Full official C | 110.33 | 109.97 |
| Full ferrocopg | 149.09 | 147.48 |

The native-client gap is only 1.2-2.2 us in this matched binary constant-query
probe, while the full API gap to C is 37.5-38.8 us. This corroborates prioritizing
integration over replacing the native client; differences between independent
paths are not additive costs or entirely recoverable work.

The separate public workloads use text results and include parameter adaptation.
Parameterized Rust/C is 1.400 in both orders, with Rust CPU 97.5-98.5 us versus
C 50.9-51.7 us. Prepared Rust/C is 1.404/1.375, with CPU 94.1/92.9 us versus
C 49.6/50.8 us. Tuple rows are 1.297/1.272, dictionary rows 1.141/1.125,
namedtuple rows 1.177/1.198, transactions 1.200/1.214, text COPY 1.224/1.217,
and binary COPY 1.059/1.033. Both connection workloads beat C in both orders.
Pool is 1.297/1.549: retain the unfavorable reverse result rather than presenting
this diagnostic as an all-workload gate pass or replacing the frozen acceptance.
The pipeline/error/recovery workload is 2.640/2.464 times C, and both protocol
orders repeat Rust's 10/12/10 completed cycles versus C/Python's 3/3/3. These
cycles alone are not general network RTTs; the ordered flow and source explain
the separate serialization opportunity.

The allocation audit found a Linux-specific interpretation gap. All totals are
consistent, but the conservative observer-effect classifier recognizes zero
profile-created frames because Linux exposes four allocation helpers before
`_PyFrame_MakeAndSetFrameObject`. The saved eight-frame stack still ends in
`PyEval_GetFrame -> call_profile_func`, so zero recognized frames must not be
read as zero profiler overhead. A bounded follow-up now recognizes only the
exact observed eight-frame Linux path; tests reject each single-frame
substitution. All 103 installed/accounting tests pass. The
[corrected classification report](performance/2026-09-20-linux-allocation-reclassification.json)
reanalyzes all 72 captured site reports without rerunning any measurements,
preserving original categories, input hashes, and every canonical total. Both
orders now identify 144,000 Rust prepared-query profiling-frame events
(32,680,000 bytes) out of 281,117 total events; C identifies 46,000 of 100,043,
and Python 111,000 of 241,134. These match the earlier macOS smoke counts.
Events are not subtracted, and other instrumented sites are not ordinary
allocation estimates. The original dedicated-run files remain unchanged.

The [bottleneck report](ferrocopg-bottlenecks.md) now records measured costs,
required savings, ownership/confidence, unknown recoverable fractions, and
explicit dispositions for the remaining plan-reuse approaches. The next
optimization/deferral decision and direct GIL/poll/wakeup coverage remain open;
syscall counts do not close those requirements by themselves.

### Allocation observer effect

The first allocation smoke captures expose a substantial profiling effect.
In CPython 3.14.6, `call_profile_func()` obtains a Python frame object before
invoking the profile callback; see [the pinned interpreter source](https://github.com/python/cpython/blob/v3.14.6/Python/legacy_tracing.c#L34).
Native allocation stacks identify the corresponding
`_PyFrame_MakeAndSetFrameObject -> PyEval_GetFrame -> call_profile_func` path.
For these earlier 1,000-operation prepared-query captures:

| Backend | All captured allocation events | Recognized profiling frame events | Bytes in those frame events |
| --- | ---: | ---: | ---: |
| Rust | 281,117 | 144,000 | 32,680,000 |
| C | 100,043 | 46,000 | 10,712,000 |
| Python | 241,134 | 111,000 | 24,912,000 |

These are instrumented allocation volumes, not live memory, unique object
counts, or unprofiled driver allocation rates. The captures were smoke tests,
not both-order comparisons. Do not subtract these frame events and call the
remainder an unbiased workload estimate: monitoring also changes execution and
can introduce other, unrecognized allocation paths. Ordinary instrumented
interpreter opcodes around application allocations are not classified as
profiling overhead merely because their names contain `INSTRUMENTED`.

The conservative analyzer distinguishes recognized profiling-frame creation,
monitoring setup, other instrumented allocations, and unresolved native sites.
It retains all classes in canonical totals and validates them against all raw
sites. Symbol availability and interpreter builds can change recognition; zero
recognized overhead does not prove zero overhead. Python-site summaries alone
would misleadingly attribute some of these events to property getters such as
`closed`, which is why the native call path matters.

[The readable evidence](performance/2026-09-20-allocation-observer-effect.json)
includes canonical statistics, classes, identities, and the ten largest sites;
[the archive](performance/2026-09-20-allocation-observer-effect.tar.gz) preserves
the original three binary captures, worker reports, complete site reports, and
the exact analyzer source. Archive members, input identities, and all counts
were independently revalidated. This narrows interpretation of the forthcoming
Linux attribution results; it does not establish a production optimization or
change any ordinary timing or beta acceptance result.
The complete installed-wheel suite passes 102 tests, including conservative
classification, canonical-total reconciliation, changed-input rejection, and
both-order orchestration checks. Ruff, formatting, codespell, and actionlint pass.
The same 102 tests also pass in the ordinary benchmark environment with Memray
confirmed absent; diagnostic imports do not add a profiler dependency to the
acceptance environment.

### Initial protocol evidence

`tools/phase5/protocol_probe.py` is a diagnostic-only loopback proxy for an
explicitly plaintext test DSN. It refuses TLS rather than downgrading it. The
decoder handles arbitrary TCP fragmentation and records message types, byte
lengths, Bind parameter/result formats, and transaction status. SQL text,
statement names, row/parameter values, credentials, and cancellation keys are
not exported. Startup ReadyForQuery responses are excluded from query-cycle
counts. See PostgreSQL's [message flow](https://www.postgresql.org/docs/18/protocol-flow.html)
and [message formats](https://www.postgresql.org/docs/18/protocol-message-formats.html).

[Initial reports](performance/2026-09-20-protocol-diagnostic.json) cover 33
backend/workload combinations and 99 observed operations, with ten warmups
before each three-operation capture. They use the retained installed macOS
wheel matching `8eddc2ec`, PostgreSQL 15.19, and official Psycopg 3.3.2. The
[compressed archive](performance/2026-09-20-protocol-diagnostic.json.gz) preserves
all original report bytes, their SHA-256 hashes, and the exact probe source.
There are no timing samples: this local, one-order proxy run is not performance
acceptance, an idle-runner replication, or a network-latency estimate.

Findings from the ordered traces:

- Prepared and unprepared scalar queries each complete one protocol cycle in
  all three backends. Prepared Rust emits Bind/Execute/Sync; C and Python also
  describe the portal. All use binary parameter and text result formats for
  this workload. An extra scalar-query round trip is not the observed cause of
  the integration gap.
- The first Rust pipeline operation sends each of eight Bind/Execute/Sync
  groups and receives ReadyForQuery before sending the next. Official backends
  send all eight commands before their shared Sync. Source inspection confirms
  `NoTlsPipelineAdapter.sync()` calls `_execute()` sequentially for the native
  parameterized path. This is a real batching opportunity; it does not explain
  prepared scalar latency or establish a safe implementation yet. A native batch
  must preserve abort/error boundaries, preparation decisions, cursor ownership,
  callback ordering, and recovery.
- Rust COPY windows contain additional Parse/Describe and statement Close
  traffic: seven completed cycles versus three in the official backends. Counts
  alone are not seven versus three network round trips: COPY input/output have
  additional protocol stages, and deferred cleanup can cross window boundaries.
- Pool observations are `2, 2, 1` cycles for every backend: ten warmups across two
  pooled connections have not removed the per-connection preparation transition.
  Preserve that fact rather than relabeling all three samples steady-state.
- Transactions show eight completed cycles for every backend, but Rust uses
  extended-query messages for some commands where official backends use simple
  Query messages. Multi-row operations show one cycle for each backend.

Each operation window runs from immediately before the public call to its
return. Complete response frames are recorded before forwarding, but background
statement cleanup may cross windows; connection-close messages can arrive later.
The full observed stream through the capture endpoint is retained for analysis;
this does not promise capture of all traffic after connection close returns.
Cross-direction ordering is the
proxy's observed forwarding order, not packet timestamps or an RTT measurement.
Repeated scalar or pipeline serialization is corroborated against source before
claiming a causal dependency. TLS, pristine upstream, direct Tokio, and a binary
result comparison remain outside this initial capture.

Next: reproduce both orders on the dedicated runner, use the CPU/allocation
evidence to prioritize the scalar gap, and evaluate a genuine parameterized
batch boundary separately from scalar latency. Do not substitute a pipeline
throughput improvement for the outstanding eleven-workload acceptance targets.

### Execution-plan prototype boundary

Source inspection after the single-owner prototype narrows 5C.2. Placeholder
parsing already uses `_query2pg`'s bounded `lru_cache`; adding another SQL parser
cache does not address the observed repeated setup. For 10,000 fresh executions,
both captured candidate profiles construct 20,000 adapter maps and 10,000
transformers. Each constructs 10,000 result-loader bindings. Parameterized
execution additionally resolves dumpers, validates parameter ordering, and
materializes bound values for every call. These are call counts, not additive
latency estimates: profiled cumulative times overlap and include instrumentation.

The next bounded hypothesis is to reuse **immutable adaptation decisions** across
fresh cursors on a connection, with separate per-execution callback state. A
reused-cursor-only optimization cannot explain away the fresh-cursor gap.

| Component | Reusable content | State that must remain execution-local |
| --- | --- | --- |
| SQL layout | Existing parsed placeholders, requested formats, name ordering | Parameter values, errors, dumped bytes, value-sensitive OIDs |
| Adapter schema | Pure class resolution from an adapter snapshot | Dumper/loader instances, recursive contexts, constructor side effects |
| Result layout | Column OIDs, wire format, supported native decoding decisions | Callback bindings, row maker, result ownership and position |
| Preparation | No new statement ownership in this prototype | Existing prepared manager, statement IDs, eviction and transaction state |

Before caching adapter schemas, establish snapshot/invalidation controls:
`AdaptersMap` has loader-registration callbacks but no general registration
generation. Copy-on-write children are independent of later parent changes;
invalidating every child on a connection registration would be incorrect too.
Mutable type-registry contents, encoding, loader replacement after execute,
unknown-OID fallback, and C/pure adapter coexistence also need coverage.
Do not hold a connection, cursor, result, or arbitrary bound callback in a
connection-lifetime plan. Bound cache capacity and verify collection after close.

The installed-package contract test now checks independent contexts for fresh
cursors, a pre-existing cursor's snapshot after connection-level registration,
and cursor-local loader replacement after execution without affecting siblings.
It passes on both frozen Rust wheels and the official C comparator. The
single-owner wheel passes all 79 Phase 5 checks including this new control.
No cache implementation or performance improvement is claimed by these tests.

Reuse must not bypass registered constructors or change callback order just to
make a builtin benchmark fast. Existing direct-transformer tests include
reentrant cache replacement and mutable defaults; preserve their observable
callback behavior, while allowing a different internal execution layout.
Use the existing four fresh/reused controls in both orders and all eleven
workloads. Reject a second parse cache or another generic native-transformer
port without new evidence; neither removes the repeated lifetime/setup work.

### Rejected adapter-schema cache

Prototype `3e51f41fd1fd8550c16aa4da86686e89313f8e24` introduced registration and
type-registry generations, a 32-entry connection-local schema cache, and lazy
execution-local adapter views. Mutating a view materialized its own map; every
execution still constructed its own callback instances. It passes all 82
installed checks and 4,749/4,749 supported synchronous source cases, including
mutation, independent snapshots, bounded retention, and close-time cleanup.

[Raw exploratory evidence](performance/2026-09-20-adapter-schema-rejected.json)
compares its exact wheel against published `8eddc2ec` with identical native
extension bytes. Each control uses 1,000 warmups and nine 5,000-operation
samples, in baseline/candidate/candidate/baseline order. No builds or tests
overlapped; this Mac was **not idle** (Ghostty and WindowServer were active).

| Control | Wall a / b | CPU a / b |
| --- | ---: | ---: |
| Fresh constant | 0.997 / 1.141 | 0.999 / 1.098 |
| Fresh parameterized | 1.008 / 0.992 | 1.004 / 1.004 |
| Reused constant | 0.965 / 0.964 | 0.956 / 0.950 |
| Reused parameterized | 0.991 / 1.007 | 0.983 / 1.016 |

The large fresh-constant order difference limits confidence; it is not proof
of a stable 14% regression. However, fresh and parameterized improvements are
not repeatable, so retention is unjustified. A separate 10,000-operation profile
confirms map construction falls from 20,000 to 10,000 calls, while adding 10,000
schema lookups/views and increasing lock acquisitions from 10,000 to 20,000.
This is not a failure to exercise the intended fast path: the simpler setup is
offset by cache/view/locking work. Profile times are not latency estimates.

**Disposition: rejected.** Production code is restored exactly to `8eddc2ec`;
the three additional public-contract tests and evidence are retained. Broader
plan reuse remains unproven, not disproven. Do not repeat this cache or waive
callback/mutation/locking requirements to manufacture a gain. Inspect the
coarser execution/outcome boundary next, and bound its opportunity before a new
prototype. The rejected prototype's passing full source run does not replace
the retained implementation's pending compatibility matrix or release gates.

### Historical investigation sequence

The following sequence motivated the probes and experiments above. Use the
current bottleneck report and active plan when resuming; do not rerun completed
comparisons or rejected prototypes from this historical checklist.

1. Reproduce the initial layer and integration comparisons on an otherwise idle
   dedicated runner, retaining both orders. Prioritize the integration gap now
   observed. If the native-client floor becomes a suspected blocker, add pinned
   pristine upstream and direct Tokio controls, holding dependency versions and
   release codegen constant, before attributing it to the underlying library.
2. Extend the matched protocol to the existing parameterized/prepared workloads,
   text versus binary results, and fresh versus reused cursors. Determine how
   much overhead is fixed per operation versus per value/row. Verify round trips
   rather than assuming equivalent SQL implies equivalent wire behavior.
3. Profile only the layer with a repeatable gap. Collect client CPU,
   allocations/copies, GIL transitions, polls/wakeups, syscalls, and Python call
   counts in separate runs. CPU and waiting time must not be conflated.
4. If the direct binding is close to native but the full API is not, prioritize
   compatibility bookkeeping and cursor/query/loader lifetime. Consolidate state
   only with exact type, subclass, encoding snapshot, callback, and adapter
   invalidation tests. Do not resurrect the rejected lazy projection or native
   transformer merely because these paths contain Python work.
5. If the Rust session is slow while its direct client is not, investigate result
   metadata and parameter conversion. If the direct client itself is slow, first
   separate our vendored patches, sync runtime driving, and protocol behavior
   before considering transport changes or a new library.
6. Repeat for transactions and pool operations, then bulk rows and COPY. Compute
   an opportunity budget in microseconds per operation: a sub-microsecond change
   cannot close a ten-microsecond gap on its own. Reject flat/mixed changes and
   require a complete same-wheel comparison before retaining a performance claim.

The original `7c1a644a` beta checkpoint and all failed runs remain historical
evidence, not acceptance of the newer integration. The current retained runtime
is `8eddc2ec`; the schema-cache experiment is removed. Neither later probes nor
documentation can change an earlier artifact's identity or failure verdict.

Initial validation of the diagnostic tooling: both native probes build in release mode;
all forward/reverse result checks pass; all 74 installed-wheel/accounting tests,
Ruff, codespell, workspace Rust formatting, and mypy (239 source files) pass.
