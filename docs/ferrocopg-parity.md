# Performance parity investigation

## Decision and scope

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

The next discriminating experiment should remove intermediate **parameter
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
This larger boundary remains unimplemented and unproven.

### Acceptance checkpoint

The retained production implementation is frozen for full acceptance at
`7bbc14eba9a573bf528317f0859a8734db589e19`; its runtime matches the green
`8eddc2ec` compatibility checkpoint. [Workflow 35511989284](https://github.com/martijnberger/ferrocopg/actions/runs/35511989284)
is queued for three complete benchmark runs and the full three-backend soak.
Follow this handle rather than restarting it on observation timeouts. The
separate checkpoint Tests `35511889731` and Lint `35511889747` are also pending.
No final acceptance or scheduled-run success is claimed.

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
