# ferrocopg Roadmap

## Goal

Build a Rust-native PostgreSQL backend on top of Psycopg's trusted Python API,
adaptation system, row factories, SQL composition, errors, and test suite.

The product will be published as a separate `ferrocopg` distribution and
import namespace:

```python
import ferrocopg as psycopg
```

The first product goal is a synchronous, Rust-default `0.1.0` beta. The
source tree already defaults synchronous connections to Rust so ordinary
development exposes backend gaps. Publishing to PyPI remains blocked
until the release gates in this document pass.

Upstreaming is deliberately undecided. It is not a prerequisite for building,
testing, or releasing the fork.

## Product Decisions

The following decisions define the roadmap:

- Synchronous `connect()` defaults to the Rust backend in this source tree.
- If the Rust extension is unavailable, the default connection fails with an
  actionable error. There is no silent fallback.
- `impl="libpq"` lazily delegates to an installed official `psycopg`
  package and returns an official `psycopg.Connection`.
- The first release is synchronous-only for the Rust support contract.
- `ferrocopg.AsyncConnection` delegates to official Psycopg in `0.1.0`.
  The existing Rust thread-offload async facade remains experimental.
- The distribution name and import namespace are both `ferrocopg`, allowing
  official `psycopg` to coexist in the same environment.
- The public API target is a drop-in import change: supported synchronous code
  should normally need only `import ferrocopg as psycopg`.
- Ferrocopg vendors Psycopg's Python implementation instead of depending on
  it for the Rust path.
- The repository remains shaped like upstream Psycopg. The release build
  stages the vendored Python source under the `ferrocopg` namespace instead
  of committing a permanent source-tree rename.
- Official `psycopg` is an optional fallback dependency, exposed through a
  documented extra such as `ferrocopg[libpq]`.
- Official `psycopg_pool` must work with synchronous ferrocopg connection
  classes. A separate pool fork is out of scope.
- The first release supports CPython 3.11-3.14 and PostgreSQL 14-18.
- The first release is `0.1.0` with beta status. Only the Rust async facade is
  labeled experimental.

## Current State

Planning checkpoint: 2026-09-19. CI statuses below are snapshots, not final
results for workflows that are still running.

### Decision summary

- Preserve Psycopg's proven Python-facing behavior while replacing its backend
  with Rust; this is not a new public database API.
- Keep the existing synchronous Rust default and explicit fallback contract.
  Changing the development default does not authorize a PyPI release.
- Treat Phases 3 and 4 as completed implementation milestones, Phase 5 as
  incomplete acceptance work, and Phases 6 and 7 as pending release work.
- Prioritize measured shared small-query overhead, then remaining COPY and
  result-adaptation gaps. Do not expand into native async or a pool fork.
- Keep upstream synchronization separate from the undecided upstreaming question.

### Validation checkpoint

Phases 3 and 4 are complete. Phase 5 is the active release blocker: the
installed-package benchmark and soak infrastructure exists, but performance
acceptance has not passed. Full 30-minute-per-backend CI soaks passed on
revisions `24b646e3`, `2ed94013`, `be46e180`, `cc7b60e2`, `e7b008c2`,
`7c740a41`, and `3a0bb3db`;
sustained validation of the final
candidate remains required. The latest complete local benchmark, for native
wait-state reuse `0dcecac2`, fails six workloads against C: parameterized and
prepared queries, namedtuple rows, transactions, text COPY, and pool.
Parameterized/prepared queries, transactions, and pool cycles miss Python parity.
Passing individual row workloads does not close the complete performance gate.
The exact-revision CI benchmark also fails: four C and four Python comparisons
miss their limits. Lint passes; compatibility validation and the full soak are
unfinished. Both connection cases pass the local run; plaintext missed Python
parity in an earlier row-drain run. Individual passes remain variable.
These are development measurements, not release acceptance. The wait-state
candidate passes 41 installed checks, 28 Rust backend unit tests, and
`3525/3525` selected synchronous C-coexistence cases. Its full local harness
passes `4734/4736` synchronous cases, failing two pool timing assertions;
the strict zero-regression gate fails. Its supported matrix and full soak
remain pending. Keep `3a0bb3db` as the last completed green checkpoint.
Phase 6 wheel-matrix validation and Phase 7 publication remain pending.
The completed Phase 4 evidence below is a historical baseline, not validation
of every subsequent performance change.

Main remains at the green `4a132959` checkpoint. The latest complete green CI
checkpoint is timeout-state revision `3a0bb3db`: its
[compatibility workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35426404068)
passes all 57 jobs, lint passes, and its full three-backend soak passes. Its
benchmark still fails four C and four Python comparisons. The preceding
checkpoint is row-drain revision `7c740a41`: its
[compatibility workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35423769875)
passes all 57 jobs and lint passes. The preceding checkpoint is `8561fa3d` on
`martijn/phase5-sql-scan`: its
[compatibility workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35413168117)
passes all 57 jobs, and its
[lint workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35413168119)
passes. This includes the factory, COPY, and earlier performance changes.
The earlier checkpoint `2ec7e030` on `martijn/phase5-inline-ranges` includes the
loader-resolution implementation `ed5e86b8`. Its
[compatibility workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35406514048)
passes all 57 jobs, including all eight Rust lanes and the standalone package
job; lint also passes. The preceding inline-offset checkpoint `60ec40d6` also
passes all 57 jobs in its
[compatibility workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35333813090)
and passes lint. The preceding metadata revision `be46e180` likewise
passes all 57 jobs in its
[compatibility workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35330892670)
and its lint workflow. Its
[Phase 5 workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35330914691)
passes the full three-backend soak but fails the benchmark. The inline-offset
local full run and unchanged repeat each pass 4,723 of 4,724 synchronous cases,
with the same pool backoff timing failure. Preserve those failures separately
from the green Linux Rust matrix. The newer loader-resolution revision `ed5e86b8`
passes installed and C-coexistence checks; its full local run passes 4,721 of
4,724 synchronous cases. All three timing failures reproduce with C/libpq.
Its supported CI run at `2ec7e030` is now green; the failed local full run is
retained separately. The
[Phase 5 run at `cc7b60e2`](https://github.com/martijnberger/ferrocopg/actions/runs/35406870159)
has failed its benchmark job and passed its full three-backend soak. Neither workflow
validates the fetch-cast or COPY-borrowing changes. The local timing
failures are not waived.
The follow-ups are pushed as `a1391070` (fetch casts) and `e7b008c2` (COPY
borrowing) on `martijn/phase5-copy-borrow`. The latter has its own
[compatibility run](https://github.com/martijnberger/ferrocopg/actions/runs/35408335346)
and [Phase 5 run](https://github.com/martijnberger/ferrocopg/actions/runs/35408378339);
all 57 compatibility jobs and the full three-backend soak pass, but the
benchmark fails. Lint passes at `e7b008c2`. Its full local
run passes `4720/4724` synchronous cases; all four pool/scheduler timing failures
reproduce under C/libpq. The short resource smoke passes, but neither result
satisfies final supported-matrix or sustained-soak acceptance.
The preceding implementation is `c07e4c26` on `martijn/phase5-factory-init`:
native result paths initialize row factories without allocating a discarded
fallback conversion closure. Its 32 installed checks and `3340/3340`
synchronous C-coexistence cases pass. The complete local harness passes
`4722/4724` synchronous cases; check-backoff and scheduler timing assertions
still fail, so the strict zero-regression gate fails. Its
[compatibility run](https://github.com/martijnberger/ferrocopg/actions/runs/35410499984)
passes all eight Rust lanes and the package job, but fails one upstream
Python 3.10 DNS weighted-order test. Its
[lint run](https://github.com/martijnberger/ferrocopg/actions/runs/35410500017)
passes. Its 60-second resource smoke passes, but no full sustained soak is
recorded for this revision. Both local timing failures also reproduce under
C/libpq in a fresh comparison. The completed COPY and loader-resolution soaks
do not validate the later factory change. Preserve the original DNS failure;
the newer `8561fa3d` matrix passes that unchanged test too.

The SQL-scanner slice `8561fa3d` avoids quote/comment scanning when an exact
`str` contains no semicolon. Its 33 installed checks, `3525/3525` selected
synchronous C-coexistence cases, supported matrix, lint, and resource smoke
pass. Transaction comparisons improve modestly in both orders; pool timings
are mixed. The full local harness passes `4732/4736` synchronous cases and
fails its strict gate on four pool/scheduler timing assertions. All four also
fail under C/libpq. A large wall-clock gap in one backoff interval is retained
in the raw report; do not describe this as a clean local validation run.

The newer native row-drain slice `7c740a41` on `martijn/phase5-row-drain`
collects buffered unprepared results in one runtime call instead of re-entering
the runtime for every row. All 34 installed checks, 28 Rust backend unit tests,
three focused vendored wait-loop tests, and `3525/3525` selected synchronous
C-coexistence cases pass. Its full local harness passes `4736/4736` synchronous
cases with zero failures or errors, satisfying the strict zero-regression gate.
Experimental async remains separately `505/620`. Its
[compatibility matrix](https://github.com/martijnberger/ferrocopg/actions/runs/35423769875)
passes all 57 jobs and lint passes. Its
[Phase 5 workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35423782206)
fails the exact-revision benchmark with four C/four Python misses; the full
three-backend soak passes. Its short local resource smoke passes. An unprepared bulk-row diagnostic improves
in both orders, but single-row parameterized timings are nearly flat and the
complete benchmark fails. The diagnostic does not replace any acceptance case.
No final candidate has passed all acceptance gates.
The follow-up `3a0bb3db` fixes a disabled-timeout state bug discovered during
query-path profiling. All 35 installed-package checks and all `4736/4736` local
synchronous cases pass; all 57 supported CI jobs, lint, and the full
three-backend soak now pass too. This is a
correctness fix, not a claimed performance improvement. Both the metadata-only
and query-classification-cache
experiments were removed after inconsistent paired timing results; see the
Phase 5 evidence below.
The acceptance status at this planning checkpoint is:

| Area | Status | Remaining evidence or work |
| --- | --- | --- |
| Synchronous API and package boundary | Implemented in Phase 4 | Revalidate after the Phase 5 optimizations |
| Official synchronous pool | Implemented and regression-tested | Retain coverage in final benchmarks, soak, and matrix |
| Performance | Not accepted; wait-state candidate misses six C/four Python limits locally and four C/four Python limits in CI | Continue measured optimization, then pass three complete candidate runs without combining passes across reports |
| Sustained reliability | Full three-backend soaks pass at `7c740a41` and `3a0bb3db`, with zero surviving sessions and no resource-budget failures | Repeat 30 minutes per backend after any further candidate changes; scheduled execution remains unverified |
| Latest compatibility validation | `3a0bb3db` remains the completed green checkpoint; wait-state candidate passes 41 installed checks but fails two local pool timing assertions, with CI pending | Finish candidate validation without waiving the strict gate; retain failed local reports and comparisons |
| Release wheels and publication | Pending | Complete Phases 6 and 7 before publishing to PyPI |

The implementation checkpoint is not a release candidate designation. Local
working-copy reports, earlier green CI runs, and short resource smokes must not
be combined into a claim that the current revision has passed all gates.

### Next milestone: close Phase 5

The immediate goal is a validated synchronous backend, not additional API
scope. Phase 3 completion means the backend foundation is implemented; it does
not mean the performance, reliability, packaging, or publication gates are done.

Work in this order:

1. Use the green timeout-state matrix and full soak at `3a0bb3db` as the
   correctness and reliability checkpoint. Preserve
   the completed loader/COPY soak artifacts separately
   from later implementation revisions. Classify
   full-harness failures and retain explicit Python/C
   comparison coverage. Investigate the local pool timing failure independently
   of the performance work; neither an isolated pass nor a green Linux matrix
   erases a failed local full run. Let each candidate's CI finish rather than
   repeatedly superseding it with new pushes to the same bookmark.
2. Profile the remaining shared query path before choosing another optimization.
   The factory and SQL-scanner slices remove measured work but do not close
   the performance gap. Prioritize shared small-query overhead and any remaining
   COPY costs. Revisit bulk-row paths when the complete comparison identifies
   a remaining failure, rather than extending the separate unprepared-row
   diagnostic. Parameter borrowing removed allocations
   but did not measurably improve the longer warmed query comparisons. Choose
   the next change from a fresh profile, not from an assumption that moving more code to
   Rust must be faster. Each slice needs a rebuilt installed wheel, regression
   coverage, and a complete eleven-workload comparison, not a selected win.
3. Freeze one candidate and collect its acceptance evidence: three passing
   benchmark runs, the full three-backend soak, and green compatibility and
   package-boundary validation. Publish revision-linked reports before closing
   Phase 5; then proceed to the release-wheel matrix and publication checklist.

Keep the Rust development default in place throughout. Do not substitute async
work, a pool fork, an upstream proposal, or relaxed thresholds for the remaining
synchronous release work. Any scope or acceptance change needs a separate
explicit decision.

### Next implementation slice

Keep the fully matrix- and soak-validated timeout-state fix `3a0bb3db` as the
completed checkpoint. The later candidate `0dcecac2` reuses native signal-wait
state per session and transfers each operation's exception before releasing
the session lock. Paired prepared-query timings improve modestly, but its
complete benchmark still fails and full-harness/CI validation is not green.
Finish its validation before treating it as a completed checkpoint; do not
attribute the earlier soak to this candidate.

The current working copy contains an uncommitted native `BackendPgResult`
projection prototype. It replaces Python metadata-wrapper construction for
buffered native results while retaining the Python shim for other result
objects. It is not part of `0dcecac2`, a validated optimization, or a release
candidate. Leave streaming and other result paths outside this experiment.

The next implementation decision is whether to retain that prototype:

1. Verify the rebuilt installed wheel and its test results, then compare it
   with the unchanged wait-state wheel in both execution orders on an idle
   machine. Record wall and CPU samples; do not overlap timing with builds or
   tests.
2. Before retaining it, add focused coverage for metadata indexing, encoding
   snapshots, text/binary formats, NULL/empty values, command counts, cache
   identity, and result lifetime after cursor and connection cleanup. Preserve
   custom factories/loaders and fallback behavior; check native indexing safety.
3. If paired timings are effectively flat or behavior cannot be preserved,
   remove the prototype and record the result. If promising, run the complete
   eleven-workload comparison and compatibility checks before promoting it.

Retained optimizations include:

- Fetch methods use static string casts instead of constructing typing objects
  per call. Profiles confirm removal of that work, but paired query timings are
  mixed; do not describe this as a proven latency gain.
- Text and binary COPY parsing borrow exact immutable `bytes`. Other inputs
  retain `bytes()` coercion and a snapshot before loader callbacks. Text fields
  without escapes are borrowed, and an iterator removes the intermediate field
  pointer list. Preserve mutable-input safety, bytes-subclass coercion, NULL,
  empty fields, escape handling, and custom-loader behavior.
- Native result paths initialize row factories directly instead of allocating
  a fallback conversion closure that is immediately discarded. Preserve eager
  initialization for ordinary execution, lazy pipeline initialization, metadata,
  callback counts, and factory exception timing.
- Separator-free SQL skips the quote/comment scanner; SQL with semicolons and
  string subclasses retain the original scanner. No SQL parse cache is added.
- Buffered unprepared rows drain in one runtime call. Streaming iteration stays
  unchanged, and column metadata, final command counts, cancellation, and
  errors remain covered by installed regressions.

The latest complete local benchmark at `0dcecac2` fails six C and four Python
limits, including text COPY against C. The preceding row-drain CI benchmark
fails four C and four Python limits. These passes have varied between
runs. The SQL-scanner slice's paired transaction gains are modest, not closure
of that gap. Keep the
earlier SQL-scanner local full-harness failure visible separately from the
row-drain revision's passing local synchronous suite. Neither selected workload
passes nor earlier sustained soaks establish final-candidate acceptance.
Row drain improves a separate
unprepared bulk-result diagnostic, not the prepared bulk-row acceptance cases
or the remaining single-row gaps. Do not change benchmark preparation policy
to turn the diagnostic win into an apparent gate pass.

After these slices, profile the remaining parameter adaptation, query setup,
loader construction, and adaptation-context work shared by the failing
parameterized, prepared, transaction, and pool workloads. A direct port of
dumper-cache dispatch was measured and rejected as effectively flat; do not
repeat it without a different measured mechanism. Preserve the local timing
failures without changing assertions, retry policy, manifests, or the regression
budget. Do not expand the API or start native async work to avoid these gates.
The native parameter-packing prototype was also measured and removed as
effectively flat. Do not reintroduce it just because it moves a loop to Rust.
Layer measurements below point toward the broader query/cursor adapter path;
any consolidation must retain custom adapters, public cursor behavior, error
translation, signal handling, and notice/notification delivery.

Prepared-query metadata construction was investigated: eliminating discarded
parameter descriptions was flat in one paired comparison and faster in the
reverse order. A bounded cache of query state-effect flags was also flat or
mixed across prepared and transaction comparisons. Both prototypes are removed;
neither establishes a repeatable improvement or justifies wider caching.
Cursor construction was measured at about `1.7 us` per operation. Compact
layouts increased retained cursor memory and slowed a prepared-query sample;
immutable result metadata gave inconsistent paired query timings. Both
experiments are removed. These constructor/container changes are not the next
priority.

The broader built-in parameter-binding hypothesis has now been tested with a
temporary native `BuiltinParamPlan`. Fresh conversion improved modestly in an
offline diagnostic, but warmed transformer reuse slowed and code inspection
found dumper-cache and exception-order incompatibilities. The prototype is
removed and the validated `3a0bb3db` wheel restored. Do not treat this as a
pending implementation to finish or a demonstrated end-to-end improvement.

Live public-query measurements now support investigating result-wrapper setup;
they do not establish that the current prototype is faster. Native batching of
loader construction was measured in both orders and removed as effectively
flat. Do not repeat that approach without a different measured mechanism.
Sparse diagnostics put the native prepared
call near `34 us` inside a roughly `62 us` public query; isolated conversion
cost is only about `3.77 us`. These instrumented measurements suggest broader
wrapper/setup and fetch overhead, not proof that parameter binding alone can
close the gap. The checked-in `tools/phase5/query_profile.py` now measures public
and phased queries across all three backends, with raw samples and installed
implementation fingerprints. Preserve its reports and the tested wheel;
diagnostics are not acceptance evidence. Installed regressions now cover cached
dumper behavior after adapter registration, cached NULL OIDs, and left-to-right
errors/callbacks. All 37
installed checks pass against the restored `3a0bb3db` wheel. Preserve these
regressions when evaluating any further binding shortcut.

Resolve actual registered classes from the correct adapter snapshot, preserve
custom/stateful/recursive adapters through the existing path, and avoid running
callbacks twice when falling back. Retain integer-width OID selection,
NULL/text/binary behavior, encoding, public query metadata, and prepared
invalidation. Do not share mutable transformer state across cursors.
Preserve concrete cursor subclasses, metadata visibility after every operation,
custom factories/loaders, callback and exception timing, and result lifetime.

- Capture warmed parameterized/prepared profiles and a complete benchmark of
  the installed current wheel; keep the official Python/C baselines unchanged.
- Select one measured hot path and compare its rebuilt wheel with the parent
  on the same otherwise idle machine. Record wall time and CPU time; allocation
  reduction alone is not evidence of a latency improvement.
- Preserve custom dumper/loader registration and cache behavior, integer OID
  selection, text/binary formats, NULL/empty distinctions, encoding and error
  semantics, cancellation, and result lifetime. Add targeted regressions before
  relying on a native fast path.
- Re-run all eleven workloads and the relevant unfiltered compatibility modules.
  If the change is effectively flat, record that result and reassess the profile
  instead of treating the slice as closure of a performance gap.

This slice is complete when it produces a tested, measured optimization or an
evidence-backed decision not to pursue that approach. Phase 5 itself remains
open until the final-candidate acceptance checklist passes.

### Historical Phase 3 and Phase 4 baseline

Phase 3 completed the backend foundation and broad compatibility harness.

Implemented Rust-backed capabilities include:

- plaintext and rustls connections
- all six libpq-style SSL modes
- custom roots, channel binding, and client certificates
- simple, parameterized, prepared, text, and binary query execution
- Psycopg dumpers, loaders, row factories, and result metadata
- transactions, savepoints, transaction characteristics, and TPC
- cancellation, diagnostics, notices, LISTEN, and NOTIFY
- text and binary COPY, type pinning, and generic writers
- named, scrollable, and withhold server cursors
- pipelined simple-query batches
- an experimental connection-affine thread-offload async facade

Historical validation evidence (not the current Phase 5 candidate):

- 26 Rust backend tests pass.
- The focused live bootstrap suite reports 205 passing tests with 8 expected
  environment or accelerator-dependent skips.
- The Rust compatibility jobs execute across PostgreSQL 14-18 and the target
  CPython 3.11-3.14 range, satisfy the `0.95` sync rate floor, and enforce zero
  non-manifested synchronous failures or errors.
- The latest complete local PostgreSQL 14 / CPython 3.14 compatibility run
  reports `4708/4718` executed synchronous cases (`0.998`); skips and async
  coverage are reported separately.
- CI enforces the current `0.80` mixed sync/async compatibility floor and the
  `0.95` sync-only release floor, plus a zero-regression budget for
  non-manifested synchronous tests.
- CI reports sync and async independently and uploads family-level JSON and
  JUnit evidence for every PostgreSQL 14-18 matrix job.
- The validated CPython 3.11 server-axis denominator is `4781` sync and `611`
  async tests on PostgreSQL 14-18, with `249` sync and `766` async manifested
  tests in every lane. Duplicate JUnit call/teardown records are collapsed by
  node ID, so fixing an execution error cannot masquerade as collection drift.
- The sync-only ratchet is `0.95`; the latest supported matrix reports `1.000`
  for executed, non-manifested synchronous cases, and the latest local run
  reports `0.998`. The matrix also proves the separate 100% release-critical
  gate through its zero-regression budget.
- The denominator-corrected workflow run `29213883257` is green for the
  standalone wheel and all eight Rust lanes. Every supported matrix key
  satisfies the behavioral floor and its committed accounting baseline.
- Lint, formatting, typing, documentation, and Rust checks pass.

The authoritative Phase 4 workflow run `30227231014` completed all `57` jobs
successfully. All eight PostgreSQL 14-18 / CPython 3.11-3.14 Rust lanes, the
standalone package job, both PyPy jobs, and the macOS CPython 3.14 job are
green. Every Rust lane passed the strict zero-regression gate. The PostgreSQL
15 / CPython 3.11 lane, for example, reports `4591/4591` executed synchronous
cases (`1.000`), zero failures, zero errors, a stable total of `4781`, and `249`
manifested boundaries. The companion lint workflow run `30227231070` is also
green.

The C/Cython coexistence failure in `test_no_tls_cursor_adapter_copy` is fixed.
The focused C and pure-Python bootstrap/harness slices are green, and the user
and contributor documentation now reflects the Phase 3 implementation and
the separate `ferrocopg` product direction.

Phase 4 is complete. Omitted synchronous `connect()` uses Rust, missing Rust is
a hard actionable error, and comparison jobs select libpq explicitly. The
staging tool builds a standalone `ferrocopg` wheel, records its vendored
upstream revision, and passes clean installed-wheel CI for Rust-only use,
side-by-side delegation, coexistence, and uninstall isolation. The full
release-critical synchronous contract was green across the supported matrix
at Phase 4 closure; subsequent optimizations require fresh validation.

## Architecture

### Source layout

Keep the repository close to upstream Psycopg so upstream changes remain
reviewable:

- `psycopg/` remains the upstream-shaped Python source and pure-Python
  baseline during development.
- `psycopg_c/` remains available only for upstream comparison and regression
  testing. It is not part of the ferrocopg distribution.
- `crates/ferrocopg-postgres/` owns connection planning, rustls transport,
  protocol sessions, queries, parameters, transactions, COPY, cancellation,
  and notifications.
- `crates/ferrocopg-python/` exposes the PyO3 extension and Rust helper
  seams.

### Release namespace

The wheel build creates a staging tree and packages the vendored Python source
as `ferrocopg`. Generated staging output is never committed.

The staging proof must verify:

- relative and absolute imports resolve under `ferrocopg`
- package resources and typing metadata are included
- exceptions, rows, SQL objects, adapters, and public symbols report sensible
  `ferrocopg` module identities
- official `psycopg` can be imported in the same interpreter
- no wheel installs files into the official `psycopg` namespace
- license and attribution requirements from vendored Psycopg are preserved

If build-time namespace staging proves unreliable, stop and revisit the
packaging decision before committing a bulk namespace rename.

### Backend selection

The target behavior of the staged `ferrocopg` package is:

```python
import ferrocopg as psycopg

# Rust by default.
conn = psycopg.connect(dsn)

# Explicit delegation to official Psycopg.
libpq_conn = psycopg.connect(dsn, impl="libpq")
```

Rules:

- omitted `impl` means Rust
- `impl="ferrocopg"` remains an accepted explicit spelling
- `impl="libpq"` performs a lazy import of official Psycopg
- delegated connections are official Psycopg objects; they are not wrapped
- missing Rust produces an installation error naming the required wheel
- missing official Psycopg on explicit libpq or async use produces an error
  naming the fallback extra
- backend choice never changes after a connection is created

During the upstream-shaped source-tree transition, the package is still
imported as `psycopg`. It cannot import a second official `psycopg` package for
delegation without module-name recursion. Therefore:

- the source-tree default changes to Rust immediately
- the existing internal libpq path remains temporarily available for upstream
  comparison jobs
- official delegation is implemented and enforced in the staged `ferrocopg`
  package once namespace isolation exists
- this temporary source-only behavior is not the PyPI product contract

## Compatibility Contract

### Release boundaries

The `0.1.0` Rust backend aims for near-total synchronous public API parity.
Only raw libpq connection and socket access may remain unsupported:

- raw `PGconn` access
- raw socket access such as `fileno()`
- tracing or debugging that requires direct libpq protocol objects

These boundaries must raise clear `NotSupportedError` exceptions and point to
`impl="libpq"`.

The following former gaps were addressed in Phase 4 and must remain covered
by the release-critical gate, not become permanent release boundaries:

- concrete `Cursor`, `RawCursor`, and `ClientCursor` behavior
- concrete COPY writer behavior
- exact public pipeline state behavior
- complete connection timeout coverage
- multi-host attempts and target selection
- cancellation behavior exposed through the public synchronous API

### Broad compatibility gate

Before publishing `0.1.0`:

- at least 95% of non-manifested synchronous tests pass
- the denominator and manifest are deterministic across PostgreSQL 14-18
- async tests are reported separately and do not affect the first release
  floor
- remaining non-critical failures are documented as beta defects, not hidden
  by broad skip rules
- no known hang, data corruption, security failure, or silent fallback exists

The trustworthy sync-only baseline is established and the synchronous floor
is now `0.95`, with a separate zero-regression gate for non-manifested
synchronous failures and errors. The mixed `0.80` ratchet is retained as an
additional CI check, not as a substitute for either synchronous release gate.

### Release-critical gate

The following synchronous contract must be 100% green:

- connection lifecycle, context management, and close/broken state
- DSN and keyword connection parameters
- PostgreSQL 14-18 plaintext and TLS connections
- all SSL modes, certificates, roots, and channel binding
- connection timeouts, multi-host attempts, and target-session behavior
- simple, parameterized, prepared, text, and binary execution
- positional and mapping parameter adaptation
- built-in types, arrays, ranges, multiranges, enums, and composites
- row factories, descriptions, metadata, and result navigation
- transactions, nested transactions, savepoints, characteristics, and TPC
- default, raw, client, and named cursor behavior
- text and binary COPY, COPY errors, writers, and connection locking
- pipeline ordering, sync, error, and recovery behavior
- diagnostics, SQLSTATE mapping, encoding, notices, and notifications
- cancellation and concurrent synchronous use
- official `psycopg_pool` synchronous interoperability
- Rust-missing and official-Psycopg-missing error behavior
- explicit official-Psycopg delegation

## Performance Contract

Publication is blocked until repeatable benchmarks show:

- ferrocopg matches or outperforms official Psycopg's pure-Python path
- ferrocopg's median duration is no more than 1.25 times Psycopg C for every
  workload in the acceptance suite
- no benchmark shows unbounded memory growth or connection/thread leakage

The benchmark suite must cover:

- connection setup with plaintext and TLS
- single-row parameterized queries
- prepared statement reuse
- multi-row result adaptation
- common row factories
- transaction and savepoint cycles
- text and binary COPY throughput
- synchronous pool checkout/query/return cycles

Results must include latency distributions, throughput, CPU time, and peak
memory. Any exception to the 25% target requires a documented rationale and an
explicit release decision.

## Release Matrix

Target interpreters:

- CPython 3.11
- CPython 3.12
- CPython 3.13
- CPython 3.14

Target wheels:

- manylinux x86_64
- manylinux aarch64
- macOS arm64
- macOS x86_64
- Windows x64

Use PyO3 `abi3-py311` if the extension and all required APIs pass the complete
test and smoke matrix. If `abi3` is not viable, build per-interpreter wheels
without reducing the supported matrix.

Target PostgreSQL servers:

- PostgreSQL 14
- PostgreSQL 15
- PostgreSQL 16
- PostgreSQL 17
- PostgreSQL 18

Each wheel must be installed into a clean environment without a source
checkout and pass:

- import and metadata smoke tests
- a live Rust connection and query
- TLS verification
- parameters and typed results
- transactions
- COPY
- explicit missing-Rust diagnostics where applicable
- coexistence with official Psycopg
- explicit libpq delegation when the fallback extra is installed

## Roadmap

### Phase 4.0: Restore a trustworthy baseline

Status: complete.

Tasks:

- [x] Fix `test_no_tls_cursor_adapter_copy` so backend COPY tests do not select a
  C formatter for a Rust-only transformer.
- [x] Restore a green complete upstream Python/C/Cython workflow.
- [x] Update the README and development guide to match Phase 3 reality.
- [x] Split compatibility reporting into sync and async result sets.
- [x] Confirm the sync-only denominator across PostgreSQL 14-18, commit the
  expected per-matrix denominator, and retain `0.85` as a conservative initial
  ratchet below the validated server-axis minimum.
- [x] Record failure counts by feature family in CI artifacts.
- [x] Validate the PyPy `mypy` marker correction in the complete workflow.

Definition of done:

- The complete workflow is green.
- Ferrocopg, pure-Python, and C/Cython validation lanes fail independently.
- Documentation and the manifest describe the same capabilities.
- The sync release denominator is stable across PostgreSQL 14-18.

### Phase 4.1: Make Rust the synchronous development default

Status: complete.

Tasks:

- [x] Change omitted synchronous `impl` from libpq to Rust.
- [x] Preserve `impl="ferrocopg"` as an explicit spelling.
- [x] Add a clear error when the Rust extension is missing.
- [x] Keep the internal source-tree libpq path as a temporary, explicit comparison
  implementation until namespace staging is available.
- [x] Keep upstream baseline jobs explicit about their selected implementation and
  document that this is transition-only behavior.
- [x] Add a Rust-default CI lane that never passes an implementation selector.

Definition of done:

- Normal source-tree synchronous use exercises Rust.
- Missing Rust never falls back silently.
- Rust-default and explicit internal-libpq comparison tests are both green.
- No source-tree behavior is mistaken for the final delegation contract.

### Phase 4.2: Prove the ferrocopg package boundary

Status: complete.

Tasks:

- [x] Build a non-committed namespace staging tool.
- [x] Package the vendored Python API as `ferrocopg`.
- [x] Package the Rust extension inside the ferrocopg wheel.
- [x] Add lazy official-Psycopg delegation for libpq and async entry points.
- [x] Make delegated return types and type overloads honest.
- [x] Define the fallback extra and installation errors.
- [x] Define and test the supported official-Psycopg version range for delegation.
- [x] Test side-by-side imports and distribution uninstall behavior.
- [x] Establish version metadata independent from upstream Psycopg while recording
  the vendored upstream revision.
- [x] Make the Rust-default package import and connect without system libpq;
  libpq may only be required when the optional official-Psycopg fallback is used.
- [x] Pass the clean installed-wheel CI smoke with a live Rust query, explicit
  delegation, coexistence, and uninstall-isolation checks.

Definition of done:

- A local `ferrocopg` wheel installs without writing into `psycopg`.
- `import ferrocopg as psycopg` supports the documented synchronous API.
- Rust-default import and use do not require system libpq.
- Official Psycopg can coexist and power explicit libpq and async calls.
- Upstream source synchronization remains practical.

### Phase 4.3: Close synchronous compatibility gaps

Status: complete.

Completed slices:

- [x] Transactions, nested savepoints, transaction status, explicit rollback,
  TPC, context ownership guards, and out-of-order nesting. The focused local
  PostgreSQL 17 run reports `102/105` non-manifested cases (`0.971`) with three
  environment-level skips; the only manifested case enters COPY through raw
  `PGconn.exec_`.
- [x] Stable compatibility accounting by node ID, so duplicate JUnit teardown
  records cannot change the release denominator when an error is fixed.
- [x] Column metadata propagation, including type modifiers, internal sizes,
  display names, precision, scale, sequence behavior, repr, and pickling. The
  focused PostgreSQL 14 run reports `49` passing cases and four expected
  PostgreSQL-version skips.
- [x] String adaptation and negotiated client encodings, including LATIN1,
  LATIN9, SQL_ASCII scalar and recursive values, unknown enum text loading, and
  COPY conversion-error timing. The focused PostgreSQL 14 run reports `134`
  passing cases and one expected failure.
- [x] Explicit text and binary extended-query results plus composite and record
  adaptation, including recursive composites, pure generated array loaders,
  quoted array `NULL`, and panic-free timestamp overflow handling. All `75`
  synchronous composite cases pass on PostgreSQL 14; the complete module is
  `77/79` on Rust with only two deferred async-facade failures, while libpq is
  `79/79`.
- [x] Connection metadata and lifecycle parity, including PGconn and
  ConnectionInfo attributes, closed and broken state, repr, error state,
  deletion warnings, SQL-issued transaction boundaries, connection-parameter
  normalization, and unsupported client-encoding timing. The focused connection
  modules improved from `74` passing and `48` failing to `103` passing and `19`
  failing, with `22` boundary or environment skips.
- [x] Connection environment and adaptation-context parity. `PGAPPNAME` and
  `PGCLIENTENCODING` follow explicit-parameter precedence, negotiated encodings
  are reflected in connection state, and caller adapter maps are copied without
  losing ferrocopg wire handling. The focused connection modules now report
  `109` passing, `13` failing, and `22` boundary or environment skips.
- [x] Concrete default, custom, server, and raw-server cursor hosting without
  libpq. The public factories retain exact Psycopg classes, while backend state,
  query metadata, binary formats, row factories, iteration, pipelines, COPY,
  and the experimental async facade route through hosted Rust adapters. The
  newly unmasked cursor and server-cursor modules pass `188/188` on both
  ferrocopg and explicit libpq; focused connection modules are now `112`
  passing, `10` failing, and `22` boundary or environment skips.
- [x] Connection attempt routing and diagnostic parity, including DNS-to-hostaddr
  resolution, ordered multi-host attempts, all target-session modes,
  prefer-standby fallback, top-level selector routing, cancellation I/O errors,
  best-effort cancellation, and idle-in-transaction timeout classification. The
  focused connection modules report `121` passing, zero failures, and `23`
  explicit boundary or environment skips. Handshake-stall timeout limitations
  remain release-critical work and are not considered closed by this slice.
- [x] Concrete `RawCursor` execution and adaptation. Removing its broad manifest
  exposes a focused module that passes `78/78` on ferrocopg; common cursor and
  client-literal behavior remain separately manifested for the next slice.
- [x] Shared default and raw cursor bookkeeping, including public result-set
  mirrors, status and row-count semantics, `executemany()` aggregation,
  empty-query results, loader invalidation, COPY rejection, per-row stream
  metadata, closed-stream handling, and non-UTF query text. The focused default
  subset passes `94` tests with two libpq-version skips; the combined common and
  raw-cursor run passes `261` tests with only ClientCursor, intentional raw
  mapping, and version-gated skips.
- [x] Libpq-free `ClientCursor` literal adaptation and `mogrify()`, including
  pure-Python string and bytea quoting, typed literal casts, negotiated
  encodings, multi-statement execution, returning `executemany()` metadata, and
  leak checks. The dedicated module passes `28/28`, and the complete synchronous
  default, server, raw, client, and common cursor surface passes `571` tests
  with 11 intentional or version-gated skips on both ferrocopg and libpq.
- [x] The complete synchronous type tail: LATIN1 enum loading and dumping,
  SQL-standard negative intervals, and tuple-row integration. Enum metadata now
  crosses the Rust/Python boundary, the non-UTF protocol bridge preserves
  ordinary PostgreSQL encoding errors, SQL composition remains libpq-free, and
  hosted cursors retain their selected row maker. The complete type, metadata,
  and row family has zero synchronous failures, and the full harness gained 25
  sync passes without changing its denominator.
- [x] Prepared statement state, reuse, invalidation, and deallocation parity.
  The Rust backend now follows Psycopg's prepare threshold and tri-state
  `prepare` behavior, reuses named statements, evicts them through the
  `prepared_max` LRU, clears stale state across rollback and invalidating DDL,
  and executes unprepared bound queries through PostgreSQL's unnamed statement.
  The focused ferrocopg module reports `32/32`; one raw `PGconn` debug-call test
  is explicitly manifested, while explicit libpq remains `33/33`. Catalog type
  discovery and simple-query result metadata no longer leak named statements or
  lose OIDs across multi-result queries.
- [x] Synchronous COPY writer and pipeline parity. Public `LibpqWriter` and
  `QueuedLibpqWriter` objects now route through the Rust COPY buffer for both
  text and binary input, while query metadata is visible throughout the COPY
  context. Pipeline execution now defers hosted cursors, supports nested
  contexts and fetch-triggered synchronization, preserves queue and status
  state, plans prepared statements before consumption, reports aborted commands,
  and rolls back failed outer and nested transactions. The complete local COPY
  and pipeline run reports `151` passing cases with six exact raw-libpq skips;
  explicit libpq reports `152` passing cases with five platform trace skips.
- [x] Core synchronous concurrency parity. Connection-level transaction
  operations are serialized, concurrent `close()` performs best-effort
  cancellation before waiting for the active operation, and Rust connection
  state probes no longer block behind the session mutex. The upstream
  `test_commit_concurrency`, `test_concurrent_close`, and
  `test_identify_closure` regressions pass together against the live Rust
  backend. Handshake-stall timeout coverage remains a separate release-critical
  connection slice.
- [x] Full synchronous connection-attempt deadlines and bounded cancellation.
  The vendored synchronous Rust client now applies `connect_timeout` to the
  entire PostgreSQL handshake, classifies both socket and protocol timeouts,
  preserves per-host diagnostics, and advances after a stalled host. Safe
  cancellation has a caller-supplied deadline and verifies endpoint
  responsiveness with a bounded follow-up handshake. Password-authentication
  use is carried from the wire protocol to `PGconn.used_password`, and SIGINT
  interrupts a stalled first attempt before failover. The complete synchronous
  connection and concurrency modules report `104` passing cases and 16 raw
  libpq, version, authentication, or environment skips. The unmasked libpq-17
  cancel-timeout case passes in about `1.2s`, and all seven synchronous handshake
  manifest rules have been removed.
- [x] Synchronous notification and official `psycopg_pool` interoperability.
  Zero-time notification drains now poll the Rust connection, query completion
  dispatches callbacks only when handlers are registered, and generator
  `stop_after` preserves the remainder of a notification batch. The complete
  notification module passes `15/15`. The shared test harness recognizes the
  existing synchronous ferrocopg connection marker, allowing the official pool
  check, lifecycle, and destructor contracts to run; the five synchronous pool
  modules pass `164` cases with two expected async/version skips. The one
  observed backoff timing miss reproduced under explicit libpq and passed on
  the complete rerun.

Active cursor closure evidence:

- The concrete cursor and server-cursor modules pass `188/188` on both
  ferrocopg and explicit libpq.
- The fully unmasked raw-cursor module passes `78/78` on ferrocopg.
- The default-cursor subset of the common cursor module passes `94` tests and
  skips two libpq-version cases; no default-cursor failures remain.
- The client-cursor module passes `28/28`, including a regression proving
  representative literal adaptation does not call `pq.Escaping`.
- No synchronous default, raw, client, common, or server cursor module remains
  manifested. Async cursor manifests remain outside the first release contract.

The latest complete local full-harness evidence, measured before the concurrency
and handshake slices above, has been superseded. The post-notification/pool
complete local run on PostgreSQL 14 and CPython 3.14 reports `4706/5056` sync
(`0.931`), with zero synchronous errors, `40` reporter-classified failures,
`310` skips, and `242` manifested cases. Twenty-five failures are experimental
async type-info cases and two are async transaction parameters collected from
mixed modules. Of the 13 true synchronous failures, four macOS waiting-duration
assertions reproduce under explicit libpq, three were stale manifest-safety
expectations, two were exact raw-libpq boundaries, and four were low-volume
connection/error/SQL/packaging gaps. The nine actionable or stale cases now pass
or skip exactly in a targeted `10` pass / `2` raw-boundary run, leaving no known
Rust-specific synchronous behavioral failure from the complete report. Cursor
coverage is `571/582` (`0.981`) with zero failures and zero manifested cases;
notifications are `15/15`, pool is `167/169` with only two expected skips,
prepared statements are `32/32`, COPY is `111/111`, and pipeline is `40/40`.

The completed type tail still reports `2590` passing tests, seven environment
skips, 37 expected failures, and only the 25 experimental async-facade failures
from mixed modules. The focused bootstrap suite passes `200` cases with nine
expected TLS or prepared-transaction skips, all 25 Rust backend tests pass, and
the focused cursor family remains `571` passing with 11 version or intentional
skips. The complete sync harness satisfies the `0.85` floor at `0.931`. This
development environment collects optional dependency cases not present in the
CI key, so its denominator is reported independently. The waiting module's four
macOS timeout-duration failures reproduce under explicit libpq and are not Rust
backend gaps; four remote-close cases are exact raw `PGconn`/socket boundaries.

The first post-closure PostgreSQL 14-18 workflow for published revision
`36e2f249` did not reach the compatibility harness. All eight Rust matrix jobs
exposed a TPC recovery dependency on Psycopg's version-specific private cursor
state; the PostgreSQL 14 job additionally exposed parser-dependent forwarding
of `channel_binding`. The package job independently proved that the standalone
wheel still called `pq.Conninfo.parse()` even though its libpq compatibility
surface intentionally rejected that operation. The follow-up now loads TPC
results through the backend-owned cursor adapter, extracts and applies channel
binding programmatically in Rust, and gives the staged package a libpq-free
keyword/URI conninfo parser with environment metadata and invalid-option
diagnostics. Local evidence is `25/25` Rust tests, `200` bootstrap passes with
nine environment skips, `6/6` staging tests including mypy, a clean pre-commit
run, and a live Rust query from an isolated CPython 3.11 wheel. At that
checkpoint, a fresh matrix run was still required before any floor or phase
status change.

The same workflow also marked many otherwise unrelated upstream jobs failed
after their behavioral suites completed because the packaging type probe made
mypy re-check the entire staged vendored tree. A representative Linux job
reached `5987` passes before two Python-version-sensitive annotations in
`types/numeric.py` failed that incidental second check. The probe now follows
package imports silently while still asserting the public `connect()` overloads;
the repository's dedicated mypy and pre-commit checks remain authoritative for
source validation.

The first follow-up run confirms the standalone wheel job is green. Its
PostgreSQL 14 source-tree job still routed the rendered `channel_binding`
attempt through the deliberately oldest libpq before reaching Rust, and several
upstream C jobs exposed an older optimized bytea dumper not recognized by the
backend adapter copy. The next checkpoint renders Rust attempts with the
existing libpq-free merger and recognizes built-in C/binary bytea dumpers by
module as well as the newer optimization map. Focused Rust-attempt routing,
explicit C literal adaptation, the full C bootstrap suite (`193` passes and 16
environment/accelerator skips), and pre-commit are green locally. The remaining
seven compatibility jobs from the superseded run were intentionally cancelled
after they spent more than 15 minutes in the full harness; the next complete
run remains the authoritative matrix evidence.

The next PostgreSQL 14 attempt proved that rendering alone was insufficient:
top-level source connection selection still parsed the fully merged DSN with
libpq to read `target_session_attrs`. The backend now owns keyword and PostgreSQL
URI parsing for all Rust connection selection, metadata, merging, DSN, and
cancellation-probe paths, with early `ProgrammingError` diagnostics for invalid
options. This source parser is covered alongside the already-green standalone
wheel parser; the complete local Python bootstrap suite is `201` passes with
nine environment skips and pre-commit is green. PostgreSQL 14 CI still had to
confirm that no old-libpq parse remained before the harness evidence could be
accepted.

Compatibility accounting now defines the release rate over executed,
non-manifested cases while retaining `total` and `manifested` as deterministic
baseline fields and reporting environment/version skips separately. Async test
functions in mixed modules are classified by their `_async` name instead of
inflating the synchronous scope. The complete local PostgreSQL 14 / CPython
3.14 report is `4669/4680` executed sync cases (`0.998`), with 310 skips, 11
timing failures, zero errors, and 242 manifested boundaries. The failures are
four macOS waiting assertions already shared with libpq, five pool scheduler
timing assertions, and two transaction/pipeline timing assertions; every other
synchronous feature family executes at `1.000`. The committed sync floor is
therefore ratcheted from `0.85` to the release contract of `0.95`. All eight
PostgreSQL 14-18 / CPython 3.11-3.14 matrix artifacts satisfy it at
`0.995-0.996`. The denominator-corrected workflow run `29213883257` completed
all eight Rust lanes and the standalone package job successfully, proving the
broad percentage gate. A separate upstream PyPy job is outside the `0.1.0`
release contract, but it remains part of the repository's complete workflow
health gate.

The same release checkpoint makes namespace staging explicitly UTF-8 for both
reads and writes. This closes the Windows locale failure on non-ASCII upstream
source comments without changing generated package contents; the combined
reporter and package regression suite passes `22/22` locally.

Completed broad-gate evidence:

1. Bootstrap, standalone-wheel, and complete compatibility jobs finish on
   PostgreSQL 14-18 and CPython 3.11-3.14.
2. Every matrix artifact matches its committed executed-rate denominator and
   manifested-boundary baseline.
3. Every server/interpreter key satisfies the ratcheted `0.95` sync floor.

Current closure backlog from workflow run `30195687685`:

- [x] Restore bytea adapter-map isolation so minimal caller maps remain valid
  while maps with bytea support retain the OID-only Rust wire dumper.
- [x] Close connection, failover, cancellation, signal, and concurrent-close
  differences, distinguishing backend defects from exact platform/version
  behavior with focused Rust and explicit-libpq comparisons.
- [x] Close synchronous cursor metadata and DB-API state differences, including
  DDL `description` and `fetchmany()`/`arraysize`.
- [x] Close notification generator, blocking, callback, and connection-lock
  behavior without weakening timing or lock assertions globally.
- [x] Close TPC prepared-state cancellation and pipeline transaction/savepoint
  differences.
- [x] Keep upstream tests that intentionally exercise libpq generator plumbing
  explicit about `impl="libpq"` instead of accidentally exercising the
  Rust-default product path.
- [x] Audit the low-frequency remote-close, `hostaddr`, GSSAPI-warning, and pool
  destructor failures across the supported matrix; fix deterministic backend
  behavior and use only narrow, justified environment/version exclusions.
- [x] Rerun focused modules, the complete synchronous harness, all eight
  PostgreSQL/CPython Rust lanes, the standalone package job, and the complete
  upstream workflow with zero release-critical failures.

Latest local closure evidence:

1. Both Rust crates compile and format cleanly; the core suite passes `26/26`,
   including bounded cancellation during a stalled PostgreSQL handshake.
2. The complete bootstrap suite passes `205` cases with eight expected local
   TLS-configuration skips.
3. The complete synchronous concurrency module passes `15` cases with one
   macOS-inapplicable fork skip. The previously failing stalled-host SIGINT,
   active-query SIGINT, remote termination, and concurrent-close cases pass
   together in `3.84s`.
4. The notification module passes `15/15` in isolation. The deterministic
   synchronous pool suite passes `131` cases with one upstream-version skip.
5. The complete local harness reports `4708/4718` executed synchronous cases
   (`0.998`), zero synchronous errors, and 242 manifested boundaries. Its ten
   synchronous failures were five notification timings, four macOS waiting
   timings, and one COPY-description regression. The timing groups pass in
   isolation or reproduce under explicit libpq; the COPY regression is fixed
   with focused COPY and DB-API description tests passing together.
6. Compatibility CI now installs the release-mode extension before running
   timing-sensitive upstream tests, matching the artifact users will run.
7. The compatibility reporter now fails a Rust lane on any non-manifested
   synchronous failure or error instead of allowing the broad `0.95` floor to
   hide a release-critical regression.
8. Workflow run `30227231014` is the authoritative Phase 4 closure evidence.
   All `57` jobs passed, including all eight PostgreSQL/CPython Rust lanes, the
   standalone package, both PyPy lanes, Linux fork behavior, and the strict
   zero release-critical regression gate. The representative PostgreSQL 15 /
   CPython 3.11 report is `4591/4591` executed synchronous cases (`1.000`),
   with zero failures, zero errors, total `4781`, and `249` manifested
   boundaries.
9. The final PostgreSQL 15 disconnect fix passes its focused upstream test ten
   consecutive times, and the vendored connection-poller regression tests pass
   `2/2`. The final combined notification, disconnect, and bootstrap run passes
   `207` tests with eight expected TLS skips.

For every slice:

- add or strengthen focused tests
- run the relevant upstream test modules
- run the full sync compatibility harness
- remove obsolete manifest entries instead of raising the floor alone
- preserve explicit libpq behavior and upstream mergeability

Definition of done:

- The sync compatibility rate is at least 95% on PostgreSQL 14-18.
- Only raw libpq/socket boundaries remain manifested for the release contract.
- The complete release-critical suite is green.

### Phase 5: Pooling, stress, and performance

Status: in progress. The installed-package harness is implemented; acceptance
runs and performance fixes remain required.

Tasks:

- [x] Prove official `psycopg_pool.ConnectionPool` integration.
- [x] Define reproducible soak and benchmark commands, machine metadata, and
  acceptance thresholds.
- [x] Add connection churn, transaction, cancellation, COPY, and pipeline soak
  tests.
- [x] Add leak checks for Python objects, Rust sessions, sockets, and threads.
- [x] Build the comparative libpq benchmark suite for latency, throughput,
  memory, sockets, and threads.
- [x] Establish a full-duration, three-backend soak baseline with published CI
  artifacts (revision `24b646e3`; not the final optimized candidate).
- [x] Configure weekly/manual reliability CI and artifact retention.
- [ ] Close the measured query, result-adaptation, transaction, COPY, and pool
  performance gaps without weakening correctness or acceptance thresholds.
- [ ] Pass the complete benchmark at least three times on the same otherwise
  idle machine using an installed release wheel built from the recorded revision.
- [ ] Pass the full 30-minute-per-backend soak, including concurrent pool use,
  on the candidate revision.
- [ ] Confirm scheduled soak execution and publish final-candidate reproducible
  results; a passing earlier CI run does not validate later code.
- [ ] Revalidate the complete supported synchronous compatibility matrix and
  installed-package boundary after the performance changes.
- [x] Align the README with the completed synchronous contract, staged-package
  usage, official async delegation, and experimental Rust async status. Remove
  obsolete Phase 4 gap claims without removing the raw libpq/socket boundaries.

The commands and acceptance budgets are documented in
`docs/ferrocopg-performance.md`. The harness under `tools/phase5` measures the
installed release wheel against pinned official Python/C packages, records raw
samples and machine metadata, and rejects incomplete or failed workers. The
weekly/manual CI workflow retains reports and logs even on failure.

Initial macOS / PostgreSQL 15 measurements exposed query and adaptation
performance gaps, and the installed pool test exposed connections closing on
checkout-context exit. Pool reuse, commit, rollback, and shutdown now pass an
installed-wheel regression. Lazy adapter lookup and native batch row loading
remove repeated registry scans and Python row conversion overhead. Result
metadata and single-row access no longer copy an entire result set.

Development evidence on 2026-09-17 includes 11 harness/installed-wheel tests,
2,687 synchronous bootstrap/type/cursor passes, and a 30-second-per-backend
resource soak. Bulk row workloads now outperform the official Python path,
but the C comparison and small-query/COPY targets still fail. The first CI
benchmark run `35273950915` published its complete failure artifacts. Its
superseded soak was canceled after the Rust pool defect was identified.
The new soak also exercises eight concurrent pool clients. Short development
runs do not establish the 30-minute soak or performance gates. Phase 5 remains
incomplete until repeated full benchmarks and sustained soaks pass.

The benchmark artifact from CI run
[`35275945289`](https://github.com/martijnberger/ferrocopg/actions/runs/35275945289)
records revision `24b646e364c261a8e39195dd9e34c29b2fee899d`. Connection
setup passes both duration limits, and bulk row adaptation beats the Python
baseline. Nine of eleven workloads still exceed the C limit: Rust/C median
duration ratios range from `1.445` for dictionary rows to `2.668` for text
COPY. Parameterized/prepared queries, transactions, COPY, and pool cycles also
exceed the Python limit. These measurements establish optimization priorities,
not release acceptance or the status of the separate soak job.

The separate soak artifact from that same run is now confirmed green on Linux
x86_64, CPython 3.14.7, and PostgreSQL 18.6. Rust ran for `1800.87` seconds,
official Python for `1800.82`, and C for `1800.35`, with no reported failures.
All sampled workload-session counts were zero. Rust completed 21,580
operations in each of the eight scenarios, including concurrent pool use;
its driver-object count stayed at 630, with one thread, one observer socket,
and five file descriptors after cleanup. Retained RSS grew from approximately
54.5 to 57.2 MiB, within the documented budget. This proves a sustained
baseline, not the absence of all leaks or acceptance of subsequent changes.

Checkpoint `2d7ca925` reduces notice-draining overhead, caches COPY formats,
and splits binary COPY output in one native pass. The next slice shares native
primitive loaders between results and COPY while retaining custom callbacks
and conversion errors. It passes 2,809 synchronous bootstrap/COPY/type tests
and 13 harness/installed-wheel tests. Its development benchmark puts binary
COPY at Rust/Python `0.919` and Rust/C `1.586`: improved, but still outside the
C gate. This is single-run development evidence, not release acceptance.
Preserve cancellation, concurrent use, custom adapters, encoding, row
factories, and COPY error behavior while optimizing these paths.

The following query-setup slice caches result wrappers, reuses the query's
adaptation context when encoding and loader state permit it, and avoids a
second dumper lookup for unnecessary UTF-8/SQL_ASCII transcoding. The focused
bootstrap/cursor/pipeline/prepared/type selection reports 2,848 passes, 22
skips, 415 deselections, and 37 expected failures; all 14 harness/installed-wheel
tests pass. A 30-second Rust cache-cleanup smoke shows stable objects, sockets,
and file descriptors with zero surviving workload sessions. The complete
development benchmark still fails nine workloads against C. Neither this
focused selection nor the short soak replaces final matrix or sustained-soak
acceptance.

Checkpoint `20434436` adds native dumping for pinned COPY primitives, with
fallback for custom dumpers, subclasses, encodings, and conversion errors.
Native codec plans now avoid reconstructing their callback lists per row and
participate in Python cycle collection. The latest development benchmark puts
binary COPY at Rust/Python `0.750` and Rust/C `1.259`; the latter still fails
the strict `1.25` limit and is not rounded down to a pass.

Revalidation also exposed two comparison/coexistence issues: a duplicate Cython
pipeline declaration prevented fresh C builds (fixed in `91cc4525`), and
standalone file COPY plus the random-data fixture selected C adaptation for a
Rust connection. Those paths now select backend-compatible adaptation without
adding skips. The C-enabled Rust bootstrap/COPY/type selection passes 2,802
tests with 24 skips and 37 expected failures. Explicit C/libpq COPY and NumPy
tests pass 278 cases with six skips; the rebuilt C pipeline/prepared suite
passes 72 with six skips. At checkpoint `b66c1549`, all 16
harness/installed-wheel checks pass, including
callback-cycle collection. A fresh complete CI matrix is still required.

The text COPY slice adds a native path for exact built-in integer
and UTF-8 string values while retaining fallback for custom adapters and other
types. Its local report, labeled `text-primitives-working-copy` at
`/tmp/phase5-text-primitives-bench/report.json`, measures text COPY at
Rust/Python `0.761` and Rust/C `1.405`. Binary COPY measures `0.728` and
`1.238`, passing both limits in this run only. Eight workloads still fail
against C: parameterized and prepared queries, all three row factories,
transactions, text COPY, and pool cycles. The query, transaction, and pool
workloads also miss the Python limit. This temporary local report is not
published exact-revision evidence and does not satisfy the three-run gate.

Rebuilt-wheel validation for this slice passes all 17 harness/installed checks.
The C-enabled synchronous bootstrap/COPY/type selection reports 2,803 passes,
23 skips, 17 deselections, and 37 expected failures. A fresh 30-second Rust
smoke reports no resource failures, stable driver-object/socket/file-descriptor
counts, and zero surviving workload sessions. These remain focused checks.
The preceding checkpoint `b66c1549` now has a complete green 57-job Tests run
[`35279919888`](https://github.com/martijnberger/ferrocopg/actions/runs/35279919888)
and green Lint run `35279919881`; subsequent changes need their own matrix.

The next slice retains validated PostgreSQL row buffers and field offsets
instead of allocating and copying every field before loading it. Results do
not retain their originating session or prepared statement; Python raw access
still returns independent values. All 18 installed-wheel checks pass,
including retained results after connection close, NULL/empty/binary fields,
and zero-column rows. The broader synchronous bootstrap/COPY/type/cursor/
prepared/pipeline selection passes 2,953 cases with 29 skips, 205 deselections,
and 37 expected failures; all 26 Rust core tests and pre-commit checks pass.
A 30-second Rust smoke has no resource failures or surviving workload sessions.

Two local `raw-rows-working-copy` benchmark runs bracket a freshly built
`5e1b59f8` parent comparison on the same setup. Tuple, dictionary, and namedtuple
Rust/C ratios improve from the parent's `1.862`, `1.434`, and `1.663` to
`1.481-1.623`, `1.281-1.332`, and `1.371-1.486`, respectively, but remain
outside the gate. Small-query timings vary across runs and still fail both
limits. Binary COPY passes in the first run (`1.148` against C) but fails in
the second (`1.290`); the repeat therefore fails nine workloads against C.
Both complete reports and the parent comparison are retained locally under
`/tmp/phase5-raw-rows-bench`, `/tmp/phase5-raw-rows-bench-2`, and
`/tmp/phase5-text-parent-bench`. These development comparisons are not the
required three passing exact-revision acceptance runs.

Checkpoint `639bbd69` removes the native per-session worker
and per-operation channel handoff. It releases the interpreter while executing
on the caller's thread, checking signals during I/O at ten-millisecond
intervals outside Tokio's runtime. Cancellation still drains the active
operation before raising the signal exception. Metadata and close calls also
release the interpreter while waiting for the session mutex. The experimental
async facade retains its separate connection-affine executor.

Validation includes 26 Rust core tests, three vendored wait-loop tests, 211
synchronous concurrency/bootstrap passes, both existing async-facade checks,
and all 20 harness/installed-wheel checks. New subprocess regressions cover a
signal handler querying another connection, cancellation recovery, and
concurrent metadata/close calls without a GIL/session-lock deadlock.
A 60.14-second Rust resource smoke reports no failures: driver objects remain
at 615, sockets at one, file descriptors at four, and workload sessions at zero;
retained RSS does not grow. This is not the required sustained candidate soak.

The complete local synchronous selection with the Rust CI adapter setting
(`PSYCOPG_IMPL=python`) reports 4,072 passes, five failures, 467 skips, 2,066
deselections, and 38 expected failures. Four waiting failures also reproduce
under explicit libpq; pool backoff passes in isolation but misses its tight
timing bound in the full run. The C-enabled selection reports 4,082 passes and
ten failures, adding a fifth libpq waiting failure and four C-transformer
array-adaptation failures that reproduce on parent `16ac4f9b`. No skips or
budgets were changed. These are not green full-matrix acceptance results;
the C-transformer follow-up below addresses the separate coexistence issue.

Both complete local `direct-execution-working-copy` benchmark reports retain
eight C-limit failures. Transaction Rust/C ratios are `1.731-1.798`, pool
`1.445-1.617`, and binary COPY `1.201-1.210`. Binary COPY passes in these two
runs, but the full performance gate remains failed and timings still vary.
Raw evidence is under `/tmp/phase5-direct-execution-bench` and
`/tmp/phase5-direct-execution-bench-2`; final exact-revision repeated benchmarks,
sustained soaks, and the supported compatibility matrix remain required.

The subsequent native row-construction slice loads values directly into PyO3
tuple storage instead of first collecting a temporary Rust vector. The exact
built-in tuple factory bypasses a redundant Python call; custom factories and
tuple subclasses still run normally. Native COPY loading shares the same
fallible conversion path. All 26 Rust core tests, 21 harness/installed-wheel
checks, and 2,906 focused synchronous bootstrap/row/cursor/COPY/type tests pass
(16 skips, 17 deselections, and 37 expected failures in the focused selection).
The new installed regression covers partial tuple/list cleanup on loader
failure, factory errors, NULL/empty values, bounds, and detached result lifetime.

Two complete `tuple-construction-working-copy` benchmarks retain seven C-limit
failures. Tuple, dictionary, and namedtuple Rust/C ratios are respectively
`1.322-1.323`, `1.198-1.233`, and `1.343-1.386`. Dictionary rows pass both
limits in these two runs; binary COPY also passes narrowly at `1.235-1.236`
against C. Small queries, transactions, pool cycles, tuple/namedtuple rows,
and text COPY still fail. Raw reports are retained locally in
`/tmp/phase5-tuple-bench` and `/tmp/phase5-tuple-bench-2`; they are not published
exact-revision acceptance. No threshold or skip rule was relaxed.
A 60.34-second Rust smoke reports no resource failures, stable driver objects,
sockets, and file descriptors, and zero surviving workload sessions. It is not
the required final-candidate sustained soak.

The source-tree C transformer now resolves backend-compatible Python adapters
when its context is a Rust connection, instead of constructing C dumpers that
require a real libpq `PGconn`. The four reproduced array-dumper failures pass.
The complete adaptation module reports 67 passes and one raw-PGconn boundary
skip with C enabled, and 66 passes with two expected skips on the Python
transformer. Explicit C/libpq passes all 68 cases, including the additional
extension-independent backend-context test. The broader C-enabled Rust
bootstrap/COPY/type selection is
2,803 passes, 23 skips, 17 deselections, and 37 expected failures. The new
backend-context regression runs in comparison CI without requiring the Rust
extension, keeping this dispatch path covered. The complete local C-enabled
synchronous selection after this fix reports 4,090 passes, five failures,
452 skips, 2,066 deselections, and 38 expected failures. All five failures are
the macOS waiting-duration assertions previously reproduced under explicit
libpq; the pool backoff test passes in this run. This is not a green full-matrix
result. Full-matrix revalidation still remains required, and the official
installed benchmark comparators are unchanged.

The next row-storage slice uses native loading for `fetchone()` and iteration,
shares loader initialization with bulk fetching without dummy rows, preallocates
bulk result lists, and avoids validating UTF-8 twice during string construction.
Legacy factories and encoding bridges retain the Python conversion path.
All 22 harness/installed-wheel checks pass, including mixed fetch methods,
loader replacement, changed factories, zero-column rows, factories returning
`None`, and partial-row cleanup on conversion failure. The C-enabled focused
bootstrap/cursor/row/COPY/adaptation/type selection passes 2,966 cases with
24 skips, 17 deselections, and 37 expected failures; all 26 Rust core tests and
pre-commit checks pass.

Its first complete benchmark with 100 operations per sample (five times the
default, with unchanged limits) still fails eight C comparisons. Tuple/dict/
namedtuple Rust/C ratios are `1.395`, `1.278`, and `1.333`; binary COPY passes
at `1.217`. The earlier dictionary-row passes are therefore not stable release
evidence. Longer 1,000-operation small-query comparisons for the single-row
sub-slice show only a modest prepared-query gain, no clear pool gain, and
inconclusive parameterized-query results. Raw development reports remain under
`/tmp/phase5-row-storage-bench` and `/tmp/phase5-single-row-*-long.json`, with
parent comparisons under `/tmp/phase5-tuple-*-long.json`. These results do not
satisfy final exact-revision acceptance.

The second row-storage run, under `/tmp/phase5-row-storage-bench-2`, also fails
eight C comparisons: dictionary rows pass at `1.210`, but binary COPY now fails
at `1.264`. Neither workload has established reliable acceptance margin.

For the earlier direct-execution revision `639bbd69`, CI run `35313980577`
completed all 57 jobs successfully, including all eight Rust compatibility
lanes, the standalone package job, and the Windows C lanes. This validates
the earlier checkpoint, not the subsequent row-storage changes.

The cursor-lifecycle follow-up removes a loader-callback reference cycle and
shares the public cursor's owned adapter map with its Rust adapter instead of
copying it twice. Before the fix, ten closed cursor adapters remained alive
until cyclic GC ran. The installed regression now proves both open and closed
cursors release immediately with cyclic GC disabled, even when their adapter
map is retained; registering a loader on that retained map remains safe.
All 23 harness/installed-wheel checks pass. The explicit cursor, adaptation,
row-factory, and bootstrap modules run without name filtering and pass 854
cases with 27 expected skips. Regenerating the synchronous cursor from its
async source produces the same file hash, and pre-commit checks pass.

Local validation must not use `-k 'not async'` as proof of full synchronous
coverage: it also excludes synchronous cases whose fixture IDs contain
`asyncio`. Earlier reports using that selection are subsets, even when all
selected cases pass. Use explicit synchronous modules without that name filter,
or the full harness with its sync/async reporter; CI's classified matrix remains
the authoritative compatibility gate.

The lifecycle benchmark (`/tmp/phase5-cursor-lifecycle-bench`, 100 operations
per sample) still fails seven C comparisons plus plaintext Python parity
(`1.044`). Prepared-query Rust/C is `1.852`, pool is `1.912`, tuple rows are
`1.290`, and namedtuple rows are `1.357`. Dictionary rows and binary COPY pass
in this run, but previous variability remains relevant. The lifecycle fix
removes confirmed retention and allocation overhead; it does not establish
the required performance contract.

The cursor-lifecycle Rust resource smoke ran for `60.32` seconds with no
reported failures and zero surviving workload sessions. This is development
evidence only. The full unfiltered compatibility run reports `4715/4720`
executed synchronous cases, five failures, and zero synchronous errors. All five
failures reproduce under explicit C/libpq: four macOS waiting-duration checks
and the pool's check-backoff timing assertion. The local run and strict
zero-regression reporter remain failed; comparison reproduction is not a waiver.
Experimental async coverage is reported separately (`504/620` executed, with
105 failures and 11 errors). The local optional-dependency denominator is not
a replacement for a supported CI matrix key.

The older three-backend CI soak in run
[`35311495257`](https://github.com/martijnberger/ferrocopg/actions/runs/35311495257)
on `5e1b59f8` completed but failed acceptance. Rust and Python each ran for at
least 1,800 seconds without reported failures; C recorded one surviving session
in one of 1,836 resource samples, although final cleanup was zero. This report
must remain a failure. Investigate bounded cleanup sampling before attributing
it to a persistent driver leak; it does not validate the newer candidate.

The sampler follow-up now requires zero sessions as well as two stable resource
readings before returning early. Previously two identical nonzero readings
could end settling before the existing five-second deadline. The deadline and
zero-session acceptance budget are unchanged, and persistent sessions still
fail. All 12 accounting tests pass, including transient cleanup, persistent
sessions, and unstable-resource cases; a 60.22-second C-backend smoke passes.
Neither this correction nor the smoke retroactively accepts the failed CI run.

The next result-bookkeeping slice avoids eager row/column materialization when
native result metadata is available and reuses the already-selected public
result wrapper instead of repeating its cache lookup. Zero-column result
metadata is tested with a row accessor that raises if materialized. Installed
coverage verifies navigation, pipeline-triggered fetching, streaming metadata,
and cursor close. The unfiltered focused modules pass 902 cases with 26 skips
under pure Python and 896 with 32 skips with the C accelerator selected;
all 27 harness/installed-wheel checks and pre-commit pass. Synchronous cursor
generation is reproducible. These are focused checks, not a new full matrix.

For 9,010 profiled prepared queries, result-cache calls fall from 36,041 to
18,021 and cumulative public-cursor synchronization time falls from about
81 ms to 62 ms. The complete 100-operation-per-sample development benchmark
under `/tmp/phase5-result-bookkeeping-bench` still fails seven C comparisons:
parameterized `1.665`, prepared `1.910`, tuple rows `1.279`, namedtuple rows
`1.373`, transactions `1.641`, text COPY `1.407`, and pool cycles `2.032`.
Dictionary rows and binary COPY pass both limits in this run. Plaintext setup
also passes, unlike the lifecycle run. Timing variability and the remaining
failures preclude an acceptance claim; final-candidate repetition is unchanged.

The `baec4faf` matrix exposed stale denominator accounting, not synchronous
behavior failures, in the inspected PostgreSQL 14, 16, and 17 lanes. Comparing
the CPython 3.12 / PostgreSQL 16 JUnit against the green `639bbd69` run proves
exactly three added adapter test IDs and no removed IDs (`4781` to `4784`).
Subsequent zero-column metadata coverage adds two more cases. The encoding
regression now also runs UTF-8 for both connection/cursor contexts, adding two
cases while marking only LATIN1 unsupported on CockroachDB through the existing
encoding marker. Its UTF-8 and custom-adapter coverage remains active there.
Expected synchronous totals are therefore `4788` for CPython 3.11-3.13 and
`4823` for 3.14. Async totals, manifests, pass-rate floors, and the zero-regression
budget are unchanged. Local Rust adaptation/accounting tests pass 86 cases with
one raw-libpq skip; explicit C/libpq adaptation passes 70. The complete
`4a132959` workflow subsequently passed all 57 jobs, validating the revised
counts in all eight Rust lanes and the upstream comparison coverage, including
CockroachDB. This does not validate the later borrowed-parameter change.

The borrowed-parameter slice removes per-parameter byte-vector clones and
boxes from the Rust wire path. Unprepared execution now passes a borrowed
typed iterator; prepared execution keeps only a vector of borrowed references.
The existing dumper output, OIDs, text/binary formats, NULL/empty distinction,
and parameter-count validation are unchanged. All 28 Rust tests and 28
harness/installed-wheel checks pass, including a 1 MiB mixed-format roundtrip,
prepared mismatch recovery, and result validity after session close. The
C-enabled adaptation/prepared/cursor selection passes 562 cases with 13 skips.
A 60.25-second Rust resource smoke passes with zero workload sessions in all
125 cleanup samples. It is not sustained-soak acceptance.

The full local unfiltered run reports `4715/4724` executed synchronous cases
with nine timing failures and no synchronous errors. Eight reproduce under
explicit C/libpq; the remaining readiness-timing case passes when rerun alone.
All synchronous connection, COPY, cursor, notification, pipeline, prepared,
transaction, and type/metadata cases in this run pass. The strict reporter
still fails, and fresh supported CI is required. Async remains independently
reported at `504/620`, with 105 failures and 11 errors.

The complete development benchmark under `/tmp/phase5-borrowed-params-bench`
still fails seven C limits and narrowly misses TLS/Python parity (`1.013`).
Rust/C ratios include parameterized `1.812`, prepared `1.819`, tuple `1.296`,
namedtuple `1.315`, transactions `1.675`, text COPY `1.393`, and pool `1.945`.
Longer warmed query comparisons are effectively flat: prepared latency is
about 68.8 microseconds versus 68.3 before, and parameterized latency is about
72.8 microseconds for both. Removing the allocations has not established a
small-query throughput improvement or closed an acceptance gate. Prioritize
the remaining measured query/adaptation costs rather than assuming this slice
satisfies the performance contract.

The result-bookkeeping revision `2ed94013` now has a second verified sustained
checkpoint in workflow
[`35320190913`](https://github.com/martijnberger/ferrocopg/actions/runs/35320190913).
The published soak reports record Rust `1800.39` seconds, Python `1800.55`, and
C `1800.64`, with no failures and zero surviving workload sessions in every
sample (2,302 / 1,817 / 2,528 samples respectively). Rust completed 46,080
operations in each of the eight scenarios. Its final cleanup had 630 driver
objects, one thread, one observer socket, and five file descriptors. The
benchmark job in the same workflow failed; a successful soak does not accept
its performance or the later parameter/metadata changes.

The next metadata slice exposes native column counts, OIDs, and indexed names
without constructing complete Python column/description objects. Row-loader
setup consumes the OIDs directly; status messages avoid eager metadata access
and unbounded token splitting. Static `Callable` casts no longer construct
typing objects on each query. Existing fallback results, custom loaders,
encodings, and row factories retain their conversion paths. The unfiltered
focused bootstrap/adaptation/cursor/row/prepared modules pass 680 cases with
22 skips, all 28 Rust tests pass, and all 29 installed-wheel/accounting checks
pass, including 64-column rows, zero-column results, index bounds, and detached
metadata. Pre-commit passes.
The final C-accelerator coexistence selection covers the same cursor/adaptation
modules plus the complete type directory without name filtering: all 3,170
executed synchronous cases pass. Its six failures are independently classified
experimental async type-info cases (`9/15` async); the subset's synchronous
zero-regression reporter passes. This is additional coexistence coverage, not
a replacement for the complete supported matrix.

The first full metadata harness reports `4720/4724` synchronous cases, four
failures and no errors. All four failures reproduce under C/libpq and reveal a
deterministic test expectation bug, not ordinary timing jitter: the non-Linux
waiting test asks its generator for two timeout cycles but expects only one
`interval`. Revision `61ab3655` corrects the async source and regenerates the
sync test to expect `nevents * interval`, matching the Linux test. The ready
case, 20% tolerance, test count, manifests, and regression budget are unchanged.
The complete affected sync/async C/libpq selection passes 24 cases with four
platform skips, and sync regeneration is reproducible. This does not waive the
failed full report. The fresh unfiltered run with the corrected test and final
metadata code passes `4724/4724` executed synchronous cases, with zero failures
or errors, 277 skips, and 242 manifested boundaries. The strict zero-regression
reporter passes. Experimental async remains separate at `505/620`, with 104
failures and 11 errors; the overall pytest invocation therefore still exits
nonzero. Raw evidence is in `/tmp/phase5-column-metadata-corrected-full.xml`
and its `-report.json` companion. Supported CI remains required; this local
optional-dependency denominator is not a replacement for a matrix key.

A native dumper-cache dispatcher was also prototyped and measured, then removed:
it was effectively flat versus the existing Python dispatcher. The retained
metadata/typing slice reduces callable-type construction from 20,069 to 67 calls
in a 10,000-query profile, and notice-draining cumulative time from about 46 ms
to 19 ms. Two warmed parent/candidate comparisons (1,000 warmups and nine samples
of 1,000 operations, with the second comparison reversing order) show prepared
latency of `70.0 -> 67.9` and `73.1 -> 71.8` microseconds. Parameterized latency
is `79.5 -> 74.6` and `79.2 -> 70.5` microseconds; that larger variability is not
proof of a stable percentage gain. Raw reports are under
`/tmp/phase5-{before-,}column-metadata-{prepared,parameterized}*.json`.

The complete metadata development benchmark in `/tmp/phase5-column-metadata-bench`
still fails seven C limits: parameterized `1.846`, prepared `1.301`, tuple rows
`1.310`, namedtuple rows `1.327`, transactions `1.700`, text COPY `1.428`, and
pool cycles `2.064`. Parameterized/prepared, transactions, and pool also miss
Python parity. Connection setup, dictionary rows, and binary COPY pass both
limits in this run. Neither these local reports nor the modest warmed gains
satisfy final-candidate performance acceptance.

The second complete metadata run, using the final rebuilt wheel and retained
under `/tmp/phase5-column-metadata-bench-2`, also fails seven C limits and four
Python limits. Rust/C ratios are parameterized `1.887`, prepared `1.513`, tuple
rows `1.259`, namedtuple rows `1.335`, transactions `1.642`, text COPY `1.468`,
and pool cycles `1.950`. Dictionary rows (`1.233`) and binary COPY (`1.208`)
pass narrowly. Do not count near misses or variability as acceptance, or combine
passing workloads from different runs.

The final metadata wheel's Rust resource smoke ran for `60.29` seconds with
129 samples, no reported failures, and zero workload sessions in every sample.
Final cleanup recorded 615 driver objects, two threads, one socket, and four
file descriptors. The report is `/tmp/phase5-column-metadata-soak.json`; this
short smoke does not satisfy the final 30-minute-per-backend soak gate.

The inline row-offset slice `578dbf9c` stores up to two field ranges
inside each row using `SmallVec`, spilling wider rows to heap storage. Both
simple and extended result paths retain owned wire bytes and preserve NULL,
empty, binary, and detached-result behavior. Five vendored unit tests pass,
including malformed-row and inline/spill boundaries; all 28 Rust backend tests
and 29 installed-wheel/accounting checks pass. The installed lifetime regression
covers one-, two-, three-, and five-column text/binary results, alongside the
existing zero- and 64-column coverage. The C-accelerator coexistence selection
passes all 3,170 synchronous cases; its six known experimental async type-info
failures remain separately classified. These targeted checks do not establish
a new full correctness baseline.

The first full inline-offset run reports `4723/4724` synchronous cases, with
one failure and no errors. `tests/pool/test_pool.py::test_check_backoff` measures
`105.185 ms` against a `105 ms` upper bound for a requested `100 ms` sleep.
The unchanged isolated assertion passes under C/libpq but fails again under
Rust. A diagnostic wrapper records the Rust sleep itself taking `105.015 ms`,
before subsequent pool processing; the pure-Python/libpq diagnostic passes
with a `103.725 ms` sleep. This locates the observed overshoot but does not
prove the failure is harmless or satisfy the zero-regression gate. No test,
tolerance, pool retry policy, or manifest was changed. Preserve the failed
`/tmp/phase5-inline-ranges-full.xml` and its classified report; the unchanged
full repeat in `/tmp/phase5-inline-ranges-full-2.xml` has the same sole
synchronous failure (`109.594 ms` for the first backoff). Both strict local
zero-regression reports fail. Async remains
`505/620`, with 104 failures and 11 errors, separately from synchronous status.

Two parent/candidate measurement orders favor inline offsets, but the gains are
small and variable. Prepared-query medians are `68.72 -> 65.83` and
`65.43 -> 65.04` microseconds; tuple-row medians are `312.31 -> 311.44` and
`301.63 -> 290.08` microseconds. Do not infer a stable percentage improvement.
The complete development report in `/tmp/phase5-inline-ranges-bench/report.json`
still fails six C limits: parameterized `1.346`, prepared `1.747`, namedtuple
rows `1.407`, transactions `1.626`, text COPY `1.428`, and pool cycles `1.886`.
Parameterized/prepared queries, transactions, and pool also fail Python parity.
Tuple rows pass narrowly at `1.248` against C in this run only. This report is
working-copy evidence, not a revision-frozen acceptance run or a replacement
for the three complete passing runs required below.

The second complete inline-offset report in `/tmp/phase5-inline-ranges-bench-2`
also misses six C limits and four Python limits. Rust/C ratios are parameterized
`1.657`, prepared `1.494`, transactions `1.628`, text COPY `1.346`, binary COPY
`1.271`, and pool cycles `1.967`. Tuple rows pass narrowly again (`1.248`), and
namedtuple rows pass (`1.174`), but binary COPY regresses across the limit.
Do not combine passing workloads across runs. The installed Rust resource smoke
ran `60.05` seconds with 136 samples, no reported failures, and zero surviving
workload sessions in every sample. Cleanup recorded 615 driver objects, two
threads, one socket, and four file descriptors. Its report is
`/tmp/phase5-inline-ranges-soak.json`; sustained final-candidate validation is
still required.

The parent metadata CI benchmark at `be46e180` also fails acceptance, independently
of local machine variability: parameterized `1.519`, prepared `1.270`,
transactions `1.261`, text COPY `1.357`, and pool `1.422` against C; four
workloads also miss Python parity. Raw reports are published in workflow
`35330914691`. Its completed soak reports Rust `1800.62` seconds, Python
`1800.67`, and C `1800.58`, with no failures and zero surviving workload sessions
in all 1,868 / 1,436 / 2,044 samples. Rust cleanup records 630 driver objects,
one thread, one observer socket, and five file descriptors. This sustained pass
belongs to `be46e180`, not to the later inline-offset or loader-resolution code.

The loader-resolution follow-up `ed5e86b8` replaces repeated imports and built-in MRO
checks with a lazy six-class table. Matching uses class identity without invoking
user metaclass hashing; the table retains no user adapter or connection state.
Text loader encoding is still inspected on each classification. Custom and
generated array classes keep their fallback behavior. All 30 installed-wheel
checks pass, including mutable encodings, custom subclasses, unhashable custom
classes, explicit remapping, and collection of custom loader classes/instances.
The C-accelerator coexistence selection passes all 3,170 synchronous cases;
the six experimental async type-info failures remain separately classified.
Formatting, typing, and spelling checks pass.

Two warmed prepared-query comparisons using the final rebuilt wheel favor the
loader table: parent/candidate medians are `64.95 -> 63.02` and
`66.01 -> 61.90` microseconds, with CPU time `43.52 -> 41.41` and
`44.39 -> 40.66`. These are development comparisons, not a stable percentage
claim. The full `/tmp/phase5-loader-codes-bench/report.json` still misses six C
limits: parameterized `1.494`, prepared `1.414`, namedtuple rows `1.287`,
transactions `1.642`, text COPY `1.428`, and pool `1.670`. Plaintext connection
setup also narrowly misses Python parity (`1.014`), in addition to the four
small-query/transaction/pool misses. Neither the local speedup nor earlier
revision-linked green checks establish final-candidate acceptance.

The final loader-table wheel's resource smoke ran `60.33` seconds with 142
samples, no reported failures, and zero surviving workload sessions throughout.
Cleanup recorded 615 driver objects, two threads, one socket, and four file
descriptors. The report is `/tmp/phase5-loader-codes-soak.json`. The full
unfiltered local run in `/tmp/phase5-loader-codes-full.xml` passes `4721/4724`
synchronous cases, with three failures and no errors. Reconnect, check-backoff,
and scheduler timing assertions measure first intervals of `111.512`, `110.173`,
and `110.112 ms` respectively. The exact three cases also fail under C/libpq in
`/tmp/phase5-loader-codes-timing-c.xml`; the Rust repeat passes reconnect but
fails the other two. The scheduler case opens no database connection. This is
comparative evidence of host timing sensitivity, not permission to mark the
failed full run green or change its tolerances. All other synchronous families
pass, and experimental async remains separately classified at `505/620`.
The strict local reporter fails its zero-regression gate. Supported CI at
`2ec7e030` now passes all 57 jobs in workflow
[`35406514048`](https://github.com/martijnberger/ferrocopg/actions/runs/35406514048).

#### Fetch and COPY follow-ups

These are development measurements, not revision-frozen acceptance evidence.
The fetch-cast slice updates the async source and generated synchronous cursor.
Its 10,000-query profile reduces `typing._type_check` calls from 10,076 to 76.
Paired prepared-query medians are mixed (`59.21 -> 60.01` and
`59.12 -> 56.89` microseconds), so no stable latency improvement is claimed.
The complete `/tmp/phase5-fetch-casts-bench/report.json` still fails five C
limits: parameterized `1.388`, prepared `1.678`, namedtuple rows `1.266`,
transactions `1.528`, and text COPY `1.407`. Parameterized/prepared queries,
transactions, and pool cycles also fail Python parity. Passing pool's C limit
in this run does not close its Python gap or establish repeatable acceptance.

The initial COPY-borrowing wheel passes all 31 installed checks, including the
new mutable-input, bytes-subclass, empty-field, NULL, and escaping regressions.
A serial parent/candidate text COPY comparison improves from `4.007` to
`3.615 ms` per operation, with CPU time `2.187 -> 1.848 ms`. Reports are
`/tmp/phase5-before-copy-borrow-text-clean.json` and
`/tmp/phase5-copy-borrow-text.json`. Exclude the earlier contaminated baseline
that overlapped compilation. The profile's text-parser time falls from about
`0.774` to `0.462` seconds, but profiling is not an acceptance benchmark.

Those initial results precede the final iterator refinement, now committed as
`e7b008c2` above fetch-cast revision `a1391070`. The final wheel passes all 31
installed checks and all 28 Rust backend tests. Formatting, typing, and spelling
checks pass. The unfiltered C-coexistence selection passes all `3281/3281`
synchronous cases, including `111/111` COPY cases. Its six failures remain the
known experimental async type-info cases (`9/15` async); the synchronous
zero-regression reporter passes. Reports are in
`/tmp/phase5-copy-borrow-final-c-types.xml` and its `-report.json` companion.

Both serial measurement orders favor the final parser: parent/candidate text
COPY medians are `3.986 -> 3.593` and `4.054 -> 3.560 ms`, with CPU times
`2.182 -> 1.805` and `2.214 -> 1.797 ms`. Reports are
`/tmp/phase5-{before-,}copy-borrow-final-text-{a,b}.json`. No builds, installs,
or test workers overlapped these comparisons.

The full `/tmp/phase5-copy-borrow-final-bench/report.json` still fails five C
limits: parameterized `1.439`, prepared `1.256`, namedtuple rows `1.341`,
transactions `1.754`, and text COPY `1.284`. Parameterized/prepared queries,
transactions, and pool cycles also miss Python parity. Text COPY is improved,
not accepted; do not round its ratio down to the `1.25` limit. These local
measurements were made before committing and retain their working-copy labels. Temporary
reports must eventually be published with the tested revision; the older
loader-resolution CI runs do not cover this code.

The complete unfiltered harness in `/tmp/phase5-copy-borrow-final-full.xml`
passes `4720/4724` synchronous cases, with four failures and no errors. Every
synchronous family except pool passes. Concurrent filling, reconnect,
check-backoff, and scheduler assertions measure `110.204`, `111.982`,
`110.193`, and `110.063 ms` against their unchanged `110/110/105/110 ms`
upper bounds. All four exact cases also fail under official C/libpq in
`/tmp/phase5-copy-borrow-final-timing-c.xml`. This supports host timing
sensitivity, but does not make the failed full run green: its strict
zero-regression reporter fails. Experimental async remains separately reported
at `505/620`, with 104 failures and 11 errors. No tolerances, manifests,
regression budgets, or pool retry behavior were changed.

The final Rust resource smoke runs `60.17` seconds with 148 samples, no reported
failures, and zero workload sessions throughout. Cleanup records 615 driver
objects, two threads, one observer socket, four file descriptors, and
66,502,656 RSS bytes. `/tmp/phase5-copy-borrow-final-soak.json` is a short smoke,
not the required three-backend sustained acceptance run.

A fresh 10,000-query profile in `/tmp/phase5-copy-borrow-final-query.pstats`
identifies query conversion and adapter/context construction as the next shared
overhead to investigate. `_convert_query_params` accounts for about `0.245 s`
cumulative time, including adaptation and query conversion. Profile timings
include instrumentation and are not unprofiled latency claims. Compare any
proposed change against the current installed wheel in both measurement orders
before retaining it; keep custom adapters, encoding, parameter OIDs, and query
lifetimes intact.

The exact-revision CI benchmark at `e7b008c2` in workflow `35408378339` misses
three C limits: prepared `1.469`, tuple rows `1.251888`, and pool `1.313`.
Prepared (`1.228`), transactions (`1.058`), and pool (`1.142`) miss Python
parity. Text COPY passes both limits in this run at Rust/Python `0.542` and
Rust/C `1.236`, and parameterized queries pass narrowly at `0.990` and `1.210`.
These do not erase the local failures or satisfy three complete passing runs.
The raw artifact is published by the workflow and downloaded locally under
`/tmp/phase5-copy-borrow-ci-benchmark/phase5-results/`.

#### Rejected packing prototype and layer diagnosis

A native helper for packing already-adapted query parameters was built and
tested, then removed. Its 32 installed checks passed, including mutable-buffer
snapshots, bytes-subclass coercion, metadata identity, callback order, and
conversion errors. It did not improve the target workloads meaningfully:
parent/prototype prepared medians were `56.57 -> 56.96` and
`56.61 -> 57.02` microseconds; parameterized medians were `61.59 -> 61.27`
and `62.49 -> 62.36`. CPU differences were similarly small. Both measurement
orders used 1,000 warmups and nine samples of 1,000 queries without concurrent
build or test workers. Reports are
`/tmp/phase5-{before-,}query-pack-{prepared,parameterized}-{a,b}.json`.
After rejecting the prototype, the source and isolated environment were
restored to the COPY implementation; all 31 retained installed checks passed.
No prototype fast path or test remains. The factory slice below supersedes
that installed-wheel checkpoint.

A separate diagnostic on that restored wheel compares four execution layers
using the same prepared calculation, rotating order over nine samples of 2,000
operations. Median wall/CPU microseconds are native execution `31.17/10.85`,
session adapter `33.69/12.91`, backend cursor `50.63/29.95`, and public cursor
`55.38/34.65`. This local result suggests the next substantial opportunity is
in query/cursor adapter work, not packing the final parameter list alone.
The lower layers deliberately omit public-API work: their timings are not
acceptance workloads or permission to bypass that behavior. The diagnostic
script and raw report are `/tmp/phase5-query-layers.py` and
`/tmp/phase5-query-layers.json`; retain the unchanged public benchmark suite.

#### Row-factory initialization follow-up

Revision `c07e4c26` avoids constructing a discarded fallback conversion closure
when native result loading only needs row-factory initialization. It retains
ordinary execution's eager factory callbacks and pipeline execution's lazy
initialization. The installed regression checks metadata across repeated
queries, callback counts, native loading without the fallback wrapper, and
factory exception identity and timing.

The staged release wheel passes all 32 installed checks. The unfiltered
C-coexistence selection passes `3340/3340` synchronous cases; six known
experimental async type-info failures remain separately classified (`9/15`
async). Reports are `/tmp/phase5-factory-init-c-types.xml` and its
`-report.json` companion. Formatting, typing, spelling, and CI lint pass.

Serial parent/candidate measurements in both orders use 1,000 warmups and
nine samples of 1,000 queries. Prepared medians are `56.13 -> 56.07` and
`57.85 -> 57.24` microseconds; parameterized medians are `65.48 -> 61.06`
and `62.63 -> 61.99`. These are modest, variable development results, not a
stable percentage improvement. Raw reports are
`/tmp/phase5-{before-,}factory-init-{prepared,parameterized}-{a,b}.json`.

The complete `/tmp/phase5-factory-init-bench/report.json`, labeled
`factory-init-working-copy`, fails four C limits: parameterized `1.879`,
transactions `1.641`, text COPY `1.262`, and pool `1.783`. Four workloads also
miss Python parity: parameterized `1.325`, prepared `1.215`, transactions
`1.335`, and pool `1.175`. All three row workloads pass in this run; that is
not repeated final-candidate acceptance and must not be combined with passes
from other reports.

The completed full local harness in `/tmp/phase5-factory-init-full.xml` passes
`4722/4724` synchronous cases, with two failures and no errors. Check-backoff
and scheduler first intervals are `110.157` and `110.099 ms`, exceeding their
unchanged `105` and `110 ms` upper bounds. All other synchronous families pass.
The strict reporter fails its zero-regression gate; the classified report is
`/tmp/phase5-factory-init-full-report.json`. Experimental async remains
separate at `505/620`, with 104 failures and 11 errors. Both timing cases also
fail in a fresh C/libpq comparison at `110.076` and `110.097 ms`; see
`/tmp/phase5-factory-init-timing-c.xml`. This is supporting diagnosis of host
timing sensitivity, not a waiver or a successful full-run validation.

The installed Rust resource smoke runs `60.31` seconds with 147 samples and
no reported failures or surviving workload sessions. Cleanup records 615 driver
objects, two threads, one observer socket, four file descriptors, and
65,732,608 RSS bytes. `/tmp/phase5-factory-init-soak.json` records the exact
`c07e4c26` revision; it is not the required full three-backend soak.

Tests workflow `35410499984` finishes with all eight Rust lanes and the package
job passing, but the upstream Python 3.10 DNS lane fails
`tests/test_dns_srv.py::test_srv` for the weighted `_pg._tcp.bar.com` case.
The unchanged test expects one particular randomly weighted order. Lint
`35410500017` passes. No factory-revision sustained soak is recorded. The newer
SQL-scanner matrix passes all 57 jobs without altering that DNS test; retain the
original factory failure rather than retroactively marking its run green.

#### SQL-scanner follow-up and completed soaks

Revision `8561fa3d` skips quote/comment scanning for exact strings containing
no semicolon, retaining the existing scanner for all other inputs. A fresh
transaction profile measured 8,000 splitter calls at approximately `0.037 s`
before and `0.004 s` after; profiling is diagnostic, not acceptance timing.
The profiles are `/tmp/phase5-factory-init-transaction.pstats` and
`/tmp/phase5-separator-fast-transaction.pstats`.

Both serial parent/candidate measurement orders favor transactions:
`388.59 -> 378.79` and `393.00 -> 386.22` microseconds, with CPU times
`207.52 -> 198.84` and `210.74 -> 202.49`. Pool medians are mixed:
`66.99 -> 67.31` and `67.37 -> 66.27` microseconds. Each comparison uses
1,000 warmups and nine samples of 1,000 operations, without overlapping
builds or tests. Reports are
`/tmp/phase5-{before-,}separator-fast-{transaction,pool}-{a,b}.json`.

The full `/tmp/phase5-separator-fast-bench/report.json`, labeled
`separator-fast-working-copy`, fails three C limits: prepared `1.875`,
transactions `1.605`, and pool `1.489`. Parameterized `1.050`, prepared `1.485`,
transactions `1.381`, and pool `1.189` miss Python parity. Text COPY's C ratio
is `1.248831`, a narrow pass in this run only. No complete passing benchmark
or stable overall speedup is claimed.

All 33 installed checks and `3525/3525` selected synchronous C-coexistence
cases pass. Six known experimental async type-info failures remain separate.
Twelve unconditional scanner regressions are added, with no cases removed;
CI's collected-case baselines increase by exactly 12. Manifests, async totals,
thresholds, and regression budgets are unchanged. All 57 jobs in
[Tests `35413168117`](https://github.com/martijnberger/ferrocopg/actions/runs/35413168117)
and [lint `35413168119`](https://github.com/martijnberger/ferrocopg/actions/runs/35413168119)
pass at the exact revision.

The unfiltered `/tmp/phase5-separator-fast-full.xml` passes `4732/4736`
synchronous cases, with four failures and no errors; its `-report.json`
companion fails the strict gate. Concurrent filling, reconnect, check-backoff,
and scheduler fail short intervals of `110.155`, `111.271`, `110.227`, and
`110.093 ms`. The backoff log also records a `931.837 s` wall-clock interval:
preserve that anomaly without inferring its cause or treating this as an idle,
clean validation run. All four cases fail again under C/libpq in
`/tmp/phase5-separator-fast-timing-c.xml`. All other synchronous families pass;
experimental async is separately `505/620` with 104 failures and 11 errors.

The exact-revision Rust resource smoke passes after `60.33 s`, with 143 samples
and zero surviving workload sessions. Cleanup records 615 driver objects, two
threads, one observer socket, four descriptors, and 60,850,176 RSS bytes; see
`/tmp/phase5-separator-fast-soak.json`. This is not a sustained acceptance run.

The earlier loader soak in workflow `35406870159` now passes at `cc7b60e2`:
Rust/Python/C durations are `1800.06/1801.00/1800.80 s`, with
`1559/1090/1659` samples. The COPY soak in `35408378339` passes at `e7b008c2`:
durations are `1800.11/1800.01/1800.50 s`, with `1865/1364/1951` samples.
Both published reports contain no failures and zero workload sessions in every
sample. Local downloads are under `/tmp/phase5-loader-ci-soak/` and
`/tmp/phase5-copy-borrow-ci-soak/`. Their benchmark jobs fail, and neither soak
validates the later factory or SQL-scanner code. Final-candidate sustained
validation and three complete passing benchmarks remain outstanding.

#### Buffered unprepared row collection

Revision `7c740a41` adds `RowIter::collect_rows()` to the vendored synchronous
driver and uses it for already-buffered unprepared results. Previously each
row re-entered the runtime and installed a wait timer. The new method drains
the row stream in one runtime call, leaving ordinary streaming iteration and
prepared-query execution unchanged. The stream still supplies column metadata
for empty results and the final command count after completion.

The staged release wheel passes all 34 installed checks, including a new
text/binary regression for 2,048 rows, empty results, DML counts, returning
rows, and error recovery. Existing signal-handler and concurrent-close tests
also pass. All 28 Rust backend unit tests and the three targeted vendored
wait-loop tests pass. The standalone vendor suite's live tests were not
validated against its separate database setup; they failed with socket
permission errors in the sandbox. The temporary vendor lockfile was removed,
and the workspace dependency lockfile is unchanged. Formatting and typing pass.

The unfiltered C-coexistence selection passes `3525/3525` synchronous cases;
six experimental async type-info failures remain separately classified.
Reports are `/tmp/phase5-row-drain-c-types.xml` and its `-report.json` companion.
No source-harness cases, baselines, manifests, or acceptance limits change.

Paired parameterized medians are essentially flat:
`67.71 -> 67.27` and `68.07 -> 67.89` microseconds. A separate public-API
diagnostic with preparation disabled and 1,000 result rows improves from
`455.74 -> 310.46` and `465.80 -> 309.49` microseconds, with CPU medians
`318.37 -> 185.63` and `325.93 -> 185.20`. Measurements are serial in both
orders with no overlapping builds/tests. Reports are
`/tmp/phase5-{before-,}row-drain-{parameterized,unprepared-rows}-{a,b}.json`;
the diagnostic script is `/tmp/phase5-unprepared-rows.py`. The standard bulk-row
benchmarks normally prepare after warmup and do not exercise this optimization
in steady state. Do not substitute the diagnostic for those unchanged cases.

The complete `/tmp/phase5-row-drain-bench/report.json`, labeled
`row-drain-working-copy`, fails five C limits: parameterized `1.473`, prepared
`1.552`, namedtuple rows `1.277`, transactions `1.576`, and pool `1.923`.
Plaintext connect `1.032`, parameterized `1.210`, prepared `1.279`, transactions
`1.239`, and pool `1.360` miss Python parity. Text COPY's C ratio is `1.245606`,
a narrow single-run pass only. These development reports are not a frozen
candidate's three complete passing benchmark runs.

The full unfiltered `/tmp/phase5-row-drain-full.xml` passes `4736/4736`
synchronous cases with zero failures or errors. All synchronous families pass,
including `167/167` pool cases, and its `-report.json` companion passes the
strict zero-regression gate. Experimental async remains separate at `505/620`,
with 104 failures and 11 errors. Preserve earlier timing failures and C/libpq
reproductions; this one passing run does not establish that row drain fixed
host timing variability, particularly the scheduler test with no connection.

The exact-revision short resource smoke in `/tmp/phase5-row-drain-soak.json`
passes after `60.32 s`, with 137 samples and zero surviving workload sessions.
Cleanup records 615 driver objects, two threads, one observer socket, four
descriptors, and 59,817,984 RSS bytes. This is not the sustained acceptance run.

The CI benchmark at `7c740a41` in workflow `35423782206` fails four C limits:
parameterized `1.390`, prepared `1.533`, text COPY `1.271`, and pool `1.318`.
Parameterized `1.142`, prepared `1.207`, transactions `1.012`, and pool `1.057`
miss Python parity. All three bulk-row cases pass both limits in this run, but
passes cannot be combined with the local report or earlier revisions. The
workflow publishes the raw artifact, downloaded under
`/tmp/phase5-row-drain-ci-benchmark/`.

Tests `35423769875` passes all 57 jobs; lint `35423769867` passes. Phase 5
workflow `35423782206` fails its benchmark but passes the full three-backend
soak at `7c740a419503123b86f9fd6d5a9e98b7e296712c`: Rust runs for
`1800.665 s` with 1,615 samples, Python for `1800.640 s` with 1,102 samples,
and C for `1801.074 s` with 1,642 samples. All workers report no failures and
zero surviving workload sessions throughout. Raw artifacts are retained under
`/tmp/phase5-row-drain-ci-soak/`. Keep Phase 5 open until final-candidate
acceptance is complete.

#### Rejected query prototypes and timeout-state fix

The fresh installed-wheel prepared profile at `7c740a41` records 10,000
executions: native execution takes `0.374 s` of profiler time, parameter
conversion `0.183 s` cumulative, native-result transformer setup `0.086 s`,
and public/backend cursor synchronization `0.067 s`. These are profiling
measurements, not uninstrumented latency or release acceptance. The profile is
`/tmp/phase5-row-drain-prepared-live.pstats`.

Two separate rebuilt-wheel experiments were compared serially with the parent,
in both orders, without concurrent local builds or tests:

- Omitting unused prepared parameter descriptions measured parent/candidate
  `59.815/59.827 us` and `60.618/56.671 us` per operation. CPU medians were
  `38.603/38.638 us` and `38.581/34.794 us`. The first pair is flat, so the
  second pair alone does not establish a repeatable latency improvement.
- Bounded query-effect classification measured prepared parent/candidate
  `61.340/60.246 us` and `60.271/60.409 us`, with CPU
  `39.688/38.616 us` and `38.412/38.892 us`. Transaction comparisons measured
  `413.038/410.324 us` and `410.990/416.824 us`, with CPU
  `223.315/221.242 us` and `220.674/223.814 us`. The reverse-order result does
  not retain a benefit. No classification cache is retained.

Prepared comparisons use nine samples of 10,000 operations after 1,000 warmup
operations; transaction comparisons use nine samples of 1,000 after 1,000
warmups. Raw reports are `/tmp/phase5-{before-,}prepared-columns-*.json`
and `/tmp/phase5-{before-,}query-effects-{prepared,transaction}-{a,b}.json`.
Use the explicitly reinstalled parent's `confirmed-a` metadata report; the
earlier unconfirmed `a` report followed a failed install attempt. These local
diagnostics are not complete eleven-workload acceptance runs.

The new state-transition regression exposed an existing correctness bug on the
unchanged parent: after enabling and then disabling
`idle_in_transaction_session_timeout`, PostgreSQL reports `0`, but the adapter
still records an active timeout. Its settings query returns native bytes;
`str(b"0")` is not `"0"`. Revision `3a0bb3db` decodes those ASCII bytes before
comparing the setting. The retained installed regression checks repeated
encoding and timeout changes across connections, savepoint rollback, commit,
and server termination after disabling the timeout. An unrelated disconnect
must not be translated to `IdleInTransactionSessionTimeout`.

All 35 installed checks and applicable pre-commit checks pass. The full local
unfiltered harness in `/tmp/phase5-timeout-state-full.xml` passes all
`4736/4736` synchronous cases, including `167/167` pool cases, with zero failures
or errors. Its `-report.json` companion passes the strict zero-regression gate.
Experimental async remains separate at `505/620`, with 104 failures and 11
errors. No source test, manifest, baseline, or regression budget was changed.
The C-transformer coexistence selection passes `3525/3525` synchronous cases;
its six experimental async type-info failures remain separate (`9/15`). The
report and raw JUnit are `/tmp/phase5-timeout-state-c-types-report.json` and
`/tmp/phase5-timeout-state-c-types.xml`.
The exact-revision short resource smoke in `/tmp/phase5-timeout-state-soak.json`
passes after `60.03 s`, with 141 samples and zero surviving workload sessions
throughout. Cleanup records 615 driver objects, two threads, one observer
socket, four descriptors, and 66,961,408 RSS bytes. This does not satisfy the
30-minute-per-backend acceptance gate.
Its
[Tests workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35426404068)
passes all 57 jobs; its
[lint workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35426404053)
passes. This is current-revision correctness evidence, separate from the
performance acceptance failure.

The complete `/tmp/phase5-timeout-state-bench/report.json` records revision
`3a0bb3dbe58b72feca86280af1c5ff90d3873c79` and fails six C limits:
parameterized `1.443`, tuple rows `1.256`, namedtuple rows `1.329`, transactions
`1.528`, text COPY `1.263`, and pool `1.839`. Parameterized `1.327`, prepared
`1.311`, transactions `1.222`, and pool `1.286` miss Python parity. Both
connection cases, dict rows, and binary COPY pass both limits in this run.
Prepared reuse passes C but not Python; that isolated result does not imply
the timeout fix improved the query path. This is not a passing acceptance run.
Its full benchmark and sustained-soak
[workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35426660959)
has completed at the exact implementation revision: benchmark fails, full
three-backend soak passes. Rust runs for `1800.228 s` with 1,768 samples,
Python for `1801.661 s` with 1,314 samples, and C for `1800.213 s` with 1,862
samples. Every worker reports no failures and zero surviving workload sessions
throughout. Rust cleanup records 630 driver objects, one thread, one observer
socket, five descriptors, and 57,110,528 RSS bytes.

The exact-revision CI benchmark fails C comparisons for parameterized queries
`1.762`, prepared reuse `1.440`, tuple rows `1.294`, and pool `1.391`.
Parameterized `1.176`, prepared `1.110`, transactions `1.098`, and pool `1.248`
miss Python parity. Text COPY and namedtuple rows pass narrowly in this run;
they failed locally. Both artifacts are under `/tmp/phase5-timeout-state-ci/`
and published in the linked workflow. No scheduled Phase 5 execution is listed
at this checkpoint; manual completion does not satisfy the scheduled-run gate.

#### Rejected cursor-layout and result-container experiments

With the validated `3a0bb3db` installed wheel, the development-only
`/tmp/phase5-cursor-layout.py` measures median cursor construction at
`1.723 us` over nine samples of 10,000 constructions. Retaining 10,000 cursors
accounts for 17,606,376 traced bytes. Adding slots to backend cursor, context,
transformer, result-cursor, and metadata-shim objects while preserving dynamic
attributes and weak references measured `1.732 us` and 19,046,376 traced bytes.
Its prepared-query sample also regressed: parent/candidate wall medians were
`60.015/64.537 us` and CPU medians `38.634/40.958 us`. The layout prototype is
removed, not retained as an allocation optimization.

A separate prototype used immutable tuples to avoid recopying result/status
sequences for single-result cursors. Prepared-query parent/candidate wall
medians were `58.518/59.633 us` and `60.313/60.117 us`, with CPU medians
`36.136/37.558 us` and `39.004/38.515 us`. The mixed comparison does not
establish a repeatable benefit. This prototype and its implementation-specific
test are removed too; the installed wheel is restored to `3a0bb3db`.
Raw query samples are `/tmp/phase5-{before-,}slots-prepared-a.json` and
`/tmp/phase5-{before-,}result-tuples-prepared-{a,b}.json`. These are local
diagnostics, not complete benchmark or compatibility validation for either
discarded prototype. No production code or acceptance rule changes remain.

#### Rejected built-in parameter plan and live-query diagnostics

A temporary native `BuiltinParamPlan` bypassed Python dumper construction for
verified built-in adapters. In the offline binding diagnostic, parent/candidate
median fresh dumping was `2.223/1.906 us` and complete parameter conversion was
`3.770/3.450 us`. Reusing a warmed transformer regressed from `0.638` to
`0.938 us`. These were not paired measurements in both orders or a complete
eleven-workload benchmark.

Code inspection also found that bypassing transformer dumper and NULL-OID
caches could expose later registrations incorrectly. Preflighting later
parameters could change which error is raised first. No compatibility claim
is made for this prototype: it was removed, leaving no production-code change,
and the installed wheel was restored to `3a0bb3db`.

Subsequent diagnostics on the restored wheel measured the native prepared call
at about `33.675 us` when invoked directly and `34.099 us` within a public
query. Total direct/public wall medians were `35.213/61.861 us`, with CPU
medians `13.598/40.353 us`. A separate sparse public-query probe measured
cursor creation, execution, fetch, and release at approximately `3.644`,
`50.021`, `5.105`, and `1.341 us`. This differs from isolated constructor
timing; do not assume an offline microbenchmark explains the live query path.
The probes include instrumentation overhead and do not establish a performance
gain or a complete cost decomposition.

Development scripts are `/tmp/phase5-binding-layers.py`,
`/tmp/phase5-native-boundary.py`, and `/tmp/phase5-query-phases.py`. Their
temporary location and diagnostic scope do not meet the durable release-report
contract. Use these observations to select the next measured slice, not to
claim that Phase 5 has passed or to weaken its thresholds.

Two installed-wheel regressions now protect the semantics exposed by this
experiment: cached dumpers/NULL OIDs remain stable after registrations while a
fresh transformer sees them, and mixed parameters preserve left-to-right
errors and custom dumper construction/callback counts. All 37 installed checks
pass against the restored `3a0bb3db` wheel. This adds regression coverage, not
a retained binding optimization or new performance acceptance evidence.

#### Reproducible public-query diagnostics

`tools/phase5/query_profile.py` now preserves the public and phased probe in
the repository; usage and caveats are documented in
`docs/ferrocopg-performance.md`. It selects the requested installed backend,
checks every query result, alternates probe order, and records wall/CPU samples,
environment metadata, and SHA-256 hashes of installed implementation files.
It is explicitly separate from the acceptance harness. All 40 installed/harness
checks pass, including three probe tests and the two new adaptation regressions.

Six sequential local runs against the restored `3a0bb3db` Rust wheel and the
pinned official comparators each recorded nine samples of 10,000 operations,
after 1,000 warmups per probe. Public median wall/CPU times in microseconds:

| Query | Rust | Python | C |
| --- | --- | --- | --- |
| Prepared | `59.184 / 37.909` | `45.650 / 32.607` | `31.964 / 17.133` |
| Parameterized, preparation disabled | `63.726 / 36.792` | `48.258 / 30.102` | `36.259 / 16.699` |

For the separate prepared phased probe, Rust/C cursor construction measured
`3.586/1.465 us`, execute `49.961/29.555 us`, fetch `5.049/0.454 us`, and
reference release `1.280/0.740 us`. The execution phase is the largest measured
gap; fetch and cursor setup also remain material. Investigate the live execute
path's conversion, native call, and post-execution bookkeeping before choosing
the next optimization. Do not interpret these instrumented phases as an exact
decomposition of the public timings or another retained performance change.

Raw reports are `/tmp/phase5-query-diagnostic-{rust,python,c}-{prepared,parameterized}.json`.
These are development evidence, not three complete passing benchmarks. The
production implementation remains unchanged and performance acceptance remains
open. Adaptation regression commit `a9c65458` is pushed on
`martijn/phase5-query-diagnostics`; its lint run `35433180561` passes, while
Tests `35433180544` is still running at this checkpoint, with the package job
passing and an upstream PyPy/libpq failure in
`test_pipeline.py::test_errors_raised_on_commit`: rollback raises
`PipelineAborted`. Preserve that failed lane, which does not execute the Rust
backend. Do not attribute that CI run to later diagnostic-tool changes.

#### Per-session signal-wait state

Revision `0dcecac25d33f3587d9dbf0e23762a68964dda6d`, pushed on
`martijn/phase5-wait-state`, retains one signal-error slot, wait callback, and
cancellation handle per native session instead of allocating/cloning them for
every operation. The callback still runs outside the query runtime. Each
operation takes its exception while holding the session mutex, before another
operation can start. No mutable signal state is shared between connections.

The installed regression repeatedly interrupts a query while another native
operation queues on the same session. It verifies exception identity,
successful completion of the queued query, and subsequent recovery. All 41
installed/harness checks pass on both the unchanged parent and candidate;
the existing cross-connection signal-handler check also passes. All 28 Rust
backend unit tests pass. A rebuilt release wheel for the committed revision
passes the same 41 checks.

Prepared-query parent/candidate wall medians were `60.341/60.002 us` and,
in reverse order, `60.471/59.741 us`; CPU medians were `38.956/38.492 us`
and `39.072/38.349 us`. Raw samples are
`/tmp/phase5-{before-,}wait-state-prepared-{a,b}.json`. These are modest
development gains, not closure of the performance gap.

The complete local report `/tmp/phase5-wait-state-bench/report.json` fails six
C comparisons: parameterized `1.458`, prepared `1.489`, namedtuple rows `1.307`,
transactions `1.580`, text COPY `1.290`, and pool `1.323`. Parameterized `1.335`,
prepared `1.271`, transactions `1.278`, and pool `1.170` miss Python parity.
Both connection cases, tuple/dict rows, and binary COPY pass both limits in
this run. Do not combine these isolated passes with earlier reports.

The full local harness `/tmp/phase5-wait-state-full.xml` and its classified
`-report.json` pass `4734/4736` synchronous cases. `test_concurrent_filling`
and `test_check_backoff` fail timing assertions; the strict gate fails.
The separate C/libpq comparison `/tmp/phase5-wait-state-c-pool-timing.xml`
reproduces check-backoff but passes concurrent filling. Do not describe both
failures as reproduced under C or waive either. An isolated candidate repeat
(`/tmp/phase5-wait-state-rust-pool-timing.xml`) fails both assertions again.
Both also reproduce on unchanged parent `3a0bb3db`, rebuilt in a separate `jj`
workspace, in `/tmp/phase5-wait-state-parent-pool-timing.xml`. This supports a
pre-existing timing classification, not a passing candidate strict gate.
Experimental async remains separately
`505/620`. The C-coexistence report `/tmp/phase5-wait-state-c-types-report.json`
passes `3525/3525` synchronous cases, with six experimental async failures.

The 60-second resource smoke `/tmp/phase5-wait-state-soak.json` records
`60.044 s`, 145 samples, no failures, and zero workload sessions throughout.
Cleanup records 615 driver objects, two threads, one observer socket, four
descriptors, and 66,486,272 RSS bytes. This is not full-duration soak evidence.

Exact-revision lint `35434124339` passes. Tests `35434124284` is unfinished;
14 jobs have completed with no failures at this snapshot, not a green full
matrix. The full benchmark/soak
[workflow](https://github.com/martijnberger/ferrocopg/actions/runs/35434146166)
has completed its benchmark with an acceptance failure; the soak is still
running. All 41 installed/harness checks pass in the benchmark job.

The CI benchmark misses C limits for prepared queries (`1.516`), tuple rows
(`1.264`), text COPY (`1.632`), and pool (`1.426`). It misses Python parity for
parameterized queries (`1.146`), prepared queries (`1.217`), transactions
(`1.088`), and pool (`1.071`). These are separate CI measurements, not updates
to the local ratios above. The
[benchmark artifact](https://github.com/martijnberger/ferrocopg/actions/runs/35434146166/artifacts/10581167962)
preserves the exact-revision reports. Neither complete run passes acceptance.

No completed candidate soak or green full matrix is claimed. Scheduled
execution remains unverified; the workflow exists on default branch `main`,
but GitHub still lists no scheduled run at this checkpoint.

#### Rejected loader-batching experiment

A temporary native helper batched result-loader construction while retaining
Python loader instances and fallback callbacks. Paired prepared-query wall
medians for parent/prototype were `59.538/59.586 us` and, in reverse order,
`60.027/59.993 us`. CPU medians were `38.150/38.280 us` and
`38.633/38.479 us`. These results are effectively flat; the helper and its
Python dispatch changes were removed before starting the result-projection
prototype. There is no retained loader-batching optimization.

Raw development samples are
`/tmp/phase5-{before-,}loader-batch-prepared-{a,b}.json`, with the parent
identified as `0dcecac25d33f3587d9dbf0e23762a68964dda6d`. These selected
workload measurements are not complete benchmarks or durable release evidence.
Do not attribute the parent's validation to the subsequent uncommitted
result-projection prototype.

Definition of done:

- Sync pooling is documented and green.
- Full-duration soaks pass the documented resource budgets without hangs;
  short smoke runs do not satisfy this gate.
- At least three complete benchmark runs meet both limits for every workload:
  Rust/Python median duration <= `1.0` and Rust/C <= `1.25`, using the same
  frozen candidate wheel on the same otherwise idle machine. Do not combine
  passing workloads from different runs or revisions.
- Published evidence identifies the tested source revision, installed packages,
  machine/server configuration, raw samples, and any failures.
- The supported synchronous compatibility and package-boundary gates remain
  green after optimization. No performance exception is assumed approved.

### Phase 6: Build and validate release wheels

Tasks:

- Adopt `abi3-py311` or document why per-interpreter wheels are required.
- Build the complete Linux, macOS, and Windows matrix.
- Run clean-environment wheel smoke tests.
- Verify PostgreSQL 14-18 with the supported matrix.
- Verify package metadata, licenses, notices, and source provenance.
- Add release reproducibility and artifact integrity checks.

Definition of done:

- Every required wheel builds and installs.
- Wheel smoke and release-critical tests pass.
- Official Psycopg coexistence and delegation pass from installed artifacts.

### Phase 7: Publish ferrocopg 0.1.0 beta

Release checklist:

- 95% sync compatibility floor passes.
- 100% release-critical suite passes.
- Performance contract passes.
- Pooling and soak tests pass.
- All required wheels pass.
- PostgreSQL 14-18 pass.
- Migration, fallback, limitations, and benchmark documentation are published.
- The vendored Psycopg revision and upstream delta are recorded.

The release remains beta while production experience is limited. A future
`1.0.0` requires a separate stability review and should target 100% of the
supported synchronous contract, not merely the `0.1.0` percentage floor.

## CI Strategy

Keep independent lanes for:

- upstream pure-Python behavior with an explicit implementation selection
- upstream C/Cython comparison behavior
- Rust-default focused and full synchronous compatibility
- experimental Rust async compatibility, reported separately
- PostgreSQL 14-18 TLS/live coverage
- official Psycopg delegation and coexistence
- sync pool integration
- wheel build and installed-wheel smoke tests
- benchmarks and scheduled soak tests

Plan-only pushes are excluded from the Tests workflow, like the existing README
and documentation exclusions, so evidence updates do not cancel an in-progress
code-validation matrix. Implementation, test, and workflow changes still trigger
the full matrix; lint remains independent.

Do not let a pass-rate job hide abnormal pytest termination. Every harness must
produce JUnit, report its denominator, and reject collection errors, crashes,
timeouts, and missing result files.

## Upstream Synchronization

Keep upstream synchronization as a recurring maintenance operation:

- fetch and review every supported Psycopg stable release
- preserve upstream commits separately from ferrocopg changes where practical
- run pure-Python, C/Cython comparison, Rust, and package-boundary tests after
  every sync
- record the vendored upstream commit in package metadata and release notes
- keep the Rust-backend delta small and documented

Do not block routine upstream synchronization on an upstreaming decision.

## Deferred Work

The following are explicitly outside the `0.1.0` release:

- a supported Rust-native async backend
- making Rust async the default
- PyPy support
- PostgreSQL versions older than 14
- musllinux wheels
- raw `PGconn` or socket emulation
- a vendored pool package
- shipping a renamed Cython accelerator
- removing upstream Cython sources from the development repository
- deciding whether to propose the backend upstream

The experimental Rust async facade remains useful for compatibility research,
but it does not affect the first release floor. Native Tokio/asyncio work
should be planned from measured scalability and cancellation evidence after
the synchronous beta is established.

## Immediate Next Actions

1. Use `3a0bb3db` as the latest complete correctness/reliability checkpoint:
   Tests `35426404068` passes all 57 jobs, lint passes, and Phase 5
   `35426660959` passes the full three-backend soak but fails its benchmark.
   Its 35 installed checks, `3525/3525` selected synchronous C-coexistence cases,
   and `4736/4736` full local synchronous cases pass. Preserve these artifacts
   against this exact revision, not a later optimization.
   Finish exact-revision validation of wait-state candidate `0dcecac2` before
   promoting it: its full local strict gate fails on two pool timing assertions,
   its local and CI performance comparisons fail, and its matrix/soak are
   unfinished. Lint passes. Preserve the completed CI benchmark artifact and
   let the existing matrix and soak finish; do not dispatch duplicate runs.
   Retain the SQL-scanner revision's failed full local report (`4732/4736`),
   four C/libpq timing reproductions, and large wall-clock anomaly without
   waiving the strict gate.
   The older loader/COPY three-backend soaks now pass; keep their artifacts
   tied to `cc7b60e2` and `e7b008c2`, not the later factory/scanner changes.
   Preserve factory CI's original random DNS-order failure despite the newer
   all-green matrix. Retain signals, cancellation recovery, concurrent close,
   custom adapters, encodings, result lifetime, and C-transformer coexistence
   coverage. Earlier local failures and the corrected per-cycle waiting
   expectation remain historical evidence, not passes for newer revisions.
   Use the full classified harness rather than filtering out `asyncio` fixture
   names when claiming synchronous coverage.
2. Reduce shared small-query overhead first: parameter adaptation, query setup,
   single-row result loading, and Python/Rust crossings affect parameterized
   queries, prepared reuse, transactions, and pool cycles. Profile the current
   direct-execution path rather than optimizing the removed worker handoff.
   Follow the next implementation slice above; parameter borrowing alone was
   effectively flat in the longer warmed comparisons, and fetch-cast removal
   has mixed latency evidence despite reducing typing work.
   The parameter-packing prototype is removed after flat paired comparisons.
   The factory and SQL-scanner slices remove measured work, but the latest
   complete wait-state benchmark still misses six C and four Python limits.
   Row drain has a strong separate unprepared bulk-row diagnostic, but does
   not close the single-row gaps or replace the standard prepared row cases.
   The metadata-only, query-effect cache, compact-layout, and immutable-result
   prototypes are removed after flat, mixed, or worse comparisons. The broader
   built-in parameter plan is also removed: modest offline fresh-conversion
   gains do not justify slower transformer reuse or cache/error-order changes.
   Follow the live-query investigation above rather than repeating these slices;
   retain the new cache, NULL-OID, and callback-order regressions when evaluating
   another binding shortcut. Preserve diagnostic samples with revision/wheel identity.
   Loader batching is also removed after flat paired measurements. Evaluate
   the uncommitted native result projection using the explicit retain-or-remove
   steps above; do not count it as completed or validated work.
   Do not add another isolated conversion helper without a measured gain.
   Keep each change independently tested and compare rebuilt release wheels
   against both official baselines on the same machine.
3. Address remaining text COPY failures next; revisit bulk result loading when
   complete comparisons show it is still a blocker. Use separate measured slices.
   The temporary-vector and redundant tuple-factory costs have been removed
   locally; inline offsets and COPY borrowing now have green supported matrices.
   Modest paired gains and narrowly passing tuple or text COPY runs do not
   close the bulk-row gate. Profile the remaining row allocation
   and conversion costs. Preserve custom row factories, loader exceptions,
   NULL/empty values, encoding behavior, and result lifetime after connection close.
   Recheck all eleven workloads after each slice. Binary COPY passes the first
   loader-resolution run but failed the second inline-offset run, so it still
   needs repeated final-candidate validation. Connection setup has also crossed
   the Python-parity boundary between runs; keep both plaintext and TLS cases
   in acceptance.
4. Keep the README aligned as optimizations land. It now removes obsolete
   Phase 4 limitations and distinguishes source-tree use, staged `ferrocopg`
   installation, official libpq/async delegation, and the experimental Rust
   async facade. Retain the fork's purpose, synchronous-first scope,
   no-silent-fallback rule, and undecided upstreaming status.
5. Freeze and record the optimized candidate, rebuild its installed release
   wheel, and run at least three complete benchmarks on the same otherwise
   idle machine. Every workload must pass Rust/Python <= `1.0` and Rust/C
   <= `1.25` in every run. Run the full 30-minute soak for each backend on that
   same candidate and rerun the supported compatibility and package-boundary
   matrix. Publish raw results, failures, revision, and environment metadata;
   temporary local paths alone are not durable release evidence.
6. Confirm a scheduled reliability run separately from push/manual coverage.
   No scheduled Phase 5 run is listed in GitHub at this checkpoint; configuration
   alone does not satisfy this requirement.
   Backend-only changes do not trigger the current Phase 5 push path filter,
   so explicitly dispatch acceptance when needed and verify the tested revision.
   Mark Phase 5 complete only when all its gates pass on the final candidate;
   earlier sustained soaks and short smokes remain supporting evidence only.
7. Proceed to Phase 6's full wheel matrix, then Phase 7's release checklist.
   Keep Rust as the development default now, but do not publish to PyPI before
   all release gates pass. Do not expand into Rust-native async or a pool fork
   to avoid the remaining synchronous blockers. An upstream proposal remains
   a separate decision.
