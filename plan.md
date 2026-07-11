# ferrocopg Rust Backend Plan

## Goal

Build and prove a Rust-native PostgreSQL backend inside this Psycopg fork.
The project starts from Psycopg's trusted Python API, adaptation system, row
factories, and mature test suite instead of inventing a new database-adapter
surface.

The near-term objective is an explicit, useful `ferrocopg` backend that can
coexist with Psycopg's existing pure-Python and Cython/libpq paths. The main
test suite now runs against that backend through the Milestone 3.1
compatibility harness and is the compatibility contract alongside focused Rust
and bootstrap tests.

Whether this work should eventually be proposed upstream, remain a separate
fork, become an additional packaged backend, or replace an existing
accelerated path is deliberately undecided. Those are evidence-based product
and maintenance decisions, not prerequisites for continuing the port.

## Summary

This repository is no longer in the bootstrap stage described by the original
plan.

The following foundation work is already in place:

- `uv` is the documented Python workflow.
- `maturin` is wired for the Rust extension package.
- A pinned Rust toolchain is present.
- A Cargo workspace exists.
- The optional Rust path is already integrated into several Python seams.
- A reusable Rust-native backend session is exposed through
  `psycopg.connect(..., impl="ferrocopg")`.
- The backend has a rustls transport, Psycopg parameter adaptation, typed
  results, row factories, and normal transaction handling.
- The main test suite is executable under `--impl=ferrocopg` with a declarative
  gap manifest and CI pass-rate check.

The next phase should optimize for making the optional Rust backend more
useful and measurable. No default-path or upstreaming decision is required to
close the remaining compatibility gaps.

## Current Priority Re-evaluation (2026-07-11)

The upstream rebase and a fresh compatibility audit changed the immediate
ordering of the work. The implementation milestones remain valid, but feature
work should not proceed ahead of CI and harness reliability.

Current evidence:

- Focused validation is healthy: pre-commit passes, the Rust workspace has 22
  passing tests, and the DSN-backed bootstrap suite has 179 passing tests with
  7 expected accelerator-dependent skips.
- The post-rebase locked `uv` sync exposed an unsatisfiable docs toolchain on
  Python 3.10/3.11 because the newest docs tools require newer Python versions.
  Version-marked docs dependencies correct the resolution in this
  re-evaluation; confirmation from the normal CI workflows remains Priority 0.
- A deterministic full `--impl=ferrocopg` run now completes and writes JUnit.
  The local Python 3.13/PostgreSQL 17 run measured `3113/4326` (`0.720`)
  non-manifested cases and supports a conservative `0.65` compatibility floor.
- A deterministic 20-failure slice stopped entirely in
  `tests/test_cursor_client.py`. The concrete `ClientCursor` path expects a
  connection lock and a full libpq `PGconn` transaction/status surface that
  the ferrocopg adapter intentionally does not provide.
- Async and concrete-cursor families are now hard-skipped where executing them
  cannot measure the sync adapter. Fixture-aware classification remains the
  preferred rule for new tests.

The resulting priority order is:

### Priority 0: Restore a green upstream-sync baseline

- Keep the universal `uv.lock` resolvable for every supported Python version.
- Require the normal lint and test workflows to reach backend validation after
  every upstream sync.
- Treat a red default workflow as a release blocker for further feature work.

### Priority 1: Make the compatibility harness trustworthy

- Make the harness complete deterministically without crashing or hanging and
  always emit its JUnit report.
- Classify known concrete-cursor incompatibilities before setting the floor.
  Crash-prone tests should be skipped until the path is safe to execute; normal
  compatibility gaps should remain non-strict xfails.
- Hard-skip files dedicated to unavailable async connection APIs, and use
  fixture- or behavior-based classification for mixed sync/async files so
  unrelated passing tests are not reported as gap XPASS noise.
- Split or shard the harness if needed so a single unsafe family cannot erase
  the compatibility result for the rest of the suite.
- Record a nonzero floor only after the same denominator and result complete
  reliably in local runs and CI.

### Priority 2: Build the backend-neutral cursor foundation

- Psycopg's existing concrete cursor classes remain a documented libpq-only
  boundary. Ferrocopg-specific custom cursors subclass `NoTlsCursorAdapter`.
- Do not grow `_PgconnEncodingShim` into a fake libpq connection merely to
  satisfy tests. Prefer a small explicit execution protocol shared by the
  ferrocopg cursor and any concrete cursor classes that are intentionally
  supported.
- `_exec_command` is implemented on the backend-native session protocol and
  now carries transaction/savepoint command-fixture coverage. It is the
  foundation for server cursors and richer COPY behavior.
- Use the stabilized harness to measure this slice before moving to another
  feature family.

### Priority 3: Close production-readiness evidence

- Confirm the new SSL-enabled PostgreSQL 14-18 matrix on remote CI. It covers
  all sslmodes, custom roots, required channel binding, and client-certificate
  authentication.
- Continue diagnostics/notices and COPY/server-cursor closure in the order
  indicated by measured compatibility impact.
- Keep async, TPC, and exact pipeline work behind the sync contract unless a
  product requirement changes that ordering.

## Coexistence Policy

During backend development, `ferrocopg` must support three coexisting
implementation modes:

- the existing Cython/C accelerated path in `psycopg_c/`
- the pure Python path in `psycopg/`
- the Rust-backed path exposed through `ferrocopg_rust`,
  `psycopg._ferrocopg`, and the public opt-in connection selector

This coexistence is not a temporary accident. It is part of the development
strategy.

The plan assumes that:

- the Cython/C path remains available as the stable baseline until Rust cutover
  gates are met
- the pure Python path remains available as the portability and fallback path
- the Rust path grows behind explicit selectors, helper seams, and backend
  adapters until it is a credible supported alternative

No implementation should be removed merely because another one exists. Removal
only happens after the replacement has explicit parity evidence and the
fallback story is clear.

## Non-goals For The Next Phase

- Redesigning the public Python connection or cursor APIs.
- Replacing `_cmodule.py` as the default implementation selector yet.
- Deleting Cython before Rust parity gates are met.
- Treating the current `ferrocopg_rust` module as a final public API.
- Deciding now whether ferrocopg will be proposed upstream.
- Assuming that proving an opt-in backend necessarily requires replacing
  Psycopg's existing implementations.

## Current Architecture Summary

The repository now has four main implementation areas:

- `psycopg/`
  The main Python package, compatibility surface, and pure Python fallback.
- `psycopg_c/`
  The existing Cython/C accelerated implementation, still the primary optional
  optimized path.
- `crates/ferrocopg-python/`
  The PyO3 Rust extension package exposed as `ferrocopg_rust`.
- `crates/ferrocopg-postgres/`
  The Rust-native backend/session crate based on the `rust-postgres`
  ecosystem.

The current Python integration points for the optional Rust path include:

- `psycopg/psycopg/_rmodule.py`
  Optional import boundary for `ferrocopg_rust`.
- `psycopg/psycopg/_ferrocopg.py`
  Transitional helper access to the Rust path.
- `psycopg/psycopg/_copy_base.py`
  Rust-backed COPY formatting/parsing helpers when available.
- `psycopg/psycopg/waiting.py`
  Rust-backed `wait_c` when available.
- `psycopg/psycopg/generators.py`
  Rust-backed generator helpers when available.
- `psycopg/psycopg/_transformer.py`
  Rust-backed transformer selection when C is absent.
- `psycopg/psycopg/types/*`
  Rust-backed helpers for selected adaptation paths.

This means the Rust backend is already functional and the plan should focus on
parity, CI enforcement, and useful opt-in operation.

Operationally, this means the repository should continue to support:

- Python-only execution without Rust or Cython acceleration
- Cython/C acceleration where `psycopg_c` is installed and selected
- Rust-backed helpers and backend flows where `ferrocopg_rust` is installed
  and selected

## Guiding Principles

1. Preserve the current Python API until the Rust port is stable.
2. Use the existing test suite as the compatibility contract.
3. Keep the Python, Cython/C, and Rust implementations simultaneously usable
   during backend development.
4. Finish parity behind the optional Rust path before changing defaults.
5. Keep cutover gates explicit and evidence-based.
6. Keep upstreaming, packaging, default selection, and implementation removal
   as separate decisions.

## Desired End State

The required end state for the current program is:

- Rust is a supported, explicitly selectable backend built on
  `rust-postgres`, not a libpq wrapper.
- Common synchronous Psycopg workflows behave compatibly on that backend.
- Compatibility is measured by the existing Psycopg test suite and enforced
  in CI.
- Unsupported behavior has an explicit error and a documented
  `impl="libpq"` alternative.
- The contributor workflow uses `uv` as the standard Python workflow.
- The repository uses a pinned Rust toolchain.
- The Python-facing Psycopg behavior remains compatible.

A later cutover may make Rust the primary accelerated path and remove Cython,
but only after a separate packaging and maintenance decision. Milestones 5 and
6 describe that conditional route; they are not assumptions about the purpose
of this fork.

## Migration Tracks

Backend development should proceed on two explicit tracks.

### Track A: Optional Rust helper parity

This track finishes the optional Rust seams already wired into Python.

Scope includes:

- COPY row formatting and parsing
- `wait_c`
- generator helpers such as `connect`, `cancel`, `send`, `fetch`,
  `fetch_many`, `execute`, and `pipeline_communicate`
- `Transformer`
- accelerated adaptation helpers currently exposed through selected
  `psycopg.types.*` modules

Expected result:

- The optional Rust path is behaviorally interchangeable with the current
  Python/Cython helper seams for covered scenarios.
- The test suite can validate the Rust helpers side by side with Python and
  Cython implementations instead of replacing either one prematurely.

### Track B: Rust-native backend session parity

This track continues the internal backend work currently exposed through
`psycopg._ferrocopg` and `ferrocopg_rust`.

Scope includes:

- connection planning and target parsing
- connect/query/describe/execute flows
- prepared statements
- transaction control
- cancellation
- COPY in/out
- LISTEN/NOTIFY

Expected result:

- The backend session API is sufficiently complete and tested to support a
  future integration into the main execution path.

## Backend Equivalency Matrix

This table tracks the intended coexistence and parity story across the three
implementation modes.

| Capability | Pure Python | Cython/C | ferrocopg | Notes |
| --- | --- | --- | --- | --- |
| Conninfo parsing and connect planning | Available | Available | Available | `ferrocopg-postgres` now exposes explicit connect target planning. |
| Session bootstrap and connect | Available | Available | Available | The opt-in backend supports plaintext and rustls-backed sessions. |
| TLS transport | Available | Available | Available | rustls connections and all libpq-style sslmodes are implemented; the PostgreSQL 14-18 CI matrix covers custom roots, required channel binding, and client-certificate authentication. |
| Simple query execution | Available | Available | Available | Covered through backend simple-query facades and adapter tests. |
| Parameterized query execution | Available | Available | Available | The public adapter uses Psycopg's `%s`/`%t`/`%b` query conversion and dumper maps, including positional and mapping parameters, typed OIDs, text/binary formats, and prepared statements. |
| Statement describe and metadata | Available | Available | Available | Parameter and column metadata are exposed on the Rust path. |
| Prepared statements | Available | Available | Available | Rust backend has prepared statement caching and reuse. |
| Transactions and savepoints | Available | Available | Available | The opt-in adapter covers autocommit, context-manager commit/rollback, nested savepoints, rollback controls, and transaction characteristics. Two-phase commit is tracked separately. |
| Cancellation | Available | Available | Available | Explicit backend cancel handle exists with live coverage. |
| COPY in/out | Available | Available | Available | Text and binary COPY, parameters, row helpers, type pinning, generic custom writers, rowcount, descriptions, error state, and connection locking are implemented. Concrete `LibpqWriter` classes remain a libpq-only boundary. |
| LISTEN/NOTIFY | Available | Available | Available | Live backend notification coverage exists. |
| Result-set shaping | Available | Available | Available | Rust backend exposes unified result-set and simple-query result blocks; parameterized/prepared result sets now carry column OID/type metadata through to adapter cursor descriptions. |
| Python-facing error mapping | Available | Available | Available | SQLSTATE classes, all `DbError` diagnostic fields, raw non-UTF8 fields, synthetic `pgresult`/connect metadata, query context, and queued notice handlers are implemented. |
| Explicit opt-in selector | Available | Available | Available | `psycopg.connect(..., impl="ferrocopg")` and the transitional `connect_ferrocopg()` helper select the Rust backend without changing the default libpq path. |
| Cursor-like adapter bridge | Available | Available | Available | The default cursor supports text/binary execution, typed rows, row factories, streaming, cursor-local adapters, and named scrollable/withhold server cursors. Psycopg's concrete cursor classes remain a libpq-only boundary. |
| Pipeline mode | Available | Available | Available with boundary | The Rust batch API polls tokio-postgres request futures concurrently on the session runtime and preserves submission order. Exact libpq sync/aborted state-machine behavior remains a documented boundary. |
| Default-path integration | Available | Available | Opt-in | `impl="ferrocopg"` is intentionally explicit. Whether it should ever become a default is undecided and separate from backend compatibility. |
| Result typing and rows protocol | Available | Available | Available | Raw Rust result bytes and OIDs flow through Psycopg loaders, cursor descriptions, adapters, and the standard `psycopg.rows` factory protocol for text and binary execution. |
| Async connection API (`AsyncConnection`) | Available | Available | Available | `FerrocopgAsyncConnection` provides serialized thread-offload connection, cursor, transaction, pipeline, COPY, TPC, server-cursor, and notification surfaces; the main async fixture suite is included in the compatibility denominator. |

## Known ferrocopg Backend Gaps

These are intentionally explicit while the Python, Cython/C, and Rust-backed
paths coexist. They should either gain parity coverage before supported-backend
or conditional-cutover claims, or remain documented fallback boundaries.

Each gap is assigned to a gap-closure milestone or explicitly declared a
permanent boundary with a documented fallback story.

| Gap | Current behavior | Compatibility impact | Milestone |
| --- | --- | --- | --- |
| Full libpq socket access | `fileno()` raises `NotSupportedError`; ferrocopg does not expose a libpq socket. | Permanent boundary: the socket is owned by the sync wrapper's internal runtime, and exposing the raw fd would invite reads that corrupt the protocol stream. Fallback story: `notifies(timeout=...)` and notification handlers cover LISTEN loops; fd-level integrations keep `impl="libpq"`; a wakeup-only self-pipe fd is an optional future nicety. | Boundary |
| Concrete cursor classes | The default ferrocopg cursor handles typed text/binary results. Psycopg's concrete cursor classes require libpq-specific locks and `PGconn` state, so injecting one now fails immediately with `NotSupportedError`; backend-specific custom cursors may subclass `NoTlsCursorAdapter`. | Documented boundary: use the default/custom ferrocopg cursor for Rust connections or `impl="libpq"` when a concrete Psycopg cursor class is required. | Boundary |
| Concrete libpq COPY writers | `LibpqWriter` and `QueuedLibpqWriter` require a real `PGconn`; ferrocopg instead handles its byte pipe directly and supports generic writer objects. | Documented boundary: use the default COPY writer or a generic custom writer with ferrocopg; use `impl="libpq"` for the concrete libpq writer classes. | Boundary |
| Exact libpq pipeline state machine | Ferrocopg pipelines queued batches but does not emulate `PQpipelineSync` or produce `PIPELINE_ABORTED` results. | Documented boundary: normal batching and ordered results are supported; applications depending on exact libpq sync/abort transitions keep `impl="libpq"`. | Boundary |

## Compatibility Contract Harness

The main test suite in `tests/` is the real compatibility contract. The
Milestone 3.1 harness routes its connection fixtures through the ferrocopg
facade so the suite can run against the Rust adapter without changing the
default libpq jobs.

Design:

- A `--impl {libpq,ferrocopg}` pytest option in `tests/fix_db.py`
  (`pytest_addoption`), defaulting from a `PSYCOPG_TEST_IMPL` environment
  variable. This is deliberately distinct from `PSYCOPG_IMPL`, which selects
  the pq wrapper and rejects `ferrocopg` by design.
- The switch point is the session-scoped `conn_cls` fixture: under
  `--impl=ferrocopg` it returns a `FerrocopgConnection` facade class (in
  `psycopg/psycopg/_ferrocopg.py`) whose `connect()` classmethod matches
  `psycopg.Connection.connect`'s signature and defaults. This also fixes the
  current default drift where `connect_ferrocopg()` defaults
  `autocommit=True` while psycopg defaults to `False`.
- Fixtures that are inherently libpq-level auto-skip under ferrocopg: the raw
  `pgconn` fixture, `aconn_cls`/`aconn` (until the async adapter exists), and
  `tests/pq/` is deselected wholesale. Combining `--pq-trace`/`--pq-debug`
  with `--impl=ferrocopg` is a `pytest.UsageError`.
- The `tests/fix_ferrocopg.py` plugin applies markers from a declarative
  manifest (`tests/ferrocopg_manifest.toml`) mapping test node-id globs to
  reason tags: `tls`, `async`, `pgconn`, `server-cursor`, `tpc`, `notice`,
  `pipeline`, `copy-options`, and `fileno`. Tags tied to a
  gap-closure milestone use `xfail(strict=False)` so fixes surface as XPASS
  and shrink the manifest; only permanent boundaries use hard `skip`.
- The `commands` fixture monkeypatches `conn._exec_command`, which the
  adapter lacks; those tests stay manifested until `_exec_command` lands
  (Milestone 3.5 needs it for server-side cursors anyway).
- Pass-rate ratchet: a CI step in the `ferrocopg-rust` job runs
  `pytest tests --impl=ferrocopg` with a JUnit report; a small script
  computes the pass rate over non-manifested tests and compares it against a
  committed floor file. CI fails if the rate regresses below the floor; the
  floor is raised manually as gaps close.

The harness and CI plumbing are active. A complete deterministic run measured
`3113/4326` (`0.720`) non-manifested cases; the committed `0.65` floor leaves
cross-version headroom while protecting the current compatibility baseline.

The conditional cutover gate derived from this harness: the floor reaches 100%
of non-manifested tests (sync and async) and the manifest contains only the
documented permanent-boundary tags.

## Milestones

### Milestone 0: Rebaseline the backend contract

Objective:
Rewrite the plan and milestone language around the current repository state.

Tasks:

- Mark toolchain/bootstrap work as complete.
- Record which Python seams already support the optional Rust path.
- Record which backend session capabilities already exist.
- Define cutover gates before any default-path change.

Definition of done:

- The plan reflects reality instead of future bootstrap intent.
- The next slices are framed around parity and cutover readiness.

### Milestone 1: Finish optional Rust helper parity

Objective:
Close the remaining parity gaps in the helper-level Rust path.

Tasks:

- Finish parity for COPY helpers.
- Finish parity for `wait_c`.
- Finish parity for generator helpers.
- Finish parity for `Transformer` and selected adaptation fast paths.
- Keep Python and Cython fallbacks intact where Rust is absent.

Definition of done:

- Focused helper-parity tests pass with the Rust path enabled.
- Python behavior remains unchanged when Rust is absent.

### Milestone 2: Finish backend session parity

Objective:
Continue the Rust-native backend session until the core behavior contract is
covered.

Tasks:

- Complete and harden session APIs for query, parameter binding, describe,
  prepare, execute, transactions, cancel, COPY, and notify flows.
- Preserve expected Python-facing error mapping and encoding behavior.
- Keep the backend session path optional and isolated from default execution.
- Keep the backend work compatible with continued coexistence of the pure
  Python and Cython/C implementations.

Definition of done:

- DSN-backed backend tests pass for the session contract.
- Known unsupported cases are documented explicitly.

### Milestone 3: Add CI enforcement for the Rust path

Objective:
Make the Rust path part of normal repository validation.

Tasks:

- Install the Rust extension path in CI.
- Run focused `ferrocopg` parity tests in CI.
- Run Rust crate tests in CI.
- Keep this coverage independent from the future default-path cutover.

Definition of done:

- CI fails if the optional Rust path regresses.
- Rust-specific tests are not documentation-only anymore.

Current status:

- The dedicated `ferrocopg-rust` GitHub Actions job installs the Rust extension
  with `maturin`, runs the focused `tests/test_ferrocopg_bootstrap.py` parity
  suite against a real PostgreSQL service, and exercises both Rust crates with
  `cargo test`.
- The job is intentionally independent from the default Python/Cython test
  matrix so Rust regressions are visible without changing default-path
  behavior.

### Milestone 3.1: Compatibility harness

Objective:
Make the main test suite executable against the ferrocopg adapter and turn
compatibility into a measured number instead of a claim.

Tasks:

- Add the `--impl` / `PSYCOPG_TEST_IMPL` switch to `tests/fix_db.py`.
- Add the `FerrocopgConnection` facade class with a
  `psycopg.Connection.connect`-compatible classmethod (adopting psycopg's
  `autocommit=False` default).
- Add `tests/fix_ferrocopg.py` and the `tests/ferrocopg_manifest.toml`
  xfail/skip manifest.
- Auto-skip libpq-level fixtures (`pgconn`, `aconn_cls`, `tests/pq/`, trace
  and debug options).
- Add the pass-rate script and ratchet step to the `ferrocopg-rust` CI job.

Definition of done:

- `pytest tests --impl=ferrocopg` completes without collection errors.
- A baseline pass rate is recorded and the CI ratchet enforces it.

Current status:

- The selector, fixtures, manifest, JUnit report, pass-rate script, and CI job
  are implemented and the full suite executes against ferrocopg.
- Concrete cursor and async families are classified as hard skips because they
  currently exercise libpq-only or unavailable adapter paths.
- Whole-handshake timeout tests are explicitly skipped: rust-postgres applies
  `connect_timeout` to socket establishment and otherwise blocks forever when
  a peer accepts TCP but never begins the PostgreSQL handshake.
- A full deterministic run completed in 320 seconds and measured `3113/4326`
  (`0.720`) non-manifested cases. The committed `0.65` floor makes the harness
  a real regression ratchet with cross-version headroom.

### Milestone 3.2: TLS via rustls

Objective:
Support TLS-backed connections with a pure-Rust stack so production DSNs can
use the ferrocopg path.

Tasks:

- Add `tokio-postgres-rustls` (rustls 0.23) to `ferrocopg-postgres`.
- Map libpq `sslmode` semantics: `disable`/`prefer`/`require` onto the
  tokio-postgres `SslMode`; `allow` as a two-attempt bootstrap (plaintext
  first, then TLS); `require` with a no-verification verifier to match libpq
  semantics; `verify-ca` with a chain-checking, hostname-skipping verifier;
  `verify-full` as stock rustls verification.
- Support `sslrootcert` (including `system` via `rustls-native-certs`) and
  `sslcert`/`sslkey` client certificates.
- Support `channel_binding` (tokio-postgres-rustls provides
  tls-server-end-point).

Definition of done:

- All six sslmodes plus root/client certificate options are covered by
  bootstrap tests against an SSL-enabled PostgreSQL service in CI.
- The `tls` tag is removed from the compatibility manifest.

Current status:

- `rustls`, `tokio-postgres-rustls`, native root loading, sslmode routing, and
  TLS-backed session construction are implemented.
- The ferrocopg PostgreSQL 14-18 workflow now starts an SSL-enabled service
  with an ephemeral CA, server certificate, and client certificate.
- Live tests cover all six sslmodes, custom-root verification, required channel
  binding, rejection without a required client certificate, and successful
  client-certificate authentication. The obsolete `tls` manifest tag is
  removed; remote matrix confirmation remains before release claims.

### Milestone 3.3: Result adaptation and rows protocol

Objective:
Return properly typed Python values and adopt psycopg's real row-factory
protocol, closing the largest compatibility gap.

Tasks:

- Plumb raw column bytes (binary wire format) plus OIDs out of
  `ferrocopg-postgres` via a passthrough `FromSql` capture.
- Feed them through psycopg's `Transformer`/loader pipeline keyed on column
  OIDs, for both text and binary loaders.
- Adopt the `psycopg.rows` factory protocol and drop the adapter-local
  `(columns, row)` contract.
- Expose an `adapters` context on the adapter connection.
- Support `execute(..., binary=True)` by selecting binary loaders.

Definition of done:

- `tests/test_adapt.py` and `tests/types/` are largely green under
  `--impl=ferrocopg`.
- The `binary` and `adapters` tags are removed from the manifest.

Current status: complete for the adaptation-and-rows scope.

- Raw result bytes and OIDs are captured by Rust and loaded through Psycopg's
  adapter maps.
- Standard `psycopg.rows` factories, connection/cursor-local adapters,
  Psycopg query parameter dumping, and default-cursor text/binary execution
  are implemented and covered.
- Remaining failures involving Psycopg's concrete cursor classes, streaming
  variants, and server cursors are cursor-bridge work tracked in Milestone
  3.5, not missing result typing.

### Milestone 3.4: Diagnostics and notices

Objective:
Bring error diagnostics and notice handling to parity.

Tasks:

- Map the full tokio-postgres `DbError` field set (severity, detail, hint,
  position, internal query/position, schema, table, column, datatype,
  constraint, file, line, routine) into `psycopg.errors.Diagnostic`.
- Register a `notice_callback` on the sync client that pushes notices into a
  queue on the session; the adapter drains it after each operation and
  dispatches to `add_notice_handler` callbacks. Never call into Python from
  the Rust callback thread.

Definition of done:

- `tests/test_errors.py` and the notice-handler tests are green under
  `--impl=ferrocopg`; the `notice` tag is removed.
- The equivalency matrix error-mapping row moves to Available.

Current status: complete.

- Every plaintext and TLS session installs a Rust-only notice callback that
  queues owned diagnostic data. The Python adapter drains the queue after
  operations and isolates exceptions raised by user notice handlers.
- SQLSTATE classes, the full `DbError` field set, query context, DB-API error
  aliases, synthetic `pgresult`, and failed-connect metadata are implemented.
- The exact locked tokio-postgres source is patched locally to retain raw
  protocol diagnostic fields, allowing Psycopg to decode errors and notices
  with the active client encoding instead of accepting lossy UTF-8 conversion.
- Focused unit and live PostgreSQL notice/diagnostic tests pass, including
  non-UTF8 error and notice text.

### Milestone 3.5: Cursor and COPY completeness

Objective:
Close the cursor-mode and COPY-option gaps.

Tasks:

- Keep `_exec_command` on the adapter connection as the internal SQL command
  channel and extend its small result protocol only when server-cursor behavior
  needs it.
- Keep Psycopg's concrete `Cursor`, `RawCursor`, and `ClientCursor` classes as
  a documented libpq-only boundary. Reject them at connect time rather than
  emulating a full `PGconn` through an expanding compatibility shim.
- Close the related streaming and per-call format override failures for the
  intentionally supported cursor surface.
- Implement server-side, scrollable, and withhold cursors with
  `DECLARE`/`FETCH`/`MOVE`/`CLOSE`, mirroring `psycopg/server_cursor.py`.
- Implement COPY options, binary COPY, and custom writers, reusing
  psycopg's COPY statement building and the Track A binary row helpers over
  rust-postgres's raw byte COPY endpoints.

Definition of done:

- `tests/test_server_cursor.py` and `tests/test_copy.py` (sync) are green
  under `--impl=ferrocopg`; the `server-cursor` and `copy-options` tags are
  removed.

Current status: complete for the backend-supported cursor and COPY surface.

- Named, scrollable, and withhold cursors use backend-native
  `DECLARE`/`FETCH`/`MOVE`/`CLOSE` commands and have focused live coverage.
- Sync COPY passes the upstream module for backend-supported behavior,
  including binary format, parameters, type pinning, generic writers,
  descriptions, error state, and locking. Only concrete libpq writer classes
  are manifested under their permanent boundary.
- The former `server-cursor` and `copy-options` gap tags are removed. Upstream
  tests that directly instantiate concrete `ServerCursor`/`RawServerCursor`
  remain under the existing concrete-cursor boundary.

### Milestone 3.6: TPC and pipeline

Objective:
Cover two-phase commit and define the pipeline parity boundary.

Tasks:

- Implement TPC via `PREPARE TRANSACTION`, `COMMIT PREPARED`, and
  `ROLLBACK PREPARED`, reusing `psycopg._tpc.Xid` verbatim and implementing
  `tpc_recover()` over `pg_prepared_xacts`.
- Build an async batch API on the crate runtime behind the pipeline adapter
  so queued statements actually pipeline on the wire, with stop-on-first-error
  batch semantics.
- Document the residual delta from exact libpq
  `PQpipelineSync`/`PIPELINE_ABORTED` state-machine behavior as a boundary.

Definition of done:

- `tests/test_tpc.py` is green under `--impl=ferrocopg`; the `tpc` tag is
  removed.
- Pipeline-marked tests pass or the residual delta is documented and
  manifested as a boundary.

Current status: complete.

- TPC begin, prepare, one- and two-phase commit/rollback, recovery, Xid
  encoding, cancellation rules, and state transitions are implemented; the
  sync upstream TPC module is green and the `tpc` gap tag is removed.
- The exact locked `postgres` wrapper is patched with a batch method that
  polls tokio-postgres simple-query futures concurrently on its existing
  runtime, preserving the active session and submission order.
- Exact `PQpipelineSync`/`PIPELINE_ABORTED` behavior is documented and
  manifested as a permanent protocol boundary.

### Milestone 3.7: Async adapter

Objective:
Provide the `AsyncConnection`-equivalent ferrocopg path so the async half of
the suite counts toward the compatibility metric.

Tasks:

- Decide the mechanism: pyo3-async-runtimes bridging of tokio futures to
  asyncio, or a thread-offload adapter as a stopgap.
- Implement the async facade and enable the `aconn_cls` fixture switch.

Definition of done:

- The async suite is included in the pass-rate denominator under
  `--impl=ferrocopg`.

Current status: implementation complete; PostgreSQL 14-18 matrix validation is
in progress.

- `FerrocopgAsyncConnection` uses the planned serialized thread-offload
  mechanism and exposes async connection, cursor, transaction, pipeline,
  COPY, TPC, named-cursor, cancellation, and notification operations.
- `aconn_cls` now selects this facade and the blanket main-async manifest rule
  is removed, so main async tests count toward the compatibility denominator.
- Concrete Psycopg async cursor classes, pool injection, and exact libpq
  pipeline behavior remain separately documented boundaries rather than
  being hidden by an `async` gap tag.

### Milestone 4: Define conditional cutover readiness

Objective:
If the project decides to consider changing the default or replacing another
implementation, create explicit measured criteria for that move. Completing
the optional backend does not itself make this decision.

Cutover gates (replacing the earlier prose gates):

- The compatibility-harness pass-rate floor reaches 100% of non-manifested
  tests, sync and async included.
- The manifest contains only the documented permanent-boundary tags
  (`fileno`, trace, `tests/pq/`).
- Packaging readiness (Milestone 5) is demonstrated in CI.

Routing decision (settled):
Unsupported features fail with hard errors plus documented boundaries; there
is no transparent fallback. Per-feature fallback to libpq is incoherent
mid-session (a server cursor or TPC fallback would need the same connection),
and connect-time fallback would silently return a different object type with
different semantics. The coexistence policy itself — the libpq path staying
installed and selectable — is the fallback: unsupported features raise
`NotSupportedError` with a message naming `impl="libpq"`. This converts the
"Unsupported feature routing" gate from Blocked into a closable checklist:
each gap is either implemented (Milestones 3.2–3.7) or on the
permanent-boundary list with a documented fallback story.

Tasks:

- Decide whether a default-path cutover is desirable at all and whether an
  upstream proposal is in scope.
- Decide whether cutover happens through compatibility naming or selector
  expansion.
- Define the coexistence period explicitly, including which selectors or
  packaging combinations continue to expose Python, Cython/C, and Rust paths.

Definition of done:

- There is a written, test-backed cutover contract measured by the harness
  ratchet.
- No one needs to infer readiness from momentum alone.

Current status:

- The current safe operating model remains explicit opt-in through
  `psycopg.connect_ferrocopg(...)` or `psycopg.connect(..., impl="ferrocopg")`.
- No upstreaming or default-cutover decision has been made.
- The compatibility floor is `0.65`, calibrated from a measured `0.720` local
  run, so regressions in the supported sync surface are now enforceable.

### Milestone 5: Conditional packaging cutover

Objective:
If a distribution or default-path decision is made, make Rust a supported
packaged backend or accelerated build path.

Tasks:

- Replace Cython-first accelerated packaging with Rust-first packaging.
- Build a maturin wheel matrix for `crates/ferrocopg-python`:
  manylinux_2_28 and musllinux for x86_64/aarch64, macOS arm64 and x86_64,
  Windows x64.
- Adopt PyO3 `abi3` (for example `abi3-py310`) to collapse per-Python-version
  wheels; nothing in the extension appears to need version-specific ABI.
- Decide the `psycopg[binary]`/`psycopg_binary` relationship: the ferrocopg
  wheel needs no libpq at all (rust-postgres speaks the wire protocol
  natively), so it can supersede the libpq-bundling binary wheel story.
- Execute the naming decision from Compatibility Decisions item 2.
- Update wheel build jobs and contributor docs (`uv` plus `maturin develop`).
- Remove Cython from dev/build requirements once no longer needed.

Definition of done:

- Wheels build in CI from Rust sources without Cython or libpq for the
  accelerated path.
- CI no longer depends on Cython to build the accelerated path.

### Milestone 6: Conditional Cython removal

Objective:
Remove the old implementation only if a future cutover explicitly chooses to
replace it. A successful standalone or upstreamed ferrocopg backend does not
require this milestone.

Precondition:

- At least one released coexistence version has shipped after the Milestone 4
  gates pass, so downstream users have a version where both paths work.

Tasks:

- Delete `.pyx`, `.pxd`, generated C files, and Cython-specific build code.
- Remove dead compatibility shims and docs.
- Update repository docs to describe the Rust-based accelerated path.

Definition of done:

- There is no Cython left in the repository.
- The repository and CI pass without Cython installed.

## Compatibility Decisions

These do not need to block the current optional-path work.

1. Upstream relationship
   Decide whether to propose the backend upstream, maintain the fork, or ship
   it as a separate package only after compatibility and maintenance evidence
   exists.

2. Naming and distribution
   Keep transitional names (`ferrocopg_rust`, `psycopg._ferrocopg`) for now,
   then decide when and where `ferrocopg` branding becomes the primary package
   or import surface.

3. Cutover mechanics
   Decide whether Rust becomes selectable through the current implementation
   selector, becomes an additional selectable backend, or replaces the current
   accelerated path outright after the coexistence period.

4. PyPy support
   Decide whether Rust acceleration remains CPython-only at first, with Python
   fallback on PyPy.

5. Backend scope
   Decide whether pipeline behavior is required for backend cutover or remains
   on a later milestone with explicit fallback behavior.

## Test Plan

Backend development should use layered validation instead of a single
“it builds” gate.

Required validation buckets:

- helper parity tests for COPY, waiting, generators, transformer, and selected
  type helpers
- DSN-backed backend tests for connect/query/prepare/transaction/cancel/COPY/
  notify behavior
- the main suite under the compatibility harness
  (`pytest tests --impl=ferrocopg`) with the pass-rate ratchet as the
  compatibility metric
- Rust crate tests for backend internals
- existing Python API tests to ensure behavior does not regress when Rust is
  absent
- selector and packaging tests to ensure Python, Cython/C, and Rust modes can
  coexist without import-path or runtime conflicts

Minimum CI coverage for the Rust path should include:

- `uv sync --dev --group rust --locked`
- `uv run maturin develop --manifest-path crates/ferrocopg-python/Cargo.toml`
- `uv run pytest tests/test_ferrocopg_bootstrap.py -q`
- `uv run pytest tests --impl=ferrocopg` plus the pass-rate ratchet check
- `cargo test -p ferrocopg-postgres`
- `cargo test -p ferrocopg-python --lib`

## Conditional Cutover Gates

These gates apply only if a default-path or implementation-removal proposal is
made. No such change should happen until all of the following are true:

- The optional Rust helper path is green in CI.
- The backend session live tests are green against a real PostgreSQL DSN.
- The compatibility-harness pass rate is at its 100% floor for non-manifested
  tests (sync and async), and the manifest contains only permanent-boundary
  tags.
- Error mapping, encoding behavior, cancel semantics, COPY semantics, and
  notify behavior have explicit parity coverage.
- The coexistence story is proven: Python-only, Cython/C, and Rust-backed
  modes all still work as intended under supported selectors and packaging
  layouts.
- The fallback story is documented for unsupported or deferred features
  (hard errors naming `impl="libpq"`; no transparent fallback).
- Packaging and contributor workflow are ready for a Rust-first path.

## Cutover Readiness Contract

The Rust backend can only move closer to the default path when each gate has
both implementation coverage and CI evidence.

| Gate | Required evidence | Current status |
| --- | --- | --- |
| Selector coexistence | Python-only, Cython/C, and ferrocopg selectors can be installed and imported in the same checkout without shadowing each other. | Available in the source workspace; explicit connection selectors are tested and default behavior remains unchanged. |
| Backend live contract | DSN-backed tests cover connect, query, parameters, describe, prepare, transactions, cancel, COPY, notify, and error mapping against real PostgreSQL. | Covered by `test_ferrocopg_bootstrap.py` across PostgreSQL 14-18, including TLS certificates, diagnostics/notices, binary COPY, named cursors, TPC, pipeline batches, and the async facade. |
| Helper seam parity | COPY helpers, waiting, generators, transformer, and selected type helpers behave equivalently to Python/Cython seams when Rust is present. | In progress; focused side-by-side tests exist for many helper seams. |
| Unsupported feature routing | Every gap in the gaps table is either implemented (Milestones 3.2–3.7) or on the permanent-boundary list with a documented fallback story; unsupported features raise hard errors naming `impl="libpq"`. | Phase 3 implementation complete; remaining entries are explicit libpq-only boundaries with no transparent mid-session fallback. |
| Error and diagnostic compatibility | SQLSTATE classes, diagnostic fields, and user-facing exception types match Psycopg expectations for common server and connection errors. | Available, including raw non-UTF8 diagnostic fields, SQLSTATE subclasses, structured constraint metadata, notices, and fatal-connection state. |
| Packaging readiness | Rust wheels build through maturin without relying on Cython for the accelerated path, while Python fallback remains usable. | Not started for default cutover; maturin is wired for the optional extension only. |
| CI default-path safety | Existing Python and Cython/C jobs stay green while Rust-path jobs fail independently on Rust regressions. | The dedicated `ferrocopg-rust` job is separate from the default matrix and enforces the measured `0.65` pass-rate floor. |

## Success Criteria

The current backend program is successful when all of the following are true:

- The Rust backend is useful for documented synchronous and asynchronous
  Psycopg workflows.
- Rust-backed behavior passes a meaningful, ratcheted compatibility contract.
- Unsupported features and libpq-specific boundaries are explicit.
- `uv` is the standard contributor workflow.
- `maturin` is the standard extension build path.
- A pinned Rust toolchain is part of the repository.
- CI validates the Rust-backed implementation.
- The Python-facing Psycopg API remains compatible.

Removing Cython is a separate success criterion only if the conditional
cutover milestones are activated.

## Recommended Next Actions

1. Close Phase 3 validation by confirming the PostgreSQL 14-18 matrix and its
   recalculated compatibility rate after the main async suite entered the
   denominator. Ratchet `ferrocopg_pass_rate.txt` to the measured floor.
2. Begin Milestone 4 only as a conditional-readiness exercise: decide whether
   the remaining concrete libpq boundaries are acceptable for a supported
   optional backend. This does not imply changing Psycopg's default.
3. Prioritize packaging evidence from Milestone 5: build and smoke-test wheels
   for supported Python/platform combinations and document installation
   without a source checkout.
4. Add performance and cancellation stress measurements for the thread-offload
   async facade and pipelined batch path before considering a native
   pyo3-async-runtimes bridge.
5. Keep upstreaming deliberately undecided. First establish a stable optional
   backend release and collect user feedback on the API and permanent
   boundaries.
