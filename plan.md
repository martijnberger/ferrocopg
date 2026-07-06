# ferrocopg Migration Plan

## Goal

Turn this fork into `ferrocopg`: a Psycopg-compatible PostgreSQL adapter for
Python with no Cython left in the tree.

The current objective remains the same: preserve Psycopg's Python-facing API
while replacing the current Cython/C acceleration and transport layers with
Rust. The existing test suite becomes the compatibility contract once the
Milestone 3.1 compatibility harness can run it against the ferrocopg adapter;
until then the contract is carried by the focused bootstrap parity suite.

## Summary

This repository is no longer in the bootstrap stage described by the original
plan.

The following foundation work is already in place:

- `uv` is the documented Python workflow.
- `maturin` is wired for the Rust extension package.
- A pinned Rust toolchain is present.
- A Cargo workspace exists.
- The optional Rust path is already integrated into several Python seams.
- A Rust-native backend session API has started behind the optional
  `ferrocopg` path.

The next phase should optimize for finishing the optional Rust path safely
before attempting any default-path cutover.

## Coexistence Policy

During the migration, `ferrocopg` must support three coexisting
implementation modes:

- the existing Cython/C accelerated path in `psycopg_c/`
- the pure Python path in `psycopg/`
- the Rust-backed path exposed through `ferrocopg_rust` and
  `psycopg._ferrocopg`

This coexistence is not a temporary accident. It is part of the migration
strategy.

The plan assumes that:

- the Cython/C path remains available as the stable baseline until Rust cutover
  gates are met
- the pure Python path remains available as the portability and fallback path
- the Rust path grows behind explicit selectors, helper seams, and backend
  adapters until it is ready to become the default

No implementation should be removed merely because another one exists. Removal
only happens after the replacement has explicit parity evidence and the
fallback story is clear.

## Non-goals For The Next Phase

- Redesigning the public Python connection or cursor APIs.
- Replacing `_cmodule.py` as the default implementation selector yet.
- Deleting Cython before Rust parity gates are met.
- Treating the current `ferrocopg_rust` module as a final public API.

## Current Architecture Summary

The repository now has three active migration layers:

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

This means the migration is already underway and the plan should focus on
parity, CI enforcement, and cutover readiness.

Operationally, this means the repository should continue to support:

- Python-only execution without Rust or Cython acceleration
- Cython/C acceleration where `psycopg_c` is installed and selected
- Rust-backed helpers and backend flows where `ferrocopg_rust` is installed
  and selected

## Guiding Principles

1. Preserve the current Python API until the Rust port is stable.
2. Use the existing test suite as the migration contract.
3. Keep the Python, Cython/C, and Rust implementations simultaneously usable
   during the migration.
4. Finish parity behind the optional Rust path before changing defaults.
5. Keep cutover gates explicit and evidence-based.
6. Delete Cython only after Rust-backed behavior is passing in CI and the
   default-path transition is complete.

## Desired End State

At the end of the program:

- There are no `.pyx` or `.pxd` files left in the repository.
- There are no Cython-specific build steps left in packaging or CI.
- Rust is the supported accelerated build path.
- The contributor workflow uses `uv` as the standard Python workflow.
- The repository uses a pinned Rust toolchain.
- CI exercises and validates the Rust-backed path.
- The Python-facing Psycopg behavior remains compatible.

## Migration Tracks

The migration should proceed on two explicit tracks.

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
| Session bootstrap and connect | Available | Available | Available | Rust path is still optional and no-TLS-first. |
| Simple query execution | Available | Available | Available | Covered through backend simple-query facades and adapter tests. |
| Parameterized query execution | Available | Available | Partial | Bound text execution is present on the Rust backend, including typed `date`, `time`, `timestamp`, `timestamptz`, `interval`, and `uuid` parameter coercion on the optional ferrocopg path; mapping-style parameters raise `ProgrammingError` and result values come back as untyped text (see the result-typing gap). |
| Statement describe and metadata | Available | Available | Available | Parameter and column metadata are exposed on the Rust path. |
| Prepared statements | Available | Available | Available | Rust backend has prepared statement caching and reuse. |
| Transactions and savepoints | Available | Available | Partial | Backend transactions exist and the opt-in adapter now covers savepoints, autocommit, context-manager commit/rollback, and transaction characteristics, but it is still not the default path. |
| Cancellation | Available | Available | Available | Explicit backend cancel handle exists with live coverage. |
| COPY in/out | Available | Available | Partial | Backend COPY facades exist and are exercised in focused tests; text row helpers now honor the ferrocopg connection encoding instead of assuming UTF-8. Binary COPY, COPY parameters, and custom writers are still rejected (see the gaps table). |
| LISTEN/NOTIFY | Available | Available | Available | Live backend notification coverage exists. |
| Result-set shaping | Available | Available | Available | Rust backend exposes unified result-set and simple-query result blocks; parameterized/prepared result sets now carry column OID/type metadata through to adapter cursor descriptions. |
| Python-facing error mapping | Available | Available | Partial | Rust backend now maps SQLSTATE-backed server errors through `psycopg.errors.lookup()`, carries basic `diag` fields such as SQLSTATE and primary message, maps unsupported no-TLS conninfo to `NotSupportedError`, backend-local bad parameters to `ProgrammingError`, and closed/connect failures to `OperationalError`; remaining work is richer connection/result metadata where psycopg exposes it. |
| Explicit opt-in selector | Available | Available | In progress | `psycopg.connect_ferrocopg(...)` can opt into the Rust adapter with row factories, cursor-factory selection, prepare-threshold handling, autocommit control, a connection `info` surface, pipeline context support, LISTEN/NOTIFY helpers including notification iteration and notify handlers, connection cancellation helpers, and transaction characteristics both at connect time and after connect; unsupported normal-connect features now fail explicitly instead of silently drifting from the default implementation contract. |
| Cursor-like adapter bridge | Available | Available | In progress | Experimental adapter exists in `psycopg._ferrocopg`, but is not the default path. |
| Pipeline mode | Available | Available | In progress | Rust backend now has an experimental batched simple-query facade plus an explicit `connection.pipeline()` bridge on the opt-in ferrocopg path, including queued parameterized execution on the adapter side; full libpq-style pipeline semantics are still a parity gap. |
| Default-path integration | Available | Available | In progress | `psycopg.connect(..., impl="ferrocopg")` now provides a narrow top-level bridge into the explicit Rust path while the normal default remains unchanged; broader cutover still stays blocked on selector and compatibility gates. |
| Result typing and rows protocol | Available | Available | Missing | The adapter returns untyped text rows and uses a local row-factory contract instead of `psycopg.rows`; closing this (Milestone 3.3) is a precondition for running the main suite meaningfully. |
| Async connection API (`AsyncConnection`) | Available | Available | Missing | There is no async ferrocopg adapter; roughly half the main test suite is async, so cutover cannot claim drop-in status without it (Milestone 3.7). |

## Known ferrocopg Backend Gaps

These are intentionally explicit while the Python, Cython/C, and Rust-backed
paths coexist. They should either gain parity coverage before cutover or remain
documented fallback boundaries.

Each gap is assigned to a gap-closure milestone or explicitly declared a
permanent boundary with a documented fallback story.

| Gap | Current behavior | Cutover impact | Milestone |
| --- | --- | --- | --- |
| Untyped text results and incompatible row-factory protocol | Adapter rows are `list[str \| None]` (raw text), and the adapter defines a local `(columns, row)` row-factory contract instead of the `psycopg.rows` protocol driven by `cursor.description`. | Largest single compatibility gap: nearly every main-suite assertion fails on values, and every test passing a `psycopg.rows` factory fails. Fix by adopting `psycopg.rows` and wiring column OIDs into the `Transformer`/loader pipeline (the Rust transformer seam from Track A already exists). rust-postgres extended-protocol results arrive binary; raw bytes are extractable via a passthrough `FromSql` newtype, then psycopg loaders do the typing. | 3.3 |
| Async adapter (`AsyncConnection`) | No async ferrocopg adapter exists at all. | Roughly half the main suite is async; drop-in claims are impossible without it. tokio-postgres is natively async, so the work is bridging (pyo3-async-runtimes) or a thread-offload stopgap. | 3.7 |
| TLS-backed connections | No-TLS bootstrap rejects TLS-required conninfo with `NotSupportedError`. | Blocks default-path cutover for normal production DSNs. Decision: pure-Rust TLS via rustls. | 3.2 |
| Full libpq socket access | `fileno()` raises `NotSupportedError`; ferrocopg does not expose a libpq socket. | Permanent boundary: the socket is owned by the sync wrapper's internal runtime, and exposing the raw fd would invite reads that corrupt the protocol stream. Fallback story: `notifies(timeout=...)` and notification handlers cover LISTEN loops; fd-level integrations keep `impl="libpq"`; a wakeup-only self-pipe fd is an optional future nicety. | Boundary |
| Server-side, scrollable, and withhold cursors | Cursor factory rejects these modes with `NotSupportedError`. | Implementable adapter-side with `DECLARE`/`FETCH`/`MOVE`/`CLOSE` SQL, mirroring `psycopg/server_cursor.py`; requires an internal `_exec_command` channel. | 3.5 |
| Binary execution results | `execute(..., binary=True)` is rejected with `NotSupportedError`. | Falls out of the result-typing work almost for free: binary is rust-postgres's native wire format, so `binary=True` selects psycopg binary loaders instead of text loaders. | 3.3 |
| COPY options | Basic text COPY in/out exists; COPY parameters, custom writers, and binary row helpers are rejected. | rust-postgres COPY endpoints are raw byte pipes with no format opinion; options and binary format are SQL-level (`WITH (FORMAT binary, ...)`), reusing psycopg's statement building and the Track A binary row helpers. | 3.5 |
| Two-phase transactions | TPC methods raise `NotSupportedError`. | Implementable adapter-side via `PREPARE TRANSACTION` / `COMMIT PREPARED` / `ROLLBACK PREPARED`, reusing `psycopg._tpc.Xid` verbatim and recovering via `pg_prepared_xacts`. | 3.6 |
| Notice handlers | Notice handler APIs raise `NotSupportedError`; notify handlers are supported separately. | Sync `postgres` exposes `Config::notice_callback`; push notices into a queue drained by the adapter (same pattern as `drain_notifications`), never calling into Python from the callback thread. | 3.4 |
| Full libpq pipeline semantics | Experimental pipeline adapter queues operations, but does not expose full libpq pipeline behavior. | tokio-postgres pipelines implicitly when futures are polled concurrently; a batch API can approximate sync points and abort-on-first-error, but exact `PQpipelineSync`/`PIPELINE_ABORTED` state-machine parity is not reachable — the residual delta becomes a documented boundary. | 3.6 |
| Rich pgconn/pgresult error attachment | SQLSTATE and primary diagnostics are attached; full `pgconn`/`pgresult` metadata is not. | tokio-postgres `DbError` exposes the full diagnostic field set (severity, detail, hint, position, schema/table/column/datatype/constraint, file/line/routine); map it into `psycopg.errors.Diagnostic`. | 3.4 |

## Compatibility Contract Harness

The main test suite in `tests/` is the real compatibility contract, but today
it cannot construct ferrocopg connections: every DB test flows through the
fixtures in `tests/fix_db.py`, which build `psycopg.Connection` objects. The
harness below makes the suite runnable and measurable against the adapter.

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
- A new plugin `tests/fix_ferrocopg.py` applies markers from a declarative
  manifest (`tests/ferrocopg_manifest.toml`) mapping test node-id globs to
  reason tags: `tls`, `async`, `pgconn`, `binary`, `server-cursor`, `tpc`,
  `notice`, `pipeline`, `copy-options`, `adapters`, `fileno`. Tags tied to a
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

The cutover gate derived from this harness: the floor reaches 100% of
non-manifested tests (sync and async) and the manifest contains only the
documented permanent-boundary tags.

## Milestones

### Milestone 0: Rebaseline the migration contract

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

### Milestone 3.5: Cursor and COPY completeness

Objective:
Close the cursor-mode and COPY-option gaps.

Tasks:

- Implement `_exec_command` on the adapter connection as the internal SQL
  command channel.
- Implement server-side, scrollable, and withhold cursors with
  `DECLARE`/`FETCH`/`MOVE`/`CLOSE`, mirroring `psycopg/server_cursor.py`.
- Implement COPY options, binary COPY, and custom writers, reusing
  psycopg's COPY statement building and the Track A binary row helpers over
  rust-postgres's raw byte COPY endpoints.

Definition of done:

- `tests/test_cursor_server.py` and `tests/test_copy.py` (sync) are green
  under `--impl=ferrocopg`; the `server-cursor` and `copy-options` tags are
  removed.

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

### Milestone 4: Define cutover readiness

Objective:
Create explicit, measured criteria for moving Rust into the main
implementation path.

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

### Milestone 5: Packaging cutover

Objective:
Make Rust the supported accelerated build path.

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
- Execute the naming decision from Compatibility Decisions item 1.
- Update wheel build jobs and contributor docs (`uv` plus `maturin develop`).
- Remove Cython from dev/build requirements once no longer needed.

Definition of done:

- Wheels build in CI from Rust sources without Cython or libpq for the
  accelerated path.
- CI no longer depends on Cython to build the accelerated path.

### Milestone 6: Delete Cython

Objective:
Remove the old implementation only after cutover is complete.

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

These should be settled before default-path cutover, but they do not need to
block the current optional-path work.

1. Naming
   Keep transitional names (`ferrocopg_rust`, `psycopg._ferrocopg`) for now,
   then decide when and where `ferrocopg` branding becomes the primary package
   or import surface.

2. Cutover mechanics
   Decide whether Rust becomes selectable through the current implementation
   selector, becomes an additional selectable backend, or replaces the current
   accelerated path outright after the coexistence period.

3. PyPy support
   Decide whether Rust acceleration remains CPython-only at first, with Python
   fallback on PyPy.

4. Backend scope
   Decide whether pipeline behavior is required for backend cutover or remains
   on a later milestone with explicit fallback behavior.

## Test Plan

The migration should use layered validation instead of a single “it builds”
gate.

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
  (once Milestone 3.1 lands)
- `cargo test -p ferrocopg-postgres`
- `cargo test -p ferrocopg-python --lib`

## Cutover Gates

No default-path change should happen until all of the following are true:

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
| Selector coexistence | Python-only, Cython/C, and ferrocopg selectors can be installed and imported in the same checkout without shadowing each other. | In progress; explicit ferrocopg selectors exist, focused tests cover selector isolation, and default behavior remains unchanged. |
| Backend live contract | DSN-backed tests cover connect, query, parameters, describe, prepare, transactions, cancel, COPY, notify, and error mapping against real PostgreSQL. | In progress; focused `test_ferrocopg_bootstrap.py` live coverage exercises connect/query/params/describe/prepare/transactions/cancel/COPY/notify/error mapping and is now CI-enforced for the Rust path. |
| Helper seam parity | COPY helpers, waiting, generators, transformer, and selected type helpers behave equivalently to Python/Cython seams when Rust is present. | In progress; focused side-by-side tests exist for many helper seams. |
| Unsupported feature routing | Every gap in the gaps table is either implemented (Milestones 3.2–3.7) or on the permanent-boundary list with a documented fallback story; unsupported features raise hard errors naming `impl="libpq"`. | In progress; the routing decision is settled (hard errors plus documented boundaries, no transparent fallback), explicit unsupported-mode tests exist, and each gap now has an assigned milestone or boundary status. |
| Error and diagnostic compatibility | SQLSTATE classes, diagnostic fields, and user-facing exception types match Psycopg expectations for common server and connection errors. | Partial; SQLSTATE, primary diagnostics, and structured constraint diagnostics are covered; richer pgconn/pgresult metadata remains. |
| Packaging readiness | Rust wheels build through maturin without relying on Cython for the accelerated path, while Python fallback remains usable. | Not started for default cutover; maturin is wired for the optional extension only. |
| CI default-path safety | Existing Python and Cython/C jobs stay green while Rust-path jobs fail independently on Rust regressions. | In progress; the dedicated `ferrocopg-rust` job is separate from the default matrix. |

## Success Criteria

The migration is successful when all of the following are true:

- There is no Cython left in the repository.
- Rust-backed accelerated behavior passes the existing compatibility contract.
- `uv` is the standard contributor workflow.
- `maturin` is the standard extension build path.
- A pinned Rust toolchain is part of the repository.
- CI validates the Rust-backed implementation.
- The Python-facing Psycopg API remains compatible.

## Recommended Next Actions

1. Land the Milestone 3.1 compatibility harness so adapter compatibility
   becomes a measured pass rate instead of a claim.
2. Start Milestone 3.2 (TLS via rustls) — the single biggest blocker for
   real-world DSNs.
3. Start Milestone 3.3 (result adaptation and rows protocol) — the change
   that moves the pass rate most.
