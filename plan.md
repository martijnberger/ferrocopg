# ferrocopg Migration Plan

## Goal

Turn this fork into `ferrocopg`: a Psycopg-compatible PostgreSQL adapter for
Python with no Cython left in the tree.

The current objective remains the same: preserve Psycopg's Python-facing API
while replacing the current Cython/C acceleration and transport layers with
Rust, using the existing test suite as the compatibility contract.

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
| Parameterized query execution | Available | Available | Available | Bound text execution is present on the Rust backend, including typed `date`, `time`, `timestamp`, `timestamptz`, `interval`, and `uuid` parameter coercion on the optional ferrocopg path. |
| Statement describe and metadata | Available | Available | Available | Parameter and column metadata are exposed on the Rust path. |
| Prepared statements | Available | Available | Available | Rust backend has prepared statement caching and reuse. |
| Transactions and savepoints | Available | Available | Partial | Backend transactions exist and the opt-in adapter now covers savepoints, autocommit, context-manager commit/rollback, and transaction characteristics, but it is still not the default path. |
| Cancellation | Available | Available | Available | Explicit backend cancel handle exists with live coverage. |
| COPY in/out | Available | Available | Available | Backend COPY facades exist and are exercised in focused tests; text row helpers now honor the ferrocopg connection encoding instead of assuming UTF-8. |
| LISTEN/NOTIFY | Available | Available | Available | Live backend notification coverage exists. |
| Result-set shaping | Available | Available | Available | Rust backend exposes unified result-set and simple-query result blocks. |
| Python-facing error mapping | Available | Available | Partial | Rust backend now maps SQLSTATE-backed server errors through `psycopg.errors.lookup()`, carries basic `diag` fields such as SQLSTATE and primary message, maps unsupported no-TLS conninfo to `NotSupportedError`, backend-local bad parameters to `ProgrammingError`, and closed/connect failures to `OperationalError`; remaining work is richer connection/result metadata where psycopg exposes it. |
| Explicit opt-in selector | Available | Available | In progress | `psycopg.connect_ferrocopg(...)` can opt into the Rust adapter with row factories, cursor-factory selection, prepare-threshold handling, autocommit control, a connection `info` surface, pipeline context support, LISTEN/NOTIFY helpers including notification iteration and notify handlers, connection cancellation helpers, and transaction characteristics both at connect time and after connect; unsupported normal-connect features now fail explicitly instead of silently drifting from the default implementation contract. |
| Cursor-like adapter bridge | Available | Available | In progress | Experimental adapter exists in `psycopg._ferrocopg`, but is not the default path. |
| Pipeline mode | Available | Available | In progress | Rust backend now has an experimental batched simple-query facade plus an explicit `connection.pipeline()` bridge on the opt-in ferrocopg path, including queued parameterized execution on the adapter side; full libpq-style pipeline semantics are still a parity gap. |
| Default-path integration | Available | Available | In progress | `psycopg.connect(..., impl="ferrocopg")` now provides a narrow top-level bridge into the explicit Rust path while the normal default remains unchanged; broader cutover still stays blocked on selector and compatibility gates. |

## Known ferrocopg Backend Gaps

These are intentionally explicit while the Python, Cython/C, and Rust-backed
paths coexist. They should either gain parity coverage before cutover or remain
documented fallback boundaries.

| Gap | Current behavior | Cutover impact |
| --- | --- | --- |
| TLS-backed connections | No-TLS bootstrap rejects TLS-required conninfo with `NotSupportedError`. | Blocks default-path cutover for normal production DSNs until TLS support or fallback routing is defined. |
| Full libpq socket access | `fileno()` raises `NotSupportedError`; ferrocopg does not expose a libpq socket. | Blocks code depending on libpq-level polling or fd integration. |
| Server-side, scrollable, withhold, and binary cursors | Cursor factory rejects these modes with `NotSupportedError`. | Can remain explicit opt-in limitations, but must be fallback-routed before default cutover. |
| Binary execution results | `execute(..., binary=True)` is rejected with `NotSupportedError`. | Blocks binary-result parity until result decoding lands. |
| COPY options | Basic text COPY in/out exists; COPY parameters, custom writers, and binary row helpers are rejected. | Text COPY can be exercised now; binary/custom COPY remains a parity gap. |
| Two-phase transactions | TPC methods raise `NotSupportedError`. | Blocks applications relying on XA/TPC semantics. |
| Notice handlers | Notice handler APIs raise `NotSupportedError`; notify handlers are supported separately. | Needs explicit fallback or implementation before broad compatibility claims. |
| Full libpq pipeline semantics | Experimental pipeline adapter queues operations, but does not expose full libpq pipeline behavior. | Must stay opt-in until semantics are proven equivalent or fallback-routed. |
| Rich pgconn/pgresult error attachment | SQLSTATE and primary diagnostics are attached; full `pgconn`/`pgresult` metadata is not. | Error class and basic diagnostics are usable, but advanced diagnostic consumers still need fallback. |

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

### Milestone 4: Define cutover readiness

Objective:
Create explicit criteria for moving Rust into the main implementation path.

Tasks:

- Define the exact behavioral gates required before touching `_cmodule.py`.
- Decide whether cutover happens through compatibility naming or selector
  expansion.
- Decide which unsupported features block cutover and which can remain on
  fallback paths.
- Define the coexistence period explicitly, including which selectors or
  packaging combinations continue to expose Python, Cython/C, and Rust paths.

Definition of done:

- There is a written, test-backed cutover contract.
- No one needs to infer readiness from momentum alone.

Current status:

- The first cutover gates are documented below, but the Rust path is not ready
  to replace the default implementation.
- The current safe operating model remains explicit opt-in through
  `psycopg.connect_ferrocopg(...)` or `psycopg.connect(..., impl="ferrocopg")`.
- Unsupported backend features must either become Rust-backed, be routed to a
  fallback implementation, or remain explicit blockers before default-path
  changes.

### Milestone 5: Packaging cutover

Objective:
Make Rust the supported accelerated build path.

Tasks:

- Replace Cython-first accelerated packaging with Rust-first packaging.
- Update wheel build jobs and contributor docs.
- Remove Cython from dev/build requirements once no longer needed.

Definition of done:

- Wheels build from Rust sources instead of Cython.
- CI no longer depends on Cython to build the accelerated path.

### Milestone 6: Delete Cython

Objective:
Remove the old implementation only after cutover is complete.

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
- Rust crate tests for backend internals
- existing Python API tests to ensure behavior does not regress when Rust is
  absent
- selector and packaging tests to ensure Python, Cython/C, and Rust modes can
  coexist without import-path or runtime conflicts

Minimum CI coverage for the Rust path should include:

- `uv sync --dev --group rust --locked`
- `uv run maturin develop --manifest-path crates/ferrocopg-python/Cargo.toml`
- `uv run pytest tests/test_ferrocopg_bootstrap.py -q`
- `cargo test -p ferrocopg-postgres`
- `cargo test -p ferrocopg-python --lib`

## Cutover Gates

No default-path change should happen until all of the following are true:

- The optional Rust helper path is green in CI.
- The backend session live tests are green against a real PostgreSQL DSN.
- Error mapping, encoding behavior, cancel semantics, COPY semantics, and
  notify behavior have explicit parity coverage.
- The coexistence story is proven: Python-only, Cython/C, and Rust-backed
  modes all still work as intended under supported selectors and packaging
  layouts.
- The fallback story is documented for unsupported or deferred features.
- Packaging and contributor workflow are ready for a Rust-first path.

## Cutover Readiness Contract

The Rust backend can only move closer to the default path when each gate has
both implementation coverage and CI evidence.

| Gate | Required evidence | Current status |
| --- | --- | --- |
| Selector coexistence | Python-only, Cython/C, and ferrocopg selectors can be installed and imported in the same checkout without shadowing each other. | In progress; explicit ferrocopg selectors exist, focused tests cover selector isolation, and default behavior remains unchanged. |
| Backend live contract | DSN-backed tests cover connect, query, parameters, describe, prepare, transactions, cancel, COPY, notify, and error mapping against real PostgreSQL. | In progress; focused `test_ferrocopg_bootstrap.py` coverage exists and is now CI-enforced for the Rust path. |
| Helper seam parity | COPY helpers, waiting, generators, transformer, and selected type helpers behave equivalently to Python/Cython seams when Rust is present. | In progress; focused side-by-side tests exist for many helper seams. |
| Unsupported feature routing | TLS, socket access, binary result mode, server-side cursors, notice handlers, TPC, and full pipeline semantics have either Rust support or an explicit fallback route. | Blocked; these are still documented ferrocopg backend gaps. |
| Error and diagnostic compatibility | SQLSTATE classes, diagnostic fields, and user-facing exception types match Psycopg expectations for common server and connection errors. | Partial; SQLSTATE and primary diagnostics are attached, richer pgconn/pgresult metadata remains. |
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

1. Update `plan.md` to reflect the repository's current state.
2. Finish the remaining optional Rust helper parity work.
3. Expand backend session coverage and DSN-backed tests.
4. Add CI enforcement for the Rust path.
5. Define explicit cutover gates before changing the default implementation
   path.
