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
source tree should switch its synchronous default to Rust immediately so
ordinary development exposes backend gaps. Publishing to PyPI remains blocked
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

Validation evidence:

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
release-critical synchronous contract is green across the supported matrix.

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

The following current gaps are not accepted as permanent release boundaries:

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

The current mixed `0.80` ratchet remains in place until a trustworthy
sync-only baseline is measured. The floor may only move upward after that
measurement.

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
- ferrocopg remains within approximately 25% of Psycopg C on core synchronous
  workloads
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

Status: next.

Tasks:

- [x] Prove official `psycopg_pool.ConnectionPool` integration.
- [ ] Define reproducible soak and benchmark commands, machine metadata, and
  acceptance thresholds.
- [ ] Add connection churn, transaction, cancellation, COPY, and pipeline soak
  tests.
- [ ] Add leak checks for Python objects, Rust sessions, sockets, and threads.
- [ ] Build the comparative libpq benchmark suite for latency, throughput,
  memory, sockets, and threads.
- [ ] Run scheduled soaks and publish reproducible results.

Definition of done:

- Sync pooling is documented and green.
- Soak tests complete without hangs or resource growth.
- Ferrocopg meets the performance contract.

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

1. Define the Phase 5 soak and benchmark commands, machine metadata, run
   duration, and pass/fail thresholds so results are reproducible.
2. Add scheduled connection-churn, transaction, cancellation, COPY, pipeline,
   and pool soaks with resource-growth assertions.
3. Establish comparative libpq baselines for latency, throughput, memory,
   sockets, and threads.
4. Use the measured results to close performance or resource-lifecycle gaps
   before beginning the Phase 6 wheel matrix.
