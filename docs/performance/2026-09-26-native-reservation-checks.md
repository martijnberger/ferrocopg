# Shared Native Preparation Reservations

Development checkpoint, not performance or final Phase 5 acceptance. The public
cursor route still uses the existing implementation. This work prepares the
shared native owner for ordinary execution and queued compatibility operations;
it does not introduce a second authoritative preparation cache.

## Implementation

- `reserve_execution` selects and reserves against the executor's preparation
  state. Its token owns the query/type signature and only weakly references the
  session. Normal immediate queries do not create tokens.
- `execute_query(..., reservation=...)` consumes a token once. Cross-session use
  and signature mismatch are rejected before preflight; concurrent/repeated
  consumption cannot send the same operation twice.
- Cache eviction, explicit clear, or an earlier invalidating command can retire
  the original selection. The unexecuted operation then selects again before I/O.
  This is not retrying or replaying completed SQL.
- Successful reserved operations do not increment counts a second time. Failed
  preparation/execution releases pending new names and owned server statements.
  Explicit cancellation removes an unexecuted new-name reservation, but leaves
  an existing cached statement available for other operations.
- Independent threshold/maximum properties preserve the other setting and use
  only the configuration lock. Both getters and setters remain usable by a signal
  handler while its query holds the session I/O lock.

## Verification

Rust 1.94.1, CPython 3.14.6, local PostgreSQL 15 on port 55436. The old temporary
environment had been partially cleaned, so checks used a new isolated environment
at `/tmp/phase5-reservation-env`, with the repository's pinned official comparators
from `tools/phase5/requirements.txt`. No benchmark was run during compilation or
testing. Missing Cargo dependencies were downloaded without changing the lockfile.

- `maturin build --release --locked` succeeds with the workspace release profile.
- Wheel: `ferrocopg-0.1.0-cp314-cp314-macosx_11_0_arm64.whl`.
- SHA-256: `23634114127a3746ee1e6ae2b94625b6bad9dad45cda0a11a580f18eb1badd42`.
- All 148 installed Phase 5 tests pass in 4.067 seconds.
- `cargo +1.94.1 test --locked -p ferrocopg-python -p ferrocopg-postgres --lib`
  passes all 32 native unit tests. Cargo fmt, Ruff check/format, and codespell pass.
- Six additional live tests cover mixed reference transitions/server statement
  counts, ownership/signatures/replay, failed/cancelled reservations, cache
  eviction/DDL, concurrent exactly-once execution, and independent properties.
  The existing subprocess signal test now exercises property access during I/O.

Temporary development evidence: `/tmp/phase5-reservation-final-tests.log`,
`/tmp/phase5-reservation-final-rust-tests.log`, and
`/tmp/phase5-reservation-wheels/`. These are not durable release artifacts.

## Published Parent and Remaining Work

The user-approved main checkpoint is `49ded12e2306efd2cc6e178950d849287abf0134`.
[Latest Tests](https://github.com/martijnberger/ferrocopg/actions/runs/36134035499)
reports all 57 jobs passing; [Lint](https://github.com/martijnberger/ferrocopg/actions/runs/36131379225)
passes. [Earlier Tests](https://github.com/martijnberger/ferrocopg/actions/runs/35997745155)
failed Linux Python/3.14/PostgreSQL14 and macOS Python/3.14 comparator jobs. The
later green verdict does not establish the cause of those failures.
[Phase 5](https://github.com/martijnberger/ferrocopg/actions/runs/35963923509)
reports a successful soak job and a failed benchmark job. These verdicts were
inspected through the GitHub CLI, not independently recomputed from raw reports
in this checkpoint. No performance limit or failed verdict was weakened.

Next connect the public preparation facade and ordinary query/result route to
this owner. The pipeline shell must explicitly cancel skipped reservations after
an abort, preserve callback/adaptation ordering, and never maintain an independent
Python names/IDs cache alongside native ownership. The native reservation tests
do not replace public pipeline compatibility tests. Encoding bridges, public
result/error projection, failed-operation transaction status, full supported-sync
compatibility, both-order public measurements, all eleven workload comparisons,
and final resource/soak/publication audits remain open.
