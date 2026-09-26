# Publishing interrupted operation state

The frozen `1bab4336` wheel retains native transaction status when a signal
exception replaces a database error, but its public Python shell only updates
state for database exceptions and KeyboardInterrupt. A custom RuntimeError
signal handler therefore leaves an interrupted explicit transaction reported as
INTRANS although cancellation completed and the server reported INERROR.

The follow-up publishes the operation-owned status on the error path, after
existing diagnostic handling and before notice/notification callbacks. It does
not mutate the signal exception, replace its identity, perform another query,
or read a connection-global status from a later operation. The successful-query
path is unchanged. Cancellation policy is unchanged.

## Controls and results

The subprocess control waits for the target backend to enter PgSleep before
signalling it. It checks autocommit and transactional execution, KeyboardInterrupt
and a custom RuntimeError, exception identity, visible state, notice-callback
state, and rollback/recovery. On the frozen predecessor, only Rust's custom
exception inside a transaction fails the corrected control (INTRANS vs INERROR).

The initial control incorrectly assumed official Psycopg cancels arbitrary
signal exceptions. Official C and pure Python instead leave these operations
ACTIVE; the control now explicitly asserts that behavior and closes those
connections. Rust's established native boundary cancels/drains any raised signal
exception. This is not evidence of identical arbitrary-signal cancellation
semantics; it proves accurate state publication for Rust's actual completed
operation, and the shared KeyboardInterrupt behavior. Both control versions and
the original failure are preserved.

- Installed release wheel: all 163 Phase 5 checks pass with official C selected.
- New subprocess control also passes with official pure Python selected.
- Focused source/bootstrap checks: 448 pass, 30 skip in 30.28 seconds.
- Configured mypy: all 239 source files pass; Ruff and codespell pass.
- No Rust production changes, timing, full compatibility matrix or soak claims.

An initial partial-directory mypy invocation failed to resolve the sibling pool
package. Running the repository's configured full invocation passes; both logs
are retained. The comparator environment is isolated from the frozen predecessor.

Release wheel SHA-256:
`703a53605a39ab1571995d8013071fca6e65ea5dc0d9cc8645bca396ce982525`.

[Raw build/test logs](2026-09-26-signal-state-local-logs.tar.gz), SHA-256:
`c7c76221117cf135d3166fb6881900efb11e18ae987244b789d6504f22ab4550`.

## Pending evidence

Main `1bab4336` Lint `36232554202` passes. Its Tests `36232554216` and
Phase 5 `36232554264` remain pending/live. Predecessor Tests `36230544349`
is terminal cancelled after main promotion, not a complete passing matrix.
Predecessor Phase 5 `36230544336` still has a live soak and a successful benchmark
job. Dedicated comparison `36231026807` is still running. Development Tests
`36231014162` has 16 completed and 41 queued/live jobs, without failures at the
latest check. Keep these handles; do not dispatch replacement comparisons or
push evidence-only updates over their active compatibility runs.

The new correctness follow-up is not the candidate running in those workflows.
Final exact-candidate gates and the remaining integration assessment stay open.
