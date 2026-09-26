# Public Native Preparation Owner

Incomplete integration checkpoint, not a retained performance candidate or a
completed Phase 5 gate. Public bound queries now execute through the native owner.
The follow-up below removes common-path request/result intermediates; performance
evaluation remains pending.

## Changes

`NoTlsConnectionAdapter` selects `_NativePreparationView` for native sessions.
Policy properties access native configuration without the I/O lock. Names/counts
are detached cold snapshots, not a second authoritative cache. The bound route
calls `execute_query`; the Python `PrepareManager`, name/ID updates, and separate
prepare/execute/maintenance calls are bypassed. Legacy/mock sessions retain their
existing implementation.

Pipeline reservations use the same owner, with cancellation of the failed or
unexecuted tail. Successful batches do not make additional per-item cancellation
calls. Errors retain normalization/error hooks and original exception identity;
notices are published from owned outcomes outside native guards. Notification
draining is deliberately deferred until after notice callbacks, which may change
handler registration. Public status messages come from the server's command tag;
native empty-query PGresult status is corrected to EMPTY_QUERY.

## Verification

Development environment: Rust 1.94.1, CPython 3.14.6, local PostgreSQL 15 on port
55436, UTF8 database `phase5`. The installed environment retains the pinned
official comparators in `tools/phase5/requirements.txt`.

- Installed Phase 5 controls: 151 pass in 4.311 seconds on the exact checkpoint
  wheel, including public shared ownership,
  detached views, policy updates, pipeline-abort cleanup, and notification-handler
  changes from notice callbacks and successful/aborted pipeline cleanup.
- Focused source suites: 239 pass/21 skip for preparation, pipeline, cursor and
  connection; 606 pass/16 skip for common/client/raw/server cursors, connection
  info and adaptation. Skips use the existing manifest/environment conditions.
- Bootstrap/legacy fallback tests: 218 pass/9 skip. A first attempt omitted the
  fixture's required literal password field and failed that assertion; the DSN
  was corrected without changing database authentication or weakening the test.
- 32 Rust unit tests, mypy's 239 sources, and Ruff pass.
- Complete local compatibility harness is terminal failure: 5,078 pass, 240 fail,
  11 errors, 1,285 skipped, 49 expected failures and 30 unexpected passes. The
  unmodified verifier reports 135 supported-sync failures and zero sync errors;
  the zero-regression gate fails even though both aggregate percentage floors pass.
- All 134 sync subprocess/tool failures report a missing `mypy` executable on
  PATH. Correcting PATH and rerunning the affected modules gives 139 passes and
  two existing skips. This does not rewrite the failed complete-run verdict.
- The remaining sync failure is pool `test_check_backoff`: the first interval is
  0.105124712 s against 0.100 +/- 0.005 s. The unchanged targeted assertion fails
  again at 0.105075121 s. A separate `--impl=libpq`, `PSYCOPG_IMPL=python` control
  also fails the same assertion. This is evidence of shared local timing behavior,
  not a diagnosed root cause or permission to relax the tolerance.
- Within the complete run, all non-manifested sync connection, cursor, COPY,
  notification, pipeline, prepared, transaction, concurrency/cancellation, and
  type/metadata families have zero failures/errors. Experimental async still has
  100 non-manifested failures and 11 errors; no async compatibility claim is made.

The snapshot test's mutation hook moved from the removed Python `prepare_bound`
call to the public native-execution entry, still before actual preparation I/O.
Both mutable-buffer forms must retain their original data, the hook must execute,
and the following query must observe the mutation. No assertions were removed.

The latest release wheel, after pipeline cleanup refinement, is in
`/tmp/phase5-public-owner-checkpoint-wheels/` with SHA-256
`64a44642b4ae22d056e3fde5e70408a1a8b91a4f9d77cde622ba0971a8fbbecb`.
Raw JUnit, failed/passing logs, control logs, and the three changed source/test
files are preserved in
[the local evidence archive](2026-09-26-public-owner-local-checks.tar.gz).
Archive SHA-256:
`a81bcd3a832d7fbc434ddf56d3f68840851999a709004972769cea3f2a5b701b`.
Temporary originals are under `/tmp/phase5-public-owner-*.log`; the full harness
process `38466` and all local build/test processes are terminal. An earlier
concurrent pytest invocation hit the repository's active-run
guard and ran no tests; the bootstrap checks were rerun after the other suite ended.

Published parent `d957b508` has passing Lint `36227989984`; Tests `36227989975`
is now complete with all 57 jobs passing. These parent runs do not validate the
new public route.

## Direct Request/Outcome Follow-Up

Common UTF8 operations with ASCII SQL now pass the existing adapted sequences
directly to Rust rather than constructing and extracting `_BoundParams` triples.
Constant queries avoid an empty request packet. Rust snapshots mutable buffers
before transaction preflight; the mutation tests now hook that preflight boundary.
Encoding bridges and legacy fallbacks retain their explicit conversion paths.

The native `BackendExecutionOutcome` supplies cursor navigation and cached public
PGresult projection directly, replacing the Python result-cursor wrapper and its
lists. Preflight exceptions preserve identity and do not run error hooks twice.
Tests cover these behaviors, cache identity, and garbage collection of cache cycles.

The release wheel SHA-256 is
`1b8b361aa0bebcc57df7affad47c83bba575ac0a2e1981d2cedfff32123dae22`.
Installed checks: 154 pass. Focused source checks: 845 pass, 37 skip. Rust unit
tests: 32 pass. Mypy checks all 239 sources; Ruff and Rust formatting checks pass.
Local logs are `/tmp/phase5-direct-outcome-final-tests.log`,
`/tmp/phase5-direct-outcome-source.log`, and
`/tmp/phase5-direct-outcome-rust-tests.log`. The earlier failed full compatibility
run remains failed and is not superseded by these focused checks.

The user approved publishing this unfinished integration to main as a revisable
checkpoint. No package release or performance acceptance is implied.

## Remaining Work

Measure this whole public boundary against the frozen pre-integration `d957b508`
wheel, using fresh/reused cursors and constant/parameterized queries in both
execution orders. This is a diagnostic control, not the accepted beta baseline.
Preserve every result and distinguish diagnostic improvements from acceptance.

Failed-operation ReadyForQuery ownership, complete final-candidate compatibility,
packaging/coexistence, both-order fresh/reused public controls, all eleven workload
comparisons, resource/soak gates, and measurement/publication audits remain open.
No performance timing or performance gain is claimed for this checkpoint.
