# Native execution operation checks

Development verification only. The ordinary native operation now exists, but
the public cursor path has not been routed through it. No performance gain,
full compatibility matrix, or soak acceptance is claimed for this prototype.

## Implementation

`BackendSyncNoTlsSession.execute_query` accepts query bytes and separate adapted
values/types/formats. It validates and copies inputs before any I/O or optional
Python transaction preflight. Preflight runs without a session mutex guard.
One native owner then selects/prepares/executes, updates the tested preparation
state, and maintains server statement IDs. It returns owned result/error/notice
data after releasing the guard. Notifications are captured only when requested.
Errors retain identity and participate in Python GC; results survive later
queries and connection close.

Policy configuration has a separate short-lived lock and immutable identity.
Native state refreshes changed policy before selection and after I/O, without
copying arbitrary-size settings on every call. Signal handlers can update policy
without acquiring the session's I/O lock. The existing cancellation/signal path
remains in use, including per-operation transfer before unlocking.

## Local Verification

- Rust 1.94.1, CPython 3.14.6, local PostgreSQL 15, UTF8 database `phase5`.
- `maturin build --release --locked` succeeds with the workspace release profile.
- Installed wheel: `ferrocopg-0.1.0-cp314-cp314-macosx_11_0_arm64.whl`.
- SHA-256: `d1f4822cf21af5ef4d361d0f153c0acc25b4b6662d6e35f56f1975a594f3ff91`.
- `unittest discover -s tools/phase5 -p 'test_*.py' -v`: 142 passed in 4.209 s.
- `cargo +1.94.1 test --locked -p ferrocopg-python -p ferrocopg-postgres --lib`:
  28 backend tests and four preparation tests passed.
- Ruff check/format, cargo fmt, and mypy's 239-source check pass.

Ten new live controls cover preparation transitions against the Python manager
and actual server statement counts; changing settings, parameter OIDs and result
formats; invalidation and failed prepare/execute cleanup; immutable results,
notices and error cycles; opt-in notifications; invalid-input preflight; mutable
buffers during I/O; transaction preflight ordering/re-entry/failure; cancellation;
and signal exception identity/notices with queued execution and policy changes.

Local raw logs: `/tmp/phase5-native-execution-final-tests.log` and
`/tmp/phase5-native-execution-final-rust-tests.log`. The wheel is in
`/tmp/phase5-native-execution-wheels/`. These temporary artifacts are development
aids, not durable release-acceptance evidence. The initial test helper accidentally
overrode unittest's `run`; it was renamed before the first live checks completed.

## Remaining Integration

Use this owner for both ordinary queries and the compatibility preparation views;
do not maintain an independent Python cache when paths mix. Public configuration
access must remain independent of the I/O lock. Remove intermediate request and
result scaffolding while preserving callback ordering and error normalization.
The current entry supports UTF8/ASCII ordinary queries. Encoding bridges,
pipeline, COPY, and client/server cursors remain on their existing path. Failed
operations do not yet carry ReadyForQuery state in an owned result. Full installed
public-path comparisons, both measurement orders, all eleven workloads, and
compatibility/resource gates remain required before retaining the redesign.
