# Native extended-query outcome checks

Development verification only. No timing, full compatibility matrix, or soak
acceptance is claimed for these changed native artifacts.

The vendored row stream retains the command tag and ReadyForQuery status of its
own extended operation. The synchronous prepared collector returns these with
the rows and affected count in one runtime entry. The session transfers the tag
into its owned result; the binding exposes read-only metadata. Simple-query
fallbacks leave this metadata absent. Error/notice ownership, native preparation
integration, and public result/status publication remain unfinished.

The new installed control exercises prepared/unprepared execution and both
result formats. It checks empty queries, typed parameters, leading comments,
WITH/INSERT RETURNING, empty SELECT results, DDL, savepoints, recovery, and COMMIT
on an aborted transaction returning ROLLBACK. Earlier metadata remains unchanged
after subsequent operations and connection close.

## Verification

- Rust 1.94.1, CPython 3.14.6, local PostgreSQL 15, UTF8 database `phase5`.
- `maturin build --release --locked` succeeds with the workspace release profile.
- Wheel: `ferrocopg-0.1.0-cp314-cp314-macosx_11_0_arm64.whl`.
- SHA-256: `172cefce503708523aaf7030d8361a9f57a8938e30a6f5dc407123820257b756`.
- Installed `unittest discover -s tools/phase5 -p 'test_*.py' -v`: 132 passed
  in 3.597 seconds, including the expanded outcome control.
- `cargo +1.94.1 test --locked -p ferrocopg-python -p ferrocopg-postgres --lib`:
  28 backend tests and four preparation tests passed.
- Ruff check/format, workspace cargo fmt, and explicit Rust 2024 formatting of
  all three changed vendored files pass.

The initial sandboxed checks could not open local sockets. Re-running with
loopback access passed; the permission failures are not classified as successful
test runs. An initial formatting check used the wrong edition for the vendored
crates; both manifests specify 2024, which the final check uses.

Local raw logs are `/tmp/phase5-native-outcome-final-tests.log` and
`/tmp/phase5-native-outcome-final-rust-tests.log`. The wheel is in
`/tmp/phase5-native-outcome-wheels/`. These temporary paths are development aids,
not durable release-acceptance evidence.
