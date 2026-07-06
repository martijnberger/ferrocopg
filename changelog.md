# Changelog

## ferrocopg-m3.2-rustls-policy.1 - 2026-07-06

- Made the rustls connector policy-aware: libpq `require` keeps no-verification
  semantics while `verify-ca`/`verify-full` use trusted-root verification.
- Added TLS root/client certificate loading for the parsed `sslrootcert`,
  `sslcert`, and `sslkey` options.
- Added clean TLS configuration errors so missing certificate files surface as
  Python `OperationalError` before a socket connection is attempted.

## ferrocopg-m3.2-libpq-sslmode-plan.1 - 2026-07-06

- Added ferrocopg-owned key/value conninfo normalization for libpq TLS
  options that tokio-postgres does not parse directly.
- Made connect planning preserve all six libpq `sslmode` values while routing
  tokio-postgres through the closest supported transport mode.
- Started explicit handling for `sslrootcert`, `sslcert`, and `sslkey` as
  parsed TLS intent for the rustls follow-up work.

## ferrocopg-m3.2-rustls-require.1 - 2026-07-06

- Added the first rustls-backed Rust session path for `sslmode=require`
  connections while keeping explicit no-TLS probe/session APIs guarded.
- Split the Python adapter seam so `connect_ferrocopg()` can use the generic
  Rust backend session and no-TLS tests continue to exercise no-TLS behavior.
- Added `tokio-postgres-rustls`, `rustls`, and native-certificate plumbing as
  the starting point for Milestone 3.2 TLS parity.

## ferrocopg-m3.1-harness.1 - 2026-07-06

- Added the first compatibility-contract harness for running the main test
  suite with `--impl=ferrocopg`.
- Added a declarative ferrocopg manifest and pass-rate floor so CI can ratchet
  adapter parity without blocking on known Milestone 3.x gaps.
- Kept the existing Python and Cython/C test paths as the default while the
  Rust-backed path remains explicit opt-in.
