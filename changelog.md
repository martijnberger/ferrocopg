# Changelog

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
