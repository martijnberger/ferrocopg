# Changelog

## ferrocopg-m3.1-harness.1 - 2026-07-06

- Added the first compatibility-contract harness for running the main test
  suite with `--impl=ferrocopg`.
- Added a declarative ferrocopg manifest and pass-rate floor so CI can ratchet
  adapter parity without blocking on known Milestone 3.x gaps.
- Kept the existing Python and Cython/C test paths as the default while the
  Rust-backed path remains explicit opt-in.
