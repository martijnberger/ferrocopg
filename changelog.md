# Changelog

## ferrocopg-m4.3-sync-compat.1 - 2026-07-13

- Completed the synchronous compatibility phase with a `0.95` release floor;
  the PostgreSQL 14-18 and CPython 3.11-3.14 matrix reports `0.995-0.996`.
- Made the standalone Rust-default `ferrocopg` wheel libpq-free while retaining
  explicit delegation to an installed official Psycopg package.
- Closed synchronous prepared statement, COPY, pipeline, concurrency,
  connection-attempt, cancellation, notification, and official pool gaps.
- Added deterministic executed-case accounting and committed per-matrix
  baselines so collection or manifest drift cannot hide compatibility changes.
- This is a source and GitHub milestone prerelease. PyPI publication remains
  blocked on the soak, performance, release-wheel, and final release gates.

## ferrocopg-m3.3-row-factory-protocol.1 - 2026-07-06

- Added a `pgresult` shim on the ferrocopg cursor so psycopg row factories can
  inspect result metadata.
- Made the cursor fetch path support real `psycopg.rows` row factories while
  preserving the transitional ferrocopg-local row factories.
- Added focused coverage for tuple, dict, namedtuple, class, and scalar
  psycopg row factories on the Rust-backed adapter path.

## ferrocopg-m3.2-verify-ca.1 - 2026-07-06

- Added a custom rustls verifier for libpq `sslmode=verify-ca` semantics:
  certificate-chain validation is enforced while hostname verification is
  intentionally skipped.
- Kept `sslmode=verify-full` on stock rustls hostname-verifying behavior.
- Updated TLS connect-plan hints and PyO3-facing tests to describe the
  `verify-ca` behavior explicitly.

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
