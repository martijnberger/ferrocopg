# ferrocopg Development and Compatibility Workflow

This repository keeps Psycopg's upstream Python and Cython implementations as
trusted baselines while developing a Rust-native PostgreSQL backend in the
same tree. The backend speaks PostgreSQL through the `rust-postgres` ecosystem;
it is not a Rust wrapper around libpq.

The Rust work has two main packages:

- `crates/ferrocopg-postgres` implements connection planning, rustls transport,
  sessions, queries, parameters, prepared statements, transactions, COPY,
  cancellation, and notifications.
- `crates/ferrocopg-python` exposes the PyO3 extension as `ferrocopg_rust` and
  connects the Rust backend and helper fast paths to the `psycopg` package.

The public backend remains explicit and experimental. Whether it is eventually
proposed upstream, kept as a fork, or packaged separately is not decided.

## Python environment

Use `uv` for local environment management:

```bash
uv venv
source .venv/bin/activate
uv sync --dev --locked
```

The default locked environment includes the shared Python test baseline. Add
the `c` group when comparing with the Cython implementation and the `rust`
group when working on ferrocopg:

```bash
uv sync --dev --group c --group rust --locked
```

To run database-backed tests, point pytest at a working PostgreSQL database:

```bash
tools/test-db start
export PSYCOPG_TEST_DSN="$(tools/test-db dsn)"
uv run pytest --test-dsn "$PSYCOPG_TEST_DSN"
```

## Rust toolchain

The repository pins its Rust toolchain in `rust-toolchain.toml` and defines the
workspace in the root `Cargo.toml`.

Install an editable extension into the active environment with:

```bash
uv run maturin develop \
  --manifest-path crates/ferrocopg-python/Cargo.toml
```

The extension import can be smoke-tested directly:

```bash
uv run python -c "import ferrocopg_rust; print(ferrocopg_rust.milestone())"
```

## Using the backend

Prefer the per-connection selector in user-facing code:

```python
import psycopg
from psycopg.rows import dict_row

with psycopg.connect(
    "postgresql://postgres:password@localhost/postgres",
    impl="ferrocopg",
    row_factory=dict_row,
) as conn:
    print(conn.execute("select %s::int4 as answer", (42,)).fetchone())
```

Use `impl="libpq"` to select the stable backend explicitly. There is no
automatic per-feature fallback: unsupported ferrocopg behavior raises an
error because switching transport in the middle of a connection would be
incorrect.

`psycopg.connect_ferrocopg()` is a transitional direct helper. Pass
`autocommit` explicitly when using it because its bootstrap default currently
differs from `psycopg.connect()`. Do not set `PSYCOPG_IMPL=ferrocopg`; that
environment variable selects Psycopg's libpq wrapper implementation, not the
connection backend.

## Current scope

The default ferrocopg connection and cursor currently cover common synchronous
Psycopg workflows:

- plaintext and rustls-backed connections
- simple, parameterized, prepared, text, and binary execution
- Psycopg dumpers/loaders, typed results, cursor descriptions, and row factories
- transactions, savepoints, transaction characteristics, and cancellation
- text COPY in/out
- LISTEN/NOTIFY and notification handlers
- an explicit pipeline adapter

Known gaps are kept in `plan.md` and `tests/ferrocopg_manifest.toml`. The main
ones are async connections, server-cursor parity, binary/custom COPY, two-phase
transactions, notice handlers, and exact libpq pipeline semantics. Raw libpq
socket access and Psycopg's concrete cursor classes are documented backend
boundaries. A ferrocopg-specific custom cursor must subclass
`psycopg._ferrocopg.NoTlsCursorAdapter`.

## Side-by-side validation

The focused bootstrap suite compares Rust helpers with Python and Cython
implementations where applicable and exercises the live Rust backend:

```bash
uv sync --dev --group c --group rust --locked
uv run maturin develop \
  --manifest-path crates/ferrocopg-python/Cargo.toml
uv run pytest \
  --test-dsn "$PSYCOPG_TEST_DSN" \
  tests/test_ferrocopg_bootstrap.py -q
```

The adapter and row-protocol slices can be checked directly with:

```bash
uv run pytest \
  --impl=ferrocopg \
  --test-dsn "$PSYCOPG_TEST_DSN" \
  tests/test_adapt.py tests/test_rows.py -q
```

## Compatibility contract harness

The full Psycopg suite is executable against the Rust backend with:

```bash
uv run pytest \
  --impl=ferrocopg \
  --test-dsn "$PSYCOPG_TEST_DSN" \
  --randomly-dont-reorganize \
  --junitxml=ferrocopg-compat.xml \
  tests
```

`tests/fix_ferrocopg.py` applies the declarative gap manifest and
`tools/ci/ferrocopg_pass_rate.py` calculates the non-manifested pass rate. The
CI ratchet enforces a conservative `0.65` floor, calibrated from a complete
local run measuring `3113/4326` (`0.720`) non-manifested cases. Known unsafe or
inapplicable families are hard-skipped; ordinary compatibility gaps remain
visible as non-strict xfails or failures.

The sync rust-postgres driver applies `connect_timeout` to socket establishment,
not to the entire PostgreSQL handshake. Tests that deliberately accept TCP and
then stall the handshake are therefore excluded under the `handshake-timeout`
tag instead of being allowed to block the report indefinitely.

The next work should be selected from the current roadmap rather than the old
bootstrap sequence: use the measured harness to prioritize diagnostics/notices
and server-cursor/COPY parity. `_exec_command` and the SSL-enabled PostgreSQL
matrix are implemented. Async support remains a separate major milestone.
