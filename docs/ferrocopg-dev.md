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

The product target is a separate `ferrocopg` distribution and import namespace
with the Rust backend as its synchronous default. The source tree remains
upstream-shaped; omitted synchronous `psycopg.connect()` calls now select Rust,
while upstream comparison jobs select libpq explicitly. Whether the work is
eventually proposed upstream remains undecided.

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

## Building the staged package

The release package is generated without renaming the upstream-shaped source
tree. Build a local wheel for the current platform with:

```bash
repo="$PWD"
uv run python tools/stage_ferrocopg.py /tmp/ferrocopg-stage
cd /tmp/ferrocopg-stage
"$repo/.venv/bin/maturin" build --release --out /tmp/ferrocopg-wheelhouse
```

The staging tool refuses to overwrite an existing directory. It copies the
vendored Python package to `ferrocopg`, rewrites public module identities,
nests the extension at `ferrocopg._rust._ferrocopg`, records
`UPSTREAM_REVISION`, includes the LGPL license, and emits independent `0.1.0`
metadata. The staged package uses a Rust-oriented `pq` compatibility surface,
so importing the Rust default does not probe or load a system libpq wrapper.
Generated staging output is never committed.

The wheel can coexist with official Psycopg. Rust is the synchronous default;
install the fallback extra when using `impl="libpq"` or delegated async
connections:

```bash
pip install /tmp/ferrocopg-wheelhouse/ferrocopg-*.whl
pip install "ferrocopg[libpq]"  # once consuming from an index
```

## Using the backend

Until `ferrocopg` is published, use the development source tree or build the
staged wheel above. Omitted `impl` selects Rust; the explicit spelling remains
useful in compatibility tests:

```python
import psycopg
from psycopg.rows import dict_row

with psycopg.connect(
    "postgresql://postgres:password@localhost/postgres",
    row_factory=dict_row,
) as conn:
    print(conn.execute("select %s::int4 as answer", (42,)).fetchone())
```

`PSYCOPG_SOURCE_IMPL=libpq` is a temporary source-tree escape hatch used by
upstream comparison automation. It is not part of the planned `ferrocopg`
package contract and should not be used by applications.

Use `impl="libpq"` to select the internal comparison backend explicitly. There
is no automatic per-feature fallback: unsupported ferrocopg behavior raises
an error because switching transport in the middle of a connection would be
incorrect.

The published package will instead use:

```python
import ferrocopg as psycopg

# Rust is the synchronous default.
conn = psycopg.connect(dsn)

# This delegates to an installed official Psycopg package.
libpq_conn = psycopg.connect(dsn, impl="libpq")
```

The delegated object will be an official `psycopg.Connection`, not a wrapper.
Type checkers infer `ferrocopg.RustConnection` for omitted or explicit Rust
selection; explicit libpq delegation is typed as `Any` because its concrete
class comes from the optional official distribution at runtime.
The first release supports the Rust synchronous backend; async entry points
delegate to official Psycopg. Missing Rust or fallback dependencies produce
actionable installation errors rather than silent fallback.

`psycopg.connect_ferrocopg()` is a transitional direct helper. Pass
`autocommit` explicitly when using it because its bootstrap default currently
differs from `psycopg.connect()`. Do not set `PSYCOPG_IMPL=ferrocopg`; that
environment variable selects Psycopg's libpq wrapper implementation, not the
connection backend.

## Current scope

The ferrocopg connection and cursor currently cover broad synchronous Psycopg
workflows:

- plaintext and rustls-backed connections
- simple, parameterized, prepared, text, and binary execution
- Psycopg dumpers/loaders, typed results, cursor descriptions, and row factories
- transactions, savepoints, transaction characteristics, TPC, and cancellation
- text and binary COPY, type pinning, and generic writers
- LISTEN/NOTIFY, notification handlers, and queued notice handlers
- named, scrollable, and withhold server cursors
- pipelined simple-query batches
- an experimental connection-affine thread-offload async facade

Known gaps are kept in `plan.md` and `tests/ferrocopg_manifest.toml`. The main
release work is concrete cursor parity, concrete libpq COPY writer behavior,
exact public pipeline state behavior, complete handshake timeout and multi-host
coverage, and cancellation/concurrency edges. Only raw `PGconn`, socket, and
libpq tracing access are intended release boundaries. A source-tree
ferrocopg-specific custom cursor must currently subclass
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
`tools/ci/ferrocopg_pass_rate.py` calculates non-manifested pass rates. CI
currently preserves the mixed sync/async `0.80` ratchet and reports sync and
async results separately. Each PostgreSQL matrix job uploads JSON and JUnit
artifacts containing denominators and counts for connection, transaction,
type/metadata, prepared, cursor, COPY, pipeline, notification, and concurrency
families. `tests/ferrocopg_compat_baselines.json` pins the expected sync and
async collection counts for every CI matrix key, so missing or newly collected
tests cannot silently distort the rate. On the fixed CPython 3.11 server axis,
the sync denominator is `4205` on PostgreSQL 14 and `4243` on PostgreSQL 15-18,
with `875` manifested tests in every lane. The initial sync ratchet is `0.85`,
below the validated server-axis range of `0.862`-`0.865`; the release target is
at least `0.95`.

Generate the same report locally after the harness with:

```bash
uv run python tools/ci/ferrocopg_pass_rate.py \
  ferrocopg-compat.xml \
  --manifest tests/ferrocopg_manifest.toml \
  --floor tests/ferrocopg_pass_rate.txt \
  --sync-floor tests/ferrocopg_sync_pass_rate.txt \
  --report ferrocopg-compat-report.json
```

The sync rust-postgres driver applies `connect_timeout` to socket establishment,
not to the entire PostgreSQL handshake. Tests that deliberately accept TCP and
then stall the handshake are therefore excluded under the `handshake-timeout`
tag instead of being allowed to block the report indefinitely.

The next work is selected from the measured synchronous failure families in
`plan.md`. Rust-native async remains experimental and is not part of the
`0.1.0` support contract.
