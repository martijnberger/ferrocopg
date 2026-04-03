# ferrocopg Bootstrap Workflow

The repository still contains the upstream Python and Cython packaging, but the
initial `ferrocopg` Rust port scaffold now lives in `crates/ferrocopg-python`.
The backend direction is now anchored on the `rust-postgres` ecosystem through
the `crates/ferrocopg-postgres` crate, with `tokio-postgres` as the intended
transport core.

## Python environment

Use `uv` for local environment management:

```bash
uv venv
source .venv/bin/activate
uv sync --dev --locked
```

The default locked `uv` environment includes the shared Python test baseline,
including optional test dependencies such as `gevent`, `shapely`, `dnspython`,
and `numpy`. Add the `c` group when you want to exercise the current
Cython-backed implementation too:

```bash
uv sync --dev --group c --locked
```

To run database-backed tests, point `pytest` at a working PostgreSQL database:

```bash
tools/test-db start
export PSYCOPG_TEST_DSN="$(tools/test-db dsn)"
uv run pytest --test-dsn "$PSYCOPG_TEST_DSN"
```

## Rust toolchain

The repository pins a Rust toolchain in `rust-toolchain.toml`.

The current workspace root is `Cargo.toml`, with the first Rust extension
package defined in `crates/ferrocopg-python`.

## Building the ferrocopg scaffold

Install the bootstrap extension into the active uv-managed environment with
`maturin`:

```bash
uv run maturin develop \
    --manifest-path crates/ferrocopg-python/Cargo.toml
```

You can then smoke test the import:

```bash
uv run python -c "import ferrocopg_rust; print(ferrocopg_rust.milestone())"
```

You can also inspect how the Rust backend currently parses conninfo:

```bash
uv run python - <<'PY'
import ferrocopg_rust

summary = ferrocopg_rust.parse_conninfo_summary(
    "host=localhost dbname=postgres user=postgres connect_timeout=1"
)
print(summary.user, summary.dbname, summary.host_count)
print(summary.effective_connect_timeout_seconds)
PY
```

## Scope of the scaffold

The bootstrap extension is intentionally small. It proves:

- the Rust workspace layout
- the `maturin` integration path
- the Python import path for future `ferrocopg` acceleration work
- the initial backend direction via the `rust-postgres` stack
- a first real backend-facing parser around `tokio-postgres::Config`
- a first Rust-backed COPY formatting/parsing seam behind `psycopg._copy_base`

The next implementation slice should attach a narrow real helper behind this
package, then begin replacing pieces of `_psycopg`.
