# ferrocopg Bootstrap Workflow

The repository still contains the upstream Python and Cython packaging, but the
initial `ferrocopg` Rust port scaffold now lives in `crates/ferrocopg-python`.
The low-level libpq direction is now anchored on `pq-sys` through the
`crates/ferrocopg-pq` crate.

## Python environment

Use `uv` for local environment management:

```bash
uv venv
source .venv/bin/activate
uv pip install --config-settings editable_mode=strict -e "./psycopg[dev,test]"
uv pip install --config-settings editable_mode=strict -e ./psycopg_pool
uv pip install ./psycopg_c
```

## Rust toolchain

The repository pins a Rust toolchain in `rust-toolchain.toml`.

The current workspace root is `Cargo.toml`, with the first Rust extension
package defined in `crates/ferrocopg-python`.

## Building the ferrocopg scaffold

Install the bootstrap extension into the active environment with `maturin`
through `uv`:

```bash
uv run --with maturin maturin develop \
    --manifest-path crates/ferrocopg-python/Cargo.toml
```

You can then smoke test the import:

```bash
python -c "import ferrocopg_rust; print(ferrocopg_rust.milestone())"
```

## Scope of the scaffold

The bootstrap extension is intentionally small. It proves:

- the Rust workspace layout
- the `maturin` integration path
- the Python import path for future `ferrocopg` acceleration work
- the initial libpq binding direction via `pq-sys`

The next implementation slice should attach a narrow real helper behind this
package, then begin replacing pieces of `_psycopg`.
