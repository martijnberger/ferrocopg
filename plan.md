# ferrocopg Migration Plan

## Goal

Turn this fork into `ferrocopg`: a Psycopg-compatible PostgreSQL adapter for
Python with no Cython left in the tree.

The immediate objective is not to redesign the public Python API. The
objective is to replace the current Cython/C acceleration and binding layers
with Rust while preserving behavior, test coverage, and packaging ergonomics.

For now, this plan assumes:

- We keep the existing Python-facing APIs and compatibility expectations.
- We focus on Cython removal first, not on replacing `libpq`.
- We defer experiments with alternate PostgreSQL backends until after the
  Cython-to-Rust migration is complete.

## Non-goals for the first phase

- Redesigning the connection/cursor API.
- Rewriting the whole library into Rust.
- Removing Python from high-level orchestration code.
- Switching away from `libpq` during the first Rust port.

## Current Architecture Summary

The repository is already split in a way that supports a staged migration:

- `psycopg/` contains the pure Python implementation and public API.
- `psycopg_c/` contains the optional Cython/C acceleration package.
- `psycopg_pool/` contains pool implementations and is largely unaffected by
  the Cython-to-Rust port except for integration testing.

The main technical seams are:

- `psycopg/psycopg/pq/`
  This is the low-level PostgreSQL wrapper contract used by the higher layers.
- `psycopg/psycopg/_cmodule.py`
  This chooses the optimized `_psycopg` implementation when available.
- `psycopg/psycopg/_transformer.py`, `generators.py`, `_copy_base.py`,
  `waiting.py`
  These already support swapping between a Python implementation and an
  optimized implementation.

This is good news for `ferrocopg`: we do not need a flag day rewrite.

## Guiding Principles

1. Preserve the current Python API until the Rust port is stable.
2. Use the existing test suite as the migration contract.
3. Replace one seam at a time.
4. Make `uv` and Rust tooling first-class early so later work lands on the
   final build path.
5. Delete Cython only after equivalent Rust-backed behavior is passing in CI.

## Desired End State

At the end of this program:

- There are no `.pyx` or `.pxd` files left in the repository.
- There are no generated C sources retained for extension builds.
- The accelerated implementation is built with Rust tooling via `maturin`.
- The contributor workflow uses `uv` as the standard Python environment and
  command runner.
- A pinned Rust toolchain is part of the repository.
- The test matrix passes against the Rust-backed implementation.

## Proposed Repository Shape

This is a first-cut target structure, not a commitment to exact names:

```text
.
├── Cargo.toml
├── rust-toolchain.toml
├── plan.md
├── pyproject.toml
├── crates/
│   ├── ferrocopg-core/
│   ├── ferrocopg-pq/
│   ├── ferrocopg-encode/
│   └── ferrocopg-python/
├── psycopg/
├── psycopg_pool/
└── python/
    └── ferrocopg_rust/   # if a dedicated Python package wrapper is useful
```

A likely split of responsibilities:

- `ferrocopg-core`
  Shared Rust data structures, errors, buffer helpers, and utilities.
- `ferrocopg-pq`
  Rust implementation of the low-level `pq` wrapper over `libpq`.
- `ferrocopg-encode`
  Fast-path binary/text adaptation helpers, copy formatting/parsing, array
  helpers, and transformer-related internals.
- `ferrocopg-python`
  PyO3 bindings that expose Python modules/classes matching what `psycopg`
  expects today.

Depending on complexity, `ferrocopg-encode` may collapse into
`ferrocopg-python` initially and be split later if needed.

## Packaging and Tooling Plan

### Step 1: Standardize on `uv`

Make `uv` the official Python workflow for local development and CI:

- Define canonical commands for dependency sync, tests, linting, and typing.
- Update contributor docs to prefer `uv` over ad hoc `pip`/venv flows.
- Keep editable installs working for the Python packages during the
  transition.
- Decide whether the repo remains a multi-package Python workspace or is
  unified behind a top-level `uv` workflow with per-package install targets.

Deliverables:

- Updated root documentation for `uv`-based setup.
- `uv`-based CI commands.
- Removal of redundant setup instructions over time.

### Step 2: Add `maturin`

Introduce `maturin` as the supported build path for Rust extensions:

- Add `maturin` to the build/development workflow.
- Decide whether Rust-backed artifacts live in the existing `psycopg_c`
  package name during transition or under a temporary `ferrocopg_*` name.
- Support local editable development and wheel builds through `maturin`.

Deliverables:

- `maturin` build integration.
- One minimal Rust extension module building in CI.
- Documentation for local Rust extension builds.

### Step 3: Add a pinned Rust toolchain

Add `rust-toolchain.toml` early:

- Pin a stable Rust channel.
- Document required components such as `rustfmt` and `clippy`.
- Ensure CI and local developer setup use the same toolchain.

Deliverables:

- `rust-toolchain.toml`
- Rust formatting/linting commands in CI or pre-commit.

## Migration Strategy

The migration should happen in two technical layers.

### Layer A: Replace `_psycopg` first

This is the acceleration layer currently surfaced through `_cmodule.py`.

Scope includes:

- `Transformer`
- nonblocking generator helpers in `generators.py`
- wait helpers used by `waiting.py`
- copy row format/parse helpers in `_copy_base.py`
- type adaptation fast paths currently in `psycopg_c/_psycopg/*`
- optimized type loaders/dumpers such as arrays, strings, numerics, UUID,
  datetime, bool, numpy if still worth keeping as accelerated paths

Why start here:

- The Python fallback implementations already exist.
- The interface boundary is narrower than the full `pq` binding layer.
- We can remove a large amount of Cython while still relying on the current
  Python/libpq path underneath.

Expected result:

- `_cmodule.py` imports a Rust-backed optimized module instead of Cython.
- Pure Python remains the fallback where necessary.

### Layer B: Replace `psycopg_c.pq`

This is the low-level wrapper around PostgreSQL client behavior.

Scope includes:

- `PGconn`
- `PGresult`
- `Conninfo`
- `Escaping`
- `PGcancel`
- connection polling and nonblocking I/O
- result retrieval and metadata access
- COPY operations
- LISTEN/NOTIFY support
- pipeline support
- trace hooks and other libpq-exposed features used by tests

Why second:

- It has a larger and more stateful contract.
- Pipeline, copy, and notify semantics are subtle and heavily tested.
- It is easier to reason about once the higher-level fast-path module has
  already moved to Rust.

Expected result:

- `psycopg.pq` can load a Rust-backed implementation with the same behavior as
  the current `c` implementation.

## Milestones

### Milestone 0: Baseline and inventory

Objective:
Establish a known-good baseline before code motion.

Tasks:

- Identify all current Cython/C artifacts under `psycopg_c/`.
- Map each Cython module to its Python import surface and tests.
- Define the acceptance matrix for sync, async, pool, copy, notify, pipeline,
  typing, and packaging.
- Decide on temporary naming for Rust extension modules during transition.

Definition of done:

- We have a module-by-module inventory.
- We have a written compatibility target for the port.

### Milestone 1: Toolchain foundation

Objective:
Make `uv`, `maturin`, and Rust available before feature porting starts.

Tasks:

- Add `rust-toolchain.toml`.
- Add `Cargo.toml` workspace.
- Add a minimal PyO3 extension wired through `maturin`.
- Update docs to use `uv`.
- Update CI to install Rust and exercise the minimal Rust extension build.

Definition of done:

- A no-op Rust extension builds locally and in CI.
- Contributors have one documented Python workflow and one documented Rust
  workflow.

### Milestone 2: Rust `_psycopg` skeleton

Objective:
Replace the import surface of `_psycopg` without feature completeness yet.

Tasks:

- Expose a Rust module matching the expected `_psycopg` import surface.
- Stub or implement `Transformer`, generator helpers, wait helpers, and copy
  helpers incrementally.
- Keep Python fallbacks active where Rust functionality is not implemented.

Definition of done:

- The Rust extension can be imported in place of the current optimized module.
- A focused subset of tests passes against the Rust-backed `_psycopg`.

### Milestone 3: Port adaptation and copy fast paths

Objective:
Move the hottest `_psycopg` pieces first.

Tasks:

- Port transformer internals used for dumping/loading.
- Port text/binary row formatting and parsing helpers.
- Port array fast paths and other high-value type helpers.
- Validate behavior against adaptation and copy tests.

Definition of done:

- Adaptation and copy tests pass with Rust-backed implementations.
- The Python fallback remains available only where needed.

### Milestone 4: Port wait/generator helpers

Objective:
Move the remaining `_psycopg` operational helpers.

Tasks:

- Port `connect`, `cancel`, `execute`, `send`, `fetch`, `fetch_many`, and
  `pipeline_communicate` helpers where Rust implementations still make sense.
- Port `wait_c` and ensure it integrates cleanly with existing sync/async
  waiting code.
- Validate gevent- and platform-related behavior where relevant.

Definition of done:

- Waiting and generator tests pass with the Rust module enabled.

### Milestone 5: Port `pq` bindings to Rust

Objective:
Replace the Cython/libpq wrapper with a Rust/libpq wrapper.

Tasks:

- Implement the `pq.abc` contract in Rust.
- Port connection, result, cancel, conninfo, escaping, and copy interfaces.
- Port notice and notify callback behavior.
- Port pipeline-mode behavior and edge cases.
- Preserve error mapping and encoding behavior expected by higher layers.

Definition of done:

- `psycopg.pq` can choose the Rust-backed implementation.
- `tests/pq` and integration tests pass against it.

### Milestone 6: Packaging cutover

Objective:
Make Rust the supported accelerated build path.

Tasks:

- Replace Cython build backend usage with `maturin`.
- Update package metadata and wheel build jobs.
- Decide whether to keep compatibility package names temporarily.
- Remove Cython from dev dependencies once it is no longer needed.

Definition of done:

- Wheels are built from Rust sources, not Cython.
- CI no longer depends on Cython to build the accelerated implementation.

### Milestone 7: Delete Cython

Objective:
Remove the old implementation cleanly.

Tasks:

- Delete `.pyx`, `.pxd`, generated `.c`, and Cython-specific build code.
- Remove dead compatibility shims and docs.
- Update documentation to describe the Rust-based accelerated implementation.

Definition of done:

- There is no Cython left in the repository.
- The repository and CI pass without Cython installed.

### Milestone 8: Post-port optimization and cleanup

Objective:
Decide what, if anything, should move from pure Python to Rust after the port.

Tasks:

- Profile real workloads and test bottlenecks.
- Identify pure-Python hotspots still worth porting.
- Keep orchestration code in Python unless data shows clear benefit.

Definition of done:

- Further Rust work is driven by profiling, not by aesthetic preference.

## First Implementation Slice

The first slice should be intentionally small and irreversible in the right
direction.

Recommended first slice:

1. Add `rust-toolchain.toml`.
2. Add a Cargo workspace and a minimal PyO3 crate.
3. Add `maturin` build wiring.
4. Update the docs and local workflow to use `uv`.
5. Make the minimal extension importable from Python.
6. Add one tiny Rust-backed helper behind `_cmodule.py` to prove the plumbing.

Good first feature candidates after plumbing:

- copy row formatting/parsing helpers
- a narrow `Transformer` helper
- a simple waiting helper

These are easier than starting with full `PGconn`.

## Risk Areas

### Behavioral compatibility

The biggest risk is subtle incompatibility rather than obvious build failure.
Particular hotspots:

- error translation
- encoding behavior
- memory ownership and object lifetime across Python/Rust boundaries
- callback behavior for notices and notifies
- copy protocol semantics
- pipeline ordering and aborted-pipeline edge cases

### Packaging complexity

The project currently has multiple Python packages and an optional accelerated
path. Introducing Rust without making local development painful is a real
design task, not clerical work.

### Platform support

Wheel building and linking across Linux, macOS, Windows, CPython versions, and
possibly PyPy need an explicit support decision. Rust may simplify some of
this, but the `libpq` dependency still matters in the first phase.

## Compatibility Decisions To Make Early

These do not need to block Milestone 1, but they should be settled early:

1. Naming
   Decide where the `ferrocopg` name appears first: repository branding only,
   package names, import names, or all of the above.

2. Transitional package layout
   Decide whether Rust temporarily lives under `psycopg_c` compatibility names
   or under new `ferrocopg_*` names with adapters.

3. PyPy support
   Decide whether the Rust accelerated path will target CPython only at first,
   with Python fallback on PyPy.

4. Build and link policy
   Decide whether the first Rust port dynamically links `libpq` exactly as
   today or changes the distribution story.

## Success Criteria

The migration is successful when all of the following are true:

- `ferrocopg` has no Cython left in the repository.
- Rust-backed accelerated modules pass the existing behavior contract.
- `uv` is the standard contributor workflow.
- `maturin` is the standard extension build path.
- A pinned Rust toolchain is part of the repository.
- CI builds and tests the Rust-backed implementation across the supported
  matrix.

## Recommended Next Actions

1. Create the Rust workspace and `rust-toolchain.toml`.
2. Decide the temporary module/package naming for the Rust extension.
3. Wire `maturin` into the repo.
4. Rewrite contributor setup docs around `uv`.
5. Implement the smallest possible Rust extension import path.
6. Port `_psycopg` helpers before touching the `pq` layer.
