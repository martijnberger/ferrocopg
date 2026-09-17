# Phase 5 reliability and performance

Phase 5 measures the installed release wheel against pinned official Psycopg
Python and C implementations. Run the commands on an otherwise idle machine
against a dedicated PostgreSQL database. The database must accept plaintext
and TLS connections and allow the test role to inspect its own sessions in
`pg_stat_activity`. The suite uses temporary tables and opens many connections.

## Build and run

Install PostgreSQL development headers and make `pg_config` available before
building the C comparator. On macOS with Homebrew PostgreSQL, also set
`DYLD_LIBRARY_PATH` to that PostgreSQL installation's `lib` directory for the
pure-Python comparator to locate libpq.

```sh
uv sync --locked --no-default-groups --group rust
uv run --no-sync python tools/stage_ferrocopg.py /tmp/phase5-stage
uv run --no-sync maturin build --release \
  --manifest-path /tmp/phase5-stage/Cargo.toml --out /tmp/phase5-wheels
uv venv /tmp/phase5-env --python 3.14
uv pip install --python /tmp/phase5-env/bin/python \
  -r tools/phase5/requirements.txt /tmp/phase5-wheels/*.whl
export PHASE5_DSN='host=127.0.0.1 user=postgres password=password dbname=postgres sslmode=disable'
revision=$(jj log -r @ --no-graph -T commit_id)
/tmp/phase5-env/bin/python tools/phase5/run.py benchmark \
  --revision "$revision" --output /tmp/phase5-benchmark
/tmp/phase5-env/bin/python tools/phase5/run.py soak \
  --revision "$revision" --output /tmp/phase5-soak
```

The revision must identify the source used to build the wheel. Rebuild after
changing backend code. Each backend runs in a fresh subprocess with its
implementation selector checked explicitly. Official Psycopg comes from the
isolated environment, not the repository's editable source installation.
The DSN stays in the environment and is not included in JSON reports.

The benchmark covers plaintext and TLS connection establishment, parameterized
queries, prepared reuse, tuple/dict/namedtuple result adaptation, transactions
with savepoint rollback, text and binary COPY in both directions, and official
pool checkout/query/return. Every workload checks returned data or recovery
behavior. TLS setup verifies encryption using `pg_stat_ssl`.

Defaults are 10 warmup operations, nine samples of 20 operations, and 1,000 rows
per bulk operation. Each worker records raw wall and process CPU measurements,
latency percentiles, work units per second, peak RSS sampled by the parent,
cleanup resources, and machine/server/package metadata. Benchmark order rotates
across workloads. Preserve all raw JSON and logs when publishing comparisons.
Run the complete benchmark at least three times on the same idle machine before
making a release performance claim; investigate disagreement across runs.

The exit status is nonzero if any workload is missing, crashes, times out, or
fails a correctness check. Each workload's median Rust duration must be no more
than the Python median and no more than 1.25 times the C median. These are the
roadmap's acceptance limits. A failure remains a release blocker unless an
explicit release decision approves a documented exception.

## Soak acceptance

The soak runs connection churn, transactions and savepoint rollback,
cancellation of an observed running query, text/binary COPY roundtrips,
pipeline ordering and error recovery, and pool cycles. After three warmup
epochs it runs for at least 30 minutes per backend, with at least six resource
samples after complete workload cleanup. All three backends run sequentially.
Allow roughly 90 minutes plus warmup for the default soak command.

The harness collects garbage, allows bounded cleanup settling, and checks:

- zero surviving workload sessions in PostgreSQL;
- no increase in native process threads, TCP sockets, or file descriptors;
- at most 32 additional live driver/pool Python objects;
- at most 16 MiB retained RSS growth between the first and last three samples.

The small object and RSS budgets accommodate runtime caches and allocator arena
retention. They do not establish absence of arbitrarily slow leaks; repeat and
extend runs when a trend appears. Missing resource-monitoring permissions fail
the run. Each worker has a 40-minute deadline; a hung worker is killed and its
log is retained. Small runs using `--seconds`, `--iterations`, and `--rows` are
useful for harness development, but do not establish the 30-minute soak gate.

The `Phase 5 reliability and performance` workflow runs weekly, supports manual
dispatch, and runs when the harness changes on `main`. It builds a release
wheel, starts PostgreSQL 18 with TLS, runs both modes, and publishes reports
and server logs for 90 days, including on failure. Shared CI timing is useful
for detecting regressions; release claims also require repeatable measurements
on an idle machine.

## Official synchronous pool

Install `psycopg_pool` alongside the staged ferrocopg wheel. Supply a connection
factory that returns the Rust connection; the pool itself remains the official
package:

```python
import ferrocopg
from psycopg_pool import ConnectionPool


class RustConnection(ferrocopg.Connection):
    @classmethod
    def connect(cls, conninfo="", **kwargs):
        return ferrocopg.connect(conninfo, **kwargs)


with ConnectionPool(
    "dbname=app", connection_class=RustConnection, min_size=2, max_size=8
) as pool:
    pool.wait()
    with pool.connection() as conn:
        result = conn.execute("select %s::int", (42,)).fetchone()
```

Pool checkout contexts commit on success and roll back on failure. Close the
pool before process shutdown. The harness uses autocommit for its single-query
checkout measurement and measures explicit transactions separately.
