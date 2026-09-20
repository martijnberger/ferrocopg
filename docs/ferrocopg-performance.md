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
workspace=$PWD
(
  cd /tmp/phase5-stage
  "$workspace/.venv/bin/maturin" build --release --out /tmp/phase5-wheels
)
uv venv /tmp/phase5-env --python 3.14
uv pip install --python /tmp/phase5-env/bin/python \
  -r tools/phase5/requirements.txt /tmp/phase5-wheels/*.whl
export PHASE5_DSN='host=127.0.0.1 user=postgres password=password dbname=postgres sslmode=disable'
revision=$(jj log -r @ --no-graph -T commit_id)
/tmp/phase5-env/bin/python tools/phase5/repeat_benchmark.py \
  --revision "$revision" --wheel /tmp/phase5-wheels/*.whl \
  --output /tmp/phase5-benchmark
/tmp/phase5-env/bin/python tools/phase5/run.py soak \
  --revision "$revision" --output /tmp/phase5-soak
```

The revision must identify the source used to build the wheel. Rebuild after
changing backend code. Each backend runs in a fresh subprocess with its
implementation selector checked explicitly. Official Psycopg comes from the
isolated environment, not the repository's editable source installation.
The DSN stays in the environment and is not included in JSON reports.

The benchmark covers plaintext and TLS connection establishment, parameterized
queries with preparation disabled, prepared reuse, tuple/dict/namedtuple result adaptation, transactions
with savepoint rollback, text and binary COPY in both directions, and official
pool checkout/query/return. Every workload checks returned data or recovery
behavior. TLS setup verifies encryption using `pg_stat_ssl`.

The repeated beta gate uses 10 warmup operations, nine samples of 100 operations,
and 1,000 rows per bulk operation. The single-run diagnostic command
`run.py benchmark` defaults to 20 operations instead. Each worker records raw wall and process CPU measurements,
individual-operation latency percentiles, work units per second, peak RSS sampled by the parent,
cleanup resources, and machine/server/package metadata. Benchmark order rotates
across workloads. Preserve all raw JSON and logs when publishing comparisons.
The repeated runner executes all three comparisons sequentially, even if a
comparison fails a performance limit. It checks the wheel checksum and installed
package fingerprints before and after every comparison and records all failures
in `summary.json`. Use an empty output directory for each attempt. Do not rebuild,
install packages, or run other tests while collecting timings. Investigate
disagreement across runs; do not cherry-pick the fastest run or workloads.

The exit status is nonzero if any workload is missing, crashes, times out, or
fails a correctness check. Each workload's median Rust duration must be no more
than 1.15 times the Python median and no more than 1.50 times the C median.
These beta ceilings were explicitly approved on 2026-09-20 and are recorded as
policy `beta-2026-09-20` in new reports. All eleven workloads must pass both
limits in every one of the three runs. Python parity and no more than 1.10 times
C are the current near-parity engineering objectives, not beta blockers. Existing reports
retain their original verdicts; the new policy does not retroactively turn an
old failure into acceptance. Compatibility, soak duration, and resource budgets
are unchanged. No candidate has yet passed the revised three-run gate.

Once accepted, preserve the frozen Rust wheel and raw results as the regression
baseline for later changes. Cross-driver ceilings are not a license to spend
that entire budget on each change: repeatable regressions require an explicit,
documented architectural tradeoff. A numerical Rust-to-Rust regression budget
will be set after measuring that baseline's repeatability, not inferred from
the new comparator ceilings.

## Query diagnostics

For attribution between the native clients, Rust session, direct binding, and
public Python APIs, see the [parity investigation](ferrocopg-parity.md).
These matched layer probes are separate from beta acceptance.

Use `tools/phase5/query_profile.py` to investigate small-query overhead without
changing acceptance workloads. Run each backend in a separate process, using
the same installed environment and DSN as the benchmark:

```sh
/tmp/phase5-env/bin/python tools/phase5/query_profile.py \
  --backend rust --workload prepared --revision "$revision" \
  --output /tmp/phase5-query-rust.json
```

Repeat with `--backend python` and `--backend c`, and with
`--workload parameterized` to disable preparation. The command checks the
selected implementation and query results, alternates public-query and
instrumented cursor/execute/fetch/release samples, and records raw timings,
CPU time, environment metadata, and hashes of installed implementation files.
For official baselines, `--revision` identifies the comparison's fork revision;
package versions and file hashes identify the installed comparator.

The phased probe creates an explicit cursor, whereas the public probe calls
`conn.execute()`. Timer overhead and different call paths mean phase medians
are not an exact decomposition of public latency. Release means dropping the
cursor reference, not an explicit `close()` call. These are diagnostics, not
acceptance reports or wheel checksums. Preserve the scripts and reports with
the tested revision and wheel; retain full benchmark and soak evidence
separately. Run probes sequentially on an idle machine, never alongside builds,
tests, or other benchmarks.

## Soak acceptance

The soak runs connection churn, transactions and savepoint rollback,
cancellation of an observed running query, text/binary COPY roundtrips,
pipeline ordering and error recovery, pool cycles, and eight concurrent pool
clients contending for four connections. After three warmup
epochs it runs for at least 30 minutes per backend, with at least six resource
samples after complete workload cleanup. All three backends run sequentially.
Allow roughly 90 minutes plus warmup for the default soak command.

The harness collects garbage and allows up to five seconds for cleanup to
settle. Early completion requires two identical resource readings with zero
workload sessions; an unchanged nonzero session count is not clean. Sessions
remaining at the deadline still fail acceptance. It checks:

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
wheel, starts PostgreSQL 18 with TLS, runs three benchmarks sequentially on one
runner and the full soak on a separate runner, and publishes wheels, reports,
server-image identity, and server logs for 90 days, including on failure. Shared CI timing is useful
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
