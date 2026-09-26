# Public native boundary: dedicated comparison

The complete public integration has a repeatable measured benefit in both
orders on the dedicated runner. Keep evaluating it as the candidate architecture;
do not treat this diagnostic as final acceptance or an irreducible performance
floor. Later correctness changes still require their own complete validation.

## Identity and verification

- Workflow [36231026807](https://github.com/martijnberger/ferrocopg/actions/runs/36231026807)
  completed successfully; artifact `10902787691` contains both wheels and raw data.
- Before: `d957b508785c9c304ad14f82b15fb3f598412cf3`.
- After: `bcd8ded9af06eb4ee10aa43fc18e6c231a2cc441`, with the same production
  runtime as locally measured `12f139ce`.
- CPython 3.14.7, Rust 1.94.1, Linux x86_64, PostgreSQL 18.6 in the dedicated
  job's container; official psycopg/psycopg-c 3.3.2, pool 3.3.0, psutil 7.2.2.
- Both builds finish before testing/timing. Before passes its own 148 installed
  checks and after passes 155; the same timing harness measures both variants.
- Independent audit checks 90 workers and 7,259,400 raw latency observations,
  recomputing medians, CPU metrics, p50/p95/p99, throughput and comparison ratios.
  Every benchmark worker reports zero remaining sessions and no worker failure.
- Uploaded wheel digests match the manifest. All 16 integration workers' full
  installed code fingerprints match their corresponding wheels. Source test,
  Cargo manifest and lock hashes match their exact repository revisions.

Before wheel SHA-256:
`d03075a51d5db082a4ccbf8f112c76960948a6fa384999f7a104dd128cf36887`.
After wheel SHA-256:
`e27ee7ab36c5b449111555c7b0d1e6029fddab9ea7505b0d7189fdf1247fb09d`.

## Both-order results

Sequential order is before-A, after-A, after-B, before-B. Long query workers
perform 10,000 warmups and nine samples of 100,000 operations. Integration
workers perform 1,000 warmups and nine samples of 5,000 operations, with normal
bookkeeping, binary prepared results, no profiler and no omitted helper bodies.

Percent reduction versus before, with positive numbers indicating improvement:

| Workload | Wall A | Wall B | CPU A | CPU B |
| --- | ---: | ---: | ---: | ---: |
| Prepared benchmark | 4.56% | 5.07% | 7.67% | 8.89% |
| Parameterized benchmark | 4.42% | 7.55% | 8.44% | 11.97% |
| Fresh cursor, constant | 6.66% | 6.11% | 8.69% | 10.70% |
| Fresh cursor, parameterized | 8.38% | 6.35% | 12.06% | 9.58% |
| Reused cursor, constant | 7.21% | 10.09% | 14.10% | 17.43% |
| Reused cursor, parameterized | 9.33% | 5.59% | 13.75% | 9.34% |

This supports the combined boundary, not attribution of its gain to any one
subchange. It agrees directionally with the earlier macOS controls. It does
not justify another packet constructor, duplicate adapter cache, generic
Transformer port, or a transport replacement.

## Complete workload context

Each variant also has one complete eleven-workload report with nine samples of
100 operations per backend/workload. Both pass every unchanged beta ceiling.
The after report's Rust/comparator median-duration ratios are:

| Workload | Python | C |
| --- | ---: | ---: |
| Plain connect | 0.585 | 0.591 |
| TLS connect | 0.634 | 0.667 |
| Parameterized | 1.076 | 1.453 |
| Prepared | 0.953 | 1.327 |
| Tuple rows | 0.096 | 1.191 |
| Dict rows | 0.154 | 1.124 |
| Namedtuple rows | 0.127 | 1.186 |
| Transaction | 0.987 | 1.162 |
| Text COPY | 0.524 | 1.207 |
| Binary COPY | 0.492 | 1.044 |
| Pool | 0.726 | 1.321 |

Do not infer per-workload optimization gains from these two short, single-order
complete reports. Comparator drift is material: parameterized C is 8.76% faster
in the later report, while pool Python is 26.05% slower. Parameterized Rust's
absolute median improves 3.31%, but its Rust/C ratio increases from 1.371 to
1.453 because C improves more. The long both-order controls are the stronger
evidence for the boundary's query benefit. Pool/C remains effectively unchanged.

Near parity remains unmet. For orientation, this after report would require
another 24.3% reduction in parameterized wall time, 17.1% in prepared queries,
and 16.7% in pool to reach C <= 1.10. Those are arithmetic gaps, not measured
recoverable overhead. Eight of eleven workloads still exceed C <= 1.10.

## Limits and next action

Before/after process snapshots show builds/tests have finished but do not
continuously prove host idleness. Query and full-benchmark workers do not embed
per-worker file fingerprints; installation logs and adjacent integration-worker
fingerprints support identity, but this is weaker than final frozen-wheel
acceptance. This run supplies no GIL/poll/wakeup attribution.

The combined public boundary is supported for continued retention testing.
Freeze the outcome/pipeline/signal-corrected successor, finish its full strict
compatibility gate, and assess remaining outcome/fallback ownership before
final performance/soak acceptance. Use new attribution on that successor to
select any further optimization; do not apply these measurements to it or
restart this already completed comparison to seek better numbers.

[Independent audit](2026-09-26-public-boundary-dedicated.json) records exact
metrics and identities. [Raw archive](2026-09-26-public-boundary-dedicated-raw.tar.gz)
preserves all reports, latencies, logs, server diagnostics and the audit script,
excluding the wheels retained in GitHub artifact `10902787691`.
Archive SHA-256:
`e612f54fed198c377ae015586ab432541e1a48a053a5557668bcffcc7c03bad0`.

Report and analysis produced with AI assistance.
