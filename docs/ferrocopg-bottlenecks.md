# Phase 5 bottlenecks and decision budget

## Evidence and scope

The fork's purpose remains a Rust-native backend behind Psycopg's supported
Python interface, not a reproduction of Cython's internal architecture.
The frozen `7bbc14eb` runtime passes the approved three-run beta benchmark,
full-duration soak, and supported synchronous/package gates. This does not
establish the separate Python-parity/C +10% engineering objective.

The dedicated Linux diagnostic at `8955d0a6` runs that same production code.
Its [audited measurements](performance/2026-09-20-dedicated-attribution.json),
[original reports](performance/2026-09-20-dedicated-attribution.json.gz), and
[corrected allocation classification](performance/2026-09-20-linux-allocation-reclassification.json)
are the basis of this report. The [parity investigation](ferrocopg-parity.md)
records the experiment protocols and earlier accepted/rejected changes.

Measurements use Python 3.14.7, PostgreSQL 18.6, official Psycopg/C 3.3.2,
and pool 3.3.0 on a dedicated Linux x86-64 runner. Before/after process snapshots
are available, not continuous host-idleness telemetry. Values below retain
both orders as ranges; no favorable-order selection or cross-order averaging
is used. These are diagnostics, not additional acceptance runs.

The subsequent [matched scalar matrix](performance/2026-09-20-matched-layer-dedicated.json)
at `0ec45e1d` independently verifies 96 native/public measurements spanning
constant/prepared/unprepared queries and both text/binary output formats.
Native Rust/libpq wall ratios are 1.016-1.096, while full Rust/C ratios are
1.335-1.407. Prepared parameters show a 1.54-4.65 us native gap versus
49.53-53.43 us at the full API. This broadens the earlier constant-query evidence
without claiming a wholly removable gap or a universal native floor. Its
periodic host records show small observed background activity, 0.83-0.92% of one
core in sampler CPU, and substantial workload-related Docker proxy CPU. Preserve
that topology and observer caveat; see the parity report for both orders and
the complete raw archive. The workload budgets below still derive from the
original all-workload attribution, not these different scalar controls.

## Required savings

The [machine-readable opportunity budget](performance/2026-09-20-integration-opportunity-budget.json)
derives each row from the raw medians. The target is the smaller of Python's
median and 1.10 times C's median. Required savings are a target, **not a forecast
of recoverable work**. No positive fraction of the remaining gap has yet been
proven removable by a new, compatible implementation.

| Workload | Rust/C | Wall excess over C (us) | Required reduction from current Rust duration |
| --- | ---: | ---: | ---: |
| Plain connection | 0.589-0.597 | Already faster | 0% |
| TLS connection | 0.664-0.675 | Already faster | 0% |
| Parameterized scalar | 1.400 | 52.1-52.5 | 39.0-39.4 us; 21.4% |
| Prepared scalar | 1.375-1.404 | 44.6-47.7 | 32.7-35.9 us; 20.0-21.7% |
| 1,000 tuple rows | 1.272-1.297 | 140.5-153.5 | 88.8-101.9 us; 13.5-15.2% |
| 1,000 dictionary rows | 1.125-1.141 | 121.9-138.3 | 24.2-40.2 us; 2.2-3.6% |
| 1,000 namedtuple rows | 1.177-1.198 | 131.1-144.7 | 56.9-71.8 us; 6.5-8.2% |
| Transaction/savepoint/recovery | 1.200-1.214 | 208.9-222.4 | 104.4-118.3 us; 8.3-9.4% |
| Text COPY | 1.217-1.224 | 1,185.6-1,208.7 | 639.1-670.2 us; 9.6-10.2% |
| Binary COPY | 1.033-1.059 | 177.4-310.8 | 0% |
| Pool checkout/query | 1.297-1.549 | 42.0-70.3 | 27.9-57.5 us; 15.2-29.0% |
| Pipeline/error/recovery diagnostic | 2.464-2.640 | 1,185.4-1,266.6 | 1,104.4-1,189.3 us; 55.4-58.3% |

The pipeline workload is separate from the eleven beta workloads. Its result
cannot stand in for scalar-query improvement. The reverse pool result exceeds
the beta C ceiling in this diagnostic; preserve it rather than relabeling this
run an acceptance pass. The independently frozen three-run acceptance evidence
remains unchanged, not retroactively invalidated or extended by this run.

## Ownership and recoverability

| Candidate bottleneck | Measured evidence | Owner and confidence | Recoverable fraction and next discriminating experiment |
| --- | --- | --- | --- |
| Native client/runtime | Native Rust 82.8-84.0 us versus libpq 81.6-81.8 us for matched prepared binary `select 42::int4` | Vendored `postgres`/Tokio; high confidence for this narrow path only | Unknown; the entire observed client gap is just 1.2-2.2 us here. Not enough evidence to justify replacing the client. Add direct Tokio/pristine upstream only if a broader native floor becomes a suspected blocker. |
| Rust session and PyO3 boundary | Session 84.0-84.5 us; binding 88.7-89.4 us | Session ownership, parameter/result boundary, interpreter handoff; moderate confidence because these are independent paths | Unknown; binding minus session is 4.7-4.9 us, not a pure GIL measurement or wholly removable cost. Count handoffs/polls separately before changing locking or interpreter-release behavior. |
| Scalar public integration | Full binary constant-query API 147.5-149.1 us versus C 110.0-110.3 us. Parameterized public workloads use 42.1-46.8 us more process CPU than C | Cursor/query/adaptation/result integration; high confidence in a repeated full-API gap, incomplete attribution within it | Unknown; current scalar target requires 20-22% wall savings. Test a coarse execution plan/boundary that removes a measured block of per-call dispatch, not another one-object cache. Preserve callback and snapshot behavior. |
| Result construction and decoding | The three 1,000-row shapes have a similar 200.0-215.1 us CPU excess despite different row factories | Shared native result construction/decoding plus fixed query setup; hypothesis, not isolated causality | Unknown. Vary rows and columns while keeping types/protocol fixed, and compare direct session/binding/public paths. A row-count slope separates recurring per-row work from fixed integration cost. |
| Transaction orchestration | Eight completed cycles in every backend; Rust CPU excess 178.0-179.4 us per savepoint/recovery workload | Repeated integration and command/protocol selection; moderate confidence | Unknown; 8-9% total wall savings required. Separate command execution and recovery bookkeeping; do not infer an extra transaction RTT from the timing gap. |
| Text COPY | Text CPU excess 967.5-1,007.2 us; binary COPY is already within C +10% | Text formatting/parsing and COPY protocol/setup; attribution still incomplete | Unknown; approximately 10% text workload reduction required. Separate text encode/decode from fixed prepare/close traffic, with unchanged callbacks and error behavior. |
| Pool usage | CPU excess 42.2-54.6 us; substantial order sensitivity in wall ratios | Underlying query integration plus pool checkout/preparation transitions; not evidence that the official pool needs a fork | Unknown; first compare identical warmed connections outside/inside the pool. Preserve all order effects and avoid a pool-specific optimization until isolated. |
| Parameterized pipeline serialization | Ordered traces and source show sequential execution. Rust completes 10/12/10 cycles versus 3/3/3 in both official backends and both orders | `NoTlsPipelineAdapter.sync()` and the native batch boundary; high confidence in serialization, not its precise latency share | Unknown until a compatible batch exists; 55-58% total reduction needed for C +10% here. Submit a genuine native batch while preserving error/abort boundaries, result ownership, preparation, notices, and recovery. |

These rows overlap. Do not add their costs or savings. In particular, the
binding/public difference includes necessary Python-visible work, process CPU
can overlap server work, and result/COPY/protocol changes can affect several
workloads together. No irreducible performance limit has been demonstrated.

### Instrumentation interpretation

Prepared-query profiles observe 2,290,001 calls for Rust versus 650,001 for C
over 10,000 operations. C hides native internals from this profiler, so this
is evidence of extra visible dispatch, not a complete instruction-count ratio.
The Rust path constructs two adapter maps per operation and spends visible
work in query conversion, execution, and result handling. Profiled CPU times
are not ordinary latency budgets and cumulative times must not be added.

All 72 allocation captures retain canonical totals. The corrected classifier
identifies 144,000 profiling-frame allocations in 1,000 Rust prepared queries,
versus 46,000 in C and 111,000 in Python. Their absence from the original Linux
classification was a stack-recognition limitation, not zero observer overhead.
Do not optimize these frame allocations as if uninstrumented queries made them.

Prepared strace workers record 1,884-1,891 Rust `epoll_wait` calls versus 1,013
`poll` and eight `epoll_wait` calls for C. These cover the whole child process,
including imports, warmup, setup, and cleanup; they are not exact per-query
poll or wakeup counts. Nonblocking `connect` errors are retained, not interpreted
as failed workload operations. Protocol cycle counts likewise are not general
network RTTs.

The subsequent [marked-loop audit](performance/2026-09-21-handoff-dedicated.json)
counts exactly 1,000 prepared binary queries after warmup in both orders. Rust
records 1,000 explicit SaveThread/RestoreThread pairs, C 9,000, and Python 28,000.
Rust records 2,000 epoll entries versus 1,000 poll entries for each comparator;
all three record approximately one scheduler wakeup per query (C reverse: 999).
No PyGILState Ensure/Release calls occur in these regions. This rules against
excess explicit interpreter handoff count as the scalar bottleneck, not against
all GIL-related costs. Counts do not measure wait duration or recoverable time.
Preserve the single blocking-I/O interpreter release; prioritize removing Python
orchestration at a coarser boundary. Direct event coverage for other workloads
and polling-duration attribution remain open.

## Decisions and open work

The current evidence supports these dispositions, without claiming broader
impossibility or silently accepting a rejected prototype:

| Hypothesis | Disposition | Reopen condition |
| --- | --- | --- |
| Single-owner cursor/result state | Retained, with modest measured gains and a separate state-ownership rationale | Preserve its contracts; do not repeat the rejected lazy-projection shortcut. |
| Connection-local adapter-schema cache | Rejected: map work fell, but lookups/views/locks replaced the benefit | A different lifetime design must first establish a larger opportunity without shared mutable callback contexts. |
| Additional SQL parser/layout cache | Do not implement: placeholder parsing already has a bounded cache | A demonstrated uncached layout cost, not repeated parsing assumed from source names. |
| Cross-execution dumper/loader instances | Do not implement as a generic cache: constructors and contexts are observable and mutable | A proof of compatible lifetime/invalidation for the exact supported case; ordinary builtin benchmarks are insufficient. |
| Immutable result-decoding/execution plan | Deferred pending a bounded design and opportunity control; not disproven | Show which per-execution decisions disappear and how adapter changes, value-dependent types, encoding, format, result shape, and prepared state invalidate them. |
| Standalone server-outcome envelope | Deferred: the prior omission control suggests a small scalar opportunity, not the whole gap | Evidence from transaction/setting/notice handling and an owned per-request event model; never apply one final pipeline status to every result. |
| Immutable native parameter packet | Rejected: fresh-parameterized benefit reversed with order | A materially coarser boundary that removes more than packaging while preserving buffer snapshots and callback ordering. |
| Native parameterized batch | Recommended independent experiment, not yet implemented or retained | Keep its hypothesis separate from scalar parity and validate error/recovery semantics before timing. |
| Transport replacement or more compiler tuning | Not the next priority | A matched native-layer blocker or a controlled compiler experiment with sufficient opportunity. |

The next scalar proposal should specify one coarse execution boundary, its
per-operation ownership, and an exact list of removed dispatch/setup work.
Built-in fast paths must still respect registered adapter overrides, cursor
subclasses, factories, encoding snapshots, signals, cancellation, notices,
reentrancy, and results that outlive the connection. Require both fresh/reused
controls, both measurement orders, and all eleven workloads before retention.
Stop flat/mixed performance-only changes instead of expanding the prototype
indefinitely. A larger native integration remains an experiment, not a promised
20-22% speedup.

The user decision between continuing that bounded prototype and deferring a
larger redesign is pending. Neither option waives remaining Phase 5 evidence:
the instrumentation/measurement-quality assessment, scheduled reliability proof,
complete publication of artifacts, and final requirement-by-requirement audit
remain open. Keep the approved beta policy and supported Python interface
unchanged. Near parity is not silently added as a beta publication gate, and
passing the beta gate does not establish near parity.
