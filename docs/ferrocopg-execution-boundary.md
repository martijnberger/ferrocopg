# Native execution boundary

Status: preparation-state core implemented; complete query boundary not yet
implemented or performance-accepted.
The retained runtime remains `7bbc14eb` / `8eddc2ec`. This document does not
supersede the compatibility contract, beta limits, or the Phase 5 completion audit.

## Decision and hypothesis

Prototype one execution-owned native boundary, not another parameter packet,
adapter-map cache, generic Transformer port, or new transport. The matched
[layer matrix](performance/2026-09-20-matched-layer-dedicated.json) finds native
Rust/libpq wall ratios of 1.016-1.096 but public Rust/C ratios of 1.335-1.407.
The [marked-loop audit](performance/2026-09-21-handoff-dedicated.json) finds only
one explicit Rust interpreter release/reacquire pair per prepared query. Neither
measurement establishes an irreducible floor or a recoverable latency budget.

The hypothesis is that one native operation can remove intermediate Python
request representations, prepared-state bookkeeping, session dispatch, and
result-publication scaffolding together. The rejected packet added a constructor
boundary while leaving those other layers intact. This design must remove work,
not reproduce the same calls from Rust or rename their objects.

Keep Python adaptation callbacks and public cursor customization. Do not claim
that Transformer construction, adapter snapshots, or custom loader construction
disappear in the first implementation. Their costs remain until a separately
justified lifecycle design can preserve their observable behavior.

## Entry point and stages

The proposed internal entry point is `native_session.execute_query(...)`, called
once with the converted query's existing byte buffer, parameter values/types/
formats, execution encoding, result format, and preparation policy. It is not a
public Psycopg API. There is no separately constructed native packet on the hot
path. The Python shell retains SQL composition, placeholder layout caching,
adaptation, preflight errors, and public descriptors/factories.

1. **Adapt and freeze.** Invoke the existing execution-local Python adaptation
   path. While holding the interpreter, validate lengths and formats and snapshot
   mutable parameter buffers directly into the native operation. Preserve current
   left-to-right callback behavior and the current snapshot point: after dumping
   parameters, before transaction/preparation I/O and its callbacks. Do not
   snapshot each value earlier than today or retain a borrowed writable buffer.
2. **Select and execute.** Native connection state owns prepared-query counts,
   names, statement IDs, eviction, and close queues. Select using query bytes and
   the actual resolved OID vector, not Python parameter classes. The operation
   performs required prepare/execute/maintenance with owned request values and
   the existing session, TLS, cancellation, and interpreter-release mechanism.
3. **Complete.** Construct one owned native execution result. The result keeps
   immutable result data, wire encoding/format, command information, and ordered
   per-operation events. Errors and successful results follow the same ownership
   discipline. Do not apply the final status of a batch to every intermediate
   result or silently substitute SQL-text guesses for unavailable protocol state.
4. **Publish and decode.** Release native session/state borrows before Python
   notice handlers, error hooks, loaders, or row factories run. Public cursor
   attributes project the owned execution result. Loader instances and row makers
   remain execution-local; a nested query cannot overwrite the outer result.

This is an end-to-end implementation slice: do not benchmark or retain a packet
constructor alone as if it implemented this boundary. Keep unusual encoding
bridges, client/server cursors, pipeline, COPY, and legacy/mock sessions on the
existing explicit path until their contracts are implemented in the new one.
Fallback happens before callbacks or I/O; it must never replay completed work.
These temporary exclusions are prototype limits, not a narrowed final API goal.

## Ownership and invalidation

| Owner | Retained state | Invalidation/release rule |
| --- | --- | --- |
| Existing SQL layout cache | Parsed placeholders and parameter ordering | Existing bounded key/lifetime; no duplicate parser cache |
| Public cursor | Its copy-on-write adapter snapshot, query class, factories, current result | Preserve existing child/parent isolation and descriptor behavior |
| Operation | Resolved types/formats, dumped buffer snapshots, encoding snapshot, callback contexts, error state | Fresh each execute; release on every success/failure path; never connection-cache callbacks |
| Native connection | Prepared keys/counts/names/IDs and bounded eviction state | Threshold/max changes, prepare=False, failed preparation, invalidating commands, rollback/discard, close/reconnect follow current behavior |
| Owned result | Wire data and metadata, encoding/format, per-result status/events | Survives later executions and connection close; release when last result owner goes away |
| Execution-local decoder | Resolved loader instances and row position | Cursor loader registration, result shape/format, encoding, nextset and new execute invalidate at existing points |

Do not add a second authoritative prepared cache. Public preparation settings and
any compatibility projections must read/write native ownership once it moves.
Preserve the current `PrepareManager` semantics, including gradual eviction,
LRU updates, disabled preparation, statement naming, and exception cleanup.
Differential state-machine tests against the Python manager are required before
switching production routing; a second cache that happens to pass scalar queries
is not sufficient.

Class identity alone does not authorize skipping arbitrary Python constructors
or mutable methods. Cached built-in specialization is outside the first slice.
If proposed later, it must account for registration snapshots, class/method
replacement, value-dependent `get_key`/`upgrade`, recursive contexts, unknown OIDs,
and mutable type registries without sharing callback instances across executes.

No PyO3 mutable borrow, session mutex guard, or borrowed parameter/result view
may span a Python callback. Python callbacks can replace adaptation state, raise,
retain their context, or execute another query. Traverse/clear every owned Python
reference for cyclic GC; immutable result bytes must not keep a connection alive.
Keep existing Python connection-lock semantics: the demonstrated same-connection
reentrancy is during result loading, not a new promise that arbitrary adaptation
or notice callbacks can recursively execute while the connection lock is held.

## Work removed and work retained

| Current path | Intended disposition |
| --- | --- |
| `_convert_query_params()` builds `_BoundParams` and a Python list of triples | Native entry snapshots existing adapted sequences directly; no intermediate packet object |
| Query bytes decoded for the adapter then encoded for `_BackendPreparedQuery` | Keep canonical query bytes for preparation identity; perform required wire/string conversion once |
| `_execute_bound()` maintains names and IDs in separate Python structures | One native connection owner, with explicit compatibility views where required |
| Session wrapper selects format-aware/fallback methods and packages argument tuples | One native ordinary-query entry; preserve the explicit legacy fallback outside that entry |
| Multiple Python result wrappers publish the same execution | One native owned result plus required public projections; no lazy projection that violates descriptors |
| Python composition, parameter layout, adapter constructors and callbacks | Retained in the first slice, with unchanged behavior |
| Encoding bridges, transaction preflight and outcome handling | Retained until implemented and verified; do not assume the earlier helper ablation removed this cost |

## Verification and stop rule

Before production routing, implement preparation-state differential tests and
run the installed ownership controls in `tools/phase5/test_installed.py` against
Rust and official C. Existing controls cover mutable-buffer snapshots, callback
lifetimes, copy-on-write adapter/type snapshots, failure ordering, custom cursor
descriptors, encoding changes, result retention, notices, signals, and locks.

Two additional public-path controls now pass on the frozen installed Rust wheel
and official C: changing integer values selects smallint/integer/bigint prepared
signatures while text/binary results alternate; a loader issues nested prepared
queries on the same connection while the outer result retains its data, identity,
row count, and position. These establish contracts, not a native implementation.
The same controls also pass against official pure Python. All 124 Phase 5
tooling/installed checks pass with the frozen installed Rust wheel.

The first implementation slice is `6d1e2313`: Rust-owned `PreparationState`,
with a separate `NativePreparationState` Python test adapter. The core contains
no Python references or callback calls. Its ordered state uses hash lookup and
an ordered recency index; query keys are shared between indexes rather than
duplicating their buffers. It is not connected to the public query path yet.

[Source/wheel evidence](performance/2026-09-21-native-preparation-state.json) and
the [raw source/test archive](performance/2026-09-21-native-preparation-state.json.gz)
preserve the exact release wheel and all 92 installed code-file checks. Six
differential tests compare independent get/add/validate/clear transitions with
the Python manager, including 5,000 seeded steps with state checked after each.
Explicit cases cover reservation, command-status boundaries, error/multiple
results, LRU order, gradual eviction, close queues, detached inspection views,
and mutable input ownership. All 130 Phase 5 checks pass on this prototype wheel.
Cargo check/format, Ruff, and codespell pass; Clippy is unavailable in the pinned
toolchain. These are not full compatibility, benchmark, or soak acceptance.

Follow-up `08238dcc` removes the fixed-width integer restriction. Settings use
native arbitrary-precision values; ordinary counters remain inline and promote
only at overflow. The internal LRU clock compacts without changing recency.
Four Rust tests exercise counter/name overflow, huge settings, and recency
compaction. The expanded Python differential test includes positive/negative
values through `2**20000`; all 131 Phase 5 checks pass on the new release wheel.
Only four supporting packages were added to the lockfile, with no existing
dependency version changes. See the
[integer and CI audit](performance/2026-09-24-preparation-and-ci-audit.json) and
[raw source/report archive](performance/2026-09-24-preparation-and-ci-audit.json.gz).

Next integrate this same owner, native statement IDs, buffer snapshots, events,
and result publication into `execute_query`. Public query routing is still
unchanged. Do not ship an unused second cache or benchmark this primitive as a
completed execution path; no performance gain has yet been demonstrated.

Compare an exact installed baseline and prototype with both backend orders and
fresh/reused constant/parameterized controls. Capture ordinary CPU/wall samples
separately from profiling. Verify that the named intermediate work actually
disappears. Reject flat/mixed performance-only results without repeatedly
resampling for a favorable order. Required savings for near parity remain the
published per-workload opportunity budgets, not a promised gain from this design.

Retention requires all eleven workload comparisons plus full supported-sync,
packaging/coexistence, cancellation, and resource/soak gates on the changed
candidate. Pipeline throughput remains a separate experiment. If this boundary
does not produce repeatable savings, preserve the evidence and seek an explicit
larger-redesign/deferral decision; do not call the Rust client irreducibly slow.
