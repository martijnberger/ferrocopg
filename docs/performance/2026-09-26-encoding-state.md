# Encoding state is a correctness blocker

The retained integration has an observable session-state defect, not just an
unmeasured fallback cost. After selected transaction boundaries, PostgreSQL and
the Python adapter disagree about client encoding. This can silently corrupt
returned text. Fix this before treating the current integration as a final
performance candidate. No production fix or performance claim is made here.

## Controlled results

The installed release wheel from runtime revision
`17c777609b1afeeac4a5641a58284d924ef844f1` was compared with official Psycopg
3.3.2/C. Both use CPython 3.14.6, macOS 26.7 arm64 and PostgreSQL 15.19 on the
same local TCP server. All operations run sequentially on separate connections.
This is a correctness comparison, not a timing or host-idleness experiment.

Rust wheel SHA-256:
`703a53605a39ab1571995d8013071fca6e65ea5dc0d9cc8645bca396ce982525`.
All 92 installed implementation files match the wheel. Runtime code is unchanged
between this revision and published main `6cdf1bdf`; the intervening changes are
documentation and benchmark tooling. This establishes that the defect applies
to the current runtime, not when it was originally introduced.

Each case starts a transaction, sets LOCAL client encoding to LATIN1, and retains
an unfetched `select chr(233)` result. The boundary is then executed with explicit
`prepare=False` and `prepare=True`. The script checks public encoding, transaction
status, retained-result decoding, and a new ASCII-only `select chr(233)` before
running `SHOW client_encoding` to compare the server setting.

| Boundary, two cases each | Official C | Rust |
| --- | --- | --- |
| Plain COMMIT and ROLLBACK | 4 pass | 4 pass |
| Public commit() and rollback() | 4 pass | 4 pass |
| Comment-prefixed COMMIT and ROLLBACK | 4 pass | 4 corrupt text |
| END and ABORT aliases | 4 pass | 4 corrupt text |
| Comment-prefixed ROLLBACK TO savepoint | 2 pass | 2 decoding errors |
| Deferred-constraint failure at COMMIT, commented COMMIT, commit() | 6 pass | 6 corrupt text |

Totals: **C 24/24 pass; Rust 8/24 pass, 16 fail**. The method-based controls are
intentionally repeated under both preparation settings, although the method
itself does not accept a preparation argument. All transaction-status checks and
all retained-result checks pass. The failed-commit controls preserve SQLSTATE
`23505`; the defect is stale encoding after that error, not lost error identity.

In fourteen cases the new row is `"\u00c3\u00a9"` instead of `"\u00e9"`:
UTF8 bytes are interpreted as LATIN1. In the two savepoint cases the client still
expects UTF8 while the server has restored LATIN1; decoding raises `ValueError`.
The probe uses ASCII query text so the UTF8 query-text bridge does not conceal
the disagreement.

## Observation matters

The initial matrix placed `SHOW client_encoding` before the text check. That
query contains the substring which triggers `_refresh_client_encoding`, so it
repairs the stale setting and hides the subsequent text failure. Its results
correctly establish stale public state, but cannot establish correct next-row
behavior. Both the initial script/results and the corrected versions are retained.
The final portable script checks text first; C exits 0 and Rust exits 1.

The final probe is in the [raw archive](2026-09-26-encoding-state-raw.tar.gz).
Extract it into an unused directory and run from the repository checkout with
an installed wheel environment and `PHASE5_DSN` set:

```sh
PYTHONPATH=tools/phase5 "$PYTHON" /path/to/phase5-encoding-state-probe.py \
  --backend rust --revision 17c777609b1afeeac4a5641a58284d924ef844f1 \
  --output /path/to/unused-rust-report.json
```

Run the matching C control with `--backend c` and a different output path. On
macOS, the C environment also needs access to its installed libpq. Reports
include package file fingerprints and machine/server metadata. The revision
argument labels the source; independently verify installed files against the
wheel as above rather than treating that argument as proof of identity.

Archive SHA-256:
`1c8a092422334af1ad6de3fffbf4e231a7c6ca53e281ff96cd06a5a17e9a9d9d`.

## Implementation diagnosis and next slice

`_refresh_client_encoding()` in `psycopg/psycopg/_ferrocopg.py` uses SQL substrings
and prefix checks, then executes an extra `current_setting` query. Commented
boundaries and aliases miss those checks; failed execution bypasses the refresh.
Existing installed setting tests exercise ordinary boundaries, not these cases.
The current green checks therefore do not prove this contract.

The wire reader in `vendor/tokio-postgres/src/connection.rs` already receives
`ParameterStatus` and updates its live parameter map. In contrast,
`vendor/postgres/src/client.rs::parameter()` delegates to the tokio client's
startup snapshot. Replacing the SQL refresh with that existing getter would
still be wrong. Reading only a batch's final global map would also lose the
encoding needed by earlier retained results.

The next implementation should carry server setting changes through the same
operation ownership boundary as result/error state, rather than expand the SQL
recognizer or add unconditional setting queries. Required validation:

- Preserve ordered setting updates and result encoding snapshots separately
  from the connection's final state, including multiple results and pipelined
  requests. Inspect actual message order for changes within a statement; do not
  assume a final ReadyForQuery snapshot describes every earlier row.
- Publish completed-operation connection state on success and recoverable error,
  including signal replacement, before user callbacks can inspect or reuse it.
  Preserve original errors when recovery cannot complete; do not replay SQL or
  wait for ReadyForQuery after fatal errors or incomplete COPY negotiation.
- Cover simple-query/internal commit paths and encoding bridges as well as the
  common extended operation. Test nested/reentrant callbacks, savepoint restore,
  unsupported encodings, retained results and cross-connection isolation.
- Turn the portable failing matrix into normal installed regression coverage
  when the implementation is ready. Keep this failed evidence and run the full
  compatibility, package, resource and performance gates on the successor.

No performance ceiling, correctness tolerance, or release scope changed.

AI-assisted investigation, verification, and documentation.
