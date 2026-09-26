# Error completion and pipeline ordering checkpoint

This is a revisable implementation checkpoint, not final Phase 5 acceptance.
The user requested publication to main; no PyPI release is authorized.

Sync-terminated extended queries now retain their own ReadyForQuery status on
recoverable database errors. Native outcomes and converted database exceptions
carry that status into Python transaction-state publication. Fatal/Panic errors
return immediately without waiting for a completion message that may never
arrive. COPY negotiation and missing completion retain explicit fallback behavior.
Successful native results also publish server status rather than SQL-text guesses.

Pipeline reservation, enqueue, execution and automatic fetch synchronization now
share the connection lock. Native reservation can release the GIL; without this
lock, queue order could differ from reservation order and a consumer could run
before its preparation owner.

## Verification

- Corrected release wheel: 162 installed checks pass.
- Workspace Rust: 32 tests pass; vendored protocol/unit checks: 9 pass.
- Focused source and bootstrap suites: 1,315 pass, 53 skip in 69.22 seconds.
- Type checking: 239 source files pass; formatting and spelling checks pass.

The frozen predecessor fails the new failed-COMMIT/commented-BEGIN controls and
the pipeline locking invariant. An initial implementation introduced a fatal
disconnect exception regression, and broader testing exposed the reservation
race. Both failures are preserved alongside the corrected passing runs, rather
than discarded or counted as passing acceptance.

[Raw logs](2026-09-26-error-outcome-local-logs.tar.gz), SHA-256:
`9109be7b24c54ab159c55a288aa9dcc4af720b2ddee1cc8c9e54122197c231d7`.

Corrected macOS arm64 CPython 3.14 wheel SHA-256:
`fc05f3e6dd101185c5b2b9c31718624d921118760dd1134eea5c5660bae6cb73`.

No timing was collected for this corrected runtime. The three passing benchmark
reports for `12f139ce` do not validate it. Full exact-candidate compatibility,
performance, soak and measurement-coverage gates remain open. Simple-query/COPY
fallbacks and signal-state publication must not be treated as comprehensively
covered by these operation-owned database-error checks.
