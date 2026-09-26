# Ferrocopg patch

This directory vendors `tokio-postgres` 0.7.17 from crates.io with local backend
integration extensions. These include explicit result formats, column and
command metadata, inline row-field storage, and raw diagnostic preservation.

In `src/error/mod.rs`, `DbError` retains the original protocol bytes for every
diagnostic field and exposes them through `field_bytes()`.

PostgreSQL sends diagnostic text in the active client encoding. Upstream
`tokio-postgres` converts those bytes with `String::from_utf8_lossy`, which
irreversibly loses non-UTF8 diagnostics before Psycopg can decode them. The
existing string accessors retain their upstream behavior; the added accessor
is used only by ferrocopg's Psycopg diagnostic adapter.

## Operation Completion

`RowStream` retains the command tag and the operation's `ReadyForQuery` status.
Sync-terminated preparation/query response consumers also drain recoverable
server errors through their own completion message. The owned Rust `Error`
exposes that transaction status without replacing the original diagnostic.
The drained row stream terminates after yielding its error.

This drain is explicit, not a global change to all `Responses` consumers. COPY
negotiation retains the immediate response path. Fatal/panic errors return
immediately: they do not promise a completion message, and delaying them can let
a transport error hide the server diagnostic. Missing completion, transport,
local, and preflight errors do not guess transaction state. No extra Sync, SQL,
retry, or connection-global status cache is introduced.

The protocol basis is PostgreSQL's
[extended-query error recovery](https://www.postgresql.org/docs/16/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY).
Deterministic response tests cover fragmented completion, missing completion,
fatal errors, and the unchanged non-draining path. Installed backend tests cover
Parse/Bind/execution failures, cancellation, retained outcomes, and recovery.
