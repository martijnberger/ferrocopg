# Ferrocopg postgres patch

This directory vendors `postgres` 0.19.13, matching `Cargo.lock`.

Ferrocopg adds `Client::pipeline_simple_query()`. The upstream synchronous
wrapper otherwise exposes only one blocking operation at a time, even though
its underlying `tokio_postgres::Client` can pipeline requests whenever their
futures are polled concurrently. The added method polls a batch with
`futures_util::future::join_all` on the wrapper's existing runtime and returns
results in submission order.

The wrapper also exposes explicit result formats and metadata for typed queries,
connection-handshake timeouts and cancellation, and a wait callback that runs
outside the runtime so Python signal handlers can safely use another client.

`RowIter::collect_rows()` drains the remaining result in one runtime call for
ferrocopg's buffered unprepared-query path. It keeps the stream's column
metadata and final command count available and propagates row-stream errors.
Ordinary `RowIter::next()` remains unchanged for streaming callers. Prepared
queries already collect rows within one runtime call.

These additions are internal backend integration seams, not libpq-object
emulation. Ferrocopg's supported pipeline behavior and raw libpq boundaries are
defined in the repository's compatibility contract.
