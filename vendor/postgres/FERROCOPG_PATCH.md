# Ferrocopg postgres patch

This directory vendors `postgres` 0.19.13, matching `Cargo.lock`.

Ferrocopg adds `Client::pipeline_simple_query()`. The upstream synchronous
wrapper otherwise exposes only one blocking operation at a time, even though
its underlying `tokio_postgres::Client` can pipeline requests whenever their
futures are polled concurrently. The added method polls a batch with
`futures_util::future::join_all` on the wrapper's existing runtime and returns
results in submission order.

The patch is intentionally limited to this method. Exact libpq pipeline sync
and `PIPELINE_ABORTED` state-machine behavior remains outside the backend
contract and is documented by ferrocopg as a compatibility boundary.
