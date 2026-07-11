# Ferrocopg patch

This directory vendors `tokio-postgres` 0.7.17 from crates.io unchanged except
for one compatibility extension in `src/error/mod.rs`: `DbError` retains the
original protocol bytes for every diagnostic field and exposes them through
`field_bytes()`.

PostgreSQL sends diagnostic text in the active client encoding. Upstream
`tokio-postgres` converts those bytes with `String::from_utf8_lossy`, which
irreversibly loses non-UTF8 diagnostics before Psycopg can decode them. The
existing string accessors retain their upstream behavior; the added accessor
is used only by ferrocopg's Psycopg diagnostic adapter.
