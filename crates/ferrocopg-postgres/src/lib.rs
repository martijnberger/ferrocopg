//! Backend selection and transport-facing helpers for ferrocopg.
//!
//! The long-term plan is to build ferrocopg on the `rust-postgres` ecosystem
//! instead of mirroring the current `libpq`/Cython transport layer.

/// The Rust backend family chosen for ferrocopg.
pub fn backend_stack() -> &'static str {
    "rust-postgres"
}

/// The transport-oriented crate ferrocopg is currently planning around.
pub fn backend_core() -> &'static str {
    let _ = core::any::type_name::<tokio_postgres::Client>();
    "tokio-postgres"
}
