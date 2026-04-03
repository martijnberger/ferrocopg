//! Thin libpq bindings for ferrocopg built on top of `pq-sys`.

/// The low-level binding crate used by ferrocopg for libpq access.
pub fn binding_name() -> &'static str {
    "pq-sys"
}

/// Return the runtime libpq version in libpq's integer format.
pub fn libpq_version() -> i32 {
    unsafe { pq_sys::PQlibVersion() }
}
