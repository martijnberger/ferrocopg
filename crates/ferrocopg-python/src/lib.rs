use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn milestone() -> &'static str {
    "milestone-1-bootstrap"
}

#[pyfunction]
fn scaffold_status() -> &'static str {
    "ferrocopg Rust extension scaffold is wired through maturin"
}

#[pyfunction]
fn libpq_binding() -> &'static str {
    ferrocopg_pq::binding_name()
}

#[pyfunction]
fn libpq_version() -> i32 {
    ferrocopg_pq::libpq_version()
}

#[pymodule]
fn _ferrocopg(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add_function(wrap_pyfunction!(milestone, m)?)?;
    m.add_function(wrap_pyfunction!(scaffold_status, m)?)?;
    m.add_function(wrap_pyfunction!(libpq_binding, m)?)?;
    m.add_function(wrap_pyfunction!(libpq_version, m)?)?;
    Ok(())
}
