use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::PyDict;

pub(crate) fn bytes_like_to_vec(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    py.import("builtins")?
        .getattr("bytes")?
        .call1((obj,))?
        .extract()
}

pub(crate) fn expected_field_count(tx: &Bound<'_, PyAny>) -> PyResult<usize> {
    if let Ok(value) = tx.getattr("_nfields") {
        return value.extract();
    }

    if let Ok(loaders) = tx.getattr("_row_loaders") {
        return loaders.len();
    }

    Ok(0)
}

pub(crate) fn psycopg_operational_error(exc_type: &Bound<'_, PyAny>, message: &str) -> PyErr {
    let exc = exc_type
        .call1((message,))
        .expect("OperationalError constructor should succeed");
    PyErr::from_value(exc)
}

pub(crate) fn psycopg_exception_with_pgconn(
    py: Python<'_>,
    exc_type: &Bound<'_, PyAny>,
    message: &str,
    pgconn: &Bound<'_, PyAny>,
) -> PyErr {
    let kwargs = PyDict::new(py);
    kwargs
        .set_item("pgconn", pgconn)
        .expect("exception kwargs should accept pgconn");
    let exc = exc_type
        .call((message,), Some(&kwargs))
        .expect("exception constructor should succeed");
    PyErr::from_value(exc)
}

pub(crate) fn handle_stop_iteration(py: Python<'_>, err: PyErr) -> PyResult<Py<PyAny>> {
    if err.is_instance_of::<PyStopIteration>(py) {
        let value = err
            .value(py)
            .getattr("value")
            .map(Bound::unbind)
            .unwrap_or_else(|_| py.None());
        Ok(value)
    } else {
        Err(err)
    }
}

pub(crate) fn hasattr(obj: &Bound<'_, PyAny>, attr: &str) -> PyResult<bool> {
    obj.hasattr(attr)
}

pub(crate) fn poll_event_mask(
    wait_mask: i32,
    wait_r: i32,
    wait_w: i32,
    pollin: i32,
    pollout: i32,
) -> i32 {
    let mut mask = 0;
    if wait_mask & wait_r != 0 {
        mask |= pollin;
    }
    if wait_mask & wait_w != 0 {
        mask |= pollout;
    }
    mask
}
