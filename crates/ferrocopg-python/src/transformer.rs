use crate::python_helpers::psycopg_import;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::pyclass::{PyTraverseError, PyVisit};
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
// Keep the Python transformer as the only owner of mutable adaptation state.
pub(crate) struct TransformerDispatch {
    recursive_dumper: Py<PyAny>,
    recursive_loader: Py<PyAny>,
    text_format: Py<PyAny>,
}

fn lookup<'py>(
    cache: &Bound<'py, PyAny>,
    key: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let value = if let Ok(cache) = cache.cast_exact::<PyDict>() {
        cache.get_item(key)
    } else {
        cache.get_item(key).map(Some)
    };
    match value {
        Ok(value) => Ok(value),
        Err(error) if error.is_instance_of::<PyKeyError>(cache.py()) => Ok(None),
        Err(error) => Err(error),
    }
}

fn configure<'py>(
    value: Bound<'py, PyAny>,
    tx: &Bound<'py, PyAny>,
    recursive: &Bound<'py, PyAny>,
    encoding_name: &Bound<'py, PyString>,
) -> PyResult<Bound<'py, PyAny>> {
    let py = tx.py();
    if value.is_instance(recursive)? {
        value.setattr(pyo3::intern!(py, "_tx"), tx)?;
    }
    if value.hasattr(pyo3::intern!(py, "_encoding"))? {
        value.setattr(pyo3::intern!(py, "_encoding"), tx.getattr(encoding_name)?)?;
    }
    Ok(value)
}

#[pymethods]
impl TransformerDispatch {
    #[new]
    fn new(
        recursive_dumper: Py<PyAny>,
        recursive_loader: Py<PyAny>,
        text_format: Py<PyAny>,
    ) -> Self {
        Self {
            recursive_dumper,
            recursive_loader,
            text_format,
        }
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.recursive_dumper)?;
        visit.call(&self.recursive_loader)?;
        visit.call(&self.text_format)
    }

    fn __clear__(&mut self, py: Python<'_>) {
        self.recursive_dumper = py.None();
        self.recursive_loader = py.None();
        self.text_format = py.None();
    }

    fn get_dumper<'py>(
        &self,
        tx: &Bound<'py, PyAny>,
        obj: &Bound<'py, PyAny>,
        format: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = tx.py();
        let key = obj.get_type().into_any();
        let cache = tx
            .getattr(pyo3::intern!(py, "_dumpers"))?
            .get_item(format)?;
        let dumper = match lookup(&cache, &key)? {
            Some(dumper) => dumper,
            None => {
                let cls = match tx
                    .getattr(pyo3::intern!(py, "adapters"))?
                    .call_method1(pyo3::intern!(py, "get_dumper"), (&key, format))
                {
                    Ok(cls) => cls,
                    Err(error) => {
                        let programming =
                            psycopg_import(py, "errors")?.getattr("ProgrammingError")?;
                        if error.is_instance(py, &programming) {
                            error.set_cause(py, None);
                        }
                        return Err(error);
                    }
                };
                let dumper = cls.call1((&key, tx))?;
                cache.set_item(&key, &dumper)?;
                dumper
            }
        };
        let upgraded_key = dumper.call_method1(pyo3::intern!(py, "get_key"), (obj, format))?;
        let dumper = if upgraded_key.is(&key) {
            dumper
        } else {
            match lookup(&cache, &upgraded_key)? {
                Some(upgraded) => upgraded,
                None => {
                    let upgraded =
                        dumper.call_method1(pyo3::intern!(py, "upgrade"), (obj, format))?;
                    cache.set_item(&upgraded_key, &upgraded)?;
                    upgraded
                }
            }
        };
        configure(
            dumper,
            tx,
            self.recursive_dumper.bind(py),
            pyo3::intern!(py, "_dumper_encoding"),
        )
    }

    fn get_loader<'py>(
        &self,
        tx: &Bound<'py, PyAny>,
        oid: &Bound<'py, PyAny>,
        format: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = tx.py();
        let cached = {
            let cache = tx
                .getattr(pyo3::intern!(py, "_loaders"))?
                .get_item(format)?;
            lookup(&cache, oid)?
        };
        let loader = match cached {
            Some(loader) => loader,
            None => {
                let adapters = tx.getattr(pyo3::intern!(py, "_adapters"))?;
                let mut cls =
                    adapters.call_method1(pyo3::intern!(py, "get_loader"), (oid, format))?;
                if !cls.is_truthy()? {
                    cls = tx
                        .getattr(pyo3::intern!(py, "_adapters"))?
                        .call_method1(pyo3::intern!(py, "get_loader"), (0, format))?;
                    if !cls.is_truthy()? {
                        let error = psycopg_import(py, "errors")?.getattr("InterfaceError")?;
                        return Err(PyErr::from_value(
                            error.call1(("unknown oid loader not found",))?,
                        ));
                    }
                }
                let loader = cls.call1((oid, tx))?;
                // A constructor can replace caches before the assignment.
                tx.getattr(pyo3::intern!(py, "_loaders"))?
                    .get_item(format)?
                    .set_item(oid, &loader)?;
                loader
            }
        };
        configure(
            loader,
            tx,
            self.recursive_loader.bind(py),
            pyo3::intern!(py, "_loader_encoding"),
        )
    }

    fn dump_sequence<'py>(
        &self,
        tx: &Bound<'py, PyAny>,
        params: &Bound<'py, PyAny>,
        formats: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let py = tx.py();
        let count = params.len()?;
        let out = PyList::new(py, (0..count).map(|_| py.None()))?;
        if tx.getattr(pyo3::intern!(py, "_row_dumpers"))?.is_truthy()? {
            let expected = tx.getattr(pyo3::intern!(py, "_row_dumpers"))?.len()?;
            if expected != count {
                let error = psycopg_import(py, "errors")?.getattr("DataError")?;
                return Err(PyErr::from_value(error.call1((format!(
                    "expected {expected} values in row, got {count}"
                ),))?));
            }
            for index in 0..count {
                let param = params.get_item(index)?;
                if !param.is_none() {
                    let dumper = tx
                        .getattr(pyo3::intern!(py, "_row_dumpers"))?
                        .get_item(index)?;
                    out.set_item(
                        index,
                        dumper.call_method1(pyo3::intern!(py, "dump"), (&param,))?,
                    )?;
                }
            }
            return Ok(out);
        }

        let mut types = Vec::with_capacity(count);
        let mut pqformats = Vec::with_capacity(count);
        let text = self.text_format.bind(py);
        for index in 0..count {
            let param = params.get_item(index)?;
            if param.is_none() {
                types.push(tx.call_method0(pyo3::intern!(py, "_get_none_oid"))?);
                pqformats.push(text.clone());
                continue;
            }
            let format = formats.get_item(index)?;
            let dumper = tx.call_method1(pyo3::intern!(py, "get_dumper"), (&param, &format))?;
            out.set_item(
                index,
                dumper.call_method1(pyo3::intern!(py, "dump"), (&param,))?,
            )?;
            types.push(dumper.getattr(pyo3::intern!(py, "oid"))?);
            pqformats.push(dumper.getattr(pyo3::intern!(py, "format"))?);
        }
        tx.setattr(pyo3::intern!(py, "types"), PyTuple::new(py, types)?)?;
        tx.setattr(pyo3::intern!(py, "formats"), PyList::new(py, pqformats)?)?;
        Ok(out)
    }
}
