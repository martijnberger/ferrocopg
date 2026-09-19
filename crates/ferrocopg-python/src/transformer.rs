use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::pyclass::{PyTraverseError, PyVisit};
use pyo3::types::{PyDict, PyList, PyString, PyTuple, PyType};

#[pyclass(subclass, module = "ferrocopg_rust._ferrocopg", get_all, set_all)]
pub(crate) struct NativeTransformer {
    types: Py<PyAny>,
    formats: Py<PyAny>,
    _conn: Py<PyAny>,
    _adapters: Py<PyAny>,
    _pgresult: Py<PyAny>,
    _dumpers: Py<PyAny>,
    _loaders: Py<PyAny>,
    _encoding: Py<PyAny>,
    _none_oid: i64,
    _oid_dumpers: Py<PyAny>,
    _oid_types: Py<PyAny>,
    _row_dumpers: Py<PyAny>,
    _row_loaders: Py<PyAny>,
    _native_api: Py<PyAny>,
}

fn lookup<'py>(
    cache: &Bound<'py, PyAny>,
    key: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let result = if let Ok(cache) = cache.cast_exact::<PyDict>() {
        cache.get_item(key)
    } else {
        cache.get_item(key).map(Some)
    };
    match result {
        Err(error) if error.is_instance_of::<PyKeyError>(cache.py()) => Ok(None),
        other => other,
    }
}

impl NativeTransformer {
    fn error(slf: &Bound<'_, Self>, name: &str, message: &str) -> PyResult<PyErr> {
        let api = slf.borrow()._native_api.clone_ref(slf.py());
        let cls = api.bind(slf.py()).get_item(4)?.getattr(name)?;
        Ok(PyErr::from_value(cls.call1((message,))?))
    }
}

#[pymethods]
impl NativeTransformer {
    #[new]
    #[classmethod]
    #[pyo3(signature = (context=None))]
    fn new(cls: &Bound<'_, PyType>, context: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let py = cls.py();
        let api = cls.getattr("_transformer_api")?;
        let (adapters, conn) = match context {
            Some(context) if context.is_truthy()? => (
                context.getattr(pyo3::intern!(py, "adapters"))?.unbind(),
                context.getattr(pyo3::intern!(py, "connection"))?.unbind(),
            ),
            _ => (api.get_item(0)?.getattr("adapters")?.unbind(), py.None()),
        };
        Ok(Self {
            types: py.None(),
            formats: py.None(),
            _conn: conn,
            _adapters: adapters,
            _pgresult: py.None(),
            _dumpers: api.get_item(1)?.call1((py.get_type::<PyDict>(),))?.unbind(),
            _loaders: PyTuple::new(py, [PyDict::new(py), PyDict::new(py)])?
                .into_any()
                .unbind(),
            _encoding: PyString::new(py, "").into_any().unbind(),
            _none_oid: -1,
            _oid_dumpers: py.None(),
            _oid_types: PyDict::new(py).into_any().unbind(),
            _row_dumpers: py.None(),
            _row_loaders: PyList::empty(py).into_any().unbind(),
            _native_api: api.unbind(),
        })
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        for value in [
            &self.types,
            &self.formats,
            &self._conn,
            &self._adapters,
            &self._pgresult,
            &self._dumpers,
            &self._loaders,
            &self._encoding,
            &self._oid_dumpers,
            &self._oid_types,
            &self._row_dumpers,
            &self._row_loaders,
            &self._native_api,
        ] {
            visit.call(value)?;
        }
        Ok(())
    }

    fn __clear__(&mut self, py: Python<'_>) {
        for value in [
            &mut self.types,
            &mut self.formats,
            &mut self._conn,
            &mut self._adapters,
            &mut self._pgresult,
            &mut self._dumpers,
            &mut self._loaders,
            &mut self._encoding,
            &mut self._oid_dumpers,
            &mut self._oid_types,
            &mut self._row_dumpers,
            &mut self._row_loaders,
            &mut self._native_api,
        ] {
            *value = py.None();
        }
    }

    fn get_dumper<'py>(
        slf: &Bound<'py, Self>,
        obj: &Bound<'py, PyAny>,
        format: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let key = obj.get_type().into_any();
        // Clone a reference, not the cache. Never hold a state borrow over Python.
        let dumpers = slf.borrow()._dumpers.clone_ref(py);
        let cache = dumpers.bind(py).get_item(format)?;
        let dumper = match lookup(&cache, &key)? {
            Some(dumper) => dumper,
            None => {
                let cls = match slf
                    .getattr(pyo3::intern!(py, "adapters"))?
                    .call_method1(pyo3::intern!(py, "get_dumper"), (&key, format))
                {
                    Ok(cls) => cls,
                    Err(error) => {
                        let api = slf.borrow()._native_api.clone_ref(py);
                        let programming = api.bind(py).get_item(4)?.getattr("ProgrammingError")?;
                        if error.is_instance(py, &programming) {
                            error.set_cause(py, None);
                        }
                        return Err(error);
                    }
                };
                let dumper = cls.call1((&key, slf))?;
                cache.set_item(&key, &dumper)?;
                dumper
            }
        };
        let upgraded_key = dumper.call_method1(pyo3::intern!(py, "get_key"), (obj, format))?;
        if upgraded_key.is(&key) {
            return Ok(dumper);
        }
        match lookup(&cache, &upgraded_key)? {
            Some(upgraded) => Ok(upgraded),
            None => {
                let upgraded = dumper.call_method1(pyo3::intern!(py, "upgrade"), (obj, format))?;
                cache.set_item(&upgraded_key, &upgraded)?;
                Ok(upgraded)
            }
        }
    }

    fn get_loader<'py>(
        slf: &Bound<'py, Self>,
        oid: &Bound<'py, PyAny>,
        format: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let loaders = slf.borrow()._loaders.clone_ref(py);
        if let Some(loader) = lookup(&loaders.bind(py).get_item(format)?, oid)? {
            return Ok(loader);
        }
        let adapters = slf.borrow()._adapters.clone_ref(py);
        let mut cls = adapters
            .bind(py)
            .call_method1(pyo3::intern!(py, "get_loader"), (oid, format))?;
        if !cls.is_truthy()? {
            let adapters = slf.borrow()._adapters.clone_ref(py);
            cls = adapters
                .bind(py)
                .call_method1(pyo3::intern!(py, "get_loader"), (0, format))?;
            if !cls.is_truthy()? {
                return Err(Self::error(
                    slf,
                    "InterfaceError",
                    "unknown oid loader not found",
                )?);
            }
        }
        let loader = cls.call1((oid, slf))?;
        // Constructors may replace the destination cache, just as in Python.
        let loaders = slf.borrow()._loaders.clone_ref(py);
        loaders.bind(py).get_item(format)?.set_item(oid, &loader)?;
        Ok(loader)
    }

    fn get_dumper_by_oid<'py>(
        slf: &Bound<'py, Self>,
        oid: &Bound<'py, PyAny>,
        format: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let dumpers = slf.borrow()._oid_dumpers.clone_ref(py);
        if !dumpers.bind(py).is_truthy()? {
            slf.borrow_mut()._oid_dumpers = PyTuple::new(py, [PyDict::new(py), PyDict::new(py)])?
                .into_any()
                .unbind();
        }
        let dumpers = slf.borrow()._oid_dumpers.clone_ref(py);
        let cache = dumpers.bind(py).get_item(format)?;
        if let Some(dumper) = lookup(&cache, oid)? {
            return Ok(dumper);
        }
        let cls = slf
            .getattr(pyo3::intern!(py, "adapters"))?
            .call_method1(pyo3::intern!(py, "get_dumper_by_oid"), (oid, format))?;
        let dumper = cls.call1((py.None().bind(py).get_type(), slf))?;
        cache.set_item(oid, &dumper)?;
        Ok(dumper)
    }

    fn _get_none_oid<'py>(slf: &Bound<'py, Self>) -> PyResult<i64> {
        let py = slf.py();
        if slf.borrow()._none_oid < 0 {
            let api = slf.borrow()._native_api.clone_ref(py);
            let text = api.bind(py).get_item(3)?;
            let adapters = slf.borrow()._adapters.clone_ref(py);
            let result = adapters
                .bind(py)
                .call_method1(
                    pyo3::intern!(py, "get_dumper"),
                    (py.None().bind(py).get_type(), text),
                )
                .and_then(|dumper| dumper.getattr(pyo3::intern!(py, "oid")))
                .and_then(|oid| oid.extract::<i64>());
            let oid = match result {
                Err(error) if error.is_instance_of::<PyKeyError>(py) => {
                    return Err(Self::error(slf, "InterfaceError", "None dumper not found")?);
                }
                other => other?,
            };
            slf.borrow_mut()._none_oid = oid;
        }
        Ok(slf.borrow()._none_oid)
    }

    fn dump_sequence<'py>(
        slf: &Bound<'py, Self>,
        params: &Bound<'py, PyAny>,
        formats: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let py = slf.py();
        let count = params.len()?;
        let out = PyList::new(py, (0..count).map(|_| py.None()))?;
        let pinned = slf.borrow()._row_dumpers.clone_ref(py);
        if pinned.bind(py).is_truthy()? {
            let pinned = slf.borrow()._row_dumpers.clone_ref(py);
            let expected = pinned.bind(py).len()?;
            if expected != count {
                return Err(Self::error(
                    slf,
                    "DataError",
                    &format!("expected {expected} values in row, got {count}"),
                )?);
            }
            for index in 0..count {
                let param = params.get_item(index)?;
                if !param.is_none() {
                    let pinned = slf.borrow()._row_dumpers.clone_ref(py);
                    let dumper = pinned.bind(py).get_item(index)?;
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
        let api = slf.borrow()._native_api.clone_ref(py);
        let text = api.bind(py).get_item(2)?;
        for index in 0..count {
            let param = params.get_item(index)?;
            if param.is_none() {
                types.push(slf.call_method0(pyo3::intern!(py, "_get_none_oid"))?);
                pqformats.push(text.clone());
                continue;
            }
            let format = formats.get_item(index)?;
            let dumper = slf.call_method1(pyo3::intern!(py, "get_dumper"), (&param, &format))?;
            out.set_item(
                index,
                dumper.call_method1(pyo3::intern!(py, "dump"), (&param,))?,
            )?;
            types.push(dumper.getattr(pyo3::intern!(py, "oid"))?);
            pqformats.push(dumper.getattr(pyo3::intern!(py, "format"))?);
        }
        slf.setattr(pyo3::intern!(py, "types"), PyTuple::new(py, types)?)?;
        slf.setattr(pyo3::intern!(py, "formats"), PyList::new(py, pqformats)?)?;
        Ok(out)
    }
}
