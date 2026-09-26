use num_bigint::BigInt;
use pyo3::exceptions::{PyBaseException, PyIndexError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::pyclass::{PyTraverseError, PyVisit};
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3::wrap_pyfunction;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, TryLockError, Weak};
use std::thread;
use std::time::Duration;

use crate::preparing::{
    ExecutionConfiguration, ExecutionError, ExecutionReservation, ExecutionState,
    NativePreparationState, SharedExecutionConfiguration,
};
use crate::python_helpers::psycopg_import;

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendConninfoSummary {
    #[pyo3(get)]
    user: Option<String>,
    #[pyo3(get)]
    dbname: Option<String>,
    #[pyo3(get)]
    application_name: Option<String>,
    #[pyo3(get)]
    host_count: usize,
    #[pyo3(get)]
    hostaddr_count: usize,
    #[pyo3(get)]
    port_count: usize,
    #[pyo3(get)]
    has_password: bool,
    #[pyo3(get)]
    connect_timeout_seconds: Option<u64>,
    #[pyo3(get)]
    effective_connect_timeout_seconds: u64,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendConnectPlan {
    #[pyo3(get)]
    backend_stack: String,
    #[pyo3(get)]
    sync_client: String,
    #[pyo3(get)]
    async_client: String,
    #[pyo3(get)]
    sync_runtime: String,
    #[pyo3(get)]
    async_runtime: String,
    #[pyo3(get)]
    tls_mode: String,
    #[pyo3(get)]
    tls_negotiation: String,
    #[pyo3(get)]
    tls_connector_hint: String,
    #[pyo3(get)]
    target_session_attrs: String,
    #[pyo3(get)]
    load_balance_hosts: String,
    #[pyo3(get)]
    can_bootstrap_with_no_tls: bool,
    #[pyo3(get)]
    requires_external_tls_connector: bool,
    #[pyo3(get)]
    summary: BackendConninfoSummary,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendConnectEndpoint {
    #[pyo3(get)]
    transport: String,
    #[pyo3(get)]
    target: String,
    #[pyo3(get)]
    hostaddr: Option<String>,
    #[pyo3(get)]
    port: u16,
    #[pyo3(get)]
    inferred: bool,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendConnectTarget {
    #[pyo3(get)]
    backend_stack: String,
    #[pyo3(get)]
    sync_client: String,
    #[pyo3(get)]
    async_client: String,
    #[pyo3(get)]
    sync_runtime: String,
    #[pyo3(get)]
    async_runtime: String,
    #[pyo3(get)]
    tls_mode: String,
    #[pyo3(get)]
    tls_negotiation: String,
    #[pyo3(get)]
    tls_connector_hint: String,
    #[pyo3(get)]
    target_session_attrs: String,
    #[pyo3(get)]
    load_balance_hosts: String,
    #[pyo3(get)]
    can_bootstrap_with_no_tls: bool,
    #[pyo3(get)]
    requires_external_tls_connector: bool,
    #[pyo3(get)]
    endpoints: Vec<BackendConnectEndpoint>,
    #[pyo3(get)]
    summary: BackendConninfoSummary,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendSyncNoTlsProbe {
    #[pyo3(get)]
    backend_pid: i32,
    #[pyo3(get)]
    current_user: String,
    #[pyo3(get)]
    current_database: String,
    #[pyo3(get)]
    server_version_num: i32,
    #[pyo3(get)]
    application_name: String,
    #[pyo3(get)]
    client_encoding: String,
    #[pyo3(get)]
    server_address: Option<String>,
    #[pyo3(get)]
    server_port: Option<u16>,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendNotification {
    #[pyo3(get)]
    process_id: i32,
    #[pyo3(get)]
    channel: String,
    #[pyo3(get)]
    payload: String,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendTextQueryResult {
    #[pyo3(get)]
    columns: Vec<String>,
    #[pyo3(get)]
    rows: Vec<Vec<Option<String>>>,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendResultSet {
    #[pyo3(get)]
    columns: Vec<String>,
    #[pyo3(get)]
    column_descriptions: Vec<BackendStatementColumn>,
    rows: Vec<ferrocopg_postgres::WireRow>,
    #[pyo3(get)]
    rows_affected: u64,
    #[pyo3(get)]
    is_tuples: bool,
    #[pyo3(get)]
    wire_format: u8,
    #[pyo3(get)]
    command_tag: Option<String>,
    #[pyo3(get)]
    transaction_status: Option<u8>,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg", frozen)]
struct BackendPgResult {
    #[pyo3(get)]
    _result: Py<BackendResultSet>,
    #[pyo3(get)]
    _encoding: String,
    #[pyo3(get)]
    _format: u8,
    #[pyo3(get)]
    status: u8,
    #[pyo3(get)]
    nfields: usize,
    #[pyo3(get)]
    ntuples: usize,
    #[pyo3(get)]
    command_status: Py<PyBytes>,
}

#[pymethods]
impl BackendPgResult {
    fn fname(&self, py: Python<'_>, index: isize) -> PyResult<Py<PyBytes>> {
        let adjusted = if index < 0 {
            self.nfields as isize + index
        } else {
            index
        };
        if adjusted < 0 || adjusted as usize >= self.nfields {
            return Err(PyIndexError::new_err(index));
        }
        let result = self._result.borrow(py);
        PyString::new(py, &result.columns[adjusted as usize])
            .call_method1("encode", (&self._encoding,))?
            .cast_into::<PyBytes>()
            .map(Bound::unbind)
            .map_err(Into::into)
    }

    fn fformat(&self, index: isize) -> PyResult<u8> {
        if index < 0 || index as usize >= self.nfields {
            return Err(PyIndexError::new_err(index));
        }
        Ok(self._format)
    }

    fn ftype(&self, py: Python<'_>, index: isize) -> PyResult<u32> {
        if index < 0 || index as usize >= self.nfields {
            return Ok(0);
        }
        self._result.borrow(py).column_oid(index as usize)
    }

    fn get_value(
        &self,
        py: Python<'_>,
        row: usize,
        column: usize,
    ) -> PyResult<Option<Py<PyBytes>>> {
        self._result.borrow(py).get_value(py, row, column)
    }
}

fn load_result_row<'py>(
    py: Python<'py>,
    row: &ferrocopg_postgres::WireRow,
    codes: &[u8],
    loaders: &[Py<PyAny>],
    make_row: &Bound<'py, PyAny>,
    tuple_row: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let values = row
        .iter()
        .zip(codes)
        .zip(loaders)
        .map(|((data, code), loader)| crate::adapt::LoadedWireValue {
            data,
            code: *code,
            loader,
        });
    let values = PyTuple::new(py, values)?;
    if tuple_row {
        Ok(values.into_any())
    } else {
        make_row.call1((values,))
    }
}

struct LoadedResultRow<'py>(PyResult<Bound<'py, PyAny>>);

impl<'py> IntoPyObject<'py> for LoadedResultRow<'py> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, _py: Python<'py>) -> PyResult<Self::Output> {
        self.0
    }
}

#[pymethods]
impl BackendResultSet {
    fn as_pgresult(
        slf: Py<Self>,
        py: Python<'_>,
        encoding: String,
        format: u8,
        command_status: Py<PyBytes>,
    ) -> BackendPgResult {
        let result = slf.borrow(py);
        let status = if result.command_tag.as_deref() == Some("") {
            0
        } else if result.is_tuples {
            2
        } else {
            1
        };
        let nfields = result.columns.len();
        let ntuples = result.rows.len();
        drop(result);
        BackendPgResult {
            _result: slf,
            _encoding: encoding,
            _format: format,
            status,
            nfields,
            ntuples,
            command_status,
        }
    }

    #[getter]
    fn column_count(&self) -> usize {
        self.columns.len()
    }

    #[getter]
    fn column_oids(&self) -> Vec<u32> {
        self.column_descriptions
            .iter()
            .map(|column| column.oid)
            .collect()
    }

    fn column_name(&self, index: usize) -> PyResult<&str> {
        self.columns
            .get(index)
            .map(String::as_str)
            .ok_or_else(|| PyIndexError::new_err(index))
    }

    fn column_oid(&self, index: usize) -> PyResult<u32> {
        self.column_descriptions
            .get(index)
            .map(|column| column.oid)
            .ok_or_else(|| PyIndexError::new_err(index))
    }

    #[getter]
    fn rows(&self) -> Vec<Vec<Option<Vec<u8>>>> {
        self.rows
            .iter()
            .map(|row| row.iter().map(|value| value.map(<[u8]>::to_vec)).collect())
            .collect()
    }

    #[getter]
    fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn row(&self, index: usize) -> PyResult<Vec<Option<Vec<u8>>>> {
        self.rows
            .get(index)
            .map(|row| row.iter().map(|value| value.map(<[u8]>::to_vec)).collect())
            .ok_or_else(|| PyIndexError::new_err(index))
    }

    fn get_value(
        &self,
        py: Python<'_>,
        row: usize,
        column: usize,
    ) -> PyResult<Option<Py<PyBytes>>> {
        let value = self
            .rows
            .get(row)
            .and_then(|r| r.get(column))
            .ok_or_else(|| PyIndexError::new_err((row, column)))?;
        Ok(value.map(|v| PyBytes::new(py, v).unbind()))
    }

    fn load_row<'py>(
        &self,
        py: Python<'py>,
        index: usize,
        codes: Vec<u8>,
        loaders: Vec<Py<PyAny>>,
        make_row: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let row = self
            .rows
            .get(index)
            .ok_or_else(|| PyIndexError::new_err(index))?;
        if codes.len() != self.columns.len() || loaders.len() != codes.len() {
            return Err(PyValueError::new_err("loader count does not match columns"));
        }
        load_result_row(
            py,
            row,
            &codes,
            &loaders,
            make_row,
            make_row.is(&py.get_type::<PyTuple>()),
        )
    }

    fn load_rows<'py>(
        &self,
        py: Python<'py>,
        start: usize,
        end: usize,
        codes: Vec<u8>,
        loaders: Vec<Py<PyAny>>,
        make_row: &Bound<'py, PyAny>,
    ) -> PyResult<Py<PyList>> {
        if start > end || end > self.rows.len() {
            return Err(PyValueError::new_err("invalid result row range"));
        }
        if codes.len() != self.columns.len() || loaders.len() != codes.len() {
            return Err(PyValueError::new_err("loader count does not match columns"));
        }
        let tuple_rows = make_row.is(&py.get_type::<PyTuple>());
        let rows = self.rows[start..end].iter().map(|row| {
            LoadedResultRow(load_result_row(
                py, row, &codes, &loaders, make_row, tuple_rows,
            ))
        });
        Ok(PyList::new(py, rows)?.unbind())
    }
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendSimpleQueryMessage {
    #[pyo3(get)]
    kind: String,
    #[pyo3(get)]
    columns: Vec<String>,
    #[pyo3(get)]
    values: Vec<Option<String>>,
    #[pyo3(get)]
    rows_affected: Option<u64>,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendSimpleQueryResult {
    #[pyo3(get)]
    columns: Vec<String>,
    #[pyo3(get)]
    column_descriptions: Vec<BackendStatementColumn>,
    #[pyo3(get)]
    rows: Vec<Vec<Option<String>>>,
    #[pyo3(get)]
    rows_affected: u64,
    #[pyo3(get)]
    is_tuples: bool,
    #[pyo3(get)]
    wire_format: u8,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendExecuteResult {
    #[pyo3(get)]
    rows_affected: u64,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendCopyOutResult {
    #[pyo3(get)]
    data: Vec<u8>,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendStatementParameter {
    #[pyo3(get)]
    oid: u32,
    #[pyo3(get)]
    type_name: String,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendStatementColumn {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    oid: u32,
    #[pyo3(get)]
    type_name: String,
    #[pyo3(get)]
    is_enum: bool,
    #[pyo3(get)]
    type_modifier: i32,
    #[pyo3(get)]
    type_size: i16,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendStatementDescription {
    #[pyo3(get)]
    params: Vec<BackendStatementParameter>,
    #[pyo3(get)]
    columns: Vec<BackendStatementColumn>,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg", skip_from_py_object)]
struct BackendPreparedStatementInfo {
    #[pyo3(get)]
    statement_id: u64,
    #[pyo3(get)]
    description: BackendStatementDescription,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendSyncNoTlsCancelHandle {
    inner: Mutex<ferrocopg_postgres::SyncNoTlsCancelHandle>,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendSyncNoTlsSession {
    inner: Arc<Mutex<OwnedSession>>,
    configuration: SharedExecutionConfiguration,
    signal_error: Arc<Mutex<Option<PyErr>>>,
    wait_callback: Arc<dyn Fn() + Sync + Send>,
    used_password: bool,
    backend_pid: Option<i32>,
    client_encoding: Option<String>,
}

struct OwnedSession {
    session: ferrocopg_postgres::SyncNoTlsSession,
    execution: ExecutionState,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg", frozen)]
struct BackendExecutionReservation {
    owner: Weak<Mutex<OwnedSession>>,
    reservation: Arc<ExecutionReservation>,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendExecutionOutcome {
    #[pyo3(get)]
    result: Option<Py<BackendResultSet>>,
    #[pyo3(get)]
    error: Option<Py<PyBaseException>>,
    #[pyo3(get)]
    preflight_failed: bool,
    #[pyo3(get)]
    encoding: Option<String>,
    #[pyo3(get)]
    statusmessage: Option<Py<PyString>>,
    #[pyo3(get, set, name = "_pos")]
    position: usize,
    pgresults_cache: Option<(String, u8, Py<PyList>)>,
    notices: Vec<ferrocopg_postgres::PostgresDiagnostic>,
    #[pyo3(get)]
    notifications: Vec<BackendNotification>,
}

#[pymethods]
impl BackendExecutionOutcome {
    #[getter]
    fn current_result(&self, py: Python<'_>) -> Option<Py<BackendResultSet>> {
        self.result.as_ref().map(|result| result.clone_ref(py))
    }

    #[getter(_index)]
    fn index(&self) -> isize {
        if self.result.is_some() { 0 } else { -1 }
    }

    #[getter]
    fn columns(&self, py: Python<'_>) -> Vec<String> {
        self.result
            .as_ref()
            .map_or_else(Vec::new, |result| result.borrow(py).columns.clone())
    }

    #[getter]
    fn rows_affected(&self, py: Python<'_>) -> i128 {
        self.result
            .as_ref()
            .map_or(-1, |result| i128::from(result.borrow(py).rows_affected))
    }

    fn set_encoding(&mut self, encoding: Option<String>) {
        self.encoding = encoding;
        self.pgresults_cache = None;
    }

    fn pgresults(&mut self, py: Python<'_>, encoding: String, format: u8) -> PyResult<Py<PyList>> {
        if let Some((cached_encoding, cached_format, results)) = &self.pgresults_cache {
            if *cached_encoding == encoding && *cached_format == format {
                return Ok(results.clone_ref(py));
            }
        }
        let results = PyList::empty(py);
        if let Some(result) = &self.result {
            let status = {
                let result = result.borrow(py);
                PyBytes::new(py, result.command_tag.as_deref().unwrap_or("").as_bytes()).unbind()
            };
            let pgresult = BackendResultSet::as_pgresult(
                result.clone_ref(py),
                py,
                self.encoding.as_ref().unwrap_or(&encoding).clone(),
                format,
                status,
            );
            results.append(Py::new(py, pgresult)?)?;
        }
        let results = results.unbind();
        self.pgresults_cache = Some((encoding, format, results.clone_ref(py)));
        Ok(results)
    }

    fn fetchone(&mut self, py: Python<'_>) -> Option<Vec<Option<Vec<u8>>>> {
        let result = self.result.as_ref()?.borrow(py);
        let row = result.rows.get(self.position)?;
        self.position += 1;
        Some(row.iter().map(|value| value.map(<[u8]>::to_vec)).collect())
    }

    fn fetchall(&mut self, py: Python<'_>) -> Vec<Vec<Option<Vec<u8>>>> {
        let Some(result) = self.result.as_ref() else {
            return Vec::new();
        };
        let result = result.borrow(py);
        let rows = result
            .rows
            .iter()
            .skip(self.position)
            .map(|row| row.iter().map(|value| value.map(<[u8]>::to_vec)).collect())
            .collect();
        self.position = result.rows.len();
        rows
    }

    fn nextset(&self) -> Option<bool> {
        None
    }

    fn set_result(slf: Py<Self>, py: Python<'_>, index: isize) -> PyResult<Py<Self>> {
        {
            let mut state = slf.borrow_mut(py);
            let count = usize::from(state.result.is_some());
            if count == 0 || !matches!(index, -1 | 0) {
                return Err(PyIndexError::new_err(format!(
                    "index {index} out of range: {count} result(s) available"
                )));
            }
            state.position = 0;
        }
        Ok(slf)
    }

    fn results(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let results = PyList::empty(py);
        if slf.borrow(py).result.is_some() {
            results.append(slf)?;
        }
        Ok(results.call_method0("__iter__")?.unbind())
    }

    #[getter]
    fn notices(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        self.notices
            .iter()
            .map(|notice| backend_diagnostic_info(py, notice).map(Bound::unbind))
            .collect()
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        visit.call(&self.result)?;
        visit.call(&self.error)?;
        visit.call(&self.statusmessage)?;
        if let Some((_, _, results)) = &self.pgresults_cache {
            visit.call(results)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        self.result = None;
        self.error = None;
        self.statusmessage = None;
        self.pgresults_cache = None;
        self.notices.clear();
        self.notifications.clear();
    }
}

impl BackendSyncNoTlsSession {
    fn new(session: ferrocopg_postgres::SyncNoTlsSession) -> Self {
        let used_password = session.used_password();
        let backend_pid = session.backend_pid().ok();
        let client_encoding = session.parameter("client_encoding");
        let signal_error = Arc::new(Mutex::new(None));
        let signals = Arc::clone(&signal_error);
        let cancel_handle = session.cancel_handle().ok();
        let wait_callback = Arc::new(move || {
            let mut signal = signals.lock().unwrap();
            if signal.is_some() {
                return;
            }
            if let Err(err) = Python::attach(|py| py.check_signals()) {
                *signal = Some(err);
                if let Some(handle) = cancel_handle.as_ref() {
                    // The hook runs outside the query runtime. Drain the
                    // cancelled operation before returning its Python error.
                    let _ = handle.cancel_timeout(Duration::from_secs(1));
                }
            }
        });
        Self {
            inner: Arc::new(Mutex::new(OwnedSession {
                session,
                execution: ExecutionState::new(),
            })),
            configuration: Mutex::new(Arc::new(ExecutionConfiguration::new(
                Some(BigInt::from(5)),
                BigInt::from(100),
            ))),
            signal_error,
            wait_callback,
            used_password,
            backend_pid,
            client_encoding,
        }
    }
}

fn backend_runtime_error(message: impl Into<String>) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(message.into())
}

fn bound_params(params: Vec<(u32, bool, Option<Vec<u8>>)>) -> Vec<ferrocopg_postgres::BoundParam> {
    params
        .into_iter()
        .map(|(oid, binary, value)| ferrocopg_postgres::BoundParam {
            oid,
            value,
            format: if binary {
                ferrocopg_postgres::ParamFormat::Binary
            } else {
                ferrocopg_postgres::ParamFormat::Text
            },
        })
        .collect()
}

fn wire_format(binary: bool) -> ferrocopg_postgres::WireFormat {
    if binary {
        ferrocopg_postgres::WireFormat::Binary
    } else {
        ferrocopg_postgres::WireFormat::Text
    }
}

enum BackendThreadError {
    Runtime(String),
    Backend(ferrocopg_postgres::ProbeError),
}

fn backend_error_sqlstate(err: &ferrocopg_postgres::ProbeError) -> Option<&str> {
    match err {
        ferrocopg_postgres::ProbeError::Parse(err) => {
            err.as_db_error().map(|db_err| db_err.code().code())
        }
        ferrocopg_postgres::ProbeError::Connect(err)
        | ferrocopg_postgres::ProbeError::Query(err) => {
            err.as_db_error().map(|db_err| db_err.code().code())
        }
        ferrocopg_postgres::ProbeError::BadParam(_)
        | ferrocopg_postgres::ProbeError::TlsConfig(_)
        | ferrocopg_postgres::ProbeError::Closed
        | ferrocopg_postgres::ProbeError::NoTlsNotSupported => None,
    }
}

fn backend_fallback_error_name(err: &ferrocopg_postgres::ProbeError) -> &'static str {
    match err {
        ferrocopg_postgres::ProbeError::NoTlsNotSupported => "NotSupportedError",
        ferrocopg_postgres::ProbeError::Connect(_)
        | ferrocopg_postgres::ProbeError::TlsConfig(_)
        | ferrocopg_postgres::ProbeError::Closed => "OperationalError",
        ferrocopg_postgres::ProbeError::Query(err) if err.as_db_error().is_none() => {
            "OperationalError"
        }
        ferrocopg_postgres::ProbeError::BadParam(_)
        | ferrocopg_postgres::ProbeError::Parse(_)
        | ferrocopg_postgres::ProbeError::Query(_) => "ProgrammingError",
    }
}

fn set_diagnostic_field(
    info: &Bound<'_, PyDict>,
    fields: &Bound<'_, PyAny>,
    name: &str,
    raw_value: Option<&[u8]>,
    fallback: Option<&str>,
) -> PyResult<()> {
    if let Some(value) = raw_value.or_else(|| fallback.map(str::as_bytes)) {
        info.set_item(fields.getattr(name)?, PyBytes::new(info.py(), value))?;
    }
    Ok(())
}

fn backend_diagnostic_info<'py>(
    py: Python<'py>,
    diagnostic: &ferrocopg_postgres::PostgresDiagnostic,
) -> PyResult<Bound<'py, PyDict>> {
    let fields = psycopg_import(py, "pq")?.getattr("DiagnosticField")?;
    let info = PyDict::new(py);
    set_diagnostic_field(
        &info,
        &fields,
        "SEVERITY",
        diagnostic.raw_field(b'S'),
        diagnostic.severity.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "SEVERITY_NONLOCALIZED",
        diagnostic.raw_field(b'V'),
        diagnostic.severity_nonlocalized.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "SQLSTATE",
        diagnostic.raw_field(b'C'),
        Some(&diagnostic.sqlstate),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "MESSAGE_PRIMARY",
        diagnostic.raw_field(b'M'),
        Some(&diagnostic.message_primary),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "MESSAGE_DETAIL",
        diagnostic.raw_field(b'D'),
        diagnostic.message_detail.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "MESSAGE_HINT",
        diagnostic.raw_field(b'H'),
        diagnostic.message_hint.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "STATEMENT_POSITION",
        diagnostic.raw_field(b'P'),
        diagnostic.statement_position.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "INTERNAL_POSITION",
        diagnostic.raw_field(b'p'),
        diagnostic.internal_position.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "INTERNAL_QUERY",
        diagnostic.raw_field(b'q'),
        diagnostic.internal_query.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "CONTEXT",
        diagnostic.raw_field(b'W'),
        diagnostic.context.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "SCHEMA_NAME",
        diagnostic.raw_field(b's'),
        diagnostic.schema_name.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "TABLE_NAME",
        diagnostic.raw_field(b't'),
        diagnostic.table_name.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "COLUMN_NAME",
        diagnostic.raw_field(b'c'),
        diagnostic.column_name.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "DATATYPE_NAME",
        diagnostic.raw_field(b'd'),
        diagnostic.datatype_name.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "CONSTRAINT_NAME",
        diagnostic.raw_field(b'n'),
        diagnostic.constraint_name.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "SOURCE_FILE",
        diagnostic.raw_field(b'F'),
        diagnostic.source_file.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "SOURCE_LINE",
        diagnostic.raw_field(b'L'),
        diagnostic.source_line.as_deref(),
    )?;
    set_diagnostic_field(
        &info,
        &fields,
        "SOURCE_FUNCTION",
        diagnostic.raw_field(b'R'),
        diagnostic.source_function.as_deref(),
    )?;
    Ok(info)
}

fn backend_error_info<'py>(
    py: Python<'py>,
    err: &ferrocopg_postgres::ProbeError,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    err.diagnostic()
        .as_ref()
        .map(|diagnostic| backend_diagnostic_info(py, diagnostic))
        .transpose()
}

fn psycopg_error_from_type(
    py: Python<'_>,
    exc_type: &Bound<'_, PyAny>,
    message: &str,
    info: Option<&Bound<'_, PyDict>>,
) -> PyErr {
    let exc = match info {
        Some(info) => {
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("info", info)
                .and_then(|()| exc_type.call((message,), Some(&kwargs)))
        }
        None => exc_type.call1((message,)),
    };

    match exc {
        Ok(exc) => PyErr::from_value(exc),
        Err(_) => backend_runtime_error(message.to_owned()),
    }
}

fn backend_py_error(py: Python<'_>, err: ferrocopg_postgres::ProbeError) -> PyErr {
    let message = err.to_string();
    let info = backend_error_info(py, &err).ok().flatten();
    let Ok(errors) = psycopg_import(py, "errors") else {
        return backend_runtime_error(message);
    };

    if let Some(sqlstate) = backend_error_sqlstate(&err) {
        if let Ok(exc_type) = errors
            .getattr("lookup")
            .and_then(|lookup| lookup.call1((sqlstate,)))
        {
            return psycopg_error_from_type(py, &exc_type, &message, info.as_ref());
        }
    }

    match errors.getattr(backend_fallback_error_name(&err)) {
        Ok(exc_type) => psycopg_error_from_type(py, &exc_type, &message, info.as_ref()),
        Err(_) => backend_runtime_error(message),
    }
}

fn map_backend_result<T>(
    py: Python<'_>,
    result: Result<T, ferrocopg_postgres::ProbeError>,
) -> PyResult<T> {
    result.map_err(|err| backend_py_error(py, err))
}

fn with_session<T, F>(py: Python<'_>, session: &BackendSyncNoTlsSession, f: F) -> PyResult<T>
where
    T: Send + 'static,
    F: FnOnce(
            &mut ferrocopg_postgres::SyncNoTlsSession,
        ) -> Result<T, ferrocopg_postgres::ProbeError>
        + Send
        + 'static,
{
    let (result, signal_error) =
        with_owned_session(py, session, move |inner| f(&mut inner.session));
    finish_owned_session(py, result, signal_error)
}

fn finish_owned_session<T>(
    py: Python<'_>,
    result: Result<T, BackendThreadError>,
    signal_error: Option<PyErr>,
) -> PyResult<T> {
    if let Some(err) = signal_error {
        return Err(err);
    }
    match result {
        Ok(value) => Ok(value),
        Err(BackendThreadError::Runtime(message)) => Err(backend_runtime_error(message)),
        Err(BackendThreadError::Backend(err)) => Err(backend_py_error(py, err)),
    }
}

fn with_owned_session<T, F>(
    py: Python<'_>,
    session: &BackendSyncNoTlsSession,
    f: F,
) -> (Result<T, BackendThreadError>, Option<PyErr>)
where
    T: Send,
    F: FnOnce(&mut OwnedSession) -> Result<T, ferrocopg_postgres::ProbeError> + Send,
{
    py.detach(|| {
        let mut inner = match session.inner.lock() {
            Ok(inner) => inner,
            Err(_) => {
                return (
                    Err(BackendThreadError::Runtime(
                        "backend session mutex is poisoned".to_owned(),
                    )),
                    None,
                );
            }
        };
        inner
            .session
            .set_wait_callback(Some(Arc::clone(&session.wait_callback)));
        let result = f(&mut inner).map_err(BackendThreadError::Backend);
        inner.session.set_wait_callback(None);
        // Transfer the error before unlocking: another operation must never
        // consume or overwrite the exception from this operation.
        let signal_error = session.signal_error.lock().unwrap().take();
        (result, signal_error)
    })
}

fn with_cancel_handle<T, F>(
    py: Python<'_>,
    handle: &BackendSyncNoTlsCancelHandle,
    f: F,
) -> PyResult<T>
where
    T: Send,
    F: FnOnce(
            &ferrocopg_postgres::SyncNoTlsCancelHandle,
        ) -> Result<T, ferrocopg_postgres::ProbeError>
        + Send,
{
    let result = py.detach(|| {
        let inner = handle.inner.lock().map_err(|_| {
            BackendThreadError::Runtime("backend cancel handle mutex is poisoned".to_owned())
        })?;
        f(&inner).map_err(BackendThreadError::Backend)
    });
    match result {
        Ok(value) => Ok(value),
        Err(BackendThreadError::Runtime(message)) => Err(backend_runtime_error(message)),
        Err(BackendThreadError::Backend(err)) => Err(backend_py_error(py, err)),
    }
}

#[pyfunction]
fn milestone() -> &'static str {
    "milestone-1-bootstrap"
}

#[pyfunction]
fn scaffold_status() -> &'static str {
    "ferrocopg Rust extension scaffold is wired through maturin"
}

#[pyfunction]
fn backend_stack() -> &'static str {
    ferrocopg_postgres::backend_stack()
}

#[pyfunction]
fn backend_core() -> &'static str {
    ferrocopg_postgres::backend_core()
}

#[pyfunction]
fn parse_conninfo_summary(conninfo: &str) -> PyResult<BackendConninfoSummary> {
    ferrocopg_postgres::bootstrap_summary(conninfo)
        .map(BackendConninfoSummary::from)
        .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
}

#[pyfunction]
fn parse_connect_plan(conninfo: &str) -> PyResult<BackendConnectPlan> {
    ferrocopg_postgres::connect_plan(conninfo)
        .map(BackendConnectPlan::from)
        .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
}

#[pyfunction]
fn parse_connect_target(conninfo: &str) -> PyResult<BackendConnectTarget> {
    ferrocopg_postgres::connect_target(conninfo)
        .map(BackendConnectTarget::from)
        .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
}

#[pyfunction]
fn probe_connect_no_tls(py: Python<'_>, conninfo: &str) -> PyResult<BackendSyncNoTlsProbe> {
    map_backend_result(py, ferrocopg_postgres::connect_no_tls_probe(conninfo))
        .map(BackendSyncNoTlsProbe::from)
}

#[pyfunction]
fn query_text_no_tls(
    py: Python<'_>,
    conninfo: &str,
    query: &str,
) -> PyResult<BackendTextQueryResult> {
    map_backend_result(py, ferrocopg_postgres::query_text_no_tls(conninfo, query))
        .map(BackendTextQueryResult::from)
}

#[pyfunction]
fn simple_query_no_tls(
    py: Python<'_>,
    conninfo: &str,
    query: &str,
) -> PyResult<Vec<BackendSimpleQueryMessage>> {
    map_backend_result(py, ferrocopg_postgres::simple_query_no_tls(conninfo, query)).map(
        |messages| {
            messages
                .into_iter()
                .map(BackendSimpleQueryMessage::from)
                .collect()
        },
    )
}

#[pyfunction]
fn simple_query_results_no_tls(
    py: Python<'_>,
    conninfo: &str,
    query: &str,
) -> PyResult<Vec<BackendSimpleQueryResult>> {
    map_backend_result(
        py,
        ferrocopg_postgres::simple_query_results_no_tls(conninfo, query),
    )
    .map(|results| {
        results
            .into_iter()
            .map(BackendSimpleQueryResult::from)
            .collect()
    })
}

#[pyfunction]
fn pipeline_simple_query_results_no_tls(
    py: Python<'_>,
    conninfo: &str,
    queries: Vec<String>,
) -> PyResult<Vec<Vec<BackendSimpleQueryResult>>> {
    map_backend_result(
        py,
        ferrocopg_postgres::pipeline_simple_query_results_no_tls(conninfo, &queries),
    )
    .map(|batches| {
        batches
            .into_iter()
            .map(|results| {
                results
                    .into_iter()
                    .map(BackendSimpleQueryResult::from)
                    .collect()
            })
            .collect()
    })
}

#[pyfunction]
fn query_text_params_no_tls(
    py: Python<'_>,
    conninfo: &str,
    query: &str,
    params: Vec<Option<String>>,
) -> PyResult<BackendTextQueryResult> {
    map_backend_result(
        py,
        ferrocopg_postgres::query_text_params_no_tls(conninfo, query, &params),
    )
    .map(BackendTextQueryResult::from)
}

#[pyfunction]
fn run_text_params_no_tls(
    py: Python<'_>,
    conninfo: &str,
    query: &str,
    params: Vec<Option<String>>,
) -> PyResult<BackendResultSet> {
    map_backend_result(
        py,
        ferrocopg_postgres::run_text_params_no_tls(conninfo, query, &params),
    )
    .map(BackendResultSet::from)
}

#[pyfunction]
fn execute_text_params_no_tls(
    py: Python<'_>,
    conninfo: &str,
    query: &str,
    params: Vec<Option<String>>,
) -> PyResult<BackendExecuteResult> {
    map_backend_result(
        py,
        ferrocopg_postgres::execute_text_params_no_tls(conninfo, query, &params),
    )
    .map(BackendExecuteResult::from)
}

#[pyfunction]
fn describe_text_no_tls(
    py: Python<'_>,
    conninfo: &str,
    query: &str,
) -> PyResult<BackendStatementDescription> {
    map_backend_result(
        py,
        ferrocopg_postgres::describe_text_no_tls(conninfo, query),
    )
    .map(BackendStatementDescription::from)
}

#[pyfunction]
fn connect_no_tls_session(py: Python<'_>, conninfo: &str) -> PyResult<BackendSyncNoTlsSession> {
    map_backend_result(py, ferrocopg_postgres::connect_no_tls_session(conninfo))
        .map(BackendSyncNoTlsSession::new)
}

#[pyfunction]
fn connect_session(py: Python<'_>, conninfo: &str) -> PyResult<BackendSyncNoTlsSession> {
    let conninfo = conninfo.to_owned();
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = Arc::clone(&cancelled);
    let (result_sender, result_receiver) = mpsc::sync_channel(1);
    let worker = thread::Builder::new()
        .name("ferrocopg-connect".to_owned())
        .spawn(move || {
            let result =
                ferrocopg_postgres::connect_session_cancelable(&conninfo, &worker_cancelled);
            let _ = result_sender.send(result);
        })
        .map_err(|err| backend_runtime_error(format!("failed to start connect worker: {err}")))?;

    let result_receiver = Mutex::new(result_receiver);
    let mut signal_error = None;
    let result = loop {
        match py.detach(|| {
            result_receiver
                .lock()
                .map_err(|_| RecvTimeoutError::Disconnected)?
                .recv_timeout(Duration::from_millis(10))
        }) {
            Ok(result) => break result,
            Err(RecvTimeoutError::Timeout) if signal_error.is_none() => {
                if let Err(err) = py.check_signals() {
                    signal_error = Some(err);
                    cancelled.store(true, Ordering::Release);
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                let _ = worker.join();
                return Err(backend_runtime_error(
                    "backend connect worker dropped its result",
                ));
            }
        }
    };
    let _ = worker.join();

    if let Some(err) = signal_error {
        return Err(err);
    }

    map_backend_result(py, result).map(BackendSyncNoTlsSession::new)
}

impl From<ferrocopg_postgres::ConninfoSummary> for BackendConninfoSummary {
    fn from(summary: ferrocopg_postgres::ConninfoSummary) -> Self {
        Self {
            user: summary.user,
            dbname: summary.dbname,
            application_name: summary.application_name,
            host_count: summary.host_count,
            hostaddr_count: summary.hostaddr_count,
            port_count: summary.port_count,
            has_password: summary.has_password,
            connect_timeout_seconds: summary.connect_timeout_seconds,
            effective_connect_timeout_seconds: summary.effective_connect_timeout_seconds,
        }
    }
}

impl From<ferrocopg_postgres::ConnectPlan> for BackendConnectPlan {
    fn from(plan: ferrocopg_postgres::ConnectPlan) -> Self {
        Self {
            backend_stack: plan.backend_stack.to_owned(),
            sync_client: plan.sync_client.to_owned(),
            async_client: plan.async_client.to_owned(),
            sync_runtime: plan.sync_runtime.to_owned(),
            async_runtime: plan.async_runtime.to_owned(),
            tls_mode: plan.tls_mode.to_owned(),
            tls_negotiation: plan.tls_negotiation.to_owned(),
            tls_connector_hint: plan.tls_connector_hint.to_owned(),
            target_session_attrs: plan.target_session_attrs.to_owned(),
            load_balance_hosts: plan.load_balance_hosts.to_owned(),
            can_bootstrap_with_no_tls: plan.can_bootstrap_with_no_tls,
            requires_external_tls_connector: plan.requires_external_tls_connector,
            summary: plan.summary.into(),
        }
    }
}

impl From<ferrocopg_postgres::ConnectEndpoint> for BackendConnectEndpoint {
    fn from(endpoint: ferrocopg_postgres::ConnectEndpoint) -> Self {
        Self {
            transport: endpoint.transport.to_owned(),
            target: endpoint.target,
            hostaddr: endpoint.hostaddr,
            port: endpoint.port,
            inferred: endpoint.inferred,
        }
    }
}

impl From<ferrocopg_postgres::ConnectTarget> for BackendConnectTarget {
    fn from(target: ferrocopg_postgres::ConnectTarget) -> Self {
        Self {
            backend_stack: target.backend_stack.to_owned(),
            sync_client: target.sync_client.to_owned(),
            async_client: target.async_client.to_owned(),
            sync_runtime: target.sync_runtime.to_owned(),
            async_runtime: target.async_runtime.to_owned(),
            tls_mode: target.tls_mode.to_owned(),
            tls_negotiation: target.tls_negotiation.to_owned(),
            tls_connector_hint: target.tls_connector_hint.to_owned(),
            target_session_attrs: target.target_session_attrs.to_owned(),
            load_balance_hosts: target.load_balance_hosts.to_owned(),
            can_bootstrap_with_no_tls: target.can_bootstrap_with_no_tls,
            requires_external_tls_connector: target.requires_external_tls_connector,
            endpoints: target
                .endpoints
                .into_iter()
                .map(BackendConnectEndpoint::from)
                .collect(),
            summary: target.summary.into(),
        }
    }
}

impl From<ferrocopg_postgres::SyncNoTlsProbe> for BackendSyncNoTlsProbe {
    fn from(probe: ferrocopg_postgres::SyncNoTlsProbe) -> Self {
        Self {
            backend_pid: probe.backend_pid,
            current_user: probe.current_user,
            current_database: probe.current_database,
            server_version_num: probe.server_version_num,
            application_name: probe.application_name,
            client_encoding: probe.client_encoding,
            server_address: probe.server_address,
            server_port: probe.server_port,
        }
    }
}

impl From<ferrocopg_postgres::BackendNotification> for BackendNotification {
    fn from(notification: ferrocopg_postgres::BackendNotification) -> Self {
        Self {
            process_id: notification.process_id,
            channel: notification.channel,
            payload: notification.payload,
        }
    }
}

impl From<ferrocopg_postgres::TextQueryResult> for BackendTextQueryResult {
    fn from(result: ferrocopg_postgres::TextQueryResult) -> Self {
        Self {
            columns: result.columns,
            rows: result.rows,
        }
    }
}

impl From<ferrocopg_postgres::ResultSet> for BackendResultSet {
    fn from(result: ferrocopg_postgres::ResultSet) -> Self {
        Self {
            columns: result.columns,
            column_descriptions: result
                .column_descriptions
                .into_iter()
                .map(BackendStatementColumn::from)
                .collect(),
            rows: result.rows,
            rows_affected: result.rows_affected,
            is_tuples: result.is_tuples,
            command_tag: result.command_tag,
            transaction_status: result.transaction_status,
            wire_format: match result.wire_format {
                ferrocopg_postgres::WireFormat::Text => 0,
                ferrocopg_postgres::WireFormat::Binary => 1,
            },
        }
    }
}

impl From<ferrocopg_postgres::SimpleQueryMessage> for BackendSimpleQueryMessage {
    fn from(message: ferrocopg_postgres::SimpleQueryMessage) -> Self {
        Self {
            kind: message.kind.to_owned(),
            columns: message.columns,
            values: message.values,
            rows_affected: message.rows_affected,
        }
    }
}

impl From<ferrocopg_postgres::SimpleQueryResult> for BackendSimpleQueryResult {
    fn from(result: ferrocopg_postgres::SimpleQueryResult) -> Self {
        Self {
            columns: result.columns,
            column_descriptions: result
                .column_descriptions
                .into_iter()
                .map(BackendStatementColumn::from)
                .collect(),
            rows: result.rows,
            rows_affected: result.rows_affected,
            is_tuples: result.is_tuples,
            wire_format: ferrocopg_postgres::WireFormat::Text as u8,
        }
    }
}

impl From<ferrocopg_postgres::ExecuteResult> for BackendExecuteResult {
    fn from(result: ferrocopg_postgres::ExecuteResult) -> Self {
        Self {
            rows_affected: result.rows_affected,
        }
    }
}

impl From<ferrocopg_postgres::CopyOutResult> for BackendCopyOutResult {
    fn from(result: ferrocopg_postgres::CopyOutResult) -> Self {
        Self { data: result.data }
    }
}

impl From<ferrocopg_postgres::StatementParameter> for BackendStatementParameter {
    fn from(param: ferrocopg_postgres::StatementParameter) -> Self {
        Self {
            oid: param.oid,
            type_name: param.type_name,
        }
    }
}

impl From<ferrocopg_postgres::StatementColumn> for BackendStatementColumn {
    fn from(column: ferrocopg_postgres::StatementColumn) -> Self {
        Self {
            name: column.name,
            oid: column.oid,
            type_name: column.type_name,
            is_enum: column.is_enum,
            type_modifier: column.type_modifier,
            type_size: column.type_size,
        }
    }
}

impl From<ferrocopg_postgres::StatementDescription> for BackendStatementDescription {
    fn from(description: ferrocopg_postgres::StatementDescription) -> Self {
        Self {
            params: description
                .params
                .into_iter()
                .map(BackendStatementParameter::from)
                .collect(),
            columns: description
                .columns
                .into_iter()
                .map(BackendStatementColumn::from)
                .collect(),
        }
    }
}

impl From<ferrocopg_postgres::PreparedStatementInfo> for BackendPreparedStatementInfo {
    fn from(info: ferrocopg_postgres::PreparedStatementInfo) -> Self {
        Self {
            statement_id: info.statement_id,
            description: info.description.into(),
        }
    }
}

#[pymethods]
impl BackendSyncNoTlsCancelHandle {
    fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        with_cancel_handle(py, self, |handle| handle.cancel())
    }

    fn cancel_timeout(&self, py: Python<'_>, timeout: f64) -> PyResult<()> {
        let timeout = Duration::try_from_secs_f64(timeout.max(0.0))
            .map_err(|_| PyValueError::new_err("timeout must be finite"))?;
        with_cancel_handle(py, self, |handle| handle.cancel_timeout(timeout))
    }
}

#[pymethods]
impl BackendSyncNoTlsSession {
    #[getter]
    fn execution_prepare_threshold(&self) -> PyResult<Option<BigInt>> {
        Ok(self
            .configuration
            .lock()
            .map_err(|_| backend_runtime_error("execution configuration mutex is poisoned"))?
            .threshold
            .clone())
    }

    #[setter]
    fn set_execution_prepare_threshold(&self, value: Option<BigInt>) -> PyResult<()> {
        let mut current = self
            .configuration
            .lock()
            .map_err(|_| backend_runtime_error("execution configuration mutex is poisoned"))?;
        *current = Arc::new(ExecutionConfiguration::new(value, current.maximum.clone()));
        Ok(())
    }

    #[getter]
    fn execution_prepared_max(&self) -> PyResult<BigInt> {
        Ok(self
            .configuration
            .lock()
            .map_err(|_| backend_runtime_error("execution configuration mutex is poisoned"))?
            .maximum
            .clone())
    }

    #[setter]
    fn set_execution_prepared_max(&self, value: BigInt) -> PyResult<()> {
        let mut current = self
            .configuration
            .lock()
            .map_err(|_| backend_runtime_error("execution configuration mutex is poisoned"))?;
        *current = Arc::new(ExecutionConfiguration::new(
            current.threshold.clone(),
            value,
        ));
        Ok(())
    }

    #[pyo3(signature = (query, types, prepare=None))]
    fn reserve_execution(
        &self,
        py: Python<'_>,
        query: Vec<u8>,
        types: Vec<u32>,
        prepare: Option<bool>,
    ) -> PyResult<BackendExecutionReservation> {
        let (result, signal) = with_owned_session(py, self, |inner| {
            inner.execution.sync_configuration(&self.configuration)?;
            Ok(inner.execution.reserve(query, types, prepare))
        });
        Ok(BackendExecutionReservation {
            owner: Arc::downgrade(&self.inner),
            reservation: Arc::new(finish_owned_session(py, result, signal)?),
        })
    }

    fn cancel_execution_reservation(
        &self,
        py: Python<'_>,
        reservation: &BackendExecutionReservation,
    ) -> PyResult<()> {
        if !Weak::ptr_eq(&reservation.owner, &Arc::downgrade(&self.inner)) {
            return Err(PyValueError::new_err(
                "reservation belongs to another session",
            ));
        }
        let (result, signal) = with_owned_session(py, self, |inner| {
            inner.execution.cancel_reservation(&reservation.reservation);
            Ok(())
        });
        finish_owned_session(py, result, signal)
    }

    #[pyo3(signature = (prepare_threshold, prepared_max))]
    fn configure_execution(
        &self,
        prepare_threshold: Option<BigInt>,
        prepared_max: BigInt,
    ) -> PyResult<()> {
        let configuration = Arc::new(ExecutionConfiguration::new(prepare_threshold, prepared_max));
        *self
            .configuration
            .lock()
            .map_err(|_| backend_runtime_error("execution configuration mutex is poisoned"))? =
            configuration;
        Ok(())
    }

    fn execution_preparation_snapshot(&self, py: Python<'_>) -> PyResult<NativePreparationState> {
        let (result, signal) = with_owned_session(py, self, |inner| {
            inner.execution.sync_configuration(&self.configuration)?;
            Ok(inner.execution.snapshot())
        });
        finish_owned_session(py, result, signal)
    }

    fn clear_execution_prepared(&self, py: Python<'_>) -> PyResult<()> {
        let (result, signal) = with_owned_session(py, self, |inner| {
            inner.execution.clear(&mut inner.session);
            Ok(())
        });
        finish_owned_session(py, result, signal)
    }

    #[pyo3(signature = (query, values, types, formats, *, binary=false, prepare=None, encoding="utf-8", capture_notifications=false, preflight=None, reservation=None, own_preflight_errors=false))]
    fn execute_query(
        &self,
        py: Python<'_>,
        query: Vec<u8>,
        values: Vec<Option<Vec<u8>>>,
        types: Vec<u32>,
        formats: Vec<u8>,
        binary: bool,
        prepare: Option<bool>,
        encoding: &str,
        capture_notifications: bool,
        preflight: Option<Py<PyAny>>,
        reservation: Option<&BackendExecutionReservation>,
        own_preflight_errors: bool,
    ) -> PyResult<BackendExecutionOutcome> {
        if values.len() != types.len() || values.len() != formats.len() {
            return Err(PyValueError::new_err(
                "parameter values, types, and formats differ in length",
            ));
        }
        if formats.iter().any(|format| *format > 1) {
            return Err(PyValueError::new_err(
                "parameter format must be text (0) or binary (1)",
            ));
        }
        if let Some(reservation) = reservation {
            if !Weak::ptr_eq(&reservation.owner, &Arc::downgrade(&self.inner)) {
                return Err(PyValueError::new_err(
                    "reservation belongs to another session",
                ));
            }
            if !reservation.reservation.matches(&query, &types) {
                return Err(PyValueError::new_err("reservation query or types differ"));
            }
        }
        let reservation = reservation.map(|plan| Arc::clone(&plan.reservation));
        if !matches!(encoding, "utf-8" | "ascii") || (encoding == "ascii" && !query.is_ascii()) {
            return Err(PyValueError::new_err(
                "native execution requires UTF8 or ASCII query encoding",
            ));
        }
        let query = String::from_utf8(query)
            .map_err(|_| PyValueError::new_err("query is not valid UTF8"))?;
        let params: Vec<_> = values
            .into_iter()
            .zip(types)
            .zip(formats)
            .map(|((value, oid), format)| ferrocopg_postgres::BoundParam {
                oid,
                value,
                format: if format == 1 {
                    ferrocopg_postgres::ParamFormat::Binary
                } else {
                    ferrocopg_postgres::ParamFormat::Text
                },
            })
            .collect();
        // Preserve snapshot-before-BEGIN ordering without retaining a packet or
        // holding native state across Python transaction hooks.
        if let Some(preflight) = preflight {
            if let Err(error) = preflight.call0(py) {
                if !own_preflight_errors {
                    return Err(error);
                }
                return Ok(BackendExecutionOutcome {
                    result: None,
                    error: Some(error.into_value(py)),
                    preflight_failed: true,
                    encoding: Some(encoding.to_owned()),
                    statusmessage: None,
                    position: 0,
                    pgresults_cache: None,
                    notices: Vec::new(),
                    notifications: Vec::new(),
                });
            }
        }
        // All Python-owned buffers have been copied before releasing the GIL.
        let (outcome, signal) = with_owned_session(py, self, move |inner| {
            let result = inner.execution.execute(
                &mut inner.session,
                &self.configuration,
                &query,
                &params,
                prepare,
                wire_format(binary),
                reservation.as_deref(),
            );
            let notices = inner.session.drain_notices();
            let notifications = if capture_notifications {
                inner.session.drain_notifications()
            } else {
                Ok(Vec::new())
            };
            Ok((result, notices, notifications))
        });
        let (result, notices, notifications) = match outcome {
            Ok(outcome) => outcome,
            Err(BackendThreadError::Runtime(message)) => {
                return Err(backend_runtime_error(message));
            }
            Err(BackendThreadError::Backend(error)) => return Err(backend_py_error(py, error)),
        };
        // Construct Python errors/diagnostics only after releasing native guards.
        let (result, mut error) = match result {
            Ok(result) => (Some(result), None),
            Err(ExecutionError::Backend(error)) => (None, Some(backend_py_error(py, error))),
            Err(ExecutionError::EmptyCache) => {
                (None, Some(PyKeyError::new_err("dictionary is empty")))
            }
            Err(ExecutionError::InvalidReservation(message)) => {
                (None, Some(PyValueError::new_err(message)))
            }
        };
        let notices = match notices {
            Ok(notices) => notices,
            Err(notice_error) => {
                if error.is_none() {
                    error = Some(backend_py_error(py, notice_error));
                }
                Vec::new()
            }
        };
        let notifications = match notifications {
            Ok(notifications) => notifications
                .into_iter()
                .map(BackendNotification::from)
                .collect(),
            Err(notification_error) => {
                if error.is_none() {
                    error = Some(backend_py_error(py, notification_error));
                }
                Vec::new()
            }
        };
        if signal.is_some() {
            error = signal;
        }
        let result = if error.is_none() {
            result
                .map(|result| Py::new(py, BackendResultSet::from(result)))
                .transpose()?
        } else {
            None
        };
        let statusmessage = result.as_ref().and_then(|result| {
            let result = result.borrow(py);
            result
                .command_tag
                .as_deref()
                .filter(|tag| !tag.is_empty())
                .map(|tag| PyString::new(py, tag).unbind())
        });
        Ok(BackendExecutionOutcome {
            result,
            error: error.map(|error| error.into_value(py)),
            preflight_failed: false,
            encoding: Some(encoding.to_owned()),
            statusmessage,
            position: 0,
            pgresults_cache: None,
            notices,
            notifications,
        })
    }

    fn used_password(&self) -> PyResult<bool> {
        Ok(self.used_password)
    }

    fn parameter(&self, py: Python<'_>, name: &str) -> PyResult<Option<String>> {
        if name == "client_encoding" {
            return Ok(self.client_encoding.clone());
        }
        py.detach(|| {
            Ok(self
                .inner
                .lock()
                .map_err(|_| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        "backend session mutex is poisoned",
                    )
                })?
                .session
                .parameter(name))
        })
    }

    fn backend_pid(&self) -> PyResult<i32> {
        self.backend_pid
            .ok_or_else(|| backend_runtime_error("backend session is closed"))
    }

    #[getter]
    fn closed(&self) -> PyResult<bool> {
        match self.inner.try_lock() {
            Ok(inner) => Ok(inner.session.closed()),
            Err(TryLockError::WouldBlock) => Ok(false),
            Err(TryLockError::Poisoned(_)) => {
                Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                ))
            }
        }
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            let mut inner = self.inner.lock().map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?;
            inner.session.close();
            inner.execution.forget();
            Ok(())
        })
    }

    fn probe(&self, py: Python<'_>) -> PyResult<BackendSyncNoTlsProbe> {
        with_session(py, self, |session| session.probe()).map(BackendSyncNoTlsProbe::from)
    }

    fn cancel_handle(&self, py: Python<'_>) -> PyResult<BackendSyncNoTlsCancelHandle> {
        with_session(py, self, |session| session.cancel_handle()).map(|handle| {
            BackendSyncNoTlsCancelHandle {
                inner: Mutex::new(handle),
            }
        })
    }

    fn query_text(&self, py: Python<'_>, query: &str) -> PyResult<BackendTextQueryResult> {
        let query = query.to_owned();
        with_session(py, self, move |session| session.query_text(&query))
            .map(BackendTextQueryResult::from)
    }

    fn simple_query(
        &self,
        py: Python<'_>,
        query: &str,
    ) -> PyResult<Vec<BackendSimpleQueryMessage>> {
        let query = query.to_owned();
        with_session(py, self, move |session| session.simple_query(&query)).map(|messages| {
            messages
                .into_iter()
                .map(BackendSimpleQueryMessage::from)
                .collect()
        })
    }

    fn simple_query_results(
        &self,
        py: Python<'_>,
        query: &str,
    ) -> PyResult<Vec<BackendSimpleQueryResult>> {
        let query = query.to_owned();
        with_session(py, self, move |session| {
            session.simple_query_results(&query)
        })
        .map(|results| {
            results
                .into_iter()
                .map(BackendSimpleQueryResult::from)
                .collect()
        })
    }

    fn pipeline_simple_query_results(
        &self,
        py: Python<'_>,
        queries: Vec<String>,
    ) -> PyResult<Vec<Vec<BackendSimpleQueryResult>>> {
        with_session(py, self, move |session| {
            session.pipeline_simple_query_results(&queries)
        })
        .map(|batches| {
            batches
                .into_iter()
                .map(|results| {
                    results
                        .into_iter()
                        .map(BackendSimpleQueryResult::from)
                        .collect()
                })
                .collect()
        })
    }

    fn query_text_params(
        &self,
        py: Python<'_>,
        query: &str,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendTextQueryResult> {
        let query = query.to_owned();
        with_session(py, self, move |session| {
            session.query_text_params(&query, &params)
        })
        .map(BackendTextQueryResult::from)
    }

    fn run_text_params(
        &self,
        py: Python<'_>,
        query: &str,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendResultSet> {
        let query = query.to_owned();
        with_session(py, self, move |session| {
            session.run_text_params(&query, &params)
        })
        .map(BackendResultSet::from)
    }

    fn run_text_params_format(
        &self,
        py: Python<'_>,
        query: &str,
        params: Vec<Option<String>>,
        binary: bool,
    ) -> PyResult<BackendResultSet> {
        let query = query.to_owned();
        with_session(py, self, move |session| {
            session.run_text_params_format(&query, &params, wire_format(binary))
        })
        .map(BackendResultSet::from)
    }

    fn run_params(
        &self,
        py: Python<'_>,
        query: &str,
        params: Vec<(u32, bool, Option<Vec<u8>>)>,
    ) -> PyResult<BackendResultSet> {
        let query = query.to_owned();
        let params = bound_params(params);
        with_session(py, self, move |session| session.run_params(&query, &params))
            .map(BackendResultSet::from)
    }

    fn run_params_format(
        &self,
        py: Python<'_>,
        query: &str,
        params: Vec<(u32, bool, Option<Vec<u8>>)>,
        binary: bool,
    ) -> PyResult<BackendResultSet> {
        let query = query.to_owned();
        let params = bound_params(params);
        with_session(py, self, move |session| {
            session.run_params_format(&query, &params, wire_format(binary))
        })
        .map(BackendResultSet::from)
    }

    fn execute_text_params(
        &self,
        py: Python<'_>,
        query: &str,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendExecuteResult> {
        let query = query.to_owned();
        with_session(py, self, move |session| {
            session.execute_text_params(&query, &params)
        })
        .map(BackendExecuteResult::from)
    }

    fn begin(&self, py: Python<'_>) -> PyResult<()> {
        with_session(py, self, |session| session.begin())
    }

    fn commit(&self, py: Python<'_>) -> PyResult<()> {
        with_session(py, self, |session| session.commit())
    }

    fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        with_session(py, self, |session| session.rollback())
    }

    fn copy_from_stdin(&self, py: Python<'_>, query: &str, data: Vec<u8>) -> PyResult<u64> {
        let query = query.to_owned();
        with_session(py, self, move |session| {
            session.copy_from_stdin(&query, &data)
        })
    }

    fn copy_to_stdout(&self, py: Python<'_>, query: &str) -> PyResult<BackendCopyOutResult> {
        let query = query.to_owned();
        with_session(py, self, move |session| session.copy_to_stdout(&query))
            .map(BackendCopyOutResult::from)
    }

    fn listen(&self, py: Python<'_>, channel: &str) -> PyResult<()> {
        let channel = channel.to_owned();
        with_session(py, self, move |session| session.listen(&channel))
    }

    fn unlisten(&self, py: Python<'_>, channel: &str) -> PyResult<()> {
        let channel = channel.to_owned();
        with_session(py, self, move |session| session.unlisten(&channel))
    }

    fn notify(&self, py: Python<'_>, channel: &str, payload: &str) -> PyResult<()> {
        let channel = channel.to_owned();
        let payload = payload.to_owned();
        with_session(py, self, move |session| session.notify(&channel, &payload))
    }

    fn drain_notifications(&self, py: Python<'_>) -> PyResult<Vec<BackendNotification>> {
        with_session(py, self, |session| session.drain_notifications()).map(|notifications| {
            notifications
                .into_iter()
                .map(BackendNotification::from)
                .collect()
        })
    }

    fn wait_for_notification(
        &self,
        py: Python<'_>,
        timeout_ms: u64,
    ) -> PyResult<Option<BackendNotification>> {
        with_session(py, self, move |session| {
            session.wait_for_notification(timeout_ms)
        })
        .map(|notification| notification.map(BackendNotification::from))
    }

    fn drain_notices(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        // Draining buffered notices does no I/O. Only release the GIL when
        // waiting for a query which may need it to check signals and finish.
        let notices = match self.inner.try_lock() {
            Ok(inner) => inner.session.drain_notices(),
            Err(TryLockError::WouldBlock) => py.detach(|| {
                self.inner
                    .lock()
                    .map(|inner| inner.session.drain_notices())
                    .map_err(|_| backend_runtime_error("backend session mutex is poisoned"))
            })?,
            Err(TryLockError::Poisoned(_)) => {
                return Err(backend_runtime_error("backend session mutex is poisoned"));
            }
        };
        let notices = map_backend_result(py, notices)?;
        notices
            .iter()
            .map(|notice| backend_diagnostic_info(py, notice).map(Bound::unbind))
            .collect()
    }

    fn describe_text(&self, py: Python<'_>, query: &str) -> PyResult<BackendStatementDescription> {
        let query = query.to_owned();
        with_session(py, self, move |session| session.describe_text(&query))
            .map(BackendStatementDescription::from)
    }

    fn prepare_text(&self, py: Python<'_>, query: &str) -> PyResult<BackendPreparedStatementInfo> {
        let query = query.to_owned();
        with_session(py, self, move |session| session.prepare_text(&query))
            .map(BackendPreparedStatementInfo::from)
    }

    fn prepare_params(
        &self,
        py: Python<'_>,
        query: &str,
        param_oids: Vec<u32>,
    ) -> PyResult<BackendPreparedStatementInfo> {
        let query = query.to_owned();
        with_session(py, self, move |session| {
            session.prepare_params(&query, &param_oids)
        })
        .map(BackendPreparedStatementInfo::from)
    }

    fn describe_prepared(
        &self,
        py: Python<'_>,
        statement_id: u64,
    ) -> PyResult<BackendStatementDescription> {
        with_session(py, self, move |session| {
            session.describe_prepared(statement_id)
        })
        .map(BackendStatementDescription::from)
    }

    fn query_prepared_text_params(
        &self,
        py: Python<'_>,
        statement_id: u64,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendTextQueryResult> {
        with_session(py, self, move |session| {
            session.query_prepared_text_params(statement_id, &params)
        })
        .map(BackendTextQueryResult::from)
    }

    fn run_prepared_text_params(
        &self,
        py: Python<'_>,
        statement_id: u64,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendResultSet> {
        with_session(py, self, move |session| {
            session.run_prepared_text_params(statement_id, &params)
        })
        .map(BackendResultSet::from)
    }

    fn run_prepared_text_params_format(
        &self,
        py: Python<'_>,
        statement_id: u64,
        params: Vec<Option<String>>,
        binary: bool,
    ) -> PyResult<BackendResultSet> {
        with_session(py, self, move |session| {
            session.run_prepared_text_params_format(statement_id, &params, wire_format(binary))
        })
        .map(BackendResultSet::from)
    }

    fn run_prepared_params(
        &self,
        py: Python<'_>,
        statement_id: u64,
        params: Vec<(u32, bool, Option<Vec<u8>>)>,
    ) -> PyResult<BackendResultSet> {
        let params = bound_params(params);
        with_session(py, self, move |session| {
            session.run_prepared_params(statement_id, &params)
        })
        .map(BackendResultSet::from)
    }

    fn run_prepared_params_format(
        &self,
        py: Python<'_>,
        statement_id: u64,
        params: Vec<(u32, bool, Option<Vec<u8>>)>,
        binary: bool,
    ) -> PyResult<BackendResultSet> {
        let params = bound_params(params);
        with_session(py, self, move |session| {
            session.run_prepared_params_format(statement_id, &params, wire_format(binary))
        })
        .map(BackendResultSet::from)
    }

    fn execute_prepared_text_params(
        &self,
        py: Python<'_>,
        statement_id: u64,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendExecuteResult> {
        with_session(py, self, move |session| {
            session.execute_prepared_text_params(statement_id, &params)
        })
        .map(BackendExecuteResult::from)
    }

    fn close_prepared(&self, py: Python<'_>, statement_id: u64) -> PyResult<()> {
        with_session(py, self, move |session| {
            session.close_prepared(statement_id)
        })
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BackendConninfoSummary>()?;
    m.add_class::<BackendConnectPlan>()?;
    m.add_class::<BackendConnectEndpoint>()?;
    m.add_class::<BackendConnectTarget>()?;
    m.add_class::<BackendSyncNoTlsProbe>()?;
    m.add_class::<BackendNotification>()?;
    m.add_class::<BackendTextQueryResult>()?;
    m.add_class::<BackendResultSet>()?;
    m.add_class::<BackendExecutionOutcome>()?;
    m.add_class::<BackendPgResult>()?;
    m.add_class::<BackendSimpleQueryMessage>()?;
    m.add_class::<BackendSimpleQueryResult>()?;
    m.add_class::<BackendExecuteResult>()?;
    m.add_class::<BackendCopyOutResult>()?;
    m.add_class::<BackendStatementParameter>()?;
    m.add_class::<BackendStatementColumn>()?;
    m.add_class::<BackendStatementDescription>()?;
    m.add_class::<BackendPreparedStatementInfo>()?;
    m.add_class::<BackendSyncNoTlsCancelHandle>()?;
    m.add_class::<BackendSyncNoTlsSession>()?;
    m.add_function(wrap_pyfunction!(milestone, m)?)?;
    m.add_function(wrap_pyfunction!(scaffold_status, m)?)?;
    m.add_function(wrap_pyfunction!(backend_stack, m)?)?;
    m.add_function(wrap_pyfunction!(backend_core, m)?)?;
    m.add_function(wrap_pyfunction!(parse_conninfo_summary, m)?)?;
    m.add_function(wrap_pyfunction!(parse_connect_plan, m)?)?;
    m.add_function(wrap_pyfunction!(parse_connect_target, m)?)?;
    m.add_function(wrap_pyfunction!(probe_connect_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(query_text_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(simple_query_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(simple_query_results_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(pipeline_simple_query_results_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(query_text_params_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(run_text_params_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(execute_text_params_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(describe_text_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(connect_no_tls_session, m)?)?;
    m.add_function(wrap_pyfunction!(connect_session, m)?)?;
    Ok(())
}
