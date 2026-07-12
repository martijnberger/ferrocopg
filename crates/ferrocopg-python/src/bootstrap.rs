use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3::wrap_pyfunction;
use std::sync::Mutex;

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
    #[pyo3(get)]
    rows: Vec<Vec<Option<Vec<u8>>>>,
    #[pyo3(get)]
    rows_affected: u64,
    #[pyo3(get)]
    is_tuples: bool,
    #[pyo3(get)]
    wire_format: u8,
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
    inner: Mutex<ferrocopg_postgres::SyncNoTlsSession>,
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
    T: Send,
    F: FnOnce(
            &mut ferrocopg_postgres::SyncNoTlsSession,
        ) -> Result<T, ferrocopg_postgres::ProbeError>
        + Send,
{
    let result = py.detach(|| {
        let mut inner = session.inner.lock().map_err(|_| {
            BackendThreadError::Runtime("backend session mutex is poisoned".to_owned())
        })?;
        f(&mut inner).map_err(BackendThreadError::Backend)
    });
    match result {
        Ok(value) => Ok(value),
        Err(BackendThreadError::Runtime(message)) => Err(backend_runtime_error(message)),
        Err(BackendThreadError::Backend(err)) => Err(backend_py_error(py, err)),
    }
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
    map_backend_result(py, ferrocopg_postgres::connect_no_tls_session(conninfo)).map(|session| {
        BackendSyncNoTlsSession {
            inner: Mutex::new(session),
        }
    })
}

#[pyfunction]
fn connect_session(py: Python<'_>, conninfo: &str) -> PyResult<BackendSyncNoTlsSession> {
    map_backend_result(py, ferrocopg_postgres::connect_session(conninfo)).map(|session| {
        BackendSyncNoTlsSession {
            inner: Mutex::new(session),
        }
    })
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
}

#[pymethods]
impl BackendSyncNoTlsSession {
    #[getter]
    fn closed(&self) -> PyResult<bool> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .closed())
    }

    fn close(&self) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .close();
        Ok(())
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
        let notices = with_session(py, self, |session| session.drain_notices())?;
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
