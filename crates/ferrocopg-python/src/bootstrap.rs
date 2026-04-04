use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::sync::Mutex;

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
    rows: Vec<Vec<Option<String>>>,
    #[pyo3(get)]
    rows_affected: u64,
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

fn with_session<T, F>(py: Python<'_>, session: &BackendSyncNoTlsSession, f: F) -> PyResult<T>
where
    T: Send,
    F: FnOnce(
            &mut ferrocopg_postgres::SyncNoTlsSession,
        ) -> Result<T, ferrocopg_postgres::ProbeError>
        + Send,
{
    py.detach(|| {
        let mut inner = session
            .inner
            .lock()
            .map_err(|_| "backend session mutex is poisoned".to_owned())?;
        f(&mut inner).map_err(|err| err.to_string())
    })
    .map_err(backend_runtime_error)
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
    py.detach(|| {
        let inner = handle
            .inner
            .lock()
            .map_err(|_| "backend cancel handle mutex is poisoned".to_owned())?;
        f(&inner).map_err(|err| err.to_string())
    })
    .map_err(backend_runtime_error)
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
fn probe_connect_no_tls(conninfo: &str) -> PyResult<BackendSyncNoTlsProbe> {
    ferrocopg_postgres::connect_no_tls_probe(conninfo)
        .map(BackendSyncNoTlsProbe::from)
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
}

#[pyfunction]
fn query_text_no_tls(conninfo: &str, query: &str) -> PyResult<BackendTextQueryResult> {
    ferrocopg_postgres::query_text_no_tls(conninfo, query)
        .map(BackendTextQueryResult::from)
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
}

#[pyfunction]
fn simple_query_no_tls(conninfo: &str, query: &str) -> PyResult<Vec<BackendSimpleQueryMessage>> {
    ferrocopg_postgres::simple_query_no_tls(conninfo, query)
        .map(|messages| {
            messages
                .into_iter()
                .map(BackendSimpleQueryMessage::from)
                .collect()
        })
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
}

#[pyfunction]
fn simple_query_results_no_tls(
    conninfo: &str,
    query: &str,
) -> PyResult<Vec<BackendSimpleQueryResult>> {
    ferrocopg_postgres::simple_query_results_no_tls(conninfo, query)
        .map(|results| {
            results
                .into_iter()
                .map(BackendSimpleQueryResult::from)
                .collect()
        })
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
}

#[pyfunction]
fn query_text_params_no_tls(
    conninfo: &str,
    query: &str,
    params: Vec<Option<String>>,
) -> PyResult<BackendTextQueryResult> {
    ferrocopg_postgres::query_text_params_no_tls(conninfo, query, &params)
        .map(BackendTextQueryResult::from)
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
}

#[pyfunction]
fn execute_text_params_no_tls(
    conninfo: &str,
    query: &str,
    params: Vec<Option<String>>,
) -> PyResult<BackendExecuteResult> {
    ferrocopg_postgres::execute_text_params_no_tls(conninfo, query, &params)
        .map(BackendExecuteResult::from)
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
}

#[pyfunction]
fn describe_text_no_tls(conninfo: &str, query: &str) -> PyResult<BackendStatementDescription> {
    ferrocopg_postgres::describe_text_no_tls(conninfo, query)
        .map(BackendStatementDescription::from)
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
}

#[pyfunction]
fn connect_no_tls_session(conninfo: &str) -> PyResult<BackendSyncNoTlsSession> {
    ferrocopg_postgres::connect_no_tls_session(conninfo)
        .map(|session| BackendSyncNoTlsSession {
            inner: Mutex::new(session),
        })
        .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
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
            rows: result.rows,
            rows_affected: result.rows_affected,
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
    m.add_function(wrap_pyfunction!(query_text_params_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(execute_text_params_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(describe_text_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(connect_no_tls_session, m)?)?;
    Ok(())
}
