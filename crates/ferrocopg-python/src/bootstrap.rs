use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::sync::Mutex;

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
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
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
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
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
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
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
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
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
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
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendNotification {
    #[pyo3(get)]
    process_id: i32,
    #[pyo3(get)]
    channel: String,
    #[pyo3(get)]
    payload: String,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendTextQueryResult {
    #[pyo3(get)]
    columns: Vec<String>,
    #[pyo3(get)]
    rows: Vec<Vec<Option<String>>>,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendExecuteResult {
    #[pyo3(get)]
    rows_affected: u64,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendStatementParameter {
    #[pyo3(get)]
    oid: u32,
    #[pyo3(get)]
    type_name: String,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendStatementColumn {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    oid: u32,
    #[pyo3(get)]
    type_name: String,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendStatementDescription {
    #[pyo3(get)]
    params: Vec<BackendStatementParameter>,
    #[pyo3(get)]
    columns: Vec<BackendStatementColumn>,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendPreparedStatementInfo {
    #[pyo3(get)]
    statement_id: u64,
    #[pyo3(get)]
    description: BackendStatementDescription,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendSyncNoTlsSession {
    inner: Mutex<ferrocopg_postgres::SyncNoTlsSession>,
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

impl From<ferrocopg_postgres::ExecuteResult> for BackendExecuteResult {
    fn from(result: ferrocopg_postgres::ExecuteResult) -> Self {
        Self {
            rows_affected: result.rows_affected,
        }
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

    fn probe(&self) -> PyResult<BackendSyncNoTlsProbe> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .probe()
            .map(BackendSyncNoTlsProbe::from)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn query_text(&self, query: &str) -> PyResult<BackendTextQueryResult> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .query_text(query)
            .map(BackendTextQueryResult::from)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn query_text_params(
        &self,
        query: &str,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendTextQueryResult> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .query_text_params(query, &params)
            .map(BackendTextQueryResult::from)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn execute_text_params(
        &self,
        query: &str,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendExecuteResult> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .execute_text_params(query, &params)
            .map(BackendExecuteResult::from)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn begin(&self) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .begin()
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn commit(&self) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .commit()
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn rollback(&self) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .rollback()
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn listen(&self, channel: &str) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .listen(channel)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn unlisten(&self, channel: &str) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .unlisten(channel)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn notify(&self, channel: &str, payload: &str) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .notify(channel, payload)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn drain_notifications(&self) -> PyResult<Vec<BackendNotification>> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .drain_notifications()
            .map(|notifications| {
                notifications
                    .into_iter()
                    .map(BackendNotification::from)
                    .collect()
            })
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn wait_for_notification(&self, timeout_ms: u64) -> PyResult<Option<BackendNotification>> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .wait_for_notification(timeout_ms)
            .map(|notification| notification.map(BackendNotification::from))
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn describe_text(&self, query: &str) -> PyResult<BackendStatementDescription> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .describe_text(query)
            .map(BackendStatementDescription::from)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn prepare_text(&self, query: &str) -> PyResult<BackendPreparedStatementInfo> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .prepare_text(query)
            .map(BackendPreparedStatementInfo::from)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn describe_prepared(&self, statement_id: u64) -> PyResult<BackendStatementDescription> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .describe_prepared(statement_id)
            .map(BackendStatementDescription::from)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn query_prepared_text_params(
        &self,
        statement_id: u64,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendTextQueryResult> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .query_prepared_text_params(statement_id, &params)
            .map(BackendTextQueryResult::from)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn execute_prepared_text_params(
        &self,
        statement_id: u64,
        params: Vec<Option<String>>,
    ) -> PyResult<BackendExecuteResult> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .execute_prepared_text_params(statement_id, &params)
            .map(BackendExecuteResult::from)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
    }

    fn close_prepared(&self, statement_id: u64) -> PyResult<()> {
        self.inner
            .lock()
            .map_err(|_| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "backend session mutex is poisoned",
                )
            })?
            .close_prepared(statement_id)
            .map_err(|err| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()))
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
    m.add_class::<BackendExecuteResult>()?;
    m.add_class::<BackendStatementParameter>()?;
    m.add_class::<BackendStatementColumn>()?;
    m.add_class::<BackendStatementDescription>()?;
    m.add_class::<BackendPreparedStatementInfo>()?;
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
    m.add_function(wrap_pyfunction!(query_text_params_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(execute_text_params_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(describe_text_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(connect_no_tls_session, m)?)?;
    Ok(())
}
