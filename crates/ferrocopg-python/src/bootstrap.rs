use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

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
struct BackendTextQueryResult {
    #[pyo3(get)]
    columns: Vec<String>,
    #[pyo3(get)]
    rows: Vec<Vec<Option<String>>>,
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
            endpoints: target.endpoints.into_iter().map(BackendConnectEndpoint::from).collect(),
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

impl From<ferrocopg_postgres::TextQueryResult> for BackendTextQueryResult {
    fn from(result: ferrocopg_postgres::TextQueryResult) -> Self {
        Self {
            columns: result.columns,
            rows: result.rows,
        }
    }
}

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BackendConninfoSummary>()?;
    m.add_class::<BackendConnectPlan>()?;
    m.add_class::<BackendConnectEndpoint>()?;
    m.add_class::<BackendConnectTarget>()?;
    m.add_class::<BackendSyncNoTlsProbe>()?;
    m.add_class::<BackendTextQueryResult>()?;
    m.add_function(wrap_pyfunction!(milestone, m)?)?;
    m.add_function(wrap_pyfunction!(scaffold_status, m)?)?;
    m.add_function(wrap_pyfunction!(backend_stack, m)?)?;
    m.add_function(wrap_pyfunction!(backend_core, m)?)?;
    m.add_function(wrap_pyfunction!(parse_conninfo_summary, m)?)?;
    m.add_function(wrap_pyfunction!(parse_connect_plan, m)?)?;
    m.add_function(wrap_pyfunction!(parse_connect_target, m)?)?;
    m.add_function(wrap_pyfunction!(probe_connect_no_tls, m)?)?;
    m.add_function(wrap_pyfunction!(query_text_no_tls, m)?)?;
    Ok(())
}
