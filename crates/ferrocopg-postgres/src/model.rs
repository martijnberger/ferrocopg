#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConninfoSummary {
    pub user: Option<String>,
    pub dbname: Option<String>,
    pub application_name: Option<String>,
    pub host_count: usize,
    pub hostaddr_count: usize,
    pub port_count: usize,
    pub has_password: bool,
    pub connect_timeout_seconds: Option<u64>,
    pub effective_connect_timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectPlan {
    pub backend_stack: &'static str,
    pub sync_client: &'static str,
    pub async_client: &'static str,
    pub sync_runtime: &'static str,
    pub async_runtime: &'static str,
    pub tls_mode: &'static str,
    pub tls_negotiation: &'static str,
    pub tls_connector_hint: &'static str,
    pub target_session_attrs: &'static str,
    pub load_balance_hosts: &'static str,
    pub can_bootstrap_with_no_tls: bool,
    pub requires_external_tls_connector: bool,
    pub summary: ConninfoSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectEndpoint {
    pub transport: &'static str,
    pub target: String,
    pub hostaddr: Option<String>,
    pub port: u16,
    pub inferred: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectTarget {
    pub backend_stack: &'static str,
    pub sync_client: &'static str,
    pub async_client: &'static str,
    pub sync_runtime: &'static str,
    pub async_runtime: &'static str,
    pub tls_mode: &'static str,
    pub tls_negotiation: &'static str,
    pub tls_connector_hint: &'static str,
    pub target_session_attrs: &'static str,
    pub load_balance_hosts: &'static str,
    pub can_bootstrap_with_no_tls: bool,
    pub requires_external_tls_connector: bool,
    pub endpoints: Vec<ConnectEndpoint>,
    pub summary: ConninfoSummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncNoTlsProbe {
    pub backend_pid: i32,
    pub current_user: String,
    pub current_database: String,
    pub server_version_num: i32,
    pub application_name: String,
    pub server_address: Option<String>,
    pub server_port: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendNotification {
    pub process_id: i32,
    pub channel: String,
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub rows_affected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementParameter {
    pub oid: u32,
    pub type_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementColumn {
    pub name: String,
    pub oid: u32,
    pub type_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementDescription {
    pub params: Vec<StatementParameter>,
    pub columns: Vec<StatementColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteResult {
    pub rows_affected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOutResult {
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleQueryMessage {
    pub kind: &'static str,
    pub columns: Vec<String>,
    pub values: Vec<Option<String>>,
    pub rows_affected: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub rows_affected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedStatementInfo {
    pub statement_id: u64,
    pub description: StatementDescription,
}
