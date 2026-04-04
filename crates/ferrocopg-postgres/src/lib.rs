//! Backend selection and transport-facing helpers for ferrocopg.
//!
//! The long-term plan is to build ferrocopg on the `rust-postgres` ecosystem
//! instead of mirroring the current `libpq`/Cython transport layer.

use postgres::types::{ToSql, Type};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::str::FromStr;

const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 130;
const MIN_CONNECT_TIMEOUT_SECS: u64 = 2;
const DEFAULT_POSTGRES_PORT: u16 = 5432;

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
pub struct TextQueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
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
pub struct PreparedStatementInfo {
    pub statement_id: u64,
    pub description: StatementDescription,
}

pub struct SyncNoTlsSession {
    client: Option<postgres::Client>,
    prepared: HashMap<u64, postgres::Statement>,
    next_statement_id: u64,
}

#[derive(Debug)]
pub enum ProbeError {
    Parse(tokio_postgres::Error),
    NoTlsNotSupported,
    Connect(postgres::Error),
    Query(postgres::Error),
    BadParam(String),
    Closed,
}

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse(err) => write!(f, "{err}"),
            Self::NoTlsNotSupported => {
                write!(
                    f,
                    "conninfo requires TLS; no-TLS bootstrap is not supported"
                )
            }
            Self::Connect(err) => write!(f, "{err}"),
            Self::Query(err) => write!(f, "{err}"),
            Self::BadParam(msg) => write!(f, "{msg}"),
            Self::Closed => write!(f, "backend session is closed"),
        }
    }
}

impl Error for ProbeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse(err) => Some(err),
            Self::Connect(err) => Some(err),
            Self::Query(err) => Some(err),
            Self::BadParam(_) => None,
            Self::NoTlsNotSupported => None,
            Self::Closed => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BootstrapConfig {
    raw_conninfo: String,
    config: tokio_postgres::Config,
}

/// The Rust backend family chosen for ferrocopg.
pub fn backend_stack() -> &'static str {
    "rust-postgres"
}

/// The transport-oriented crate ferrocopg is currently planning around.
pub fn backend_core() -> &'static str {
    let _ = core::any::type_name::<tokio_postgres::Client>();
    "tokio-postgres"
}

impl BootstrapConfig {
    pub fn parse(conninfo: &str) -> Result<Self, tokio_postgres::Error> {
        let config = tokio_postgres::Config::from_str(conninfo)?;
        Ok(Self {
            raw_conninfo: conninfo.to_owned(),
            config,
        })
    }

    pub fn raw_conninfo(&self) -> &str {
        &self.raw_conninfo
    }

    pub fn config(&self) -> &tokio_postgres::Config {
        &self.config
    }

    pub fn summary(&self) -> ConninfoSummary {
        let connect_timeout_seconds = self.config.get_connect_timeout().map(|timeout| {
            let secs = timeout.as_secs();
            if timeout.subsec_nanos() == 0 || secs > 0 {
                secs
            } else {
                1
            }
        });

        ConninfoSummary {
            user: self.config.get_user().map(str::to_owned),
            dbname: self.config.get_dbname().map(str::to_owned),
            application_name: self.config.get_application_name().map(str::to_owned),
            host_count: self.config.get_hosts().len(),
            hostaddr_count: self.config.get_hostaddrs().len(),
            port_count: self.config.get_ports().len(),
            has_password: self.config.get_password().is_some(),
            connect_timeout_seconds,
            effective_connect_timeout_seconds: normalize_connect_timeout(connect_timeout_seconds),
        }
    }
}

pub fn bootstrap_summary(conninfo: &str) -> Result<ConninfoSummary, tokio_postgres::Error> {
    BootstrapConfig::parse(conninfo).map(|config| config.summary())
}

pub fn connect_plan(conninfo: &str) -> Result<ConnectPlan, tokio_postgres::Error> {
    let config = BootstrapConfig::parse(conninfo)?;
    Ok(config.connect_plan())
}

pub fn connect_target(conninfo: &str) -> Result<ConnectTarget, tokio_postgres::Error> {
    let config = BootstrapConfig::parse(conninfo)?;
    Ok(config.connect_target())
}

pub fn connect_no_tls_probe(conninfo: &str) -> Result<SyncNoTlsProbe, ProbeError> {
    let config = BootstrapConfig::parse(conninfo).map_err(ProbeError::Parse)?;
    config.connect_no_tls_probe()
}

pub fn query_text_no_tls(conninfo: &str, query: &str) -> Result<TextQueryResult, ProbeError> {
    let config = BootstrapConfig::parse(conninfo).map_err(ProbeError::Parse)?;
    config.query_text_no_tls(query)
}

pub fn query_text_params_no_tls(
    conninfo: &str,
    query: &str,
    params: &[Option<String>],
) -> Result<TextQueryResult, ProbeError> {
    let config = BootstrapConfig::parse(conninfo).map_err(ProbeError::Parse)?;
    config.query_text_params_no_tls(query, params)
}

pub fn connect_no_tls_session(conninfo: &str) -> Result<SyncNoTlsSession, ProbeError> {
    let config = BootstrapConfig::parse(conninfo).map_err(ProbeError::Parse)?;
    config.connect_no_tls_session()
}

pub fn describe_text_no_tls(
    conninfo: &str,
    query: &str,
) -> Result<StatementDescription, ProbeError> {
    let config = BootstrapConfig::parse(conninfo).map_err(ProbeError::Parse)?;
    config.describe_text_no_tls(query)
}

pub fn execute_text_params_no_tls(
    conninfo: &str,
    query: &str,
    params: &[Option<String>],
) -> Result<ExecuteResult, ProbeError> {
    let config = BootstrapConfig::parse(conninfo).map_err(ProbeError::Parse)?;
    config.execute_text_params_no_tls(query, params)
}

impl BootstrapConfig {
    pub fn connect_plan(&self) -> ConnectPlan {
        let summary = self.summary();
        let ssl_mode = ssl_mode_name(self.config.get_ssl_mode());
        let tls_negotiation = ssl_negotiation_name(self.config.get_ssl_negotiation());
        let can_bootstrap_with_no_tls = matches!(
            self.config.get_ssl_mode(),
            tokio_postgres::config::SslMode::Disable | tokio_postgres::config::SslMode::Prefer
        );

        ConnectPlan {
            backend_stack: backend_stack(),
            sync_client: sync_client(),
            async_client: async_client(),
            sync_runtime: sync_runtime(),
            async_runtime: async_runtime(),
            tls_mode: ssl_mode,
            tls_negotiation,
            tls_connector_hint: tls_connector_hint(self.config.get_ssl_mode()),
            target_session_attrs: target_session_attrs_name(self.config.get_target_session_attrs()),
            load_balance_hosts: load_balance_hosts_name(self.config.get_load_balance_hosts()),
            can_bootstrap_with_no_tls,
            requires_external_tls_connector: !can_bootstrap_with_no_tls,
            summary,
        }
    }

    pub fn connect_target(&self) -> ConnectTarget {
        let plan = self.connect_plan();

        ConnectTarget {
            backend_stack: plan.backend_stack,
            sync_client: plan.sync_client,
            async_client: plan.async_client,
            sync_runtime: plan.sync_runtime,
            async_runtime: plan.async_runtime,
            tls_mode: plan.tls_mode,
            tls_negotiation: plan.tls_negotiation,
            tls_connector_hint: plan.tls_connector_hint,
            target_session_attrs: plan.target_session_attrs,
            load_balance_hosts: plan.load_balance_hosts,
            can_bootstrap_with_no_tls: plan.can_bootstrap_with_no_tls,
            requires_external_tls_connector: plan.requires_external_tls_connector,
            endpoints: connect_endpoints(&self.config),
            summary: plan.summary,
        }
    }

    pub fn connect_no_tls_probe(&self) -> Result<SyncNoTlsProbe, ProbeError> {
        if !self.connect_plan().can_bootstrap_with_no_tls {
            return Err(ProbeError::NoTlsNotSupported);
        }

        let mut client = postgres::Config::from_str(self.raw_conninfo())
            .map_err(ProbeError::Parse)?
            .connect(postgres::NoTls)
            .map_err(ProbeError::Connect)?;

        let row = client
            .query_one(
                "select \
                    pg_backend_pid(), \
                    current_user::text, \
                    current_database()::text, \
                    current_setting('server_version_num')::int4, \
                    coalesce(current_setting('application_name', true), '')::text, \
                    inet_server_addr()::text, \
                    inet_server_port()",
                &[],
            )
            .map_err(ProbeError::Connect)?;

        let server_port = row
            .get::<_, Option<i32>>(6)
            .and_then(|port| u16::try_from(port).ok());

        Ok(SyncNoTlsProbe {
            backend_pid: row.get(0),
            current_user: row.get(1),
            current_database: row.get(2),
            server_version_num: row.get(3),
            application_name: row.get(4),
            server_address: row.get(5),
            server_port,
        })
    }

    pub fn query_text_no_tls(&self, query: &str) -> Result<TextQueryResult, ProbeError> {
        self.connect_no_tls_session()?.query_text(query)
    }

    pub fn query_text_params_no_tls(
        &self,
        query: &str,
        params: &[Option<String>],
    ) -> Result<TextQueryResult, ProbeError> {
        self.connect_no_tls_session()?
            .query_text_params(query, params)
    }

    pub fn describe_text_no_tls(&self, query: &str) -> Result<StatementDescription, ProbeError> {
        self.connect_no_tls_session()?.describe_text(query)
    }

    pub fn execute_text_params_no_tls(
        &self,
        query: &str,
        params: &[Option<String>],
    ) -> Result<ExecuteResult, ProbeError> {
        self.connect_no_tls_session()?
            .execute_text_params(query, params)
    }

    pub fn connect_no_tls_session(&self) -> Result<SyncNoTlsSession, ProbeError> {
        if !self.connect_plan().can_bootstrap_with_no_tls {
            return Err(ProbeError::NoTlsNotSupported);
        }

        let client = postgres::Config::from_str(self.raw_conninfo())
            .map_err(ProbeError::Parse)?
            .connect(postgres::NoTls)
            .map_err(ProbeError::Connect)?;

        Ok(SyncNoTlsSession {
            client: Some(client),
            prepared: HashMap::new(),
            next_statement_id: 1,
        })
    }
}

impl SyncNoTlsSession {
    pub fn closed(&self) -> bool {
        self.client.is_none()
    }

    pub fn close(&mut self) {
        self.prepared.clear();
        self.client.take();
    }

    pub fn probe(&mut self) -> Result<SyncNoTlsProbe, ProbeError> {
        let row = self
            .client_mut()?
            .query_one(
                "select \
                    pg_backend_pid(), \
                    current_user::text, \
                    current_database()::text, \
                    current_setting('server_version_num')::int4, \
                    coalesce(current_setting('application_name', true), '')::text, \
                    inet_server_addr()::text, \
                    inet_server_port()",
                &[],
            )
            .map_err(ProbeError::Query)?;

        let server_port = row
            .get::<_, Option<i32>>(6)
            .and_then(|port| u16::try_from(port).ok());

        Ok(SyncNoTlsProbe {
            backend_pid: row.get(0),
            current_user: row.get(1),
            current_database: row.get(2),
            server_version_num: row.get(3),
            application_name: row.get(4),
            server_address: row.get(5),
            server_port,
        })
    }

    pub fn query_text(&mut self, query: &str) -> Result<TextQueryResult, ProbeError> {
        let rows = self
            .client_mut()?
            .query(query, &[])
            .map_err(ProbeError::Query)?;
        text_query_result(rows)
    }

    pub fn query_text_params(
        &mut self,
        query: &str,
        params: &[Option<String>],
    ) -> Result<TextQueryResult, ProbeError> {
        let statement = self
            .client_mut()?
            .prepare(query)
            .map_err(ProbeError::Query)?;
        let params = parsed_query_params(&statement, params)?;
        let refs = query_param_refs(&params);
        let rows = self
            .client_mut()?
            .query(&statement, &refs)
            .map_err(ProbeError::Query)?;
        text_query_result(rows)
    }

    pub fn describe_text(&mut self, query: &str) -> Result<StatementDescription, ProbeError> {
        let statement = self
            .client_mut()?
            .prepare(query)
            .map_err(ProbeError::Query)?;
        Ok(statement_description(&statement))
    }

    pub fn execute_text_params(
        &mut self,
        query: &str,
        params: &[Option<String>],
    ) -> Result<ExecuteResult, ProbeError> {
        let statement = self
            .client_mut()?
            .prepare(query)
            .map_err(ProbeError::Query)?;
        let params = parsed_query_params(&statement, params)?;
        let refs = query_param_refs(&params);
        let rows_affected = self
            .client_mut()?
            .execute(&statement, &refs)
            .map_err(ProbeError::Query)?;
        Ok(ExecuteResult { rows_affected })
    }

    pub fn prepare_text(&mut self, query: &str) -> Result<PreparedStatementInfo, ProbeError> {
        let statement = self
            .client_mut()?
            .prepare(query)
            .map_err(ProbeError::Query)?;
        let statement_id = self.next_statement_id;
        self.next_statement_id += 1;
        let description = statement_description(&statement);
        self.prepared.insert(statement_id, statement);
        Ok(PreparedStatementInfo {
            statement_id,
            description,
        })
    }

    pub fn describe_prepared(
        &mut self,
        statement_id: u64,
    ) -> Result<StatementDescription, ProbeError> {
        let statement = self.prepared_statement(statement_id)?;
        Ok(statement_description(statement))
    }

    pub fn query_prepared_text_params(
        &mut self,
        statement_id: u64,
        params: &[Option<String>],
    ) -> Result<TextQueryResult, ProbeError> {
        let statement = self.prepared_statement(statement_id)?.clone();
        let params = parsed_query_params(&statement, params)?;
        let refs = query_param_refs(&params);
        let rows = self
            .client_mut()?
            .query(&statement, &refs)
            .map_err(ProbeError::Query)?;
        text_query_result(rows)
    }

    pub fn execute_prepared_text_params(
        &mut self,
        statement_id: u64,
        params: &[Option<String>],
    ) -> Result<ExecuteResult, ProbeError> {
        let statement = self.prepared_statement(statement_id)?.clone();
        let params = parsed_query_params(&statement, params)?;
        let refs = query_param_refs(&params);
        let rows_affected = self
            .client_mut()?
            .execute(&statement, &refs)
            .map_err(ProbeError::Query)?;
        Ok(ExecuteResult { rows_affected })
    }

    pub fn close_prepared(&mut self, statement_id: u64) -> Result<(), ProbeError> {
        self.prepared
            .remove(&statement_id)
            .map(|_| ())
            .ok_or_else(|| missing_statement(statement_id))
    }

    pub fn begin(&mut self) -> Result<(), ProbeError> {
        self.client_mut()?
            .batch_execute("begin")
            .map_err(ProbeError::Query)
    }

    pub fn commit(&mut self) -> Result<(), ProbeError> {
        self.client_mut()?
            .batch_execute("commit")
            .map_err(ProbeError::Query)
    }

    pub fn rollback(&mut self) -> Result<(), ProbeError> {
        self.client_mut()?
            .batch_execute("rollback")
            .map_err(ProbeError::Query)
    }

    fn client_mut(&mut self) -> Result<&mut postgres::Client, ProbeError> {
        self.client.as_mut().ok_or(ProbeError::Closed)
    }

    fn prepared_statement(&self, statement_id: u64) -> Result<&postgres::Statement, ProbeError> {
        if self.closed() {
            return Err(ProbeError::Closed);
        }

        self.prepared
            .get(&statement_id)
            .ok_or_else(|| missing_statement(statement_id))
    }
}

fn normalize_connect_timeout(timeout_seconds: Option<u64>) -> u64 {
    match timeout_seconds {
        None | Some(0) => DEFAULT_CONNECT_TIMEOUT_SECS,
        Some(timeout) => timeout.max(MIN_CONNECT_TIMEOUT_SECS),
    }
}

fn query_param_refs(params: &[Box<dyn ToSql + Sync>]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|value| value.as_ref()).collect()
}

fn parsed_query_params(
    statement: &postgres::Statement,
    params: &[Option<String>],
) -> Result<Vec<Box<dyn ToSql + Sync>>, ProbeError> {
    let expected = statement.params();
    if expected.len() != params.len() {
        return Err(ProbeError::BadParam(format!(
            "expected {} params but got {}",
            expected.len(),
            params.len()
        )));
    }

    expected
        .iter()
        .zip(params.iter())
        .enumerate()
        .map(|(index, (ty, value))| parse_query_param(index, ty, value))
        .collect()
}

fn parse_query_param(
    index: usize,
    ty: &Type,
    value: &Option<String>,
) -> Result<Box<dyn ToSql + Sync>, ProbeError> {
    match value {
        None => parse_null_query_param(index, ty),
        Some(value) => parse_text_query_param(index, ty, value),
    }
}

fn parse_null_query_param(index: usize, ty: &Type) -> Result<Box<dyn ToSql + Sync>, ProbeError> {
    Ok(match *ty {
        Type::BOOL => Box::new(Option::<bool>::None),
        Type::INT2 => Box::new(Option::<i16>::None),
        Type::INT4 => Box::new(Option::<i32>::None),
        Type::INT8 => Box::new(Option::<i64>::None),
        Type::OID => Box::new(Option::<u32>::None),
        Type::FLOAT4 => Box::new(Option::<f32>::None),
        Type::FLOAT8 => Box::new(Option::<f64>::None),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            Box::new(Option::<String>::None)
        }
        _ => {
            return Err(ProbeError::BadParam(format!(
                "unsupported null parameter type at ${}: {}",
                index + 1,
                ty.name()
            )));
        }
    })
}

fn parse_text_query_param(
    index: usize,
    ty: &Type,
    value: &str,
) -> Result<Box<dyn ToSql + Sync>, ProbeError> {
    Ok(match *ty {
        Type::BOOL => Box::new(parse_bool_param(index, value)?),
        Type::INT2 => Box::new(parse_numeric_param::<i16>(index, ty, value)?),
        Type::INT4 => Box::new(parse_numeric_param::<i32>(index, ty, value)?),
        Type::INT8 => Box::new(parse_numeric_param::<i64>(index, ty, value)?),
        Type::OID => Box::new(parse_numeric_param::<u32>(index, ty, value)?),
        Type::FLOAT4 => Box::new(parse_numeric_param::<f32>(index, ty, value)?),
        Type::FLOAT8 => Box::new(parse_numeric_param::<f64>(index, ty, value)?),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            Box::new(value.to_owned())
        }
        _ => {
            return Err(ProbeError::BadParam(format!(
                "unsupported parameter type at ${}: {}",
                index + 1,
                ty.name()
            )));
        }
    })
}

fn parse_bool_param(index: usize, value: &str) -> Result<bool, ProbeError> {
    match value {
        "t" | "true" | "TRUE" | "1" => Ok(true),
        "f" | "false" | "FALSE" | "0" => Ok(false),
        _ => Err(ProbeError::BadParam(format!(
            "invalid boolean value at ${}: {}",
            index + 1,
            value
        ))),
    }
}

fn parse_numeric_param<T>(index: usize, ty: &Type, value: &str) -> Result<T, ProbeError>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    value.parse::<T>().map_err(|err| {
        ProbeError::BadParam(format!(
            "invalid {} value at ${}: {} ({err})",
            ty.name(),
            index + 1,
            value
        ))
    })
}

fn text_query_result(rows: Vec<postgres::Row>) -> Result<TextQueryResult, ProbeError> {
    let columns = rows
        .first()
        .map(|row| {
            row.columns()
                .iter()
                .map(|col| col.name().to_owned())
                .collect()
        })
        .unwrap_or_default();
    let rows = rows
        .into_iter()
        .map(|row| {
            (0..row.len())
                .map(|index| {
                    row.try_get::<_, Option<String>>(index)
                        .map_err(ProbeError::Query)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(TextQueryResult { columns, rows })
}

fn statement_description(statement: &postgres::Statement) -> StatementDescription {
    StatementDescription {
        params: statement
            .params()
            .iter()
            .map(|ty| StatementParameter {
                oid: ty.oid(),
                type_name: ty.name().to_owned(),
            })
            .collect(),
        columns: statement
            .columns()
            .iter()
            .map(|column| StatementColumn {
                name: column.name().to_owned(),
                oid: column.type_().oid(),
                type_name: column.type_().name().to_owned(),
            })
            .collect(),
    }
}

fn missing_statement(statement_id: u64) -> ProbeError {
    ProbeError::BadParam(format!("unknown prepared statement id: {statement_id}"))
}

fn sync_client() -> &'static str {
    let _ = core::any::type_name::<postgres::Client>();
    "postgres::Client"
}

fn async_client() -> &'static str {
    let _ = core::any::type_name::<tokio_postgres::Client>();
    "tokio_postgres::Client"
}

fn sync_runtime() -> &'static str {
    "postgres crate-managed tokio runtime"
}

fn async_runtime() -> &'static str {
    "caller-managed tokio runtime"
}

fn tls_connector_hint(ssl_mode: tokio_postgres::config::SslMode) -> &'static str {
    match ssl_mode {
        tokio_postgres::config::SslMode::Disable => "NoTls is sufficient",
        tokio_postgres::config::SslMode::Prefer => {
            "NoTls can bootstrap, but a real TLS connector is preferred"
        }
        tokio_postgres::config::SslMode::Require => "external TLS connector required",
        _ => "external TLS policy decision required",
    }
}

fn ssl_mode_name(ssl_mode: tokio_postgres::config::SslMode) -> &'static str {
    match ssl_mode {
        tokio_postgres::config::SslMode::Disable => "disable",
        tokio_postgres::config::SslMode::Prefer => "prefer",
        tokio_postgres::config::SslMode::Require => "require",
        _ => "unknown",
    }
}

fn ssl_negotiation_name(ssl_negotiation: tokio_postgres::config::SslNegotiation) -> &'static str {
    match ssl_negotiation {
        tokio_postgres::config::SslNegotiation::Postgres => "postgres",
        tokio_postgres::config::SslNegotiation::Direct => "direct",
        _ => "unknown",
    }
}

fn target_session_attrs_name(
    target_session_attrs: tokio_postgres::config::TargetSessionAttrs,
) -> &'static str {
    match target_session_attrs {
        tokio_postgres::config::TargetSessionAttrs::Any => "any",
        tokio_postgres::config::TargetSessionAttrs::ReadWrite => "read-write",
        _ => "unknown",
    }
}

fn load_balance_hosts_name(
    load_balance_hosts: tokio_postgres::config::LoadBalanceHosts,
) -> &'static str {
    match load_balance_hosts {
        tokio_postgres::config::LoadBalanceHosts::Disable => "disable",
        tokio_postgres::config::LoadBalanceHosts::Random => "random",
        _ => "unknown",
    }
}

fn connect_endpoints(config: &tokio_postgres::Config) -> Vec<ConnectEndpoint> {
    let hosts = config.get_hosts();
    let hostaddrs = config.get_hostaddrs();
    let ports = config.get_ports();

    if hosts.is_empty() {
        return vec![ConnectEndpoint {
            transport: "tcp",
            target: "localhost".to_owned(),
            hostaddr: None,
            port: default_port(ports, 0),
            inferred: true,
        }];
    }

    hosts
        .iter()
        .enumerate()
        .map(|(index, host)| {
            let (transport, target) = match host {
                tokio_postgres::config::Host::Tcp(name) => ("tcp", name.clone()),
                tokio_postgres::config::Host::Unix(path) => ("unix", path.display().to_string()),
            };

            ConnectEndpoint {
                transport,
                target,
                hostaddr: hostaddrs.get(index).map(ToString::to_string),
                port: default_port(ports, index),
                inferred: false,
            }
        })
        .collect()
}

fn default_port(ports: &[u16], index: usize) -> u16 {
    match ports {
        [] => DEFAULT_POSTGRES_PORT,
        [port] => *port,
        _ => ports
            .get(index)
            .copied()
            .or_else(|| ports.last().copied())
            .unwrap_or(DEFAULT_POSTGRES_PORT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_target_defaults_to_localhost_when_host_missing() {
        let target = connect_target("dbname=postgres").expect("conninfo should parse");
        assert_eq!(target.endpoints.len(), 1);
        assert_eq!(target.endpoints[0].transport, "tcp");
        assert_eq!(target.endpoints[0].target, "localhost");
        assert_eq!(target.endpoints[0].port, DEFAULT_POSTGRES_PORT);
        assert!(target.endpoints[0].inferred);
    }

    #[test]
    fn connect_target_preserves_host_port_and_hostaddr_pairs() {
        let target =
            connect_target("host=a,b hostaddr=10.0.0.1,10.0.0.2 port=5433,5434 dbname=postgres")
                .expect("conninfo should parse");
        assert_eq!(
            target.endpoints,
            vec![
                ConnectEndpoint {
                    transport: "tcp",
                    target: "a".to_owned(),
                    hostaddr: Some("10.0.0.1".to_owned()),
                    port: 5433,
                    inferred: false,
                },
                ConnectEndpoint {
                    transport: "tcp",
                    target: "b".to_owned(),
                    hostaddr: Some("10.0.0.2".to_owned()),
                    port: 5434,
                    inferred: false,
                }
            ]
        );
    }

    #[test]
    fn connect_target_uses_single_port_for_many_hosts() {
        let target =
            connect_target("host=a,b,c port=6543 dbname=postgres").expect("conninfo should parse");
        assert_eq!(
            target
                .endpoints
                .iter()
                .map(|ep| ep.port)
                .collect::<Vec<_>>(),
            vec![6543, 6543, 6543]
        );
    }

    #[test]
    fn no_tls_probe_rejects_tls_required_conninfo() {
        let err = connect_no_tls_probe("host=localhost sslmode=require dbname=postgres")
            .expect_err("no-TLS probe should reject TLS-required conninfo");
        assert!(matches!(err, ProbeError::NoTlsNotSupported));
    }

    #[test]
    fn query_text_no_tls_rejects_tls_required_conninfo() {
        let err = query_text_no_tls(
            "host=localhost sslmode=require dbname=postgres",
            "select 'ok'::text",
        )
        .expect_err("no-TLS query should reject TLS-required conninfo");
        assert!(matches!(err, ProbeError::NoTlsNotSupported));
    }

    #[test]
    fn session_rejects_operations_after_close() {
        let mut session = SyncNoTlsSession {
            client: None,
            prepared: HashMap::new(),
            next_statement_id: 1,
        };
        assert!(session.closed());
        assert!(matches!(
            session.query_text("select 1"),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(
            session.execute_text_params("select 1", &[]),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(session.begin(), Err(ProbeError::Closed)));
        assert!(matches!(session.commit(), Err(ProbeError::Closed)));
        assert!(matches!(session.rollback(), Err(ProbeError::Closed)));
        assert!(matches!(
            session.describe_text("select 1"),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(session.probe(), Err(ProbeError::Closed)));
    }
}
