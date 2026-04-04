use crate::error::ProbeError;
use crate::model::{
    ConnectEndpoint, ConnectPlan, ConnectTarget, ConninfoSummary, ExecuteResult,
    SimpleQueryMessage, SimpleQueryResult, StatementDescription, SyncNoTlsProbe, TextQueryResult,
};
use crate::session::SyncNoTlsSession;
use std::str::FromStr;

const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 130;
const MIN_CONNECT_TIMEOUT_SECS: u64 = 2;
pub(crate) const DEFAULT_POSTGRES_PORT: u16 = 5432;

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

    pub fn simple_query_no_tls(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, ProbeError> {
        self.connect_no_tls_session()?.simple_query(query)
    }

    pub fn simple_query_results_no_tls(
        &self,
        query: &str,
    ) -> Result<Vec<SimpleQueryResult>, ProbeError> {
        self.connect_no_tls_session()?.simple_query_results(query)
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

        Ok(SyncNoTlsSession::from_client(client))
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

pub fn simple_query_no_tls(
    conninfo: &str,
    query: &str,
) -> Result<Vec<SimpleQueryMessage>, ProbeError> {
    let config = BootstrapConfig::parse(conninfo).map_err(ProbeError::Parse)?;
    config.simple_query_no_tls(query)
}

pub fn simple_query_results_no_tls(
    conninfo: &str,
    query: &str,
) -> Result<Vec<SimpleQueryResult>, ProbeError> {
    let config = BootstrapConfig::parse(conninfo).map_err(ProbeError::Parse)?;
    config.simple_query_results_no_tls(query)
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

fn normalize_connect_timeout(timeout_seconds: Option<u64>) -> u64 {
    match timeout_seconds {
        None | Some(0) => DEFAULT_CONNECT_TIMEOUT_SECS,
        Some(timeout) => timeout.max(MIN_CONNECT_TIMEOUT_SECS),
    }
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
