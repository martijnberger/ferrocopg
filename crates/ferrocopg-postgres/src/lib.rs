//! Backend selection and transport-facing helpers for ferrocopg.
//!
//! The long-term plan is to build ferrocopg on the `rust-postgres` ecosystem
//! instead of mirroring the current `libpq`/Cython transport layer.

use std::str::FromStr;

const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 130;
const MIN_CONNECT_TIMEOUT_SECS: u64 = 2;

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
    pub can_bootstrap_with_no_tls: bool,
    pub requires_external_tls_connector: bool,
    pub summary: ConninfoSummary,
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
            can_bootstrap_with_no_tls,
            requires_external_tls_connector: !can_bootstrap_with_no_tls,
            summary,
        }
    }
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
        tokio_postgres::config::SslMode::Require => {
            "external TLS connector required"
        }
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

fn ssl_negotiation_name(
    ssl_negotiation: tokio_postgres::config::SslNegotiation,
) -> &'static str {
    match ssl_negotiation {
        tokio_postgres::config::SslNegotiation::Postgres => "postgres",
        tokio_postgres::config::SslNegotiation::Direct => "direct",
        _ => "unknown",
    }
}
