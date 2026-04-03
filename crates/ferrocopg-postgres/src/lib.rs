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

fn normalize_connect_timeout(timeout_seconds: Option<u64>) -> u64 {
    match timeout_seconds {
        None | Some(0) => DEFAULT_CONNECT_TIMEOUT_SECS,
        Some(timeout) => timeout.max(MIN_CONNECT_TIMEOUT_SECS),
    }
}
