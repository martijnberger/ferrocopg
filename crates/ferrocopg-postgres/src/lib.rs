//! Backend selection and transport-facing helpers for ferrocopg.
//!
//! The long-term plan is to build ferrocopg on the `rust-postgres` ecosystem
//! instead of mirroring the current `libpq`/Cython transport layer.

mod bootstrap;
mod error;
mod model;
mod params;
mod session;

pub use bootstrap::{
    BootstrapConfig, backend_core, backend_stack, bootstrap_summary, connect_no_tls_probe,
    connect_no_tls_session, connect_plan, connect_target, describe_text_no_tls,
    execute_text_params_no_tls, query_text_no_tls, query_text_params_no_tls, simple_query_no_tls,
};
pub use error::ProbeError;
pub use model::{
    BackendNotification, ConnectEndpoint, ConnectPlan, ConnectTarget, ConninfoSummary,
    CopyOutResult, ExecuteResult, PreparedStatementInfo, SimpleQueryMessage, StatementColumn,
    StatementDescription, StatementParameter, SyncNoTlsProbe, TextQueryResult,
};
pub use session::{SyncNoTlsCancelHandle, SyncNoTlsSession};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::DEFAULT_POSTGRES_PORT;

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
        let mut session = SyncNoTlsSession::closed_for_tests();
        assert!(session.closed());
        assert!(matches!(session.cancel_handle(), Err(ProbeError::Closed)));
        assert!(matches!(
            session.query_text("select 1"),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(
            session.simple_query("select 1"),
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
            session.copy_from_stdin("copy demo from stdin", b""),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(
            session.copy_to_stdout("copy demo to stdout"),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(
            session.listen("ferrocopg"),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(
            session.unlisten("ferrocopg"),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(
            session.notify("ferrocopg", "payload"),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(
            session.drain_notifications(),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(
            session.wait_for_notification(10),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(
            session.describe_text("select 1"),
            Err(ProbeError::Closed)
        ));
        assert!(matches!(session.probe(), Err(ProbeError::Closed)));
    }
}
