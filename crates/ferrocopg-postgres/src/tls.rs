use crate::conninfo::{LibpqSslMode, TlsOptions};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};
use std::sync::Arc;
use tokio_postgres_rustls::MakeRustlsConnect;

#[derive(Debug)]
struct AcceptAllServerVerifier;

impl ServerCertVerifier for AcceptAllServerVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub(crate) fn make_tls_connector(tls: &TlsOptions) -> Result<MakeRustlsConnect, String> {
    let verifier = verifier_for_mode(tls)?;
    let builder = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(verifier);
    let mut config = match (&tls.sslcert, &tls.sslkey) {
        (Some(cert_path), Some(key_path)) => builder
            .with_client_auth_cert(
                load_certificate_chain(cert_path)?,
                load_private_key(key_path)?,
            )
            .map_err(|err| format!("failed to configure TLS client certificate: {err}"))?,
        _ => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"postgresql".to_vec()];
    Ok(MakeRustlsConnect::new(config))
}

fn verifier_for_mode(tls: &TlsOptions) -> Result<Arc<dyn ServerCertVerifier>, String> {
    match tls.sslmode {
        LibpqSslMode::Disable
        | LibpqSslMode::Allow
        | LibpqSslMode::Prefer
        | LibpqSslMode::Require => Ok(Arc::new(AcceptAllServerVerifier)),
        LibpqSslMode::VerifyCa | LibpqSslMode::VerifyFull => {
            let roots = Arc::new(load_root_store(tls)?);
            WebPkiServerVerifier::builder(roots)
                .build()
                .map(|verifier| verifier as Arc<dyn ServerCertVerifier>)
                .map_err(|err| format!("failed to configure TLS verifier: {err}"))
        }
    }
}

fn load_root_store(tls: &TlsOptions) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    match tls.sslrootcert.as_deref() {
        Some("system") | None => {
            let native = rustls_native_certs::load_native_certs();
            for cert in native.certs {
                roots
                    .add(cert)
                    .map_err(|err| format!("failed to add native TLS root: {err}"))?;
            }
            if roots.is_empty() {
                return Err("no native TLS root certificates were available".to_owned());
            }
        }
        Some(path) => {
            for cert in CertificateDer::pem_file_iter(path)
                .map_err(|err| format!("failed to open sslrootcert {path}: {err}"))?
            {
                roots
                    .add(cert.map_err(|err| format!("failed to read sslrootcert {path}: {err}"))?)
                    .map_err(|err| format!("failed to add sslrootcert {path}: {err}"))?;
            }
            if roots.is_empty() {
                return Err(format!("sslrootcert {path} did not contain certificates"));
            }
        }
    }
    Ok(roots)
}

fn load_certificate_chain(path: &str) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_file_iter(path)
        .map_err(|err| format!("failed to open sslcert {path}: {err}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("failed to read sslcert {path}: {err}"))?;
    if certs.is_empty() {
        return Err(format!("sslcert {path} did not contain certificates"));
    }
    Ok(certs)
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path).map_err(|err| format!("failed to read sslkey {path}: {err}"))
}
