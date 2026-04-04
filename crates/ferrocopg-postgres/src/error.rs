use std::error::Error;
use std::fmt;

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
            Self::Connect(err) => write!(f, "{}", postgres_error_message(err)),
            Self::Query(err) => write!(f, "{}", postgres_error_message(err)),
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
            Self::BadParam(_) | Self::NoTlsNotSupported | Self::Closed => None,
        }
    }
}

fn postgres_error_message(err: &postgres::Error) -> String {
    err.as_db_error()
        .map(|db_err| db_err.message().to_owned())
        .unwrap_or_else(|| err.to_string())
}
