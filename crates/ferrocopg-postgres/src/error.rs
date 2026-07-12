use std::error::Error;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresDiagnostic {
    pub severity: Option<String>,
    pub severity_nonlocalized: Option<String>,
    pub sqlstate: String,
    pub message_primary: String,
    pub message_detail: Option<String>,
    pub message_hint: Option<String>,
    pub statement_position: Option<String>,
    pub internal_position: Option<String>,
    pub internal_query: Option<String>,
    pub context: Option<String>,
    pub schema_name: Option<String>,
    pub table_name: Option<String>,
    pub column_name: Option<String>,
    pub datatype_name: Option<String>,
    pub constraint_name: Option<String>,
    pub source_file: Option<String>,
    pub source_line: Option<String>,
    pub source_function: Option<String>,
    raw_fields: Vec<(u8, Vec<u8>)>,
}

impl PostgresDiagnostic {
    pub fn raw_field(&self, field_type: u8) -> Option<&[u8]> {
        self.raw_fields
            .iter()
            .find_map(|(type_, value)| (*type_ == field_type).then_some(value.as_slice()))
    }
}

#[derive(Debug)]
pub enum ProbeError {
    Parse(tokio_postgres::Error),
    TlsConfig(String),
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
            Self::TlsConfig(msg) => write!(f, "{msg}"),
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

impl ProbeError {
    pub fn diagnostic(&self) -> Option<PostgresDiagnostic> {
        match self {
            Self::Parse(err) => err.as_db_error().map(postgres_diagnostic),
            Self::Connect(err) | Self::Query(err) => err.as_db_error().map(postgres_diagnostic),
            Self::BadParam(_) | Self::TlsConfig(_) | Self::NoTlsNotSupported | Self::Closed => None,
        }
    }
}

impl Error for ProbeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Parse(err) => Some(err),
            Self::Connect(err) => Some(err),
            Self::Query(err) => Some(err),
            Self::BadParam(_) | Self::TlsConfig(_) | Self::NoTlsNotSupported | Self::Closed => None,
        }
    }
}

fn postgres_error_message(err: &postgres::Error) -> String {
    if let Some(db_err) = err.as_db_error() {
        return db_err.message().to_owned();
    }
    if err.is_timeout() {
        return "connection timeout expired".to_owned();
    }

    let mut messages = vec![err.to_string()];
    let mut source = err.source();
    while let Some(error) = source {
        let message = error.to_string();
        if messages.last() != Some(&message) {
            messages.push(message);
        }
        source = error.source();
    }
    messages.join(": ")
}

pub(crate) fn postgres_diagnostic(db_err: &postgres::error::DbError) -> PostgresDiagnostic {
    const FIELD_TYPES: &[u8] = b"SVCMDHPpqWstcdnFLR";
    let (statement_position, internal_position, internal_query) = match db_err.position() {
        Some(postgres::error::ErrorPosition::Original(position)) => {
            (Some(position.to_string()), None, None)
        }
        Some(postgres::error::ErrorPosition::Internal { position, query }) => {
            (None, Some(position.to_string()), Some(query.clone()))
        }
        None => (None, None, None),
    };

    PostgresDiagnostic {
        severity: Some(db_err.severity().to_owned()),
        severity_nonlocalized: db_err
            .parsed_severity()
            .map(|severity| format!("{severity:?}").to_ascii_uppercase()),
        sqlstate: db_err.code().code().to_owned(),
        message_primary: db_err.message().to_owned(),
        message_detail: db_err.detail().map(str::to_owned),
        message_hint: db_err.hint().map(str::to_owned),
        statement_position,
        internal_position,
        internal_query,
        context: db_err.where_().map(str::to_owned),
        schema_name: db_err.schema().map(str::to_owned),
        table_name: db_err.table().map(str::to_owned),
        column_name: db_err.column().map(str::to_owned),
        datatype_name: db_err.datatype().map(str::to_owned),
        constraint_name: db_err.constraint().map(str::to_owned),
        source_file: db_err.file().map(str::to_owned),
        source_line: db_err.line().map(|line| line.to_string()),
        source_function: db_err.routine().map(str::to_owned),
        raw_fields: FIELD_TYPES
            .iter()
            .filter_map(|type_| {
                db_err
                    .field_bytes(*type_)
                    .map(|value| (*type_, value.to_vec()))
            })
            .collect(),
    }
}
