#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LibpqSslMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl LibpqSslMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Allow => "allow",
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::VerifyCa => "verify-ca",
            Self::VerifyFull => "verify-full",
        }
    }

    pub(crate) fn tokio_sslmode(self) -> &'static str {
        match self {
            Self::Disable => "disable",
            Self::Allow | Self::Prefer => "prefer",
            Self::Require | Self::VerifyCa | Self::VerifyFull => "require",
        }
    }

    pub(crate) fn can_bootstrap_with_no_tls(self) -> bool {
        matches!(self, Self::Disable | Self::Allow | Self::Prefer)
    }

    pub(crate) fn requires_rustls_connector(self) -> bool {
        !matches!(self, Self::Disable)
    }
}

impl Default for LibpqSslMode {
    fn default() -> Self {
        Self::Prefer
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TlsOptions {
    pub(crate) sslmode: LibpqSslMode,
    pub(crate) sslrootcert: Option<String>,
    pub(crate) sslcert: Option<String>,
    pub(crate) sslkey: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedConninfo {
    pub(crate) tokio_conninfo: String,
    pub(crate) tls: TlsOptions,
    pub(crate) channel_binding: Option<LibpqChannelBinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LibpqChannelBinding {
    Disable,
    Prefer,
    Require,
}

impl LibpqChannelBinding {
    pub(crate) fn tokio(self) -> tokio_postgres::config::ChannelBinding {
        match self {
            Self::Disable => tokio_postgres::config::ChannelBinding::Disable,
            Self::Prefer => tokio_postgres::config::ChannelBinding::Prefer,
            Self::Require => tokio_postgres::config::ChannelBinding::Require,
        }
    }

    pub(crate) fn postgres(self) -> postgres::config::ChannelBinding {
        match self {
            Self::Disable => postgres::config::ChannelBinding::Disable,
            Self::Prefer => postgres::config::ChannelBinding::Prefer,
            Self::Require => postgres::config::ChannelBinding::Require,
        }
    }
}

pub(crate) fn normalize_conninfo(conninfo: &str) -> NormalizedConninfo {
    if looks_like_url(conninfo) {
        return NormalizedConninfo {
            tokio_conninfo: conninfo.to_owned(),
            tls: TlsOptions::default(),
            channel_binding: None,
        };
    }

    let Some(params) = parse_keyword_conninfo(conninfo) else {
        return NormalizedConninfo {
            tokio_conninfo: conninfo.to_owned(),
            tls: TlsOptions::default(),
            channel_binding: None,
        };
    };

    let mut tls = TlsOptions::default();
    let mut channel_binding = None;
    let mut normalized = Vec::new();

    for (key, value) in params {
        match key.as_str() {
            "sslmode" => {
                tls.sslmode = match value.as_str() {
                    "disable" => LibpqSslMode::Disable,
                    "allow" => LibpqSslMode::Allow,
                    "prefer" => LibpqSslMode::Prefer,
                    "require" => LibpqSslMode::Require,
                    "verify-ca" => LibpqSslMode::VerifyCa,
                    "verify-full" => LibpqSslMode::VerifyFull,
                    _ => {
                        normalized.push((key, value));
                        continue;
                    }
                };
                normalized.push(("sslmode".to_owned(), tls.sslmode.tokio_sslmode().to_owned()));
            }
            "sslrootcert" => tls.sslrootcert = Some(value),
            "sslcert" => tls.sslcert = Some(value),
            "sslkey" => tls.sslkey = Some(value),
            "channel_binding" => {
                channel_binding = match value.as_str() {
                    "disable" => Some(LibpqChannelBinding::Disable),
                    "prefer" => Some(LibpqChannelBinding::Prefer),
                    "require" => Some(LibpqChannelBinding::Require),
                    _ => {
                        normalized.push((key, value));
                        continue;
                    }
                };
            }
            _ => normalized.push((key, value)),
        }
    }

    NormalizedConninfo {
        tokio_conninfo: render_keyword_conninfo(&normalized),
        tls,
        channel_binding,
    }
}

fn looks_like_url(conninfo: &str) -> bool {
    conninfo.starts_with("postgres://") || conninfo.starts_with("postgresql://")
}

fn parse_keyword_conninfo(conninfo: &str) -> Option<Vec<(String, String)>> {
    let mut chars = conninfo.char_indices().peekable();
    let mut params = Vec::new();

    loop {
        while matches!(chars.peek(), Some((_, ch)) if ch.is_whitespace()) {
            chars.next();
        }

        let key_start = match chars.peek() {
            Some((idx, _)) => *idx,
            None => break,
        };

        while matches!(chars.peek(), Some((_, ch)) if !ch.is_whitespace() && *ch != '=') {
            chars.next();
        }

        let key_end = chars.peek().map_or(conninfo.len(), |(idx, _)| *idx);
        let key = &conninfo[key_start..key_end];
        if key.is_empty() {
            return None;
        }

        while matches!(chars.peek(), Some((_, ch)) if ch.is_whitespace()) {
            chars.next();
        }

        if !matches!(chars.next(), Some((_, '='))) {
            return None;
        }

        while matches!(chars.peek(), Some((_, ch)) if ch.is_whitespace()) {
            chars.next();
        }

        let value = if matches!(chars.peek(), Some((_, '\''))) {
            chars.next();
            let mut value = String::new();
            loop {
                match chars.next() {
                    Some((_, '\'')) => break value,
                    Some((_, '\\')) => match chars.next() {
                        Some((_, escaped)) => value.push(escaped),
                        None => return None,
                    },
                    Some((_, ch)) => value.push(ch),
                    None => return None,
                }
            }
        } else {
            let value_start = chars.peek().map_or(conninfo.len(), |(idx, _)| *idx);
            while matches!(chars.peek(), Some((_, ch)) if !ch.is_whitespace()) {
                chars.next();
            }
            let value_end = chars.peek().map_or(conninfo.len(), |(idx, _)| *idx);
            if value_start == value_end {
                return None;
            }
            conninfo[value_start..value_end].to_owned()
        };

        params.push((key.to_owned(), value));
    }

    Some(params)
}

fn render_keyword_conninfo(params: &[(String, String)]) -> String {
    params
        .iter()
        .map(|(key, value)| format!("{key}={}", quote_value(value)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn quote_value(value: &str) -> String {
    if value
        .chars()
        .all(|ch| !ch.is_whitespace() && ch != '\'' && ch != '\\')
    {
        return value.to_owned();
    }

    let mut quoted = String::from("'");
    for ch in value.chars() {
        if ch == '\'' || ch == '\\' {
            quoted.push('\\');
        }
        quoted.push(ch);
    }
    quoted.push('\'');
    quoted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_libpq_tls_options_for_tokio_postgres() {
        let normalized = normalize_conninfo(
            "host=localhost sslmode=verify-full sslrootcert=system sslcert='client cert.pem' sslkey=client.key",
        );

        assert_eq!(normalized.tls.sslmode, LibpqSslMode::VerifyFull);
        assert_eq!(normalized.tls.sslrootcert.as_deref(), Some("system"));
        assert_eq!(normalized.tls.sslcert.as_deref(), Some("client cert.pem"));
        assert_eq!(normalized.tls.sslkey.as_deref(), Some("client.key"));
        assert_eq!(normalized.tokio_conninfo, "host=localhost sslmode=require");
        assert_eq!(normalized.channel_binding, None);
    }

    #[test]
    fn preserves_quoted_values_when_rendering_sanitized_conninfo() {
        let normalized = normalize_conninfo("application_name='ferro copg' sslmode=allow");

        assert_eq!(normalized.tls.sslmode, LibpqSslMode::Allow);
        assert_eq!(
            normalized.tokio_conninfo,
            "application_name='ferro copg' sslmode=prefer"
        );
    }

    #[test]
    fn extracts_channel_binding_for_programmatic_configuration() {
        let normalized = normalize_conninfo(
            "host=localhost sslmode=verify-full channel_binding=require dbname=postgres",
        );

        assert_eq!(
            normalized.tokio_conninfo,
            "host=localhost sslmode=require dbname=postgres"
        );
        assert_eq!(
            normalized.channel_binding,
            Some(LibpqChannelBinding::Require)
        );
    }
}
