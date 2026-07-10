use crate::conninfo::TlsOptions;
use crate::error::ProbeError;
use crate::model::{
    BackendNotification, BoundParam, CopyOutResult, ExecuteResult, PreparedStatementInfo,
    ResultSet, SimpleQueryMessage, SimpleQueryResult, StatementColumn, StatementDescription,
    StatementParameter, SyncNoTlsProbe, TextQueryResult,
};
use crate::params::{
    bound_param_types, bound_query_params, param_types_from_oids, parsed_query_params,
    query_param_refs,
};
use fallible_iterator::FallibleIterator;
use postgres::types::{FromSql, Type};
use std::collections::HashMap;
use std::error::Error;
use std::io::{Read, Write};
use std::time::Duration;

#[derive(Clone, Copy)]
enum SessionTlsMode {
    NoTls,
    Tls,
}

#[derive(Clone)]
pub struct SyncNoTlsCancelHandle {
    inner: postgres::CancelToken,
    tls_mode: SessionTlsMode,
    tls: Option<TlsOptions>,
}

impl SyncNoTlsCancelHandle {
    pub fn cancel(&self) -> Result<(), ProbeError> {
        match self.tls_mode {
            SessionTlsMode::NoTls => self
                .inner
                .cancel_query(postgres::NoTls)
                .map_err(ProbeError::Connect),
            SessionTlsMode::Tls => {
                self.inner
                    .cancel_query(
                        crate::tls::make_tls_connector(self.tls.as_ref().ok_or_else(|| {
                            ProbeError::TlsConfig("missing TLS options".to_owned())
                        })?)
                        .map_err(ProbeError::TlsConfig)?,
                    )
                    .map_err(ProbeError::Connect)
            }
        }
    }
}

/// Capture a PostgreSQL value without assigning it a Rust type first.
///
/// `postgres` always receives extended-protocol result values in binary
/// format. Keeping the bytes intact lets the Python adapter use Psycopg's
/// established OID-specific loaders instead of duplicating them in Rust.
#[derive(Debug)]
struct WireValue(Vec<u8>);

impl<'a> FromSql<'a> for WireValue {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(raw.to_vec()))
    }

    fn accepts(_: &Type) -> bool {
        true
    }
}

pub struct SyncNoTlsSession {
    client: Option<postgres::Client>,
    tls_mode: SessionTlsMode,
    tls: Option<TlsOptions>,
    prepared: HashMap<u64, postgres::Statement>,
    prepared_queries: HashMap<u64, String>,
    next_statement_id: u64,
}

impl SyncNoTlsSession {
    pub(crate) fn from_client(client: postgres::Client) -> Self {
        Self {
            client: Some(client),
            tls_mode: SessionTlsMode::NoTls,
            tls: None,
            prepared: HashMap::new(),
            prepared_queries: HashMap::new(),
            next_statement_id: 1,
        }
    }

    pub(crate) fn from_tls_client(client: postgres::Client, tls: TlsOptions) -> Self {
        Self {
            client: Some(client),
            tls_mode: SessionTlsMode::Tls,
            tls: Some(tls),
            prepared: HashMap::new(),
            prepared_queries: HashMap::new(),
            next_statement_id: 1,
        }
    }

    #[cfg(test)]
    pub(crate) fn closed_for_tests() -> Self {
        Self {
            client: None,
            tls_mode: SessionTlsMode::NoTls,
            tls: None,
            prepared: HashMap::new(),
            prepared_queries: HashMap::new(),
            next_statement_id: 1,
        }
    }

    pub fn closed(&self) -> bool {
        self.client.is_none()
    }

    pub fn close(&mut self) {
        self.prepared.clear();
        self.prepared_queries.clear();
        self.client.take();
    }

    pub fn cancel_handle(&self) -> Result<SyncNoTlsCancelHandle, ProbeError> {
        let client = self.client.as_ref().ok_or(ProbeError::Closed)?;
        Ok(SyncNoTlsCancelHandle {
            inner: client.cancel_token(),
            tls_mode: self.tls_mode,
            tls: self.tls.clone(),
        })
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

    pub fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, ProbeError> {
        let messages = self
            .client_mut()?
            .simple_query(query)
            .map_err(ProbeError::Query)?;
        simple_query_messages(messages)
    }

    pub fn simple_query_results(
        &mut self,
        query: &str,
    ) -> Result<Vec<SimpleQueryResult>, ProbeError> {
        let messages = self
            .client_mut()?
            .simple_query(query)
            .map_err(ProbeError::Query)?;
        simple_query_results(messages)
    }

    pub fn pipeline_simple_query_results(
        &mut self,
        queries: &[String],
    ) -> Result<Vec<Vec<SimpleQueryResult>>, ProbeError> {
        queries
            .iter()
            .map(|query| self.simple_query_results(query))
            .collect()
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

    pub fn run_text_params(
        &mut self,
        query: &str,
        params: &[Option<String>],
    ) -> Result<ResultSet, ProbeError> {
        let statement = self
            .client_mut()?
            .prepare(query)
            .map_err(ProbeError::Query)?;

        // A zero-column SELECT is still a tuple result. The postgres crate
        // represents it with an empty statement column list, indistinguishable
        // from a command unless we inspect simple-query messages.
        if statement.columns().is_empty() && params.is_empty() {
            return self.run_no_column_statement(query);
        }

        self.run_statement_params(&statement, params)
    }

    pub fn run_params(
        &mut self,
        query: &str,
        params: &[BoundParam],
    ) -> Result<ResultSet, ProbeError> {
        let types = bound_param_types(params);
        let statement = self
            .client_mut()?
            .prepare_typed(query, &types)
            .map_err(ProbeError::Query)?;
        self.run_bound_statement_params(&statement, params)
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
        self.prepared_queries.insert(statement_id, query.to_owned());
        Ok(PreparedStatementInfo {
            statement_id,
            description,
        })
    }

    pub fn prepare_params(
        &mut self,
        query: &str,
        param_oids: &[u32],
    ) -> Result<PreparedStatementInfo, ProbeError> {
        let types = param_types_from_oids(param_oids);
        let statement = self
            .client_mut()?
            .prepare_typed(query, &types)
            .map_err(ProbeError::Query)?;
        let statement_id = self.next_statement_id;
        self.next_statement_id += 1;
        let description = statement_description(&statement);
        self.prepared.insert(statement_id, statement);
        self.prepared_queries.insert(statement_id, query.to_owned());
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

    pub fn run_prepared_text_params(
        &mut self,
        statement_id: u64,
        params: &[Option<String>],
    ) -> Result<ResultSet, ProbeError> {
        let statement = self.prepared_statement(statement_id)?.clone();
        if statement.columns().is_empty() && params.is_empty() {
            let query = self.prepared_query(statement_id)?.to_owned();
            return self.run_no_column_statement(&query);
        }
        self.run_statement_params(&statement, params)
    }

    pub fn run_prepared_params(
        &mut self,
        statement_id: u64,
        params: &[BoundParam],
    ) -> Result<ResultSet, ProbeError> {
        let statement = self.prepared_statement(statement_id)?.clone();
        self.run_bound_statement_params(&statement, params)
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
            .map(|_| {
                self.prepared_queries.remove(&statement_id);
            })
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

    pub fn copy_from_stdin(&mut self, query: &str, data: &[u8]) -> Result<u64, ProbeError> {
        let mut writer = self
            .client_mut()?
            .copy_in(query)
            .map_err(ProbeError::Query)?;
        writer
            .write_all(data)
            .map_err(io_error_as_postgres_bad_param)?;
        writer.finish().map_err(ProbeError::Query)
    }

    pub fn copy_to_stdout(&mut self, query: &str) -> Result<CopyOutResult, ProbeError> {
        let mut reader = self
            .client_mut()?
            .copy_out(query)
            .map_err(ProbeError::Query)?;
        let mut data = Vec::new();
        reader
            .read_to_end(&mut data)
            .map_err(io_error_as_postgres_bad_param)?;
        Ok(CopyOutResult { data })
    }

    pub fn listen(&mut self, channel: &str) -> Result<(), ProbeError> {
        let query = format!("listen {}", quoted_identifier(channel));
        self.client_mut()?
            .batch_execute(&query)
            .map_err(ProbeError::Query)
    }

    pub fn unlisten(&mut self, channel: &str) -> Result<(), ProbeError> {
        let query = format!("unlisten {}", quoted_identifier(channel));
        self.client_mut()?
            .batch_execute(&query)
            .map_err(ProbeError::Query)
    }

    pub fn notify(&mut self, channel: &str, payload: &str) -> Result<(), ProbeError> {
        self.client_mut()?
            .execute(
                "select pg_notify($1::text, $2::text)",
                &[&channel, &payload],
            )
            .map(|_| ())
            .map_err(ProbeError::Query)
    }

    pub fn drain_notifications(&mut self) -> Result<Vec<BackendNotification>, ProbeError> {
        let mut notifications = self.client_mut()?.notifications();
        let mut iter = notifications.iter();
        let mut drained = Vec::new();

        while let Some(notification) = iter.next().map_err(ProbeError::Query)? {
            drained.push(backend_notification(notification));
        }

        Ok(drained)
    }

    pub fn wait_for_notification(
        &mut self,
        timeout_ms: u64,
    ) -> Result<Option<BackendNotification>, ProbeError> {
        let mut notifications = self.client_mut()?.notifications();
        notifications
            .timeout_iter(Duration::from_millis(timeout_ms))
            .next()
            .map_err(ProbeError::Query)
            .map(|notification| notification.map(backend_notification))
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

    fn prepared_query(&self, statement_id: u64) -> Result<&str, ProbeError> {
        if self.closed() {
            return Err(ProbeError::Closed);
        }

        self.prepared_queries
            .get(&statement_id)
            .map(String::as_str)
            .ok_or_else(|| missing_statement(statement_id))
    }

    fn run_no_column_statement(&mut self, query: &str) -> Result<ResultSet, ProbeError> {
        let messages = self
            .client_mut()?
            .simple_query(query)
            .map_err(ProbeError::Query)?;

        let mut is_tuples = false;
        let mut rows = Vec::new();

        for message in messages {
            match message {
                postgres::SimpleQueryMessage::RowDescription(columns) => {
                    if !columns.is_empty() {
                        return Err(ProbeError::BadParam(
                            "expected a zero-column result".to_owned(),
                        ));
                    }
                    is_tuples = true;
                }
                postgres::SimpleQueryMessage::Row(row) => {
                    if row.len() != 0 {
                        return Err(ProbeError::BadParam(
                            "expected a zero-column row".to_owned(),
                        ));
                    }
                    rows.push(Vec::new());
                }
                postgres::SimpleQueryMessage::CommandComplete(rows_affected) => {
                    return Ok(ResultSet {
                        columns: Vec::new(),
                        column_descriptions: Vec::new(),
                        rows,
                        rows_affected,
                        is_tuples,
                    });
                }
                _ => {
                    return Err(ProbeError::BadParam(
                        "unsupported simple query message from backend".to_owned(),
                    ));
                }
            }
        }

        Err(ProbeError::BadParam(
            "backend returned no completion message".to_owned(),
        ))
    }

    fn run_statement_params(
        &mut self,
        statement: &postgres::Statement,
        params: &[Option<String>],
    ) -> Result<ResultSet, ProbeError> {
        let params = parsed_query_params(statement, params)?;
        let refs = query_param_refs(&params);

        self.run_statement_refs(statement, &refs)
    }

    fn run_bound_statement_params(
        &mut self,
        statement: &postgres::Statement,
        params: &[BoundParam],
    ) -> Result<ResultSet, ProbeError> {
        let params = bound_query_params(statement, params)?;
        let refs = query_param_refs(&params);

        self.run_statement_refs(statement, &refs)
    }

    fn run_statement_refs(
        &mut self,
        statement: &postgres::Statement,
        refs: &[&(dyn postgres::types::ToSql + Sync)],
    ) -> Result<ResultSet, ProbeError> {
        if statement.columns().is_empty() {
            let rows_affected = self
                .client_mut()?
                .execute(statement, &refs)
                .map_err(ProbeError::Query)?;
            Ok(ResultSet {
                columns: Vec::new(),
                column_descriptions: Vec::new(),
                rows: Vec::new(),
                rows_affected,
                is_tuples: false,
            })
        } else {
            let rows = self
                .client_mut()?
                .query(statement, &refs)
                .map_err(ProbeError::Query)?;
            result_set_from_statement_rows(statement, rows)
        }
    }
}

pub(crate) fn statement_description(statement: &postgres::Statement) -> StatementDescription {
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
    let rows = rows_to_text_values(rows)?;

    Ok(TextQueryResult { columns, rows })
}

fn result_set_from_statement_rows(
    statement: &postgres::Statement,
    rows: Vec<postgres::Row>,
) -> Result<ResultSet, ProbeError> {
    let column_descriptions = statement_description(statement).columns;
    let columns = column_descriptions
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let rows = rows_to_wire_values(rows)?;
    let rows_affected = rows.len() as u64;
    Ok(ResultSet {
        columns,
        column_descriptions,
        rows,
        rows_affected,
        is_tuples: true,
    })
}

fn rows_to_wire_values(rows: Vec<postgres::Row>) -> Result<Vec<Vec<Option<Vec<u8>>>>, ProbeError> {
    rows.into_iter()
        .map(|row| {
            (0..row.len())
                .map(|index| {
                    row.try_get::<_, Option<WireValue>>(index)
                        .map(|value| value.map(|value| value.0))
                        .map_err(ProbeError::Query)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect()
}

fn rows_to_text_values(rows: Vec<postgres::Row>) -> Result<Vec<Vec<Option<String>>>, ProbeError> {
    rows.into_iter()
        .map(|row| {
            (0..row.len())
                .map(|index| {
                    row.try_get::<_, Option<String>>(index)
                        .map_err(ProbeError::Query)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect()
}

fn simple_query_messages(
    messages: Vec<postgres::SimpleQueryMessage>,
) -> Result<Vec<SimpleQueryMessage>, ProbeError> {
    messages.into_iter().map(simple_query_message).collect()
}

fn simple_query_results(
    messages: Vec<postgres::SimpleQueryMessage>,
) -> Result<Vec<SimpleQueryResult>, ProbeError> {
    let mut results = Vec::new();
    let mut current_columns = Vec::new();
    let mut current_rows = Vec::new();

    for message in messages {
        match message {
            postgres::SimpleQueryMessage::RowDescription(columns) => {
                current_columns = columns
                    .iter()
                    .map(|column| column.name().to_owned())
                    .collect();
                current_rows.clear();
            }
            postgres::SimpleQueryMessage::Row(row) => {
                let values = (0..row.len())
                    .map(|index| {
                        row.try_get(index)
                            .map(|value| value.map(str::to_owned))
                            .map_err(ProbeError::Query)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                current_rows.push(values);
            }
            postgres::SimpleQueryMessage::CommandComplete(rows_affected) => {
                results.push(SimpleQueryResult {
                    columns: std::mem::take(&mut current_columns),
                    rows: std::mem::take(&mut current_rows),
                    rows_affected,
                });
            }
            _ => {
                return Err(ProbeError::BadParam(
                    "unsupported simple query message from backend".to_owned(),
                ));
            }
        }
    }

    Ok(results)
}

fn simple_query_message(
    message: postgres::SimpleQueryMessage,
) -> Result<SimpleQueryMessage, ProbeError> {
    match message {
        postgres::SimpleQueryMessage::RowDescription(columns) => Ok(SimpleQueryMessage {
            kind: "row_description",
            columns: columns
                .iter()
                .map(|column| column.name().to_owned())
                .collect(),
            values: Vec::new(),
            rows_affected: None,
        }),
        postgres::SimpleQueryMessage::Row(row) => {
            let columns = row
                .columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect();
            let values = (0..row.len())
                .map(|index| {
                    row.try_get(index)
                        .map(|value| value.map(str::to_owned))
                        .map_err(ProbeError::Query)
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(SimpleQueryMessage {
                kind: "row",
                columns,
                values,
                rows_affected: None,
            })
        }
        postgres::SimpleQueryMessage::CommandComplete(rows_affected) => Ok(SimpleQueryMessage {
            kind: "command_complete",
            columns: Vec::new(),
            values: Vec::new(),
            rows_affected: Some(rows_affected),
        }),
        _ => Err(ProbeError::BadParam(
            "unsupported simple query message from backend".to_owned(),
        )),
    }
}

fn backend_notification(notification: postgres::Notification) -> BackendNotification {
    BackendNotification {
        process_id: notification.process_id(),
        channel: notification.channel().to_owned(),
        payload: notification.payload().to_owned(),
    }
}

fn quoted_identifier(identifier: &str) -> String {
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn missing_statement(statement_id: u64) -> ProbeError {
    ProbeError::BadParam(format!("unknown prepared statement id: {statement_id}"))
}

fn io_error_as_postgres_bad_param(err: std::io::Error) -> ProbeError {
    ProbeError::BadParam(err.to_string())
}
