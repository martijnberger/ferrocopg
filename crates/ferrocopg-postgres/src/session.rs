use crate::error::ProbeError;
use crate::model::{
    BackendNotification, CopyOutResult, ExecuteResult, PreparedStatementInfo, SimpleQueryMessage,
    StatementColumn, StatementDescription, StatementParameter, SyncNoTlsProbe, TextQueryResult,
};
use crate::params::{parsed_query_params, query_param_refs};
use fallible_iterator::FallibleIterator;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::time::Duration;

#[derive(Clone)]
pub struct SyncNoTlsCancelHandle {
    inner: postgres::CancelToken,
}

impl SyncNoTlsCancelHandle {
    pub fn cancel(&self) -> Result<(), ProbeError> {
        self.inner
            .cancel_query(postgres::NoTls)
            .map_err(ProbeError::Connect)
    }
}

pub struct SyncNoTlsSession {
    client: Option<postgres::Client>,
    prepared: HashMap<u64, postgres::Statement>,
    next_statement_id: u64,
}

impl SyncNoTlsSession {
    pub(crate) fn from_client(client: postgres::Client) -> Self {
        Self {
            client: Some(client),
            prepared: HashMap::new(),
            next_statement_id: 1,
        }
    }

    #[cfg(test)]
    pub(crate) fn closed_for_tests() -> Self {
        Self {
            client: None,
            prepared: HashMap::new(),
            next_statement_id: 1,
        }
    }

    pub fn closed(&self) -> bool {
        self.client.is_none()
    }

    pub fn close(&mut self) {
        self.prepared.clear();
        self.client.take();
    }

    pub fn cancel_handle(&self) -> Result<SyncNoTlsCancelHandle, ProbeError> {
        let client = self.client.as_ref().ok_or(ProbeError::Closed)?;
        Ok(SyncNoTlsCancelHandle {
            inner: client.cancel_token(),
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

fn simple_query_messages(
    messages: Vec<postgres::SimpleQueryMessage>,
) -> Result<Vec<SimpleQueryMessage>, ProbeError> {
    messages.into_iter().map(simple_query_message).collect()
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
