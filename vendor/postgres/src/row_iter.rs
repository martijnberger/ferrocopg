use crate::connection::ConnectionRef;
use fallible_iterator::FallibleIterator;
use futures_util::StreamExt;
use std::pin::Pin;
use tokio_postgres::{Column, Error, Row, RowStream};

/// The iterator returned by `query_raw`.
pub struct RowIter<'a> {
    connection: ConnectionRef<'a>,
    it: Pin<Box<RowStream>>,
}

impl<'a> RowIter<'a> {
    pub(crate) fn new(connection: ConnectionRef<'a>, stream: RowStream) -> RowIter<'a> {
        RowIter {
            connection,
            it: Box::pin(stream),
        }
    }

    /// Returns the number of rows affected by the query.
    ///
    /// This function will return `None` until the iterator has been exhausted.
    pub fn rows_affected(&self) -> Option<u64> {
        self.it.rows_affected()
    }

    /// Returns the server's command tag, including an empty-query response.
    pub fn command_tag(&self) -> Option<&str> {
        self.it.command_tag()
    }

    /// Transfers the command tag to an owned execution result without copying it.
    pub fn take_command_tag(&mut self) -> Option<String> {
        self.it.as_mut().take_command_tag()
    }

    /// Returns the ReadyForQuery status after this iterator is exhausted.
    pub fn transaction_status(&self) -> Option<u8> {
        self.it.transaction_status()
    }

    /// Returns information about the columns produced by the query.
    pub fn columns(&self) -> &[Column] {
        self.it.columns()
    }

    /// Collects the remaining rows in a single runtime call.
    ///
    /// After success, `rows_affected()` contains the final command count.
    pub fn collect_rows(&mut self) -> Result<Vec<Row>, Error> {
        let it = &mut self.it;
        self.connection.block_on(async {
            let mut rows = Vec::new();
            while let Some(row) = it.next().await.transpose()? {
                rows.push(row);
            }
            Ok(rows)
        })
    }
}

impl FallibleIterator for RowIter<'_> {
    type Item = Row;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Row>, Error> {
        let it = &mut self.it;
        self.connection
            .block_on(async { it.next().await.transpose() })
    }
}
