//! Rows.

use crate::row::sealed::{AsName, Sealed};
use crate::simple_query::SimpleColumn;
use crate::statement::Column;
use crate::types::{FromSql, Type, WrongType};
use crate::{Error, Statement};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::DataRowBody;
use smallvec::SmallVec;
use std::fmt;
use std::ops::Range;
use std::str;
use std::sync::Arc;

mod sealed {
    pub trait Sealed {}

    pub trait AsName {
        fn as_name(&self) -> &str;
    }
}

impl AsName for Column {
    fn as_name(&self) -> &str {
        self.name()
    }
}

impl AsName for String {
    fn as_name(&self) -> &str {
        self
    }
}

/// A trait implemented by types that can index into columns of a row.
///
/// This cannot be implemented outside of this crate.
pub trait RowIndex: Sealed {
    #[doc(hidden)]
    fn __idx<T>(&self, columns: &[T]) -> Option<usize>
    where
        T: AsName;
}

impl Sealed for usize {}

impl RowIndex for usize {
    #[inline]
    fn __idx<T>(&self, columns: &[T]) -> Option<usize>
    where
        T: AsName,
    {
        if *self >= columns.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl Sealed for str {}

impl RowIndex for str {
    #[inline]
    fn __idx<T>(&self, columns: &[T]) -> Option<usize>
    where
        T: AsName,
    {
        if let Some(idx) = columns.iter().position(|d| d.as_name() == self) {
            return Some(idx);
        };

        // FIXME ASCII-only case insensitivity isn't really the right thing to
        // do. Postgres itself uses a dubious wrapper around tolower and JDBC
        // uses the US locale.
        columns
            .iter()
            .position(|d| d.as_name().eq_ignore_ascii_case(self))
    }
}

impl<T> Sealed for &T where T: ?Sized + Sealed {}

impl<T> RowIndex for &T
where
    T: ?Sized + RowIndex,
{
    #[inline]
    fn __idx<U>(&self, columns: &[U]) -> Option<usize>
    where
        U: AsName,
    {
        T::__idx(*self, columns)
    }
}

/// A row of data returned from the database by a query.
#[derive(Clone)]
pub struct Row {
    statement: Statement,
    body: DataRowBody,
    ranges: RowRanges,
}

/// Validated field offsets, stored inline for the common one- and two-column rows.
pub type RowRanges = SmallVec<[Option<Range<usize>>; 2]>;

fn row_ranges(body: &DataRowBody) -> Result<RowRanges, Error> {
    let mut fields = body.ranges();
    let mut ranges = RowRanges::with_capacity(fields.size_hint().0);
    while let Some(range) = fields.next().map_err(Error::parse)? {
        ranges.push(range);
    }
    Ok(ranges)
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row")
            .field("columns", &self.columns())
            .finish()
    }
}

impl Row {
    pub(crate) fn new(statement: Statement, body: DataRowBody) -> Result<Row, Error> {
        let ranges = row_ranges(&body)?;
        Ok(Row {
            statement,
            body,
            ranges,
        })
    }

    /// Returns information about the columns of data in the row.
    pub fn columns(&self) -> &[Column] {
        self.statement.columns()
    }

    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.columns().len()
    }

    /// Deserializes a value from the row.
    ///
    /// The value can be specified either by its numeric index in the row, or by its column name.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    #[track_caller]
    pub fn get<'a, I, T>(&'a self, idx: I) -> T
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        match self.get_inner(&idx) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
        }
    }

    /// Like `Row::get`, but returns a `Result` rather than panicking.
    pub fn try_get<'a, I, T>(&'a self, idx: I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        self.get_inner(&idx)
    }

    fn get_inner<'a, I, T>(&'a self, idx: &I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        let idx = match idx.__idx(self.columns()) {
            Some(idx) => idx,
            None => return Err(Error::column(idx.to_string())),
        };

        let ty = self.columns()[idx].type_();
        if !T::accepts(ty) {
            return Err(Error::from_sql(
                Box::new(WrongType::new::<T>(ty.clone())),
                idx,
            ));
        }

        FromSql::from_sql_nullable(ty, self.col_buffer(idx)).map_err(|e| Error::from_sql(e, idx))
    }

    /// Returns the raw size of the row in bytes.
    pub fn raw_size_bytes(&self) -> usize {
        self.body.buffer_bytes().len()
    }

    /// Consumes the row, retaining its wire buffer and validated field ranges.
    ///
    /// Unlike retaining `Row`, these parts do not keep the statement alive.
    pub fn into_raw_parts(self) -> (bytes::Bytes, RowRanges) {
        (self.body.buffer_bytes().clone(), self.ranges)
    }

    /// Get the raw bytes for the column at the given index.
    fn col_buffer(&self, idx: usize) -> Option<&[u8]> {
        let range = self.ranges[idx].to_owned()?;
        Some(&self.body.buffer()[range])
    }
}

impl AsName for SimpleColumn {
    fn as_name(&self) -> &str {
        self.name()
    }
}

/// A row of data returned from the database by a simple query.
#[derive(Debug)]
pub struct SimpleQueryRow {
    columns: Arc<[SimpleColumn]>,
    body: DataRowBody,
    ranges: RowRanges,
}

impl SimpleQueryRow {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        columns: Arc<[SimpleColumn]>,
        body: DataRowBody,
    ) -> Result<SimpleQueryRow, Error> {
        let ranges = row_ranges(&body)?;
        Ok(SimpleQueryRow {
            columns,
            body,
            ranges,
        })
    }

    /// Returns information about the columns of data in the row.
    pub fn columns(&self) -> &[SimpleColumn] {
        &self.columns
    }

    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns a value from the row.
    ///
    /// The value can be specified either by its numeric index in the row, or by its column name.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    #[track_caller]
    pub fn get<I>(&self, idx: I) -> Option<&str>
    where
        I: RowIndex + fmt::Display,
    {
        match self.get_inner(&idx) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
        }
    }

    /// Like `SimpleQueryRow::get`, but returns a `Result` rather than panicking.
    pub fn try_get<I>(&self, idx: I) -> Result<Option<&str>, Error>
    where
        I: RowIndex + fmt::Display,
    {
        self.get_inner(&idx)
    }

    fn get_inner<I>(&self, idx: &I) -> Result<Option<&str>, Error>
    where
        I: RowIndex + fmt::Display,
    {
        let idx = match idx.__idx(&self.columns) {
            Some(idx) => idx,
            None => return Err(Error::column(idx.to_string())),
        };

        let buf = self.ranges[idx].clone().map(|r| &self.body.buffer()[r]);
        FromSql::from_sql_nullable(&Type::TEXT, buf).map_err(|e| Error::from_sql(e, idx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use postgres_protocol::message::backend::Message;

    fn data_row(count: u16, payload: &[u8]) -> DataRowBody {
        let mut message = BytesMut::new();
        message.extend_from_slice(b"D");
        message.extend_from_slice(&((payload.len() + 6) as i32).to_be_bytes());
        message.extend_from_slice(&count.to_be_bytes());
        message.extend_from_slice(payload);
        match Message::parse(&mut message).unwrap().unwrap() {
            Message::DataRow(body) => body,
            _ => panic!("expected a data row"),
        }
    }

    #[test]
    fn field_ranges_preserve_inline_and_spilled_values() {
        let values: [Option<&[u8]>; 3] = [None, Some(b""), Some(b"\0\xffvalue")];
        for count in [0, 1, 2, 3, 64] {
            let mut payload = Vec::new();
            for value in values.iter().cycle().take(count) {
                match value {
                    Some(value) => {
                        payload.extend_from_slice(&(value.len() as i32).to_be_bytes());
                        payload.extend_from_slice(value);
                    }
                    None => payload.extend_from_slice(&(-1_i32).to_be_bytes()),
                }
            }
            let body = data_row(count as u16, &payload);
            let ranges = row_ranges(&body).unwrap();
            let reference: Vec<_> = body.ranges().collect().unwrap();
            assert_eq!(ranges.as_slice(), reference.as_slice());
            assert_eq!(ranges.spilled(), count > 2);
            let detached = body.buffer_bytes().clone();
            drop(body);
            let actual: Vec<_> = ranges
                .iter()
                .map(|range| range.clone().map(|range| &detached[range]))
                .collect();
            assert_eq!(
                actual,
                values
                    .iter()
                    .copied()
                    .cycle()
                    .take(count)
                    .collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn field_ranges_reject_malformed_rows() {
        for (count, payload) in [
            (0, &b"extra"[..]),
            (1, &b"\0\0"[..]),
            (1, &b"\0\0\0\x05four"[..]),
            (2, &b"\xff\xff\xff\xff"[..]),
            (3, &b"\xff\xff\xff\xff\0\0\0\0\0"[..]),
        ] {
            assert!(row_ranges(&data_row(count, payload)).is_err());
        }
    }
}
