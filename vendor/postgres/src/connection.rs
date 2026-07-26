use crate::{Error, Notification};
use futures_util::Stream;
use std::collections::VecDeque;
use std::future::{self, Future};
use std::ops::{Deref, DerefMut};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::runtime::Runtime;
use tokio_postgres::AsyncMessage;
use tokio_postgres::error::DbError;

pub struct Connection {
    runtime: Runtime,
    connection: Pin<Box<dyn Stream<Item = Result<AsyncMessage, Error>> + Send>>,
    notifications: VecDeque<Notification>,
    notice_callback: Arc<dyn Fn(DbError) + Sync + Send>,
}

impl Connection {
    pub fn new<S, T>(
        runtime: Runtime,
        connection: tokio_postgres::Connection<S, T>,
        notice_callback: Arc<dyn Fn(DbError) + Sync + Send>,
    ) -> Connection
    where
        S: AsyncRead + AsyncWrite + Unpin + 'static + Send,
        T: AsyncRead + AsyncWrite + Unpin + 'static + Send,
    {
        Connection {
            runtime,
            connection: Box::pin(ConnectionStream { connection }),
            notifications: VecDeque::new(),
            notice_callback,
        }
    }

    pub fn as_ref(&mut self) -> ConnectionRef<'_> {
        ConnectionRef { connection: self }
    }

    pub fn enter<F, T>(&self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        let _guard = self.runtime.enter();
        f()
    }

    pub fn block_on<F, T>(&mut self, future: F) -> Result<T, Error>
    where
        F: Future<Output = Result<T, Error>>,
    {
        let mut future = pin!(future);
        self.poll_block_on(|cx, _, _| future.as_mut().poll(cx))
    }

    pub fn poll_block_on<F, T>(&mut self, mut f: F) -> Result<T, Error>
    where
        F: FnMut(&mut Context<'_>, &mut VecDeque<Notification>, bool) -> Poll<Result<T, Error>>,
    {
        let connection = &mut self.connection;
        let notifications = &mut self.notifications;
        let notice_callback = &mut self.notice_callback;
        self.runtime.block_on({
            future::poll_fn(|cx| {
                let done = loop {
                    match connection.as_mut().poll_next(cx) {
                        Poll::Ready(Some(Ok(AsyncMessage::Notification(notification)))) => {
                            notifications.push_back(notification);
                        }
                        Poll::Ready(Some(Ok(AsyncMessage::Notice(notice)))) => {
                            notice_callback(notice)
                        }
                        Poll::Ready(Some(Ok(_))) => {}
                        Poll::Ready(Some(Err(e))) => {
                            // A fatal ErrorResponse and EOF can arrive together.
                            // Prefer the operation error already delivered by the
                            // connection before falling back to the terminal error.
                            return match f(cx, notifications, true) {
                                Poll::Ready(result) => Poll::Ready(result),
                                Poll::Pending => Poll::Ready(Err(e)),
                            };
                        }
                        Poll::Ready(None) => break true,
                        Poll::Pending => break false,
                    }
                };

                f(cx, notifications, done)
            })
        })
    }

    pub fn notifications(&self) -> &VecDeque<Notification> {
        &self.notifications
    }

    pub fn notifications_mut(&mut self) -> &mut VecDeque<Notification> {
        &mut self.notifications
    }
}

pub struct ConnectionRef<'a> {
    connection: &'a mut Connection,
}

// no-op impl to extend the borrow until drop
impl Drop for ConnectionRef<'_> {
    #[inline]
    fn drop(&mut self) {}
}

impl Deref for ConnectionRef<'_> {
    type Target = Connection;

    #[inline]
    fn deref(&self) -> &Connection {
        self.connection
    }
}

impl DerefMut for ConnectionRef<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Connection {
        self.connection
    }
}

struct ConnectionStream<S, T> {
    connection: tokio_postgres::Connection<S, T>,
}

impl<S, T> Stream for ConnectionStream<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<AsyncMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.connection.poll_message(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use tokio::runtime::Builder;

    #[test]
    fn operation_result_wins_terminal_connection_error() {
        let mut connection = connection_with_error();

        let result = connection.poll_block_on(|_, _, _| Poll::Ready(Ok(42)));

        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn terminal_connection_error_wins_pending_operation() {
        let mut connection = connection_with_error();

        let result: Result<(), Error> =
            connection.poll_block_on(|_, _, _| Poll::Pending);

        assert!(result.is_err());
    }

    fn connection_with_error() -> Connection {
        let error = "invalid-option=1"
            .parse::<tokio_postgres::Config>()
            .unwrap_err();
        Connection {
            runtime: Builder::new_current_thread().build().unwrap(),
            connection: Box::pin(stream::iter([Err(error)])),
            notifications: VecDeque::new(),
            notice_callback: Arc::new(|_| {}),
        }
    }
}
