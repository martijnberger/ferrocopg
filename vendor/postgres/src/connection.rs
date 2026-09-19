use crate::{Error, Notification};
use futures_util::Stream;
use std::collections::VecDeque;
use std::future::{self, Future};
use std::ops::{Deref, DerefMut};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::runtime::Runtime;
use tokio_postgres::AsyncMessage;
use tokio_postgres::error::DbError;

pub struct Connection {
    runtime: Runtime,
    connection: Pin<Box<dyn Stream<Item = Result<AsyncMessage, Error>> + Send>>,
    notifications: VecDeque<Notification>,
    notice_callback: Arc<dyn Fn(DbError) + Sync + Send>,
    wait_callback: Option<Arc<dyn Fn() + Sync + Send>>,
    next_wait_check: Instant,
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
            wait_callback: None,
            next_wait_check: Instant::now(),
        }
    }

    pub fn set_wait_callback(&mut self, callback: Option<Arc<dyn Fn() + Sync + Send>>) {
        self.wait_callback = callback;
        self.next_wait_check = Instant::now() + Duration::from_millis(10);
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
        self.poll_block_on_inner(|cx, _, _| future.as_mut().poll(cx), true)
    }

    pub fn poll_block_on<F, T>(&mut self, f: F) -> Result<T, Error>
    where
        F: FnMut(&mut Context<'_>, &mut VecDeque<Notification>, bool) -> Poll<Result<T, Error>>,
    {
        self.poll_block_on_inner(f, false)
    }

    fn poll_block_on_inner<F, T>(&mut self, mut f: F, mut prime: bool) -> Result<T, Error>
    where
        F: FnMut(&mut Context<'_>, &mut VecDeque<Notification>, bool) -> Poll<Result<T, Error>>,
    {
        let connection = &mut self.connection;
        let notifications = &mut self.notifications;
        let notice_callback = &mut self.notice_callback;
        let mut poll = |cx: &mut Context<'_>| {
            // Queue a new operation before driving the connection, but still
            // drain notices/errors before returning an immediately ready result.
            let primed_result = if std::mem::take(&mut prime) {
                match f(cx, notifications, false) {
                    Poll::Ready(result) => Some(result),
                    Poll::Pending => None,
                }
            } else {
                None
            };
            let done = loop {
                match connection.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(AsyncMessage::Notification(notification)))) => {
                        notifications.push_back(notification);
                    }
                    Poll::Ready(Some(Ok(AsyncMessage::Notice(notice)))) => notice_callback(notice),
                    Poll::Ready(Some(Ok(_))) => {}
                    Poll::Ready(Some(Err(e))) => {
                        // A fatal ErrorResponse and EOF can arrive together.
                        // Prefer the operation error already delivered by the
                        // connection before falling back to the terminal error.
                        if let Some(result) = primed_result {
                            return Poll::Ready(result);
                        }
                        return match f(cx, notifications, true) {
                            Poll::Ready(result) => Poll::Ready(result),
                            Poll::Pending => Poll::Ready(Err(e)),
                        };
                    }
                    Poll::Ready(None) => break true,
                    Poll::Pending => break false,
                }
            };

            match primed_result {
                Some(result) => Poll::Ready(result),
                None => f(cx, notifications, done),
            }
        };
        let Some(callback) = self.wait_callback.as_ref() else {
            return self.runtime.block_on(future::poll_fn(poll));
        };
        let next_check = &mut self.next_wait_check;
        loop {
            let result = self.runtime.block_on(async {
                let mut timer = pin!(tokio::time::sleep_until((*next_check).into()));
                future::poll_fn(|cx| {
                    if timer.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(None);
                    }
                    poll(cx).map(Some)
                })
                .await
            });
            if let Some(result) = result {
                return result;
            }
            // Exit the runtime before invoking external code, which may drive
            // another client (for example, to send a cancellation request).
            callback();
            *next_check = Instant::now() + Duration::from_millis(10);
        }
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

        let result: Result<(), Error> = connection.poll_block_on(|_, _, _| Poll::Pending);

        assert!(result.is_err());
    }

    #[test]
    fn primed_result_is_not_repolled_after_terminal_connection_error() {
        let mut connection = connection_with_error();
        assert_eq!(connection.block_on(async { Ok(42) }).unwrap(), 42);
    }

    #[test]
    fn terminal_connection_error_wins_pending_primed_operation() {
        let mut connection = connection_with_error();
        let result: Result<(), Error> = connection.block_on(future::pending());
        assert!(result.is_err());
    }

    #[test]
    fn ready_primed_operation_still_drives_connection() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let polls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&polls);
        let mut connection = connection_with_error();
        connection.connection = Box::pin(stream::poll_fn(move |_| {
            seen.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }));

        assert_eq!(connection.block_on(async { Ok(42) }).unwrap(), 42);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn notification_polling_still_drives_connection_before_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let driven = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&driven);
        let mut connection = connection_with_error();
        connection.connection = Box::pin(stream::poll_fn(move |_| {
            seen.store(true, Ordering::SeqCst);
            Poll::Pending
        }));

        let result = connection.poll_block_on(|_, _, _| {
            assert!(driven.load(Ordering::SeqCst));
            Poll::Ready(Ok(42))
        });
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn request_is_queued_before_first_connection_poll() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        let queued = Arc::new(AtomicBool::new(false));
        let driven = Arc::new(AtomicBool::new(false));
        let polls = Arc::new(AtomicUsize::new(0));
        let mut connection = connection_with_error();
        connection.runtime = Builder::new_current_thread().enable_time().build().unwrap();
        connection.set_wait_callback(Some(Arc::new(|| {
            assert!(tokio::runtime::Handle::try_current().is_err());
        })));
        connection.connection = Box::pin(stream::poll_fn({
            let queued = Arc::clone(&queued);
            let driven = Arc::clone(&driven);
            let polls = Arc::clone(&polls);
            move |_| {
                polls.fetch_add(1, Ordering::SeqCst);
                driven.store(queued.load(Ordering::SeqCst), Ordering::SeqCst);
                Poll::Pending
            }
        }));

        let result = connection.block_on(future::poll_fn(|cx| {
            if !queued.swap(true, Ordering::SeqCst) {
                cx.waker().wake_by_ref();
            }
            if driven.load(Ordering::SeqCst) {
                Poll::Ready(Ok(42))
            } else {
                Poll::Pending
            }
        }));

        assert_eq!(result.unwrap(), 42);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn wait_callback_runs_outside_runtime_and_resumes_pending_operation() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let mut connection = connection_with_error();
        connection.runtime = Builder::new_current_thread().enable_time().build().unwrap();
        connection.connection = Box::pin(stream::pending());
        connection.set_wait_callback(Some(Arc::new(move || {
            assert!(tokio::runtime::Handle::try_current().is_err());
            let runtime = Builder::new_current_thread().build().unwrap();
            assert_eq!(runtime.block_on(async { 42 }), 42);
            seen.fetch_add(1, Ordering::SeqCst);
        })));

        let result = connection.poll_block_on(|_, _, _| {
            if calls.load(Ordering::SeqCst) >= 2 {
                Poll::Ready(Ok(42))
            } else {
                Poll::Pending
            }
        });

        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        connection.set_wait_callback(None);
        assert_eq!(connection.block_on(async { Ok(43) }).unwrap(), 43);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
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
            wait_callback: None,
            next_wait_check: Instant::now(),
        }
    }
}
