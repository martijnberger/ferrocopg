mod adapt;
mod bootstrap;
mod python_helpers;

use crate::python_helpers::{
    handle_stop_iteration, hasattr, poll_event_mask, psycopg_exception_with_pgconn,
    psycopg_operational_error,
};
use pyo3::PyErr;
use pyo3::exceptions::{PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use pyo3::wrap_pyfunction;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct SendGenerator {
    pgconn: Py<PyAny>,
    wait_rw: Py<PyAny>,
    ready_r: i32,
    state: SendState,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct FetchGenerator {
    pgconn: Py<PyAny>,
    wait_r: Py<PyAny>,
    state: FetchState,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct FetchManyGenerator {
    pgconn: Py<PyAny>,
    current_fetch: Option<Py<PyAny>>,
    results: Vec<Py<PyAny>>,
    copy_in: i32,
    copy_out: i32,
    copy_both: i32,
    pipeline_sync: i32,
    fatal_error: i32,
    has_fatal_result: bool,
    state: FetchManyState,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct ExecuteGenerator {
    pgconn: Py<PyAny>,
    current: Option<Py<PyAny>>,
    state: ExecuteState,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct PipelineCommunicateGenerator {
    pgconn: Py<PyAny>,
    commands: Py<PyAny>,
    wait_rw: Py<PyAny>,
    ready_r: i32,
    ready_w: i32,
    copy_in: i32,
    copy_out: i32,
    copy_both: i32,
    pipeline_sync: i32,
    pending_ready: i32,
    results: Vec<Vec<Py<PyAny>>>,
    current_batch: Vec<Py<PyAny>>,
    state: PipelineState,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct CancelGenerator {
    cancel_conn: Py<PyAny>,
    wait_r: Py<PyAny>,
    wait_w: Py<PyAny>,
    poll_ok: i32,
    poll_reading: i32,
    poll_writing: i32,
    poll_failed: i32,
    deadline: Option<f64>,
}

#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct ConnectGenerator {
    conn: Py<PyAny>,
    conninfo: String,
    wait_r: Py<PyAny>,
    wait_w: Py<PyAny>,
    bad: i32,
    poll_ok: i32,
    poll_reading: i32,
    poll_writing: i32,
    poll_failed: i32,
    deadline: Option<f64>,
    state: ConnectState,
    pending_wait: Option<Py<PyAny>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SendState {
    StartOrFlush,
    AwaitReady,
    Done,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FetchState {
    Start,
    AwaitReady,
    ConsumeInput,
    Done,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum FetchManyState {
    StartFetch,
    PollFetchNext,
    PollFetchSend,
    Done,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ExecuteState {
    StartSend,
    PollSendNext,
    PollSendSend,
    StartFetchMany,
    PollFetchManyNext,
    PollFetchManySend,
    Done,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum PipelineState {
    AwaitReady,
    ProcessRead,
    ProcessWrite,
    Done,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ConnectState {
    Poll,
    AwaitReady,
    Done,
}

#[pyfunction(signature = (conninfo, timeout=0.0))]
fn connect(py: Python<'_>, conninfo: &str, timeout: f64) -> PyResult<ConnectGenerator> {
    let waiting = py.import("psycopg.waiting")?;
    let pq = py.import("psycopg.pq")?;
    let conn_status = pq.getattr("ConnStatus")?;
    let polling_status = pq.getattr("PollingStatus")?;
    let conn = pq
        .getattr("PGconn")?
        .call_method1("connect_start", (conninfo.as_bytes(),))?
        .unbind();
    let deadline = if timeout > 0.0 {
        Some(
            py.import("time")?
                .getattr("monotonic")?
                .call0()?
                .extract::<f64>()?
                + timeout,
        )
    } else {
        None
    };

    Ok(ConnectGenerator {
        conn,
        conninfo: conninfo.to_owned(),
        wait_r: waiting.getattr("WAIT_R")?.unbind(),
        wait_w: waiting.getattr("WAIT_W")?.unbind(),
        bad: conn_status.getattr("BAD")?.extract::<i32>()?,
        poll_ok: polling_status.getattr("OK")?.extract::<i32>()?,
        poll_reading: polling_status.getattr("READING")?.extract::<i32>()?,
        poll_writing: polling_status.getattr("WRITING")?.extract::<i32>()?,
        poll_failed: polling_status.getattr("FAILED")?.extract::<i32>()?,
        deadline,
        state: ConnectState::Poll,
        pending_wait: None,
    })
}

#[pyfunction(signature = (cancel_conn, timeout=0.0))]
fn cancel(
    py: Python<'_>,
    cancel_conn: &Bound<'_, PyAny>,
    timeout: f64,
) -> PyResult<CancelGenerator> {
    let waiting = py.import("psycopg.waiting")?;
    let pq = py.import("psycopg.pq")?;
    let polling_status = pq.getattr("PollingStatus")?;
    let deadline = if timeout > 0.0 {
        Some(
            py.import("time")?
                .getattr("monotonic")?
                .call0()?
                .extract::<f64>()?
                + timeout,
        )
    } else {
        None
    };

    Ok(CancelGenerator {
        cancel_conn: cancel_conn.clone().unbind(),
        wait_r: waiting.getattr("WAIT_R")?.unbind(),
        wait_w: waiting.getattr("WAIT_W")?.unbind(),
        poll_ok: polling_status.getattr("OK")?.extract::<i32>()?,
        poll_reading: polling_status.getattr("READING")?.extract::<i32>()?,
        poll_writing: polling_status.getattr("WRITING")?.extract::<i32>()?,
        poll_failed: polling_status.getattr("FAILED")?.extract::<i32>()?,
        deadline,
    })
}

#[pyfunction]
fn send(py: Python<'_>, pgconn: &Bound<'_, PyAny>) -> PyResult<SendGenerator> {
    let waiting = py.import("psycopg.waiting")?;
    let wait_rw = waiting.getattr("WAIT_RW")?.unbind();
    let ready_r = waiting.getattr("READY_R")?.extract::<i32>()?;

    Ok(SendGenerator {
        pgconn: pgconn.clone().unbind(),
        wait_rw,
        ready_r,
        state: SendState::StartOrFlush,
    })
}

#[pyfunction]
fn fetch(py: Python<'_>, pgconn: &Bound<'_, PyAny>) -> PyResult<FetchGenerator> {
    let waiting = py.import("psycopg.waiting")?;
    let wait_r = waiting.getattr("WAIT_R")?.unbind();

    Ok(FetchGenerator {
        pgconn: pgconn.clone().unbind(),
        wait_r,
        state: FetchState::Start,
    })
}

#[pyfunction]
fn fetch_many(py: Python<'_>, pgconn: &Bound<'_, PyAny>) -> PyResult<FetchManyGenerator> {
    let exec_status = py.import("psycopg.pq")?.getattr("ExecStatus")?;

    Ok(FetchManyGenerator {
        pgconn: pgconn.clone().unbind(),
        current_fetch: None,
        results: Vec::new(),
        copy_in: exec_status.getattr("COPY_IN")?.extract::<i32>()?,
        copy_out: exec_status.getattr("COPY_OUT")?.extract::<i32>()?,
        copy_both: exec_status.getattr("COPY_BOTH")?.extract::<i32>()?,
        pipeline_sync: exec_status.getattr("PIPELINE_SYNC")?.extract::<i32>()?,
        fatal_error: exec_status.getattr("FATAL_ERROR")?.extract::<i32>()?,
        has_fatal_result: false,
        state: FetchManyState::StartFetch,
    })
}

#[pyfunction]
fn execute(_py: Python<'_>, pgconn: &Bound<'_, PyAny>) -> PyResult<ExecuteGenerator> {
    Ok(ExecuteGenerator {
        pgconn: pgconn.clone().unbind(),
        current: None,
        state: ExecuteState::StartSend,
    })
}

#[pyfunction]
fn pipeline_communicate(
    py: Python<'_>,
    pgconn: &Bound<'_, PyAny>,
    commands: &Bound<'_, PyAny>,
) -> PyResult<PipelineCommunicateGenerator> {
    let waiting = py.import("psycopg.waiting")?;
    let exec_status = py.import("psycopg.pq")?.getattr("ExecStatus")?;

    Ok(PipelineCommunicateGenerator {
        pgconn: pgconn.clone().unbind(),
        commands: commands.clone().unbind(),
        wait_rw: waiting.getattr("WAIT_RW")?.unbind(),
        ready_r: waiting.getattr("READY_R")?.extract::<i32>()?,
        ready_w: waiting.getattr("READY_W")?.extract::<i32>()?,
        copy_in: exec_status.getattr("COPY_IN")?.extract::<i32>()?,
        copy_out: exec_status.getattr("COPY_OUT")?.extract::<i32>()?,
        copy_both: exec_status.getattr("COPY_BOTH")?.extract::<i32>()?,
        pipeline_sync: exec_status.getattr("PIPELINE_SYNC")?.extract::<i32>()?,
        pending_ready: 0,
        results: Vec::new(),
        current_batch: Vec::new(),
        state: PipelineState::AwaitReady,
    })
}

#[pyfunction(signature = (generator, fileno, interval=0.0))]
fn wait_c(
    py: Python<'_>,
    generator: &Bound<'_, PyAny>,
    fileno: i32,
    interval: Option<f64>,
) -> PyResult<Py<PyAny>> {
    let Some(interval) = interval else {
        return Err(PyErr::new::<PyValueError, _>(
            "indefinite wait not supported anymore",
        ));
    };
    if interval.is_nan() {
        return Err(PyErr::new::<PyValueError, _>("interval cannot be NaN"));
    }
    if interval.is_infinite() {
        return Err(PyErr::new::<PyValueError, _>("interval cannot be infinite"));
    }

    let waiting = py.import("psycopg.waiting")?;
    let select = py.import("select")?;
    let errors = py.import("psycopg.errors")?;
    let operational_error = errors.getattr("OperationalError")?;

    let wait_r = waiting.getattr("WAIT_R")?.extract::<i32>()?;
    let wait_w = waiting.getattr("WAIT_W")?.extract::<i32>()?;
    let ready_none = waiting.getattr("READY_NONE")?;
    let ready_r = waiting.getattr("READY_R")?;
    let ready_w = waiting.getattr("READY_W")?;
    let ready_rw = waiting.getattr("READY_RW")?;
    let check_fd_closed = waiting.getattr("_check_fd_closed")?;
    let send = generator.getattr("send")?;
    let timeout = interval.max(0.0);
    let mut wait = match generator.call_method0("__next__") {
        Ok(wait) => wait,
        Err(err) => return handle_stop_iteration(py, err),
    };

    if hasattr(&select, "poll")? {
        let poll = select.getattr("poll")?.call0()?;
        let pollin = select.getattr("POLLIN")?.extract::<i32>()?;
        let pollout = select.getattr("POLLOUT")?.extract::<i32>()?;
        let poll_bad = !(pollin | pollout);
        let timeout_ms = (timeout * 1000.0) as i32;
        let mut current_mask = wait.extract::<i32>()?;
        poll.call_method1(
            "register",
            (
                fileno,
                poll_event_mask(current_mask, wait_r, wait_w, pollin, pollout),
            ),
        )?;

        loop {
            let file_events = poll.call_method1("poll", (timeout_ms,))?;
            let next_wait = if file_events.len()? == 0 {
                send.call1((ready_none.clone(),))
            } else {
                let event = file_events.get_item(0)?.get_item(1)?.extract::<i32>()?;
                let mut ready = 0;
                if event & pollin != 0 {
                    ready |= ready_r.extract::<i32>()?;
                }
                if event & pollout != 0 {
                    ready |= ready_w.extract::<i32>()?;
                }
                if ready == 0 && event & poll_bad != 0 {
                    if let Err(err) = check_fd_closed.call1((fileno,)) {
                        return Err(err);
                    }
                    return Err(psycopg_operational_error(
                        &operational_error,
                        "connection socket closed",
                    ));
                }
                send.call1((ready,))
            };

            match next_wait {
                Ok(next_wait) => {
                    wait = next_wait;
                    current_mask = wait.extract::<i32>()?;
                    poll.call_method1(
                        "modify",
                        (
                            fileno,
                            poll_event_mask(current_mask, wait_r, wait_w, pollin, pollout),
                        ),
                    )?;
                }
                Err(err) => return handle_stop_iteration(py, err),
            }
        }
    }

    loop {
        let wait_mask = wait.extract::<i32>()?;
        let wants_read = wait_mask & wait_r != 0;
        let wants_write = wait_mask & wait_w != 0;

        let read_fds = if wants_read { vec![fileno] } else { Vec::new() };
        let write_fds = if wants_write {
            vec![fileno]
        } else {
            Vec::new()
        };
        let except_fds = vec![fileno];

        let ready_sets =
            match select.call_method1("select", (read_fds, write_fds, except_fds, timeout)) {
                Ok(ready_sets) => ready_sets,
                Err(err) => return Err(err),
            };
        let (readable, writable, exceptional): (Vec<i32>, Vec<i32>, Vec<i32>) =
            ready_sets.extract()?;

        let ready = if !exceptional.is_empty() {
            if let Err(err) = check_fd_closed.call1((fileno,)) {
                return Err(err);
            }
            return Err(psycopg_operational_error(
                &operational_error,
                "connection socket closed",
            ));
        } else if !readable.is_empty() && !writable.is_empty() {
            ready_rw.clone()
        } else if !readable.is_empty() {
            ready_r.clone()
        } else if !writable.is_empty() {
            ready_w.clone()
        } else {
            ready_none.clone()
        };

        match send.call1((ready,)) {
            Ok(next_wait) => wait = next_wait,
            Err(err) => return handle_stop_iteration(py, err),
        }
    }
}

#[pymodule]
fn _ferrocopg(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add(
        "Transformer",
        py.import("psycopg._py_transformer")?
            .getattr("Transformer")?,
    )?;
    bootstrap::register(m)?;
    m.add_class::<ConnectGenerator>()?;
    m.add_class::<SendGenerator>()?;
    m.add_class::<FetchGenerator>()?;
    m.add_class::<FetchManyGenerator>()?;
    m.add_class::<ExecuteGenerator>()?;
    m.add_class::<PipelineCommunicateGenerator>()?;
    m.add_class::<CancelGenerator>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(cancel, m)?)?;
    adapt::register(m)?;
    m.add_function(wrap_pyfunction!(send, m)?)?;
    m.add_function(wrap_pyfunction!(fetch, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_many, m)?)?;
    m.add_function(wrap_pyfunction!(execute, m)?)?;
    m.add_function(wrap_pyfunction!(pipeline_communicate, m)?)?;
    m.add_function(wrap_pyfunction!(wait_c, m)?)?;
    Ok(())
}

#[pymethods]
impl ConnectGenerator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.advance(py, None)
    }

    fn send(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        self.advance(py, ready)
    }
}

#[pymethods]
impl SendGenerator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.advance(py, None)
    }

    fn send(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        self.advance(py, ready)
    }
}

#[pymethods]
impl FetchGenerator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.advance(py, None)
    }

    fn send(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        self.advance(py, ready)
    }
}

#[pymethods]
impl FetchManyGenerator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.advance(py, None)
    }

    fn send(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        self.advance(py, ready)
    }
}

#[pymethods]
impl ExecuteGenerator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.advance(py, None)
    }

    fn send(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        self.advance(py, ready)
    }
}

#[pymethods]
impl PipelineCommunicateGenerator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.advance(py, None)
    }

    fn send(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        self.advance(py, ready)
    }
}

#[pymethods]
impl CancelGenerator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.advance(py)
    }

    fn send(&mut self, py: Python<'_>, _ready: Option<i32>) -> PyResult<Py<PyAny>> {
        self.advance(py)
    }
}

impl ConnectGenerator {
    fn advance(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        loop {
            match self.state {
                ConnectState::Done => {
                    return Err(PyStopIteration::new_err((py.None(),)));
                }
                ConnectState::Poll => {
                    let status_now = self.conn.bind(py).getattr("status")?.extract::<i32>()?;
                    if status_now == self.bad {
                        let encoding = py
                            .import("psycopg._encodings")?
                            .getattr("conninfo_encoding")?
                            .call1((self.conninfo.as_str(),))?;
                        let msg = format!(
                            "connection is bad: {}",
                            self.conn
                                .bind(py)
                                .call_method1("get_error_message", (encoding,))?
                                .extract::<String>()?
                        );
                        return Err(psycopg_exception_with_pgconn(
                            py,
                            &py.import("psycopg.errors")?.getattr("OperationalError")?,
                            &msg,
                            self.conn.bind(py),
                        ));
                    }

                    let poll_status = self
                        .conn
                        .bind(py)
                        .call_method0("connect_poll")?
                        .extract::<i32>()?;
                    if poll_status == self.poll_reading || poll_status == self.poll_writing {
                        let wait = if poll_status == self.poll_reading {
                            self.wait_r.clone_ref(py)
                        } else {
                            self.wait_w.clone_ref(py)
                        };
                        self.pending_wait = Some(wait.clone_ref(py));
                        self.state = ConnectState::AwaitReady;
                        return Ok(PyTuple::new(
                            py,
                            [self.conn.bind(py).getattr("socket")?, wait.bind(py).clone()],
                        )?
                        .unbind()
                        .into_any());
                    }
                    if poll_status == self.poll_ok {
                        self.conn.bind(py).setattr("nonblocking", 1)?;
                        self.state = ConnectState::Done;
                        return Err(PyStopIteration::new_err((self.conn.clone_ref(py),)));
                    }
                    if poll_status == self.poll_failed {
                        let encoding = py
                            .import("psycopg._encodings")?
                            .getattr("conninfo_encoding")?
                            .call1((self.conninfo.as_str(),))?;
                        let finished = py
                            .import("psycopg.errors")?
                            .getattr("finish_pgconn")?
                            .call1((self.conn.bind(py),))?;
                        let msg = format!(
                            "connection failed: {}",
                            self.conn
                                .bind(py)
                                .call_method1("get_error_message", (encoding,))?
                                .extract::<String>()?
                        );
                        return Err(psycopg_exception_with_pgconn(
                            py,
                            &py.import("psycopg.errors")?.getattr("OperationalError")?,
                            &msg,
                            &finished,
                        ));
                    }

                    let finished = py
                        .import("psycopg.errors")?
                        .getattr("finish_pgconn")?
                        .call1((self.conn.bind(py),))?;
                    return Err(psycopg_exception_with_pgconn(
                        py,
                        &py.import("psycopg.errors")?.getattr("InternalError")?,
                        &format!("unexpected poll status: {poll_status}"),
                        &finished,
                    ));
                }
                ConnectState::AwaitReady => {
                    if let Some(deadline) = self.deadline {
                        let now = py
                            .import("time")?
                            .getattr("monotonic")?
                            .call0()?
                            .extract::<f64>()?;
                        if now > deadline {
                            return Err(psycopg_operational_error(
                                &py.import("psycopg.errors")?.getattr("ConnectionTimeout")?,
                                "connection timeout expired",
                            ));
                        }
                    }

                    if ready.unwrap_or_default() != 0 {
                        self.state = ConnectState::Poll;
                        continue;
                    }

                    let wait = self
                        .pending_wait
                        .as_ref()
                        .expect("pending wait must exist in AwaitReady")
                        .clone_ref(py);
                    return Ok(PyTuple::new(
                        py,
                        [self.conn.bind(py).getattr("socket")?, wait.bind(py).clone()],
                    )?
                    .unbind()
                    .into_any());
                }
            }
        }
    }
}

impl SendGenerator {
    fn advance(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        loop {
            match self.state {
                SendState::Done => {
                    return Err(PyStopIteration::new_err((py.None(),)));
                }
                SendState::StartOrFlush => {
                    let flush = self
                        .pgconn
                        .bind(py)
                        .call_method0("flush")?
                        .extract::<i32>()?;
                    if flush == 0 {
                        self.state = SendState::Done;
                        return Err(PyStopIteration::new_err((py.None(),)));
                    }
                    self.state = SendState::AwaitReady;
                    return Ok(self.wait_rw.clone_ref(py));
                }
                SendState::AwaitReady => {
                    let ready = ready.unwrap_or_default();
                    if ready == 0 {
                        return Ok(self.wait_rw.clone_ref(py));
                    }

                    if ready & self.ready_r != 0 {
                        self.pgconn.bind(py).call_method0("consume_input")?;
                    }

                    self.state = SendState::StartOrFlush;
                }
            }
        }
    }
}

impl FetchGenerator {
    fn advance(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        loop {
            match self.state {
                FetchState::Done => {
                    return Err(PyStopIteration::new_err((py.None(),)));
                }
                FetchState::Start => {
                    let busy = self
                        .pgconn
                        .bind(py)
                        .call_method0("is_busy")?
                        .extract::<i32>()?
                        != 0;
                    if busy {
                        self.state = FetchState::AwaitReady;
                        return Ok(self.wait_r.clone_ref(py));
                    }

                    let result = self.finish(py)?;
                    self.state = FetchState::Done;
                    return Err(PyStopIteration::new_err((result,)));
                }
                FetchState::AwaitReady => {
                    if ready.unwrap_or_default() == 0 {
                        return Ok(self.wait_r.clone_ref(py));
                    }
                    self.state = FetchState::ConsumeInput;
                }
                FetchState::ConsumeInput => {
                    self.pgconn.bind(py).call_method0("consume_input")?;
                    let busy = self
                        .pgconn
                        .bind(py)
                        .call_method0("is_busy")?
                        .extract::<i32>()?
                        != 0;
                    if busy {
                        self.state = FetchState::AwaitReady;
                        return Ok(self.wait_r.clone_ref(py));
                    }

                    let result = self.finish(py)?;
                    self.state = FetchState::Done;
                    return Err(PyStopIteration::new_err((result,)));
                }
            }
        }
    }

    fn finish(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        py.import("psycopg.generators")?
            .getattr("_consume_notifies")?
            .call1((self.pgconn.bind(py),))?;
        self.pgconn
            .bind(py)
            .call_method0("get_result")
            .map(Bound::unbind)
    }
}

impl FetchManyGenerator {
    fn advance(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        loop {
            match self.state {
                FetchManyState::Done => {
                    return Err(PyStopIteration::new_err((self.finish_results(py)?,)));
                }
                FetchManyState::StartFetch => {
                    let fetch_gen = Py::new(py, fetch(py, self.pgconn.bind(py))?)?;
                    self.current_fetch = Some(fetch_gen.into_any());
                    self.state = FetchManyState::PollFetchNext;
                }
                FetchManyState::PollFetchNext => {
                    let Some(current_fetch) = self.current_fetch.as_ref() else {
                        self.state = FetchManyState::Done;
                        continue;
                    };
                    match current_fetch.bind(py).call_method0("__next__") {
                        Ok(wait) => {
                            self.state = FetchManyState::PollFetchSend;
                            return Ok(wait.unbind());
                        }
                        Err(err) => return self.handle_fetch_error(py, err),
                    }
                }
                FetchManyState::PollFetchSend => {
                    let Some(current_fetch) = self.current_fetch.as_ref() else {
                        self.state = FetchManyState::Done;
                        continue;
                    };
                    match current_fetch.bind(py).call_method1("send", (ready,)) {
                        Ok(wait) => return Ok(wait.unbind()),
                        Err(err) => return self.handle_fetch_error(py, err),
                    }
                }
            }
        }
    }

    fn handle_fetch_error(&mut self, py: Python<'_>, err: PyErr) -> PyResult<Py<PyAny>> {
        if err.is_instance_of::<PyStopIteration>(py) {
            let value = err
                .value(py)
                .getattr("value")
                .map(Bound::unbind)
                .unwrap_or_else(|_| py.None());
            self.current_fetch = None;

            if value.bind(py).is_none() {
                self.state = FetchManyState::Done;
                return Err(PyStopIteration::new_err((self.finish_results(py)?,)));
            }

            let status = value.bind(py).getattr("status")?.extract::<i32>()?;
            if status == self.fatal_error {
                self.has_fatal_result = true;
            }
            self.results.push(value.clone_ref(py));

            if status == self.copy_in
                || status == self.copy_out
                || status == self.copy_both
                || status == self.pipeline_sync
            {
                self.state = FetchManyState::Done;
                return Err(PyStopIteration::new_err((self.finish_results(py)?,)));
            }

            self.state = FetchManyState::StartFetch;
            return self.advance(py, None);
        }

        let errors = py.import("psycopg.errors")?;
        let database_error = errors.getattr("DatabaseError")?;
        if err.is_instance(py, &database_error) && self.has_fatal_result {
            self.state = FetchManyState::Done;
            return Err(PyStopIteration::new_err((self.finish_results(py)?,)));
        }

        Err(err)
    }

    fn finish_results(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = PyList::empty(py);
        for result in &self.results {
            list.append(result.bind(py))?;
        }
        Ok(list.unbind().into_any())
    }
}

impl ExecuteGenerator {
    fn advance(&mut self, py: Python<'_>, ready: Option<i32>) -> PyResult<Py<PyAny>> {
        loop {
            match self.state {
                ExecuteState::Done => {
                    return Err(PyStopIteration::new_err((py.None(),)));
                }
                ExecuteState::StartSend => {
                    let send_gen = Py::new(py, send(py, self.pgconn.bind(py))?)?;
                    self.current = Some(send_gen.into_any());
                    self.state = ExecuteState::PollSendNext;
                }
                ExecuteState::PollSendNext => {
                    let Some(current) = self.current.as_ref() else {
                        self.state = ExecuteState::StartFetchMany;
                        continue;
                    };
                    match current.bind(py).call_method0("__next__") {
                        Ok(wait) => {
                            self.state = ExecuteState::PollSendSend;
                            return Ok(wait.unbind());
                        }
                        Err(err) => {
                            if err.is_instance_of::<PyStopIteration>(py) {
                                self.current = None;
                                self.state = ExecuteState::StartFetchMany;
                                continue;
                            }
                            return Err(err);
                        }
                    }
                }
                ExecuteState::PollSendSend => {
                    let Some(current) = self.current.as_ref() else {
                        self.state = ExecuteState::StartFetchMany;
                        continue;
                    };
                    match current.bind(py).call_method1("send", (ready,)) {
                        Ok(wait) => return Ok(wait.unbind()),
                        Err(err) => {
                            if err.is_instance_of::<PyStopIteration>(py) {
                                self.current = None;
                                self.state = ExecuteState::StartFetchMany;
                                continue;
                            }
                            return Err(err);
                        }
                    }
                }
                ExecuteState::StartFetchMany => {
                    let fetch_many_gen = Py::new(py, fetch_many(py, self.pgconn.bind(py))?)?;
                    self.current = Some(fetch_many_gen.into_any());
                    self.state = ExecuteState::PollFetchManyNext;
                }
                ExecuteState::PollFetchManyNext => {
                    let Some(current) = self.current.as_ref() else {
                        self.state = ExecuteState::Done;
                        continue;
                    };
                    match current.bind(py).call_method0("__next__") {
                        Ok(wait) => {
                            self.state = ExecuteState::PollFetchManySend;
                            return Ok(wait.unbind());
                        }
                        Err(err) => return self.finish_fetch_many(py, err),
                    }
                }
                ExecuteState::PollFetchManySend => {
                    let Some(current) = self.current.as_ref() else {
                        self.state = ExecuteState::Done;
                        continue;
                    };
                    match current.bind(py).call_method1("send", (ready,)) {
                        Ok(wait) => return Ok(wait.unbind()),
                        Err(err) => return self.finish_fetch_many(py, err),
                    }
                }
            }
        }
    }

    fn finish_fetch_many(&mut self, py: Python<'_>, err: PyErr) -> PyResult<Py<PyAny>> {
        if err.is_instance_of::<PyStopIteration>(py) {
            let value = err
                .value(py)
                .getattr("value")
                .map(Bound::unbind)
                .unwrap_or_else(|_| py.None());
            self.current = None;
            self.state = ExecuteState::Done;
            return Err(PyStopIteration::new_err((value,)));
        }

        Err(err)
    }
}

impl PipelineCommunicateGenerator {
    fn advance(&mut self, py: Python<'_>, mut ready: Option<i32>) -> PyResult<Py<PyAny>> {
        loop {
            match self.state {
                PipelineState::Done => {
                    return Err(PyStopIteration::new_err((self.finish_results(py)?,)));
                }
                PipelineState::AwaitReady => {
                    let ready = ready.unwrap_or_default();
                    if ready == 0 {
                        return Ok(self.wait_rw.clone_ref(py));
                    }

                    self.pending_ready = ready;
                    if ready & self.ready_r != 0 {
                        self.state = PipelineState::ProcessRead;
                    } else {
                        self.state = PipelineState::ProcessWrite;
                    }
                }
                PipelineState::ProcessRead => {
                    self.pgconn.bind(py).call_method0("consume_input")?;
                    py.import("psycopg.generators")?
                        .getattr("_consume_notifies")?
                        .call1((self.pgconn.bind(py),))?;

                    loop {
                        let busy = self
                            .pgconn
                            .bind(py)
                            .call_method0("is_busy")?
                            .extract::<i32>()?
                            != 0;
                        if busy {
                            break;
                        }

                        let result = self.pgconn.bind(py).call_method0("get_result")?;
                        if result.is_none() {
                            if self.current_batch.is_empty() {
                                break;
                            }
                            self.results.push(std::mem::take(&mut self.current_batch));
                        } else {
                            let status = result.getattr("status")?.extract::<i32>()?;
                            if status == self.pipeline_sync {
                                self.results.push(vec![result.unbind()]);
                            } else if status == self.copy_in
                                || status == self.copy_out
                                || status == self.copy_both
                            {
                                let errors = py.import("psycopg.errors")?;
                                return Err(psycopg_operational_error(
                                    &errors.getattr("NotSupportedError")?,
                                    "COPY cannot be used in pipeline mode",
                                ));
                            } else {
                                self.current_batch.push(result.unbind());
                            }
                        }
                    }

                    if self.pending_ready & self.ready_w != 0 {
                        self.state = PipelineState::ProcessWrite;
                    } else {
                        self.state = PipelineState::AwaitReady;
                    }
                }
                PipelineState::ProcessWrite => {
                    self.pgconn.bind(py).call_method0("flush")?;
                    if self.commands.bind(py).len()? == 0 {
                        self.state = PipelineState::Done;
                        return Err(PyStopIteration::new_err((self.finish_results(py)?,)));
                    }
                    let command = self.commands.bind(py).call_method0("popleft")?;
                    command.call0()?;
                    self.state = PipelineState::AwaitReady;
                    ready = None;
                }
            }
        }
    }

    fn finish_results(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let outer = PyList::empty(py);
        for batch in &self.results {
            let inner = PyList::empty(py);
            for result in batch {
                inner.append(result.bind(py))?;
            }
            outer.append(inner)?;
        }
        Ok(outer.unbind().into_any())
    }
}

impl CancelGenerator {
    fn advance(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(deadline) = self.deadline {
            let now = py
                .import("time")?
                .getattr("monotonic")?
                .call0()?
                .extract::<f64>()?;
            if now > deadline {
                let errors = py.import("psycopg.errors")?;
                return Err(psycopg_operational_error(
                    &errors.getattr("CancellationTimeout")?,
                    "cancellation timeout expired",
                ));
            }
        }

        let status = self
            .cancel_conn
            .bind(py)
            .call_method0("poll")?
            .extract::<i32>()?;
        if status == self.poll_ok {
            return Err(PyStopIteration::new_err((py.None(),)));
        }
        if status == self.poll_reading {
            return Ok(PyTuple::new(
                py,
                [
                    self.cancel_conn.bind(py).getattr("socket")?,
                    self.wait_r.bind(py).clone(),
                ],
            )?
            .unbind()
            .into_any());
        }
        if status == self.poll_writing {
            return Ok(PyTuple::new(
                py,
                [
                    self.cancel_conn.bind(py).getattr("socket")?,
                    self.wait_w.bind(py).clone(),
                ],
            )?
            .unbind()
            .into_any());
        }
        if status == self.poll_failed {
            let errors = py.import("psycopg.errors")?;
            let message = format!(
                "cancellation failed: {}",
                self.cancel_conn
                    .bind(py)
                    .call_method0("get_error_message")?
                    .extract::<String>()?
            );
            return Err(psycopg_operational_error(
                &errors.getattr("OperationalError")?,
                &message,
            ));
        }

        let errors = py.import("psycopg.errors")?;
        Err(psycopg_operational_error(
            &errors.getattr("InternalError")?,
            &format!("unexpected poll status: {status}"),
        ))
    }
}
