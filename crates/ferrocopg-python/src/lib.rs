use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use pyo3::wrap_pyfunction;
use pyo3::{
    PyErr,
    exceptions::{PyStopIteration, PyValueError},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendConninfoSummary {
    #[pyo3(get)]
    user: Option<String>,
    #[pyo3(get)]
    dbname: Option<String>,
    #[pyo3(get)]
    application_name: Option<String>,
    #[pyo3(get)]
    host_count: usize,
    #[pyo3(get)]
    hostaddr_count: usize,
    #[pyo3(get)]
    port_count: usize,
    #[pyo3(get)]
    has_password: bool,
    #[pyo3(get)]
    connect_timeout_seconds: Option<u64>,
    #[pyo3(get)]
    effective_connect_timeout_seconds: u64,
}

#[derive(Clone)]
#[pyclass(module = "ferrocopg_rust._ferrocopg")]
struct BackendConnectPlan {
    #[pyo3(get)]
    backend_stack: String,
    #[pyo3(get)]
    sync_client: String,
    #[pyo3(get)]
    async_client: String,
    #[pyo3(get)]
    sync_runtime: String,
    #[pyo3(get)]
    async_runtime: String,
    #[pyo3(get)]
    tls_mode: String,
    #[pyo3(get)]
    tls_negotiation: String,
    #[pyo3(get)]
    tls_connector_hint: String,
    #[pyo3(get)]
    target_session_attrs: String,
    #[pyo3(get)]
    load_balance_hosts: String,
    #[pyo3(get)]
    can_bootstrap_with_no_tls: bool,
    #[pyo3(get)]
    requires_external_tls_connector: bool,
    #[pyo3(get)]
    summary: BackendConninfoSummary,
}

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

#[pyfunction]
fn milestone() -> &'static str {
    "milestone-1-bootstrap"
}

#[pyfunction]
fn scaffold_status() -> &'static str {
    "ferrocopg Rust extension scaffold is wired through maturin"
}

#[pyfunction]
fn backend_stack() -> &'static str {
    ferrocopg_postgres::backend_stack()
}

#[pyfunction]
fn backend_core() -> &'static str {
    ferrocopg_postgres::backend_core()
}

#[pyfunction]
fn parse_conninfo_summary(conninfo: &str) -> PyResult<BackendConninfoSummary> {
    ferrocopg_postgres::bootstrap_summary(conninfo)
        .map(BackendConninfoSummary::from)
        .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
}

#[pyfunction]
fn parse_connect_plan(conninfo: &str) -> PyResult<BackendConnectPlan> {
    ferrocopg_postgres::connect_plan(conninfo)
        .map(BackendConnectPlan::from)
        .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
}

#[pyfunction]
fn format_row_text(
    py: Python<'_>,
    row: &Bound<'_, PyAny>,
    tx: &Bound<'_, PyAny>,
    out: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let adapted = dump_sequence(py, row, tx, "TEXT")?;
    let mut buffer = Vec::new();

    if adapted.is_empty() {
        buffer.push(b'\n');
    } else {
        for field in adapted {
            match field {
                Some(data) => append_escaped_text_field(&mut buffer, &data),
                None => buffer.extend_from_slice(br"\N"),
            }
            buffer.push(b'\t');
        }
        buffer.pop();
        buffer.push(b'\n');
    }

    extend_bytearray(out, &buffer)
}

#[pyfunction]
fn format_row_binary(
    py: Python<'_>,
    row: &Bound<'_, PyAny>,
    tx: &Bound<'_, PyAny>,
    out: &Bound<'_, PyAny>,
) -> PyResult<()> {
    let adapted = dump_sequence(py, row, tx, "BINARY")?;
    let row_len = i16::try_from(adapted.len())
        .map_err(|_| PyErr::new::<PyValueError, _>("too many fields in COPY row"))?;

    let mut buffer = Vec::new();
    buffer.extend_from_slice(&row_len.to_be_bytes());

    for field in adapted {
        match field {
            Some(data) => {
                let len = i32::try_from(data.len()).map_err(|_| {
                    PyErr::new::<PyValueError, _>("COPY binary field larger than i32")
                })?;
                buffer.extend_from_slice(&len.to_be_bytes());
                buffer.extend_from_slice(&data);
            }
            None => buffer.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }

    extend_bytearray(out, &buffer)
}

#[pyfunction]
fn parse_row_text(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    tx: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let raw = bytes_like_to_vec(py, data)?;
    let expected_fields = expected_field_count(tx).unwrap_or_default();
    let mut fields = if expected_fields == 0 && raw == b"\n" {
        Vec::new()
    } else {
        raw.split(|byte| *byte == b'\t')
            .map(|field| field.to_vec())
            .collect::<Vec<_>>()
    };

    if let Some(last) = fields.last_mut() {
        if last.last() == Some(&b'\n') {
            last.pop();
        }
    }

    let row = PyList::empty(py);
    for field in fields {
        if field == br"\N" {
            row.append(py.None())?;
        } else {
            row.append(PyBytes::new(py, &unescape_text_field(&field)))?;
        }
    }

    tx.call_method1("load_sequence", (row,)).map(Bound::unbind)
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
fn parse_row_binary(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    tx: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let data = bytes_like_to_vec(py, data)?;
    if data.len() < 2 {
        return Err(PyErr::new::<PyValueError, _>(
            "COPY binary row is truncated",
        ));
    }

    let nfields = i16::from_be_bytes([data[0], data[1]]);
    if nfields < 0 {
        return Err(PyErr::new::<PyValueError, _>(
            "COPY binary row has a negative field count",
        ));
    }

    let row = PyList::empty(py);
    let mut pos = 2_usize;
    for _ in 0..usize::try_from(nfields).unwrap_or(0) {
        if data.len().saturating_sub(pos) < 4 {
            return Err(PyErr::new::<PyValueError, _>(
                "COPY binary row is truncated",
            ));
        }

        let length = i32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
        pos += 4;

        if length < 0 {
            row.append(py.None())?;
            continue;
        }

        let length = usize::try_from(length).map_err(|_| {
            PyErr::new::<PyValueError, _>("COPY binary field length cannot fit in usize")
        })?;
        if data.len().saturating_sub(pos) < length {
            return Err(PyErr::new::<PyValueError, _>(
                "COPY binary field payload is truncated",
            ));
        }

        row.append(PyBytes::new(py, &data[pos..pos + length]))?;
        pos += length;
    }

    tx.call_method1("load_sequence", (row,)).map(Bound::unbind)
}

#[pyfunction(signature = (generator, fileno, interval=0.0))]
fn wait_c(
    py: Python<'_>,
    generator: &Bound<'_, PyAny>,
    fileno: i32,
    interval: f64,
) -> PyResult<Py<PyAny>> {
    if interval.is_nan() {
        return Err(PyErr::new::<PyValueError, _>("interval cannot be NaN"));
    }
    if interval.is_infinite() {
        return Err(PyErr::new::<PyValueError, _>(
            "indefinite wait not supported anymore",
        ));
    }

    let waiting = py.import("psycopg.waiting")?;
    let select = py.import("select")?;
    let os = py.import("os")?;
    let errors = py.import("psycopg.errors")?;
    let operational_error = errors.getattr("OperationalError")?;

    let wait_r = waiting.getattr("WAIT_R")?.extract::<i32>()?;
    let wait_w = waiting.getattr("WAIT_W")?.extract::<i32>()?;
    let ready_none = waiting.getattr("READY_NONE")?;
    let ready_r = waiting.getattr("READY_R")?;
    let ready_w = waiting.getattr("READY_W")?;
    let ready_rw = waiting.getattr("READY_RW")?;
    let send = generator.getattr("send")?;

    let timeout = interval.max(0.0);
    let mut wait = generator.call_method0("__next__")?;

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

        match select.call_method1("select", (read_fds, write_fds, except_fds, timeout)) {
            Ok(ready_sets) => {
                let (readable, writable, exceptional): (Vec<i32>, Vec<i32>, Vec<i32>) =
                    ready_sets.extract()?;

                let ready = if !exceptional.is_empty() {
                    if os.call_method1("fstat", (fileno,)).is_err() {
                        return Err(psycopg_operational_error(
                            &operational_error,
                            "connection socket closed",
                        ));
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
                    Err(err) => {
                        if err.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                            let value = err
                                .value(py)
                                .getattr("value")
                                .map(Bound::unbind)
                                .unwrap_or_else(|_| py.None());
                            return Ok(value);
                        }
                        return Err(err);
                    }
                }
            }
            Err(err) => return Err(err),
        }
    }
}

impl From<ferrocopg_postgres::ConninfoSummary> for BackendConninfoSummary {
    fn from(summary: ferrocopg_postgres::ConninfoSummary) -> Self {
        Self {
            user: summary.user,
            dbname: summary.dbname,
            application_name: summary.application_name,
            host_count: summary.host_count,
            hostaddr_count: summary.hostaddr_count,
            port_count: summary.port_count,
            has_password: summary.has_password,
            connect_timeout_seconds: summary.connect_timeout_seconds,
            effective_connect_timeout_seconds: summary.effective_connect_timeout_seconds,
        }
    }
}

impl From<ferrocopg_postgres::ConnectPlan> for BackendConnectPlan {
    fn from(plan: ferrocopg_postgres::ConnectPlan) -> Self {
        Self {
            backend_stack: plan.backend_stack.to_owned(),
            sync_client: plan.sync_client.to_owned(),
            async_client: plan.async_client.to_owned(),
            sync_runtime: plan.sync_runtime.to_owned(),
            async_runtime: plan.async_runtime.to_owned(),
            tls_mode: plan.tls_mode.to_owned(),
            tls_negotiation: plan.tls_negotiation.to_owned(),
            tls_connector_hint: plan.tls_connector_hint.to_owned(),
            target_session_attrs: plan.target_session_attrs.to_owned(),
            load_balance_hosts: plan.load_balance_hosts.to_owned(),
            can_bootstrap_with_no_tls: plan.can_bootstrap_with_no_tls,
            requires_external_tls_connector: plan.requires_external_tls_connector,
            summary: plan.summary.into(),
        }
    }
}

#[pymodule]
fn _ferrocopg(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", VERSION)?;
    m.add_class::<BackendConninfoSummary>()?;
    m.add_class::<BackendConnectPlan>()?;
    m.add_class::<SendGenerator>()?;
    m.add_class::<FetchGenerator>()?;
    m.add_class::<FetchManyGenerator>()?;
    m.add_class::<ExecuteGenerator>()?;
    m.add_function(wrap_pyfunction!(milestone, m)?)?;
    m.add_function(wrap_pyfunction!(scaffold_status, m)?)?;
    m.add_function(wrap_pyfunction!(backend_stack, m)?)?;
    m.add_function(wrap_pyfunction!(backend_core, m)?)?;
    m.add_function(wrap_pyfunction!(parse_conninfo_summary, m)?)?;
    m.add_function(wrap_pyfunction!(parse_connect_plan, m)?)?;
    m.add_function(wrap_pyfunction!(format_row_text, m)?)?;
    m.add_function(wrap_pyfunction!(format_row_binary, m)?)?;
    m.add_function(wrap_pyfunction!(send, m)?)?;
    m.add_function(wrap_pyfunction!(fetch, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_many, m)?)?;
    m.add_function(wrap_pyfunction!(execute, m)?)?;
    m.add_function(wrap_pyfunction!(parse_row_text, m)?)?;
    m.add_function(wrap_pyfunction!(parse_row_binary, m)?)?;
    m.add_function(wrap_pyfunction!(wait_c, m)?)?;
    Ok(())
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

fn dump_sequence(
    py: Python<'_>,
    row: &Bound<'_, PyAny>,
    tx: &Bound<'_, PyAny>,
    format_name: &str,
) -> PyResult<Vec<Option<Vec<u8>>>> {
    let pyformat = py
        .import("psycopg.adapt")?
        .getattr("PyFormat")?
        .getattr(format_name)?;
    let formats = PyList::empty(py);
    for _ in 0..row.len()? {
        formats.append(pyformat.clone())?;
    }

    let adapted = tx.call_method1("dump_sequence", (row, formats))?;
    let mut out = Vec::new();
    for item in adapted.try_iter()? {
        let item = item?;
        if item.is_none() {
            out.push(None);
        } else {
            out.push(Some(bytes_like_to_vec(py, &item)?));
        }
    }

    Ok(out)
}

fn bytes_like_to_vec(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    py.import("builtins")?
        .getattr("bytes")?
        .call1((obj,))?
        .extract()
}

fn expected_field_count(tx: &Bound<'_, PyAny>) -> PyResult<usize> {
    if let Ok(value) = tx.getattr("_nfields") {
        return value.extract();
    }

    if let Ok(loaders) = tx.getattr("_row_loaders") {
        return loaders.len();
    }

    Ok(0)
}

fn psycopg_operational_error(exc_type: &Bound<'_, PyAny>, message: &str) -> PyErr {
    let exc = exc_type
        .call1((message,))
        .expect("OperationalError constructor should succeed");
    PyErr::from_value(exc)
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
                        .extract::<bool>()?;
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
                        .extract::<bool>()?;
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

fn extend_bytearray(out: &Bound<'_, PyAny>, data: &[u8]) -> PyResult<()> {
    out.call_method1("extend", (PyBytes::new(out.py(), data),))?;
    Ok(())
}

fn append_escaped_text_field(out: &mut Vec<u8>, field: &[u8]) {
    for byte in field {
        match byte {
            0x08 => out.extend_from_slice(br"\b"),
            b'\t' => out.extend_from_slice(br"\t"),
            b'\n' => out.extend_from_slice(br"\n"),
            0x0b => out.extend_from_slice(br"\v"),
            0x0c => out.extend_from_slice(br"\f"),
            b'\r' => out.extend_from_slice(br"\r"),
            b'\\' => out.extend_from_slice(br"\\"),
            _ => out.push(*byte),
        }
    }
}

fn unescape_text_field(field: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(field.len());
    let mut pos = 0;
    while pos < field.len() {
        if field[pos] == b'\\' && pos + 1 < field.len() {
            match field[pos + 1] {
                b'b' => {
                    out.push(0x08);
                    pos += 2;
                    continue;
                }
                b't' => {
                    out.push(b'\t');
                    pos += 2;
                    continue;
                }
                b'n' => {
                    out.push(b'\n');
                    pos += 2;
                    continue;
                }
                b'v' => {
                    out.push(0x0b);
                    pos += 2;
                    continue;
                }
                b'f' => {
                    out.push(0x0c);
                    pos += 2;
                    continue;
                }
                b'r' => {
                    out.push(b'\r');
                    pos += 2;
                    continue;
                }
                b'\\' => {
                    out.push(b'\\');
                    pos += 2;
                    continue;
                }
                _ => {}
            }
        }

        out.push(field[pos]);
        pos += 1;
    }

    out
}
