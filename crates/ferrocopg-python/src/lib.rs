use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
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
fn array_load_binary(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    tx: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let data = bytes_like_to_vec(py, data)?;
    let value = parse_binary_array(py, &data, tx)?;
    Ok(value.unbind().into_any())
}

#[pyfunction(signature = (data, loader, delimiter=None))]
fn array_load_text(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    loader: &Bound<'_, PyAny>,
    delimiter: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let data = bytes_like_to_vec(py, data)?;
    let delimiter = delimiter
        .map(|value| bytes_like_to_vec(py, value))
        .transpose()?
        .unwrap_or_else(|| vec![b',']);
    let value = parse_text_array(py, &data, loader, &delimiter)?;
    Ok(value.unbind().into_any())
}

#[pyfunction]
fn uuid_load_text(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let data = bytes_like_to_vec(py, data)?;
    let text =
        std::str::from_utf8(&data).map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))?;
    let uuid = py.import("uuid")?.getattr("UUID")?.call1((text,))?;
    Ok(uuid.unbind().into_any())
}

#[pyfunction]
fn uuid_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let data = bytes_like_to_vec(py, data)?;
    if data.len() != 16 {
        return Err(PyErr::new::<PyValueError, _>("Invalid UUID data"));
    }

    let kwargs = PyDict::new(py);
    kwargs.set_item("bytes", PyBytes::new(py, &data))?;
    let uuid = py
        .import("uuid")?
        .getattr("UUID")?
        .call((), Some(&kwargs))?;
    Ok(uuid.unbind().into_any())
}

#[pyfunction]
fn bool_dump_text(obj: bool) -> &'static [u8] {
    if obj { b"t" } else { b"f" }
}

#[pyfunction]
fn bool_dump_binary(obj: bool) -> &'static [u8] {
    if obj { b"\x01" } else { b"\x00" }
}

#[pyfunction]
fn bool_load_text(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(bytes_like_to_vec(py, data)? == b"t")
}

#[pyfunction]
fn bool_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(bytes_like_to_vec(py, data)? != b"\x00")
}

#[pyfunction]
fn str_dump_binary(py: Python<'_>, obj: &str, encoding: &str) -> PyResult<Py<PyAny>> {
    if encoding == "utf-8" {
        return Ok(PyBytes::new(py, obj.as_bytes()).unbind().into_any());
    }

    let encoded = PyString::new(py, obj).call_method1("encode", (encoding,))?;
    Ok(encoded.unbind())
}

#[pyfunction]
fn str_dump_text(py: Python<'_>, obj: &str, encoding: &str) -> PyResult<Py<PyAny>> {
    if obj.contains('\0') {
        return Err(psycopg_operational_error(
            &py.import("psycopg.errors")?.getattr("DataError")?,
            "PostgreSQL text fields cannot contain NUL (0x00) bytes",
        ));
    }

    str_dump_binary(py, obj, encoding)
}

#[pyfunction]
fn text_load(py: Python<'_>, data: &Bound<'_, PyAny>, encoding: &str) -> PyResult<Py<PyAny>> {
    let data = bytes_like_to_vec(py, data)?;
    if encoding.is_empty() {
        return Ok(PyBytes::new(py, &data).unbind().into_any());
    }

    if encoding == "utf-8" {
        let decoded = std::str::from_utf8(&data)
            .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))?;
        return Ok(PyString::new(py, decoded).unbind().into_any());
    }

    let decoded = PyBytes::new(py, &data).call_method1("decode", (encoding,))?;
    Ok(decoded.unbind())
}

#[pyfunction]
fn bytes_dump_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    Ok(PyBytes::new(py, &bytes_like_to_vec(py, data)?)
        .unbind()
        .into_any())
}

#[pyfunction]
fn bytea_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    Ok(PyBytes::new(py, &bytes_like_to_vec(py, data)?)
        .unbind()
        .into_any())
}

#[pyfunction]
fn composite_dump_text_sequence(
    py: Python<'_>,
    seq: &Bound<'_, PyAny>,
    tx: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let seq_items: Vec<Py<PyAny>> = seq
        .try_iter()?
        .map(|item| item.map(Bound::unbind))
        .collect::<PyResult<_>>()?;
    if seq_items.is_empty() {
        return Ok(PyBytes::new(py, b"()").unbind().into_any());
    }

    let pyformat_text = py
        .import("psycopg.adapt")?
        .getattr("PyFormat")?
        .getattr("TEXT")?;
    let mut out = Vec::from([b'(']);

    for item in seq_items {
        let bound = item.bind(py);
        if bound.is_none() {
            out.push(b',');
            continue;
        }

        let dumper = tx.call_method1("get_dumper", (bound.clone(), pyformat_text.clone()))?;
        let dumped = dumper.call_method1("dump", (bound.clone(),))?;
        if dumped.is_none() {
            out.extend_from_slice(b",");
            continue;
        }

        let raw = bytes_like_to_vec(py, &dumped)?;
        if raw.is_empty() {
            out.push(b'"');
            out.push(b'"');
            out.push(b',');
            continue;
        }

        if composite_needs_quotes(&raw) {
            out.push(b'"');
            for byte in raw {
                if byte == b'\\' || byte == b'"' {
                    out.push(byte);
                }
                out.push(byte);
            }
            out.push(b'"');
        } else {
            out.extend_from_slice(&raw);
        }
        out.push(b',');
    }

    if let Some(last) = out.last_mut() {
        *last = b')';
    }
    Ok(PyBytes::new(py, &out).unbind().into_any())
}

#[pyfunction]
fn composite_dump_binary_sequence(
    py: Python<'_>,
    seq: &Bound<'_, PyAny>,
    types: &Bound<'_, PyAny>,
    formats: &Bound<'_, PyAny>,
    tx: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let seq_len = seq.len()?;
    let adapted: Vec<Option<Vec<u8>>> = tx
        .call_method1("dump_sequence", (seq, formats))?
        .try_iter()?
        .map(|item| {
            let item = item?;
            if item.is_none() {
                Ok(None)
            } else {
                bytes_like_to_vec(py, &item).map(Some)
            }
        })
        .collect::<PyResult<_>>()?;

    let mut out = Vec::new();
    out.extend_from_slice(
        &i32::try_from(seq_len)
            .map_err(|_| PyErr::new::<PyValueError, _>("too many composite fields"))?
            .to_be_bytes(),
    );

    for (index, oid_obj) in types.try_iter()?.enumerate() {
        let oid = oid_obj?.extract::<u32>()?;
        out.extend_from_slice(&oid.to_be_bytes());
        match adapted.get(index).and_then(|item| item.as_ref()) {
            Some(buf) => {
                out.extend_from_slice(
                    &i32::try_from(buf.len())
                        .map_err(|_| {
                            PyErr::new::<PyValueError, _>("composite field larger than i32")
                        })?
                        .to_be_bytes(),
                );
                out.extend_from_slice(buf);
            }
            None => out.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }

    Ok(PyBytes::new(py, &out).unbind().into_any())
}

#[pyfunction]
fn composite_parse_text_record(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let data = bytes_like_to_vec(py, data)?;
    let parsed = parse_composite_text_record(&data);
    let out = PyList::empty(py);
    for field in parsed {
        match field {
            Some(value) => out.append(PyBytes::new(py, &value))?,
            None => out.append(py.None())?,
        }
    }
    Ok(out.unbind().into_any())
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
    m.add_class::<ConnectGenerator>()?;
    m.add_class::<SendGenerator>()?;
    m.add_class::<FetchGenerator>()?;
    m.add_class::<FetchManyGenerator>()?;
    m.add_class::<ExecuteGenerator>()?;
    m.add_class::<PipelineCommunicateGenerator>()?;
    m.add_class::<CancelGenerator>()?;
    m.add_function(wrap_pyfunction!(milestone, m)?)?;
    m.add_function(wrap_pyfunction!(scaffold_status, m)?)?;
    m.add_function(wrap_pyfunction!(backend_stack, m)?)?;
    m.add_function(wrap_pyfunction!(backend_core, m)?)?;
    m.add_function(wrap_pyfunction!(parse_conninfo_summary, m)?)?;
    m.add_function(wrap_pyfunction!(parse_connect_plan, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(cancel, m)?)?;
    m.add_function(wrap_pyfunction!(array_load_text, m)?)?;
    m.add_function(wrap_pyfunction!(array_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(uuid_load_text, m)?)?;
    m.add_function(wrap_pyfunction!(uuid_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(bool_dump_text, m)?)?;
    m.add_function(wrap_pyfunction!(bool_dump_binary, m)?)?;
    m.add_function(wrap_pyfunction!(bool_load_text, m)?)?;
    m.add_function(wrap_pyfunction!(bool_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(str_dump_binary, m)?)?;
    m.add_function(wrap_pyfunction!(str_dump_text, m)?)?;
    m.add_function(wrap_pyfunction!(text_load, m)?)?;
    m.add_function(wrap_pyfunction!(bytes_dump_binary, m)?)?;
    m.add_function(wrap_pyfunction!(bytea_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(composite_dump_text_sequence, m)?)?;
    m.add_function(wrap_pyfunction!(composite_dump_binary_sequence, m)?)?;
    m.add_function(wrap_pyfunction!(composite_parse_text_record, m)?)?;
    m.add_function(wrap_pyfunction!(format_row_text, m)?)?;
    m.add_function(wrap_pyfunction!(format_row_binary, m)?)?;
    m.add_function(wrap_pyfunction!(send, m)?)?;
    m.add_function(wrap_pyfunction!(fetch, m)?)?;
    m.add_function(wrap_pyfunction!(fetch_many, m)?)?;
    m.add_function(wrap_pyfunction!(execute, m)?)?;
    m.add_function(wrap_pyfunction!(pipeline_communicate, m)?)?;
    m.add_function(wrap_pyfunction!(parse_row_text, m)?)?;
    m.add_function(wrap_pyfunction!(parse_row_binary, m)?)?;
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

fn psycopg_exception_with_pgconn(
    py: Python<'_>,
    exc_type: &Bound<'_, PyAny>,
    message: &str,
    pgconn: &Bound<'_, PyAny>,
) -> PyErr {
    let kwargs = PyDict::new(py);
    kwargs
        .set_item("pgconn", pgconn)
        .expect("exception kwargs should accept pgconn");
    let exc = exc_type
        .call((message,), Some(&kwargs))
        .expect("exception constructor should succeed");
    PyErr::from_value(exc)
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
                            .extract::<bool>()?;
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

fn parse_binary_array<'py>(
    py: Python<'py>,
    data: &[u8],
    tx: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if data.len() < 12 {
        return Err(PyErr::new::<PyValueError, _>(
            "binary array payload is truncated",
        ));
    }

    let ndims = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if ndims == 0 {
        return Ok(PyList::empty(py).into_any());
    }

    let oid = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
    let dims_end = 12 + ndims * 8;
    if data.len() < dims_end {
        return Err(PyErr::new::<PyValueError, _>(
            "binary array dimensions are truncated",
        ));
    }

    let mut dims = Vec::with_capacity(ndims);
    let mut pos = 12;
    for _ in 0..ndims {
        dims.push(
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize,
        );
        pos += 8;
    }

    let pq_binary = py
        .import("psycopg.pq")?
        .getattr("Format")?
        .getattr("BINARY")?;
    let loader = tx.call_method1("get_loader", (oid, pq_binary))?;
    let load = loader.getattr("load")?;
    let mut cursor = dims_end;
    parse_binary_array_level(py, data, &dims, 0, &mut cursor, &load)
}

fn parse_text_array<'py>(
    py: Python<'py>,
    data: &[u8],
    loader: &Bound<'py, PyAny>,
    delimiter: &[u8],
) -> PyResult<Bound<'py, PyAny>> {
    if data.is_empty() {
        return Err(PyErr::new::<PyValueError, _>("malformed array: empty data"));
    }

    let mut cursor = 0usize;
    if data[0] == b'[' {
        let Some(eq_pos) = data.iter().position(|&b| b == b'=') else {
            return Err(PyErr::new::<PyValueError, _>(
                "malformed array: no '=' after dimension information",
            ));
        };
        cursor = eq_pos + 1;
    }

    let delim = delimiter.first().copied().unwrap_or(b',');
    let mut stack: Vec<Bound<'py, PyList>> = Vec::new();
    let mut current = PyList::empty(py);
    let mut root = current.clone();

    while cursor < data.len() {
        match data[cursor] {
            b'{' => {
                if !stack.is_empty() {
                    stack.last().expect("stack not empty").append(&current)?;
                }
                stack.push(current);
                current = PyList::empty(py);
                cursor += 1;
            }
            b'}' => {
                if stack.is_empty() {
                    return Err(PyErr::new::<PyValueError, _>(
                        "malformed array: unexpected '}'",
                    ));
                }
                root = stack.pop().expect("stack not empty");
                cursor += 1;
            }
            c if c == delim => {
                cursor += 1;
            }
            _ => {
                let (token, next_cursor) = parse_text_array_token(data, cursor, delim)?;
                cursor = next_cursor;
                if stack.is_empty() {
                    let wat = if token.len() > 10 {
                        format!("{}...", String::from_utf8_lossy(&token[..10]))
                    } else {
                        String::from_utf8_lossy(&token).into_owned()
                    };
                    return Err(PyErr::new::<PyValueError, _>(format!(
                        "malformed array: unexpected '{wat}'"
                    )));
                }
                if token == b"NULL" {
                    stack.last().expect("stack not empty").append(py.None())?;
                } else {
                    let item = loader.call_method1("load", (PyBytes::new(py, &token),))?;
                    stack.last().expect("stack not empty").append(item)?;
                }
            }
        }
    }

    Ok(root.into_any())
}

fn composite_needs_quotes(raw: &[u8]) -> bool {
    raw.iter().any(|byte| {
        *byte == b'"'
            || *byte == b','
            || *byte == b'\\'
            || *byte == b'('
            || *byte == b')'
            || byte.is_ascii_whitespace()
    })
}

fn parse_composite_text_record(data: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut fields = Vec::new();
    let mut current = Vec::new();
    let mut i = 0usize;
    let mut saw_token = false;
    let mut in_quotes = false;

    while i < data.len() {
        let ch = data[i];
        if in_quotes {
            if ch == b'"' {
                if i + 1 < data.len() && data[i + 1] == b'"' {
                    current.push(b'"');
                    i += 2;
                    continue;
                }
                in_quotes = false;
                i += 1;
                continue;
            }
            current.push(ch);
            i += 1;
            continue;
        }

        match ch {
            b',' => {
                if saw_token {
                    fields.push(Some(std::mem::take(&mut current)));
                } else {
                    fields.push(None);
                }
                saw_token = false;
                i += 1;
            }
            b'"' => {
                saw_token = true;
                in_quotes = true;
                i += 1;
            }
            _ => {
                saw_token = true;
                current.push(ch);
                i += 1;
            }
        }
    }

    if saw_token {
        fields.push(Some(current));
    } else if data.ends_with(b",") {
        fields.push(None);
    }

    fields
}

fn parse_text_array_token(data: &[u8], start: usize, delim: u8) -> PyResult<(Vec<u8>, usize)> {
    let mut cursor = start;
    let mut quoted = data[cursor] == b'"';
    let mut token = Vec::new();
    if quoted {
        cursor += 1;
    }

    while cursor < data.len() {
        let ch = data[cursor];
        if quoted {
            if ch == b'\\' {
                cursor += 1;
                if cursor >= data.len() {
                    return Err(PyErr::new::<PyValueError, _>(
                        "malformed array: hit the end of the buffer",
                    ));
                }
                token.push(data[cursor]);
                cursor += 1;
                continue;
            }
            if ch == b'"' {
                quoted = false;
                cursor += 1;
                continue;
            }
            token.push(ch);
            cursor += 1;
            continue;
        }

        if ch == delim || ch == b'}' {
            break;
        }
        token.push(ch);
        cursor += 1;
    }

    if quoted {
        return Err(PyErr::new::<PyValueError, _>(
            "malformed array: hit the end of the buffer",
        ));
    }

    Ok((token, cursor))
}

fn parse_binary_array_level<'py>(
    py: Python<'py>,
    data: &[u8],
    dims: &[usize],
    dim_index: usize,
    cursor: &mut usize,
    load: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let out = PyList::empty(py);
    let nelems = dims[dim_index];

    if dim_index == dims.len() - 1 {
        for _ in 0..nelems {
            if data.len().saturating_sub(*cursor) < 4 {
                return Err(PyErr::new::<PyValueError, _>(
                    "binary array element length is truncated",
                ));
            }
            let size = i32::from_be_bytes([
                data[*cursor],
                data[*cursor + 1],
                data[*cursor + 2],
                data[*cursor + 3],
            ]);
            *cursor += 4;
            if size == -1 {
                out.append(py.None())?;
                continue;
            }
            let size = usize::try_from(size).map_err(|_| {
                PyErr::new::<PyValueError, _>("binary array element length is invalid")
            })?;
            if data.len().saturating_sub(*cursor) < size {
                return Err(PyErr::new::<PyValueError, _>(
                    "binary array element payload is truncated",
                ));
            }
            let item = load.call1((PyBytes::new(py, &data[*cursor..*cursor + size]),))?;
            *cursor += size;
            out.append(item)?;
        }
    } else {
        for _ in 0..nelems {
            let item = parse_binary_array_level(py, data, dims, dim_index + 1, cursor, load)?;
            out.append(item)?;
        }
    }

    Ok(out.into_any())
}
