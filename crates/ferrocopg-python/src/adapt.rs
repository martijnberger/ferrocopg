use crate::python_helpers::{bytes_like_to_vec, expected_field_count, psycopg_operational_error};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};
use pyo3::wrap_pyfunction;
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};
use uuid::Uuid;

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;
const DEC_DIGITS: usize = 4;

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
    let uuid =
        Uuid::parse_str(text).map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))?;
    let value = py
        .import("uuid")?
        .getattr("UUID")?
        .call1((uuid.hyphenated().to_string(),))?;
    Ok(value.unbind().into_any())
}

#[pyfunction]
fn uuid_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let data = bytes_like_to_vec(py, data)?;
    let uuid =
        Uuid::from_slice(&data).map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))?;

    let kwargs = PyDict::new(py);
    kwargs.set_item("bytes", PyBytes::new(py, uuid.as_bytes()))?;
    let value = py
        .import("uuid")?
        .getattr("UUID")?
        .call((), Some(&kwargs))?;
    Ok(value.unbind().into_any())
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
fn dump_decimal_to_text(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    if obj.call_method0("is_nan")?.is_truthy()? {
        return Ok(PyBytes::new(py, b"NaN").unbind().into_any());
    }

    let text = obj.str()?.to_str()?.as_bytes().to_vec();
    Ok(PyBytes::new(py, &text).unbind().into_any())
}

#[pyfunction]
fn dump_decimal_to_numeric_binary(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let data = decimal_to_numeric_binary(obj)?;
    Ok(PyBytes::new(py, &data).unbind().into_any())
}

#[pyfunction]
fn dump_int_to_numeric_binary(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let data = int_to_numeric_binary(&obj.str()?.to_str()?);
    Ok(PyBytes::new(py, &data).unbind().into_any())
}

#[pyfunction]
fn numeric_load_text(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let text = String::from_utf8(bytes_like_to_vec(py, data)?)
        .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))?;
    let decimal = py.import("decimal")?.getattr("Decimal")?.call1((text,))?;
    Ok(decimal.unbind().into_any())
}

#[pyfunction]
fn numeric_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let raw = bytes_like_to_vec(py, data)?;
    let decimal = numeric_binary_to_decimal(py, &raw)?;
    Ok(decimal.unbind().into_any())
}

#[pyfunction]
fn date_dump_text(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let text = obj.str()?.to_str()?.as_bytes().to_vec();
    Ok(PyBytes::new(py, &text).unbind().into_any())
}

#[pyfunction]
fn date_dump_binary(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let days = python_date(obj)?.to_julian_day() - postgres_epoch_date().to_julian_day();
    Ok(PyBytes::new(py, &(days as i32).to_be_bytes())
        .unbind()
        .into_any())
}

#[pyfunction]
fn date_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let raw = bytes_like_to_vec(py, data)?;
    if raw.len() != 4 {
        return Err(PyErr::new::<PyValueError, _>(
            "date binary payload has an invalid size",
        ));
    }
    let raw_days = i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]);
    if raw_days == i32::MIN || raw_days == i32::MAX {
        return Err(date_range_error(py, raw_days < 0));
    }
    let days = i64::from(raw_days) + 730120;
    match py
        .import("datetime")?
        .getattr("date")?
        .call_method1("fromordinal", (days,))
    {
        Ok(value) => Ok(value.unbind().into_any()),
        Err(_) => Err(date_range_error(py, days < 1)),
    }
}

#[pyfunction]
fn time_dump_text(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let text = obj.str()?.to_str()?.as_bytes().to_vec();
    Ok(PyBytes::new(py, &text).unbind().into_any())
}

#[pyfunction]
fn time_dump_binary(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let micros = time_to_micros(obj)?;
    Ok(PyBytes::new(py, &micros.to_be_bytes()).unbind().into_any())
}

#[pyfunction]
fn timetz_dump_binary(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let micros = time_to_micros(obj)?;
    let offset = timezone_offset_seconds(py, obj)?;
    let mut out = Vec::with_capacity(12);
    out.extend_from_slice(&micros.to_be_bytes());
    out.extend_from_slice(&(-offset).to_be_bytes());
    Ok(PyBytes::new(py, &out).unbind().into_any())
}

#[pyfunction]
fn time_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let raw = bytes_like_to_vec(py, data)?;
    if raw.len() != 8 {
        return Err(PyErr::new::<PyValueError, _>(
            "time binary payload has an invalid size",
        ));
    }

    let value = i64::from_be_bytes(raw.try_into().expect("validated length"));
    let (hour, minute, second, microsecond) = micros_to_time_parts(value);
    let Some(time) = time_from_micros(value) else {
        let errors = py.import("psycopg.errors")?;
        let data_error = errors.getattr("DataError")?;
        return Err(psycopg_operational_error(
            &data_error,
            &format!("time not supported by Python: hour={hour}"),
        ));
    };

    let _ = (minute, second, microsecond);
    python_time_to_object(py, time, None)
}

#[pyfunction]
fn timetz_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let raw = bytes_like_to_vec(py, data)?;
    if raw.len() != 12 {
        return Err(PyErr::new::<PyValueError, _>(
            "timetz binary payload has an invalid size",
        ));
    }
    let micros = i64::from_be_bytes(raw[..8].try_into().expect("validated length"));
    let offset = i32::from_be_bytes(raw[8..12].try_into().expect("validated length"));
    let Some(time) = time_from_micros(micros) else {
        let (hour, _, _, _) = micros_to_time_parts(micros);
        let errors = py.import("psycopg.errors")?;
        let data_error = errors.getattr("DataError")?;
        return Err(psycopg_operational_error(
            &data_error,
            &format!("time not supported by Python: hour={hour}"),
        ));
    };
    let dt = py.import("datetime")?;
    let delta = dt
        .getattr("timedelta")?
        .call1((0, -offset, 0, 0, 0, 0, 0))?;
    let tz = dt.getattr("timezone")?.call1((delta,))?;
    python_time_to_object(py, time, Some(&tz))
}

#[pyfunction]
fn datetime_dump_text(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let text = obj.str()?.to_str()?.as_bytes().to_vec();
    Ok(PyBytes::new(py, &text).unbind().into_any())
}

#[pyfunction]
fn datetime_dump_binary(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let offset = timezone_offset_seconds(py, obj)?;
    let datetime = python_primitive_datetime(obj)?
        .assume_offset(offset_from_seconds(py, offset)?)
        .to_offset(UtcOffset::UTC);
    let micros = duration_to_i64_micros(datetime - postgres_epoch_timestamptz())?;
    Ok(PyBytes::new(py, &micros.to_be_bytes()).unbind().into_any())
}

#[pyfunction]
fn datetime_notz_dump_binary(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let datetime = python_primitive_datetime(obj)?;
    let micros = duration_to_i64_micros(datetime - postgres_epoch_timestamp())?;
    Ok(PyBytes::new(py, &micros.to_be_bytes()).unbind().into_any())
}

#[pyfunction]
fn timestamp_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let raw = bytes_like_to_vec(py, data)?;
    if raw.len() != 8 {
        return Err(PyErr::new::<PyValueError, _>(
            "timestamp binary payload has an invalid size",
        ));
    }
    let micros = i64::from_be_bytes(raw.try_into().expect("validated length"));
    let epoch = py
        .import("datetime")?
        .getattr("datetime")?
        .call1((2000, 1, 1, 0, 0, 0, 0))?;
    let delta = py
        .import("datetime")?
        .getattr("timedelta")?
        .call1((0, 0, micros))?;
    match epoch.call_method1("__add__", (delta,)) {
        Ok(value) => Ok(value.unbind().into_any()),
        Err(_) => Err(timestamp_range_error(py, micros <= 0)),
    }
}

#[pyfunction]
fn timestamptz_load_binary(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    timezone_obj: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let raw = bytes_like_to_vec(py, data)?;
    if raw.len() != 8 {
        return Err(PyErr::new::<PyValueError, _>(
            "timestamptz binary payload has an invalid size",
        ));
    }
    let micros = i64::from_be_bytes(raw.try_into().expect("validated length"));
    let dt = py.import("datetime")?;
    let utc = dt.getattr("timezone")?.getattr("utc")?;
    let epoch = dt
        .getattr("datetime")?
        .call1((2000, 1, 1, 0, 0, 0, 0, utc.clone()))?;
    let delta = dt.getattr("timedelta")?.call1((0, 0, micros))?;
    match epoch.call_method1("__add__", (delta,)) {
        Ok(value) => Ok(value
            .call_method1("astimezone", (timezone_obj,))?
            .unbind()
            .into_any()),
        Err(_) => {
            if !timezone_obj.is_none() {
                if let Ok(utcoff) = timezone_obj.call_method1(
                    "utcoffset",
                    (if micros < 0 {
                        dt.getattr("datetime")?.getattr("min")?
                    } else {
                        dt.getattr("datetime")?.getattr("max")?
                    },),
                ) {
                    if !utcoff.is_none() {
                        let usoff = 1_000_000_i64
                            * utcoff.call_method0("total_seconds")?.extract::<f64>()? as i64;
                        let naive_epoch =
                            dt.getattr("datetime")?.call1((2000, 1, 1, 0, 0, 0, 0))?;
                        let delta = dt.getattr("timedelta")?.call1((0, 0, micros + usoff))?;
                        if let Ok(value) = naive_epoch.call_method1("__add__", (delta,)) {
                            let kwargs = PyDict::new(py);
                            kwargs.set_item("tzinfo", timezone_obj)?;
                            return Ok(value
                                .call_method("replace", (), Some(&kwargs))?
                                .unbind()
                                .into_any());
                        }
                    }
                }
            }

            Err(timestamp_range_error(py, micros <= 0))
        }
    }
}

#[pyfunction]
fn timedelta_dump_binary(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let days = obj.getattr("days")?.extract::<i32>()?;
    let seconds = obj.getattr("seconds")?.extract::<i64>()?;
    let micros = obj.getattr("microseconds")?.extract::<i64>()? + seconds * 1_000_000;
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&micros.to_be_bytes());
    out.extend_from_slice(&days.to_be_bytes());
    out.extend_from_slice(&0_i32.to_be_bytes());
    Ok(PyBytes::new(py, &out).unbind().into_any())
}

#[pyfunction]
fn interval_load_binary(py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let raw = bytes_like_to_vec(py, data)?;
    if raw.len() != 16 {
        return Err(PyErr::new::<PyValueError, _>(
            "interval binary payload has an invalid size",
        ));
    }
    let micros = i64::from_be_bytes(raw[..8].try_into().expect("validated length"));
    let mut days = i32::from_be_bytes(raw[8..12].try_into().expect("validated length"));
    let months = i32::from_be_bytes(raw[12..16].try_into().expect("validated length"));
    if months > 0 {
        let years = months / 12;
        let rem = months % 12;
        days += 365 * years + 30 * rem;
    } else if months < 0 {
        let abs = -months;
        let years = abs / 12;
        let rem = abs % 12;
        days -= 365 * years + 30 * rem;
    }
    let value = match py
        .import("datetime")?
        .getattr("timedelta")?
        .call1((days, 0, micros))
    {
        Ok(value) => value,
        Err(err) => {
            let errors = py.import("psycopg.errors")?;
            let data_error = errors.getattr("DataError")?;
            return Err(psycopg_operational_error(
                &data_error,
                &format!("can't parse interval: {err}"),
            ));
        }
    };
    Ok(value.unbind().into_any())
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

fn numeric_header(ndigits: u16, weight: i16, sign: u16, dscale: u16) -> [u8; 8] {
    let mut out = [0_u8; 8];
    out[..2].copy_from_slice(&ndigits.to_be_bytes());
    out[2..4].copy_from_slice(&weight.to_be_bytes());
    out[4..6].copy_from_slice(&sign.to_be_bytes());
    out[6..8].copy_from_slice(&dscale.to_be_bytes());
    out
}

fn decimal_to_numeric_binary(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let tuple = obj.call_method0("as_tuple")?;
    let sign = tuple.getattr("sign")?.extract::<u8>()? != 0;
    let digits: Vec<u8> = tuple.getattr("digits")?.extract()?;
    let exponent = tuple.getattr("exponent")?;

    if let Ok(exp) = exponent.extract::<String>() {
        let sign_code = match exp.as_str() {
            "n" | "N" => NUMERIC_NAN,
            "F" => {
                if sign {
                    NUMERIC_NINF
                } else {
                    NUMERIC_PINF
                }
            }
            other => {
                return Err(PyErr::new::<PyValueError, _>(format!(
                    "unsupported Decimal exponent {other}"
                )));
            }
        };
        return Ok(numeric_header(0, 0, sign_code, 0).to_vec());
    }

    let exp = exponent.extract::<i32>()?;
    let mut wi = 0_usize;
    let weights = [1000_u16, 100_u16, 10_u16, 1_u16];
    let mut ndigits = digits.len();
    let mut nzdigits = digits.len();

    while nzdigits > 0 && digits[nzdigits - 1] == 0 {
        nzdigits -= 1;
    }

    let dscale = if exp <= 0 {
        (-exp) as usize
    } else {
        ndigits += (exp as usize) % DEC_DIGITS;
        0
    };

    if nzdigits == 0 {
        return Ok(numeric_header(0, 0, NUMERIC_POS, dscale as u16).to_vec());
    }

    let rem = (ndigits as i32 - dscale as i32).rem_euclid(DEC_DIGITS as i32) as usize;
    if rem != 0 {
        wi = DEC_DIGITS - rem;
        ndigits += wi;
    }

    let tmp = nzdigits + wi;
    let out_ndigits = (tmp / DEC_DIGITS) + usize::from(tmp % DEC_DIGITS != 0);
    let weight = ((ndigits as i32) + exp) / (DEC_DIGITS as i32) - 1;
    let mut out = numeric_header(
        u16::try_from(out_ndigits)
            .map_err(|_| PyErr::new::<PyValueError, _>("numeric too large"))?,
        i16::try_from(weight).map_err(|_| PyErr::new::<PyValueError, _>("numeric too large"))?,
        if sign { NUMERIC_NEG } else { NUMERIC_POS },
        u16::try_from(dscale).map_err(|_| PyErr::new::<PyValueError, _>("numeric too large"))?,
    )
    .to_vec();

    let mut pgdigit = 0_u16;
    for digit in digits.into_iter().take(nzdigits) {
        pgdigit += weights[wi] * u16::from(digit);
        wi += 1;
        if wi >= DEC_DIGITS {
            out.extend_from_slice(&pgdigit.to_be_bytes());
            pgdigit = 0;
            wi = 0;
        }
    }

    if pgdigit != 0 {
        out.extend_from_slice(&pgdigit.to_be_bytes());
    }

    Ok(out)
}

fn int_to_numeric_binary(text: &str) -> Vec<u8> {
    let (sign, digits) = if let Some(rest) = text.strip_prefix('-') {
        (NUMERIC_NEG, rest)
    } else if let Some(rest) = text.strip_prefix('+') {
        (NUMERIC_POS, rest)
    } else {
        (NUMERIC_POS, text)
    };

    let digits = digits.trim_start_matches('0');
    if digits.is_empty() {
        let mut out = numeric_header(1, 0, NUMERIC_POS, 0).to_vec();
        out.extend_from_slice(&0_u16.to_be_bytes());
        return out;
    }

    let groups = digits.len().div_ceil(DEC_DIGITS);
    let mut out = numeric_header(groups as u16, groups as i16 - 1, sign, 0).to_vec();

    let first_len = digits.len() % DEC_DIGITS;
    let mut pos = 0_usize;
    if first_len != 0 {
        let chunk = digits[..first_len].parse::<u16>().unwrap_or(0);
        out.extend_from_slice(&chunk.to_be_bytes());
        pos = first_len;
    }

    while pos < digits.len() {
        let chunk = digits[pos..pos + DEC_DIGITS].parse::<u16>().unwrap_or(0);
        out.extend_from_slice(&chunk.to_be_bytes());
        pos += DEC_DIGITS;
    }

    out
}

fn numeric_binary_to_decimal<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    if data.len() < 8 {
        return Err(PyErr::new::<PyValueError, _>(
            "numeric binary payload is truncated",
        ));
    }

    let ndigits = u16::from_be_bytes([data[0], data[1]]) as usize;
    let weight = i16::from_be_bytes([data[2], data[3]]);
    let sign = u16::from_be_bytes([data[4], data[5]]);
    let dscale = u16::from_be_bytes([data[6], data[7]]) as usize;
    if data.len() != 8 + ndigits * 2 {
        return Err(PyErr::new::<PyValueError, _>(
            "numeric binary payload has an invalid size",
        ));
    }

    let decimal = py.import("decimal")?.getattr("Decimal")?;
    match sign {
        NUMERIC_NAN => return decimal.call1(("NaN",)),
        NUMERIC_PINF => return decimal.call1(("Infinity",)),
        NUMERIC_NINF => return decimal.call1(("-Infinity",)),
        NUMERIC_POS | NUMERIC_NEG => {}
        _ => {
            let errors = py.import("psycopg.errors")?;
            let data_error = errors.getattr("DataError")?;
            return Err(psycopg_operational_error(
                &data_error,
                &format!("bad value for numeric sign: 0x{sign:X}"),
            ));
        }
    }

    let mut digits = String::new();
    if weight < -1 {
        digits.push_str(&"0".repeat(((-weight - 1) as usize) * DEC_DIGITS));
    }
    for chunk in data[8..].chunks_exact(2) {
        let pgdigit = u16::from_be_bytes([chunk[0], chunk[1]]);
        digits.push_str(&format!("{pgdigit:04}"));
    }

    let digits_before = if weight >= -1 {
        ((i32::from(weight) + 1) as usize) * DEC_DIGITS
    } else {
        0
    };
    let target_len = digits_before + dscale;
    if digits.len() < target_len {
        digits.push_str(&"0".repeat(target_len - digits.len()));
    }

    let (int_raw, frac_raw) = digits.split_at(digits_before.min(digits.len()));
    let int_part = int_raw.trim_start_matches('0');
    let int_part = if int_part.is_empty() { "0" } else { int_part };

    let mut text = String::new();
    if sign == NUMERIC_NEG {
        text.push('-');
    }
    text.push_str(int_part);
    if dscale != 0 {
        text.push('.');
        text.push_str(&frac_raw[..dscale]);
    }

    decimal.call1((text,))
}

fn postgres_epoch_date() -> Date {
    Date::from_calendar_date(2000, Month::January, 1).expect("valid postgres epoch date")
}

fn postgres_epoch_timestamp() -> PrimitiveDateTime {
    PrimitiveDateTime::new(
        postgres_epoch_date(),
        Time::from_hms(0, 0, 0).expect("valid postgres epoch time"),
    )
}

fn postgres_epoch_timestamptz() -> OffsetDateTime {
    postgres_epoch_timestamp().assume_utc()
}

fn python_date(obj: &Bound<'_, PyAny>) -> PyResult<Date> {
    let year = obj.getattr("year")?.extract::<i32>()?;
    let month = month_from_number(obj.getattr("month")?.extract::<u8>()?)?;
    let day = obj.getattr("day")?.extract::<u8>()?;
    Date::from_calendar_date(year, month, day)
        .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
}

fn python_time(obj: &Bound<'_, PyAny>) -> PyResult<Time> {
    let hour = obj.getattr("hour")?.extract::<u8>()?;
    let minute = obj.getattr("minute")?.extract::<u8>()?;
    let second = obj.getattr("second")?.extract::<u8>()?;
    let microsecond = obj.getattr("microsecond")?.extract::<u32>()?;
    Time::from_hms_micro(hour, minute, second, microsecond)
        .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
}

fn python_primitive_datetime(obj: &Bound<'_, PyAny>) -> PyResult<PrimitiveDateTime> {
    Ok(PrimitiveDateTime::new(python_date(obj)?, python_time(obj)?))
}

fn month_from_number(month: u8) -> PyResult<Month> {
    Month::try_from(month).map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
}

fn time_to_micros(obj: &Bound<'_, PyAny>) -> PyResult<i64> {
    let time = python_time(obj)?;
    Ok(i64::from(time.hour()) * 3_600_000_000
        + i64::from(time.minute()) * 60_000_000
        + i64::from(time.second()) * 1_000_000
        + i64::from(time.microsecond()))
}

fn micros_to_time_parts(value: i64) -> (i64, i64, i64, i64) {
    let (value, microsecond) = (value / 1_000_000, value % 1_000_000);
    let (value, second) = (value / 60, value % 60);
    let (hour, minute) = (value / 60, value % 60);
    (hour, minute, second, microsecond)
}

fn time_from_micros(value: i64) -> Option<Time> {
    if !(0..86_400_000_000).contains(&value) {
        return None;
    }
    let hour = (value / 3_600_000_000) as u8;
    let minute = ((value / 60_000_000) % 60) as u8;
    let second = ((value / 1_000_000) % 60) as u8;
    let microsecond = (value % 1_000_000) as u32;
    Time::from_hms_micro(hour, minute, second, microsecond).ok()
}

fn offset_from_seconds(py: Python<'_>, seconds: i32) -> PyResult<UtcOffset> {
    UtcOffset::from_whole_seconds(seconds).map_err(|err| {
        let errors = py
            .import("psycopg.errors")
            .expect("psycopg.errors should import");
        let data_error = errors
            .getattr("DataError")
            .expect("psycopg.errors.DataError should exist");
        psycopg_operational_error(&data_error, &err.to_string())
    })
}

fn duration_to_i64_micros(duration: Duration) -> PyResult<i64> {
    i64::try_from(duration.whole_microseconds())
        .map_err(|_| PyErr::new::<PyValueError, _>("timestamp is out of range"))
}

fn timezone_offset_seconds(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<i32> {
    let offset = obj.call_method0("utcoffset")?;
    if offset.is_none() {
        let errors = py.import("psycopg.errors")?;
        let data_error = errors.getattr("DataError")?;
        return Err(psycopg_operational_error(
            &data_error,
            &format!(
                "cannot calculate the offset of tzinfo '{}' without a date",
                obj.getattr("tzinfo")?.str()?.to_str()?
            ),
        ));
    }

    offset
        .call_method0("total_seconds")?
        .extract::<f64>()
        .map(|seconds| seconds as i32)
}

fn python_time_to_object(
    py: Python<'_>,
    value: Time,
    tz: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let dt = py.import("datetime")?;
    let args = match tz {
        Some(tz) => (
            value.hour(),
            value.minute(),
            value.second(),
            value.microsecond(),
            tz.clone(),
        )
            .into_pyobject(py)?
            .unbind(),
        None => (
            value.hour(),
            value.minute(),
            value.second(),
            value.microsecond(),
        )
            .into_pyobject(py)?
            .unbind(),
    };
    Ok(dt.getattr("time")?.call1(args)?.unbind().into_any())
}

fn date_range_error(py: Python<'_>, too_small: bool) -> PyErr {
    let errors = py
        .import("psycopg.errors")
        .expect("psycopg.errors should import");
    let data_error = errors
        .getattr("DataError")
        .expect("psycopg.errors.DataError should exist");
    psycopg_operational_error(
        &data_error,
        if too_small {
            "date too small (before year 1)"
        } else {
            "date too large (after year 10K)"
        },
    )
}

fn timestamp_range_error(py: Python<'_>, too_small: bool) -> PyErr {
    let errors = py
        .import("psycopg.errors")
        .expect("psycopg.errors should import");
    let data_error = errors
        .getattr("DataError")
        .expect("psycopg.errors.DataError should exist");
    psycopg_operational_error(
        &data_error,
        if too_small {
            "timestamp too small (before year 1)"
        } else {
            "timestamp too large (after year 10K)"
        },
    )
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

pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
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
    m.add_function(wrap_pyfunction!(dump_decimal_to_text, m)?)?;
    m.add_function(wrap_pyfunction!(dump_decimal_to_numeric_binary, m)?)?;
    m.add_function(wrap_pyfunction!(dump_int_to_numeric_binary, m)?)?;
    m.add_function(wrap_pyfunction!(numeric_load_text, m)?)?;
    m.add_function(wrap_pyfunction!(numeric_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(date_dump_text, m)?)?;
    m.add_function(wrap_pyfunction!(date_dump_binary, m)?)?;
    m.add_function(wrap_pyfunction!(date_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(time_dump_text, m)?)?;
    m.add_function(wrap_pyfunction!(time_dump_binary, m)?)?;
    m.add_function(wrap_pyfunction!(timetz_dump_binary, m)?)?;
    m.add_function(wrap_pyfunction!(time_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(timetz_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(datetime_dump_text, m)?)?;
    m.add_function(wrap_pyfunction!(datetime_dump_binary, m)?)?;
    m.add_function(wrap_pyfunction!(datetime_notz_dump_binary, m)?)?;
    m.add_function(wrap_pyfunction!(timestamp_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(timestamptz_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(timedelta_dump_binary, m)?)?;
    m.add_function(wrap_pyfunction!(interval_load_binary, m)?)?;
    m.add_function(wrap_pyfunction!(composite_dump_text_sequence, m)?)?;
    m.add_function(wrap_pyfunction!(composite_dump_binary_sequence, m)?)?;
    m.add_function(wrap_pyfunction!(composite_parse_text_record, m)?)?;
    m.add_function(wrap_pyfunction!(format_row_text, m)?)?;
    m.add_function(wrap_pyfunction!(format_row_binary, m)?)?;
    m.add_function(wrap_pyfunction!(parse_row_text, m)?)?;
    m.add_function(wrap_pyfunction!(parse_row_binary, m)?)?;
    Ok(())
}
