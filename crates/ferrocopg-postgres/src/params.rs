use crate::error::ProbeError;
use postgres::types::{ToSql, Type};
use std::fmt;
use time::format_description::FormatItem;
use time::macros::format_description;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};
use uuid::Uuid;

const TIME_TEXT_FORMAT: &[FormatItem<'static>] = format_description!("[hour]:[minute]:[second]");
const TIME_TEXT_FORMAT_FRACTIONAL: &[FormatItem<'static>] =
    format_description!("[hour]:[minute]:[second].[subsecond]");
const TIMESTAMP_TEXT_FORMAT: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
const TIMESTAMP_TEXT_FORMAT_FRACTIONAL: &[FormatItem<'static>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");
const TIMESTAMPTZ_TEXT_FORMAT: &[FormatItem<'static>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]"
);
const TIMESTAMPTZ_TEXT_FORMAT_FRACTIONAL: &[FormatItem<'static>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]:[offset_minute]"
);

pub(crate) fn query_param_refs(params: &[Box<dyn ToSql + Sync>]) -> Vec<&(dyn ToSql + Sync)> {
    params.iter().map(|value| value.as_ref()).collect()
}

pub(crate) fn parsed_query_params(
    statement: &postgres::Statement,
    params: &[Option<String>],
) -> Result<Vec<Box<dyn ToSql + Sync>>, ProbeError> {
    let expected = statement.params();
    if expected.len() != params.len() {
        return Err(ProbeError::BadParam(format!(
            "expected {} params but got {}",
            expected.len(),
            params.len()
        )));
    }

    expected
        .iter()
        .zip(params.iter())
        .enumerate()
        .map(|(index, (ty, value))| parse_query_param(index, ty, value))
        .collect()
}

fn parse_query_param(
    index: usize,
    ty: &Type,
    value: &Option<String>,
) -> Result<Box<dyn ToSql + Sync>, ProbeError> {
    match value {
        None => parse_null_query_param(index, ty),
        Some(value) => parse_text_query_param(index, ty, value),
    }
}

fn parse_null_query_param(index: usize, ty: &Type) -> Result<Box<dyn ToSql + Sync>, ProbeError> {
    Ok(match *ty {
        Type::BOOL => Box::new(Option::<bool>::None),
        Type::INT2 => Box::new(Option::<i16>::None),
        Type::INT4 => Box::new(Option::<i32>::None),
        Type::INT8 => Box::new(Option::<i64>::None),
        Type::OID => Box::new(Option::<u32>::None),
        Type::FLOAT4 => Box::new(Option::<f32>::None),
        Type::FLOAT8 => Box::new(Option::<f64>::None),
        Type::DATE => Box::new(Option::<Date>::None),
        Type::TIME => Box::new(Option::<Time>::None),
        Type::TIMESTAMP => Box::new(Option::<PrimitiveDateTime>::None),
        Type::TIMESTAMPTZ => Box::new(Option::<OffsetDateTime>::None),
        Type::UUID => Box::new(Option::<Uuid>::None),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            Box::new(Option::<String>::None)
        }
        _ => {
            return Err(ProbeError::BadParam(format!(
                "unsupported null parameter type at ${}: {}",
                index + 1,
                ty.name()
            )));
        }
    })
}

fn parse_text_query_param(
    index: usize,
    ty: &Type,
    value: &str,
) -> Result<Box<dyn ToSql + Sync>, ProbeError> {
    Ok(match *ty {
        Type::BOOL => Box::new(parse_bool_param(index, value)?),
        Type::INT2 => Box::new(parse_numeric_param::<i16>(index, ty, value)?),
        Type::INT4 => Box::new(parse_numeric_param::<i32>(index, ty, value)?),
        Type::INT8 => Box::new(parse_numeric_param::<i64>(index, ty, value)?),
        Type::OID => Box::new(parse_numeric_param::<u32>(index, ty, value)?),
        Type::FLOAT4 => Box::new(parse_numeric_param::<f32>(index, ty, value)?),
        Type::FLOAT8 => Box::new(parse_numeric_param::<f64>(index, ty, value)?),
        Type::DATE => Box::new(parse_date_param(index, value)?),
        Type::TIME => Box::new(parse_time_param(index, value)?),
        Type::TIMESTAMP => Box::new(parse_timestamp_param(index, value)?),
        Type::TIMESTAMPTZ => Box::new(parse_timestamptz_param(index, value)?),
        Type::UUID => Box::new(parse_uuid_param(index, value)?),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
            Box::new(value.to_owned())
        }
        _ => {
            return Err(ProbeError::BadParam(format!(
                "unsupported parameter type at ${}: {}",
                index + 1,
                ty.name()
            )));
        }
    })
}

fn parse_date_param(index: usize, value: &str) -> Result<Date, ProbeError> {
    let mut parts = value.split('-');
    let year = parse_date_component::<i32>(index, value, parts.next(), "year")?;
    let month_number = parse_date_component::<u8>(index, value, parts.next(), "month")?;
    let day = parse_date_component::<u8>(index, value, parts.next(), "day")?;
    if parts.next().is_some() {
        return Err(invalid_date_param(index, value, "too many components"));
    }

    let month =
        Month::try_from(month_number).map_err(|err| invalid_date_param(index, value, err))?;
    Date::from_calendar_date(year, month, day).map_err(|err| invalid_date_param(index, value, err))
}

fn parse_date_component<T>(
    index: usize,
    value: &str,
    component: Option<&str>,
    name: &str,
) -> Result<T, ProbeError>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    let component =
        component.ok_or_else(|| invalid_date_param(index, value, format!("missing {name}")))?;
    component
        .parse::<T>()
        .map_err(|err| invalid_date_param(index, value, format!("invalid {name} ({err})")))
}

fn invalid_date_param(index: usize, value: &str, reason: impl fmt::Display) -> ProbeError {
    ProbeError::BadParam(format!(
        "invalid date value at ${}: {} ({reason})",
        index + 1,
        value
    ))
}

fn parse_uuid_param(index: usize, value: &str) -> Result<Uuid, ProbeError> {
    Uuid::parse_str(value).map_err(|err| {
        ProbeError::BadParam(format!(
            "invalid uuid value at ${}: {} ({err})",
            index + 1,
            value
        ))
    })
}

fn parse_time_param(index: usize, value: &str) -> Result<Time, ProbeError> {
    Time::parse(value, TIME_TEXT_FORMAT_FRACTIONAL)
        .or_else(|_| Time::parse(value, TIME_TEXT_FORMAT))
        .map_err(|err| invalid_time_family_param(index, "time", value, err))
}

fn parse_timestamp_param(index: usize, value: &str) -> Result<PrimitiveDateTime, ProbeError> {
    PrimitiveDateTime::parse(value, TIMESTAMP_TEXT_FORMAT_FRACTIONAL)
        .or_else(|_| PrimitiveDateTime::parse(value, TIMESTAMP_TEXT_FORMAT))
        .map_err(|err| invalid_time_family_param(index, "timestamp", value, err))
}

fn parse_timestamptz_param(index: usize, value: &str) -> Result<OffsetDateTime, ProbeError> {
    OffsetDateTime::parse(value, TIMESTAMPTZ_TEXT_FORMAT_FRACTIONAL)
        .or_else(|_| OffsetDateTime::parse(value, TIMESTAMPTZ_TEXT_FORMAT))
        .map_err(|err| invalid_time_family_param(index, "timestamptz", value, err))
}

fn invalid_time_family_param(
    index: usize,
    ty_name: &str,
    value: &str,
    reason: impl fmt::Display,
) -> ProbeError {
    ProbeError::BadParam(format!(
        "invalid {ty_name} value at ${}: {} ({reason})",
        index + 1,
        value
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::{Month, UtcOffset};

    #[test]
    fn parses_date_values() {
        let value = parse_date_param(0, "2024-01-02").expect("date should parse");
        assert_eq!(
            value,
            Date::from_calendar_date(2024, Month::January, 2).expect("valid date")
        );
    }

    #[test]
    fn rejects_invalid_date_values() {
        let err = parse_date_param(1, "2024-13-02").expect_err("date should fail");
        assert!(matches!(err, ProbeError::BadParam(_)));
        assert!(err.to_string().contains("invalid date value at $2"));
    }

    #[test]
    fn parses_time_values() {
        let value = parse_time_param(0, "03:04:05.678901").expect("time should parse");
        assert_eq!(
            value,
            Time::from_hms_micro(3, 4, 5, 678_901).expect("valid time")
        );
    }

    #[test]
    fn parses_timestamp_values() {
        let value =
            parse_timestamp_param(0, "2024-01-02 03:04:05.678901").expect("timestamp should parse");
        assert_eq!(
            value,
            PrimitiveDateTime::new(
                Date::from_calendar_date(2024, Month::January, 2).expect("valid date"),
                Time::from_hms_micro(3, 4, 5, 678_901).expect("valid time"),
            )
        );
    }

    #[test]
    fn parses_timestamptz_values() {
        let value = parse_timestamptz_param(0, "2024-01-02 03:04:05.678901+02:30")
            .expect("timestamptz should parse");
        let expected = PrimitiveDateTime::new(
            Date::from_calendar_date(2024, Month::January, 2).expect("valid date"),
            Time::from_hms_micro(3, 4, 5, 678_901).expect("valid time"),
        )
        .assume_offset(UtcOffset::from_hms(2, 30, 0).expect("valid offset"));
        assert_eq!(value, expected);
    }

    #[test]
    fn parses_uuid_values() {
        let value =
            parse_uuid_param(0, "12345678-1234-5678-1234-567812345678").expect("uuid should parse");
        assert_eq!(
            value,
            Uuid::parse_str("12345678-1234-5678-1234-567812345678").expect("valid expected uuid")
        );
    }

    #[test]
    fn rejects_invalid_timestamptz_values() {
        let err = parse_timestamptz_param(2, "2024-01-02 03:04:05Z")
            .expect_err("unsupported offset format should fail");
        assert!(matches!(err, ProbeError::BadParam(_)));
        assert!(err.to_string().contains("invalid timestamptz value at $3"));
    }
}

fn parse_bool_param(index: usize, value: &str) -> Result<bool, ProbeError> {
    match value {
        "t" | "true" | "TRUE" | "1" => Ok(true),
        "f" | "false" | "FALSE" | "0" => Ok(false),
        _ => Err(ProbeError::BadParam(format!(
            "invalid boolean value at ${}: {}",
            index + 1,
            value
        ))),
    }
}

fn parse_numeric_param<T>(index: usize, ty: &Type, value: &str) -> Result<T, ProbeError>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    value.parse::<T>().map_err(|err| {
        ProbeError::BadParam(format!(
            "invalid {} value at ${}: {} ({err})",
            ty.name(),
            index + 1,
            value
        ))
    })
}
