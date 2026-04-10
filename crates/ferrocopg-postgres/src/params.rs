use crate::error::ProbeError;
use postgres::types::{ToSql, Type};
use std::fmt;
use time::{Date, Month};
use uuid::Uuid;

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
