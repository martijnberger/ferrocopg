use crate::error::ProbeError;
use postgres::types::{ToSql, Type};
use std::fmt;

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
