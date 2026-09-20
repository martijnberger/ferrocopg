//! Diagnostic only: matched scalar query cases, one query in flight, no Python.

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use ferrocopg_postgres::{BoundParam, ParamFormat, WireFormat, connect_no_tls_session};
use postgres::types::{FromSql, Type};

const QUERY: &str = "select 42::int4";
const PARAM_QUERY: &str = "select $1::int + 1";

#[derive(Debug)]
struct Raw<'a>(&'a [u8]);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(raw))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT4
    }
}

fn measure(mut query: impl FnMut(), iterations: usize, samples: usize) -> Vec<f64> {
    for _ in 0..1000 {
        query();
    }
    (0..samples)
        .map(|_| {
            let start = Instant::now();
            for _ in 0..iterations {
                query();
            }
            start.elapsed().as_secs_f64() * 1e6 / iterations as f64
        })
        .collect()
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 && args.len() != 6 {
        return Err(
            "usage: layer_probe mode iterations samples [constant|prepared|unprepared binary|text]"
                .into(),
        );
    }
    let mode = &args[1];
    let iterations: usize = args[2].parse()?;
    let samples: usize = args[3].parse()?;
    if iterations == 0 || samples < 3 {
        return Err("positive iterations and at least three samples required".into());
    }
    let case = args.get(4).map(String::as_str).unwrap_or("constant");
    let format = args.get(5).map(String::as_str).unwrap_or("binary");
    if !matches!(case, "constant" | "prepared" | "unprepared")
        || !matches!(format, "binary" | "text")
    {
        return Err("invalid query case or result format".into());
    }
    let parameterized = case != "constant";
    let prepared = case != "unprepared";
    let binary = format == "binary";
    let query_text = if parameterized { PARAM_QUERY } else { QUERY };
    let expected: &[u8] = if binary { &[0, 0, 0, 42] } else { b"42" };
    let dsn = std::env::var("PHASE5_DSN")?;
    let timings = match mode.as_str() {
        "postgres" | "postgres-wait" => {
            let mut client = postgres::Client::connect(&dsn, postgres::NoTls)?;
            let types = if parameterized {
                vec![Type::INT2]
            } else {
                vec![]
            };
            let statement = if prepared {
                Some(client.prepare_typed(query_text, &types)?)
            } else {
                None
            };
            let value = 41_i16;
            let params: Vec<&(dyn postgres::types::ToSql + Sync)> =
                if parameterized { vec![&value] } else { vec![] };
            let callback: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
            measure(
                || {
                    if mode == "postgres-wait" {
                        client.set_wait_callback(Some(Arc::clone(&callback)));
                    }
                    let rows = if let Some(statement) = &statement {
                        client
                            .query_with_result_format(statement, &params, binary)
                            .unwrap()
                    } else {
                        client
                            .query_typed_raw_with_result_format(
                                query_text,
                                [(&value, Type::INT2)],
                                binary,
                            )
                            .unwrap()
                            .collect_rows()
                            .unwrap()
                    };
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].len(), 1);
                    if binary {
                        assert_eq!(rows[0].get::<_, i32>(0), 42);
                    } else {
                        assert_eq!(rows[0].get::<_, Raw<'_>>(0).0, expected);
                    }
                    if mode == "postgres-wait" {
                        client.set_wait_callback(None);
                    }
                },
                iterations,
                samples,
            )
        }
        "session" => {
            let mut session = connect_no_tls_session(&dsn)?;
            let oids = if parameterized { vec![21] } else { vec![] };
            let statement = if prepared {
                Some(session.prepare_params(query_text, &oids)?.statement_id)
            } else {
                None
            };
            let params = if parameterized {
                vec![BoundParam {
                    oid: 21,
                    value: Some(vec![0, 41]),
                    format: ParamFormat::Binary,
                }]
            } else {
                vec![]
            };
            let wire_format = if binary {
                WireFormat::Binary
            } else {
                WireFormat::Text
            };
            measure(
                || {
                    let result = if let Some(statement) = statement {
                        session.run_prepared_params_format(statement, &params, wire_format)
                    } else {
                        session.run_params_format(query_text, &params, wire_format)
                    }
                    .unwrap();
                    assert_eq!(result.rows.len(), 1);
                    assert_eq!(result.rows[0].len(), 1);
                    assert_eq!(result.rows[0].get(0), Some(Some(expected)));
                },
                iterations,
                samples,
            )
        }
        _ => return Err("unknown probe mode".into()),
    };
    println!(
        "{{\"backend\":\"{mode}\",\"iterations\":{iterations},\"warmup\":1000,\"wall_us\":{timings:?},\"case\":\"{case}\",\"result_format\":\"{format}\",\"parameter_oids\":{}}}",
        if parameterized { "[21]" } else { "[]" }
    );
    Ok(())
}
