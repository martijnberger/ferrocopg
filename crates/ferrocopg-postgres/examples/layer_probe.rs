//! Diagnostic only: prepared binary int4, one query in flight, no Python.

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use ferrocopg_postgres::{WireFormat, connect_no_tls_session};

const QUERY: &str = "select 42::int4";

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
    if args.len() != 4 {
        return Err("usage: layer_probe postgres|postgres-wait|session iterations samples".into());
    }
    let mode = &args[1];
    let iterations: usize = args[2].parse()?;
    let samples: usize = args[3].parse()?;
    if iterations == 0 || samples < 3 {
        return Err("positive iterations and at least three samples required".into());
    }
    let dsn = std::env::var("PHASE5_DSN")?;
    let timings = match mode.as_str() {
        "postgres" | "postgres-wait" => {
            let mut client = postgres::Client::connect(&dsn, postgres::NoTls)?;
            let statement = client.prepare(QUERY)?;
            let callback: Arc<dyn Fn() + Send + Sync> = Arc::new(|| {});
            measure(
                || {
                    if mode == "postgres-wait" {
                        client.set_wait_callback(Some(Arc::clone(&callback)));
                    }
                    let rows = client.query(&statement, &[]).unwrap();
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].len(), 1);
                    assert_eq!(rows[0].get::<_, i32>(0), 42);
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
            let statement = session.prepare_text(QUERY)?.statement_id;
            measure(
                || {
                    let result = session
                        .run_prepared_params_format(statement, &[], WireFormat::Binary)
                        .unwrap();
                    assert_eq!(result.rows.len(), 1);
                    assert_eq!(result.rows[0].len(), 1);
                    assert_eq!(result.rows[0].get(0), Some(Some(&[0, 0, 0, 42][..])));
                },
                iterations,
                samples,
            )
        }
        _ => return Err("unknown probe mode".into()),
    };
    println!(
        "{{\"backend\":\"{mode}\",\"iterations\":{iterations},\"warmup\":1000,\"wall_us\":{timings:?}}}"
    );
    Ok(())
}
