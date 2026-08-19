//! `socstat-verify` — a host-side integration harness for the `socstat` SDK.
//!
//! Consumes `socstat` as an external dependency (the same shape any real host
//! application would) and drives every public module end-to-end:
//!
//! - [`examples/host_demo.rs`](examples/host_demo.rs) — a realistic call chain
//!   across data, statistics, distributions and I/O (`cargo run --example host_demo`).
//! - [`tests/integration_test.rs`](tests/integration_test.rs) — module-by-module
//!   integration tests (`cargo test --test integration_test`).
//!
//! This harness deliberately lives *outside* the `socstat` repo so it exercises
//! the public API surface exactly as an external embedder would.

use socstat::data::{Dataset, Value, Variable};
use socstat::error::SocStatResult;

/// A realistic survey-style dataset shared by the demo and the integration suite.
pub fn survey() -> SocStatResult<Dataset> {
    let mut ds = Dataset::new();
    ds.add_var(Variable::text("group").label("Study arm"))?;
    ds.add_var(Variable::numeric("dose"))?;
    ds.add_var(Variable::numeric("score"))?;
    ds.add_var(Variable::numeric("pre"))?;
    ds.add_var(Variable::numeric("post"))?;
    ds.add_var(Variable::numeric("q1"))?;
    ds.add_var(Variable::numeric("q2"))?;
    ds.add_var(Variable::numeric("q3"))?;
    ds.add_var(Variable::numeric("income"))?;
    ds.add_var(Variable::numeric("spend"))?;
    ds.add_var(Variable::numeric("binary"))?;
    ds.add_var(Variable::text("outcome"))?;
    ds.add_var(Variable::numeric("w").weight())?;

    // 20 rows: 10 per arm. post > pre; q* strongly correlated; income ~ spend.
    let rows: &[(&str, f64, f64, f64, f64, f64, f64, f64, f64, f64, f64, &str)] = &[
        ("A", 1.0, 85.0, 10.0, 11.0, 3.0, 4.0, 5.0, 2000.0, 800.0, 1.0, "yes"),
        ("A", 1.0, 90.0, 12.0, 13.0, 4.0, 5.0, 6.0, 2200.0, 900.0, 0.0, "yes"),
        ("A", 2.0, 78.0, 9.0, 10.0, 2.0, 3.0, 4.0, 1800.0, 700.0, 1.0, "no"),
        ("A", 2.0, 92.0, 14.0, 16.0, 5.0, 6.0, 7.0, 2600.0, 1100.0, 0.0, "no"),
        ("A", 3.0, 88.0, 11.0, 12.0, 3.0, 4.0, 5.0, 2100.0, 850.0, 1.0, "yes"),
        ("A", 3.0, 72.0, 8.0, 9.0, 1.0, 2.0, 3.0, 1500.0, 600.0, 1.0, "no"),
        ("A", 1.0, 95.0, 15.0, 17.0, 6.0, 7.0, 8.0, 3000.0, 1300.0, 0.0, "yes"),
        ("A", 2.0, 80.0, 10.0, 11.0, 2.0, 3.0, 4.0, 1900.0, 750.0, 0.0, "no"),
        ("A", 3.0, 87.0, 11.0, 12.5, 3.0, 4.0, 5.0, 2150.0, 880.0, 1.0, "yes"),
        ("A", 1.0, 75.0, 9.0, 10.0, 2.0, 3.0, 4.0, 1700.0, 680.0, 1.0, "no"),
        ("B", 1.0, 91.0, 13.0, 15.0, 4.0, 5.0, 6.0, 2400.0, 1000.0, 1.0, "yes"),
        ("B", 2.0, 70.0, 8.0, 9.0, 1.0, 2.0, 3.0, 1400.0, 550.0, 1.0, "no"),
        ("B", 3.0, 89.0, 12.0, 13.5, 3.0, 4.0, 5.0, 2250.0, 920.0, 0.0, "yes"),
        ("B", 1.0, 76.0, 9.0, 10.0, 2.0, 3.0, 4.0, 1650.0, 660.0, 0.0, "no"),
        ("B", 2.0, 93.0, 14.0, 16.0, 5.0, 6.0, 7.0, 2700.0, 1150.0, 1.0, "yes"),
        ("B", 3.0, 79.0, 10.0, 11.0, 2.0, 3.0, 4.0, 1850.0, 720.0, 0.0, "no"),
        ("B", 1.0, 86.0, 11.0, 12.0, 3.0, 4.0, 5.0, 2050.0, 830.0, 1.0, "yes"),
        ("B", 2.0, 84.0, 10.5, 12.0, 3.0, 4.0, 5.0, 2000.0, 810.0, 0.0, "no"),
        ("B", 3.0, 82.0, 10.0, 11.5, 3.0, 4.0, 5.0, 1950.0, 790.0, 1.0, "yes"),
        ("B", 1.0, 94.0, 15.0, 17.0, 6.0, 7.0, 8.0, 2900.0, 1250.0, 0.0, "yes"),
    ];
    for (g, dose, s, pre, post, q1, q2, q3, inc, spd, bin, out) in rows {
        ds.push_row(vec![
            Value::Text((*g).into()),
            Value::Number(*dose),
            Value::Number(*s),
            Value::Number(*pre),
            Value::Number(*post),
            Value::Number(*q1),
            Value::Number(*q2),
            Value::Number(*q3),
            Value::Number(*inc),
            Value::Number(*spd),
            Value::Number(*bin),
            Value::Text((*out).into()),
            Value::Number(1.0),
        ])?;
    }
    Ok(ds)
}