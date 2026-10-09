//! Summarises `compare/results/results.csv` as Markdown tables: per scenario and engine, the
//! median over all rows with the range (min-max) beside it.
//!
//! Run: `cargo run --release --manifest-path compare/Cargo.toml --bin report`
//! Env: `CMP_RESULTS` CSV path.

use std::collections::BTreeMap;

use harness::run::{CSV_HEADER, results_path};
use harness::scenarios;

/// Engines in report order; any other name follows, alphabetically.
const ORDER: [&str; 4] = ["ours", "exchange-core", "liquibook", "orderbook-rs"];

struct Row {
    throughput: f64,
    chunk: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    p999: f64,
    p9999: f64,
    verified: bool,
}

fn main() {
    let path = results_path();
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    let columns: Vec<&str> = CSV_HEADER.split(',').collect();
    let col = |name: &str| {
        columns
            .iter()
            .position(|c| *c == name)
            .expect("known column")
    };
    let mut rows: BTreeMap<(String, String), Vec<Row>> = BTreeMap::new();
    for line in text.lines().skip(1).filter(|l| !l.trim().is_empty()) {
        let f: Vec<&str> = line.split(',').collect();
        assert_eq!(f.len(), columns.len(), "malformed row: {line}");
        let num = |name: &str| f[col(name)].parse::<f64>().expect("number");
        rows.entry((f[col("scenario")].to_string(), f[col("engine")].to_string()))
            .or_default()
            .push(Row {
                throughput: num("throughput_mcmd_s"),
                chunk: num("chunk_median_mcmd_s"),
                p50: num("p50_ns"),
                p90: num("p90_ns"),
                p99: num("p99_ns"),
                p999: num("p99_9_ns"),
                p9999: num("p99_99_ns"),
                verified: f[col("verified")] == "yes",
            });
    }

    println!("Source: {}\n", path.display());
    println!(
        "Median over runs, range (min-max) in brackets. Throughput: all measured commands over \
         their total time, and the median 100k-command chunk, which preemption by other \
         processes cannot move; \"vs ours\" compares chunk medians. Latency is per command, in \
         ns, timer overhead included.\n"
    );
    for scenario in scenarios::all() {
        let mut engines: Vec<&String> = rows
            .keys()
            .filter(|(s, _)| s == scenario.name)
            .map(|(_, e)| e)
            .collect();
        if engines.is_empty() {
            continue;
        }
        engines.sort_by_key(|e| {
            (
                ORDER.iter().position(|o| o == e).unwrap_or(ORDER.len()),
                e.to_string(),
            )
        });
        let ours = rows
            .get(&(scenario.name.to_string(), "ours".to_string()))
            .map(|r| median(r, |x| x.chunk));
        println!("### {} ({})\n", scenario.name, scenario.about);
        println!(
            "| Engine | Runs | Throughput (M cmd/s) | Chunk median | vs ours | p50 | p90 | p99 | \
             p99.9 | p99.99 |"
        );
        println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
        for engine in engines {
            let r = &rows[&(scenario.name.to_string(), engine.clone())];
            let chunk = median(r, |x| x.chunk);
            let relative = ours.map_or("".to_string(), |o| format!("{:.2}x", chunk / o));
            let unverified = r.iter().filter(|x| !x.verified).count();
            let name = if unverified == 0 {
                engine.clone()
            } else {
                format!("{engine} (**{unverified} unverified**)")
            };
            println!(
                "| {name} | {} | {} | {} | {relative} | {} | {} | {} | {} | {} |",
                r.len(),
                spread(r, |x| x.throughput, 2),
                spread(r, |x| x.chunk, 2),
                spread(r, |x| x.p50, 0),
                spread(r, |x| x.p90, 0),
                spread(r, |x| x.p99, 0),
                spread(r, |x| x.p999, 0),
                spread(r, |x| x.p9999, 0),
            );
        }
        println!();
    }
}

fn sorted(rows: &[Row], f: impl Fn(&Row) -> f64) -> Vec<f64> {
    let mut v: Vec<f64> = rows.iter().map(f).collect();
    v.sort_by(f64::total_cmp);
    v
}

fn median(rows: &[Row], f: impl Fn(&Row) -> f64) -> f64 {
    let v = sorted(rows, f);
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// `median [min-max]`, or just the value for a single run.
fn spread(rows: &[Row], f: impl Fn(&Row) -> f64 + Copy, decimals: usize) -> String {
    let v = sorted(rows, f);
    let m = median(rows, f);
    if v.len() == 1 {
        format!("{m:.decimals$}")
    } else {
        format!(
            "{m:.decimals$} [{:.decimals$}-{:.decimals$}]",
            v[0],
            v[v.len() - 1]
        )
    }
}
