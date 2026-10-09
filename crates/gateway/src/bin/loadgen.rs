//! Drives a running gateway with clients that trade against each other, and prints the
//! throughput and the latency of order acknowledgements.
//!
//! Usage: `loadgen --accounts <file> [--connect 127.0.0.1:9000] [--clients 4] [--seconds 5]
//! [--window 16] [--mid 50000] [--spread 20] [--seed 1]`
//!
//! Each client logs in with one account of the file, in order, so the file needs at least
//! `--clients` accounts; use ones with generous limits.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use gateway::accounts;
use gateway::load::{self, LoadConfig};

const USAGE: &str = "usage: loadgen --accounts <file> [--connect <addr>] [--clients <n>] \
[--seconds <n>] [--window <n>] [--mid <price>] [--spread <ticks>] [--seed <n>]";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("loadgen: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut args: HashMap<String, String> = HashMap::new();
    let mut words = std::env::args().skip(1);
    while let Some(flag) = words.next() {
        let name = flag
            .strip_prefix("--")
            .ok_or_else(|| format!("unexpected {flag}\n{USAGE}"))?;
        let value = words
            .next()
            .ok_or_else(|| format!("--{name} needs a value\n{USAGE}"))?;
        args.insert(name.to_owned(), value);
    }
    let mut take = |name: &str| args.remove(name);
    let file = take("accounts").ok_or(USAGE)?;
    let addr: SocketAddr = parse(take("connect"), "127.0.0.1:9000", "connect")?;
    let clients: usize = parse(take("clients"), "4", "clients")?;
    let defaults = LoadConfig::default();
    let config = LoadConfig {
        duration: Duration::from_secs(parse(take("seconds"), "5", "seconds")?),
        window: parse(take("window"), "16", "window")?,
        mid: parse(take("mid"), &defaults.mid.to_string(), "mid")?,
        spread: parse(take("spread"), &defaults.spread.to_string(), "spread")?,
        seed: parse(take("seed"), "1", "seed")?,
        ..defaults
    };
    if let Some(name) = args.keys().next() {
        return Err(format!("unknown flag --{name}\n{USAGE}"));
    }
    let text = std::fs::read_to_string(&file).map_err(|e| format!("reading {file}: {e}"))?;
    let accounts = accounts::parse(&text, u32::MAX).map_err(|e| format!("{file}: {e}"))?;
    if accounts.len() < clients {
        return Err(format!(
            "{file} holds {} accounts, fewer than {clients} clients",
            accounts.len()
        ));
    }
    let report = load::run(addr, &accounts[..clients], config).map_err(|e| e.to_string())?;
    let seconds = report.elapsed.as_secs_f64();
    println!(
        "{} orders in {seconds:.2} s: {:.0} orders/s",
        report.orders,
        report.orders as f64 / seconds
    );
    println!(
        "accepted {}, refused by the book {}, by the gateway {}, cancels {}, fills {}",
        report.accepted, report.rejected, report.refused, report.cancels, report.fills
    );
    let micros = |q: f64| report.percentile(q).map_or(0.0, |ns| ns as f64 / 1_000.0);
    println!(
        "acknowledged in µs: p50 {:.1}, p90 {:.1}, p99 {:.1}, p99.9 {:.1}, max {:.1}",
        micros(0.5),
        micros(0.9),
        micros(0.99),
        micros(0.999),
        micros(1.0)
    );
    Ok(())
}

fn parse<T: std::str::FromStr>(
    value: Option<String>,
    default: &str,
    name: &str,
) -> Result<T, String> {
    let value = value.as_deref().unwrap_or(default);
    value
        .parse()
        .map_err(|_| format!("--{name}: cannot read {value}"))
}
