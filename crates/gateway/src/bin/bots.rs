//! Runs bots against a gateway, to keep a demo market alive.
//!
//! Usage: `bots --accounts <file> [--connect 127.0.0.1:9000] [--makers 3] [--noise 3]
//! [--trend 1] [--passive 2] [--icebergs 1] [--stops 1] [--whales 1] [--mid 10000]
//! [--seed 1]`
//!
//! Each bot logs in with the next account of the file, in that order: market makers first,
//! then noise traders, trend followers, passive traders, icebergs, stops and whales; twelve
//! accounts by default. Use accounts without funds and with generous rates.
//! A bot whose connection fails, because the gateway restarted say, tries again every few
//! seconds.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;

use gateway::accounts;
use gateway::bots::{self, Bot, Strategy};

const USAGE: &str = "usage: bots --accounts <file> [--connect <addr>] [--makers <n>] \
[--noise <n>] [--trend <n>] [--passive <n>] [--icebergs <n>] [--stops <n>] [--whales <n>] \
[--mid <price>] [--seed <n>]";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("bots: {error}");
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
    let counts = [
        (
            Strategy::MarketMaker,
            parse(take("makers"), "3", "makers")?,
            500,
        ),
        (Strategy::Noise, parse(take("noise"), "3", "noise")?, 1_500),
        (
            Strategy::Trend,
            parse::<usize>(take("trend"), "1", "trend")?,
            3_000,
        ),
        (
            Strategy::Passive,
            parse(take("passive"), "2", "passive")?,
            900,
        ),
        (
            Strategy::Iceberg,
            parse(take("icebergs"), "1", "icebergs")?,
            2_000,
        ),
        (Strategy::Stops, parse(take("stops"), "1", "stops")?, 2_500),
        (
            Strategy::Whale,
            parse(take("whales"), "1", "whales")?,
            30_000,
        ),
    ];
    let mid: i64 = parse(take("mid"), "10000", "mid")?;
    let seed: u64 = parse(take("seed"), "1", "seed")?;
    if let Some(name) = args.keys().next() {
        return Err(format!("unknown flag --{name}\n{USAGE}"));
    }
    let text = std::fs::read_to_string(&file).map_err(|e| format!("reading {file}: {e}"))?;
    let mut accounts = accounts::parse(&text, u32::MAX)
        .map_err(|e| format!("{file}: {e}"))?
        .into_iter();
    let mut bots = Vec::new();
    for (strategy, count, interval_ms) in counts {
        for _ in 0..count {
            let account = accounts
                .next()
                .ok_or_else(|| format!("{file} holds too few accounts for the bots"))?;
            bots.push(Bot {
                strategy,
                account,
                mid,
                interval: Duration::from_millis(interval_ms),
                seed: seed.wrapping_add(bots.len() as u64),
            });
        }
    }
    eprintln!("bots: {} bots trading on {addr}", bots.len());
    let threads: Vec<_> = bots
        .into_iter()
        .map(|bot| {
            thread::spawn(move || {
                // Nothing sets it: the bots run until the process is stopped.
                let stop = AtomicBool::new(false);
                loop {
                    if let Err(error) = bots::run(addr, bot, &stop) {
                        eprintln!("bots: {:?} {}: {error}", bot.strategy, bot.account.id);
                    }
                    thread::sleep(Duration::from_secs(3));
                }
            })
        })
        .collect();
    for thread in threads {
        let _ = thread.join();
    }
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
