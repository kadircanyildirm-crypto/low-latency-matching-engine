//! Runs the exchange: an engine on a directory, behind a TCP gateway.
//!
//! Usage: `gateway --dir <dir> --accounts <file> [--listen 127.0.0.1:9000] [--sync always|os]
//! [--min-price 1] [--max-price 100000] [--max-orders 1000000] [--max-owners 1024]
//! [--snapshot-every 1000000] [--max-sessions 1024] [--engine thread|pipeline]
//! [--wait spin|backoff] [--cores network,writer,matcher]`
//!
//! `--engine pipeline` runs the journal and the book on threads of their own, waiting as
//! `--wait` says; `--cores` pins the three threads to those logical cores.
//!
//! The accounts file holds one account per line: id, token, open-order limit and message
//! rate. The book's settings are part of the journal: a directory opens only with the ones
//! it was created with. Stop it with Ctrl+C or by killing it; the journal recovers either
//! way, and resting orders stay on the book.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::atomic::AtomicBool;

use engine::{Discard, Engine, EngineConfig, SyncPolicy};
use gateway::{Core, Exchange, Pipeline, PipelineConfig, Server, ServerConfig, Timing, accounts};
use orderbook::BookConfig;
use ring::Wait;

const USAGE: &str = "usage: gateway --dir <dir> --accounts <file> [--listen <addr>] \
[--sync always|os] [--min-price <n>] [--max-price <n>] [--max-orders <n>] \
[--max-owners <n>] [--snapshot-every <n>] [--max-sessions <n>] [--engine thread|pipeline] \
[--wait spin|backoff] [--cores <network>,<writer>,<matcher>]";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("gateway: {error}");
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
    let dir = take("dir").ok_or(USAGE)?;
    let accounts_file = take("accounts").ok_or(USAGE)?;
    let listen: SocketAddr = parse(take("listen"), "127.0.0.1:9000", "listen")?;
    let sync = match take("sync").as_deref() {
        None | Some("always") => SyncPolicy::Always,
        Some("os") => SyncPolicy::Os,
        Some(other) => return Err(format!("--sync must be always or os, not {other}")),
    };
    let book = BookConfig {
        max_owners: parse(take("max-owners"), "1024", "max-owners")?,
        ..BookConfig::new(
            parse(take("min-price"), "1", "min-price")?,
            parse(take("max-price"), "100000", "max-price")?,
            parse(take("max-orders"), "1000000", "max-orders")?,
        )
    };
    book.check().map_err(|e| format!("book settings: {e}"))?;
    let snapshot_every: u64 = parse(take("snapshot-every"), "1000000", "snapshot-every")?;
    let server = ServerConfig {
        max_sessions: parse(take("max-sessions"), "1024", "max-sessions")?,
        ..ServerConfig::default()
    };
    let pipelined = match take("engine").as_deref() {
        None | Some("thread") => false,
        Some("pipeline") => true,
        Some(other) => return Err(format!("--engine must be thread or pipeline, not {other}")),
    };
    let wait = match take("wait").as_deref() {
        None | Some("backoff") => Wait::Backoff,
        Some("spin") => Wait::Spin,
        Some(other) => return Err(format!("--wait must be spin or backoff, not {other}")),
    };
    let cores: Vec<usize> = match take("cores") {
        None => Vec::new(),
        Some(list) => list
            .split(',')
            .map(|core| {
                core.trim()
                    .parse()
                    .map_err(|_| format!("--cores: cannot read {core}"))
            })
            .collect::<Result<_, _>>()?,
    };
    if let Some(name) = args.keys().next() {
        return Err(format!("unknown flag --{name}\n{USAGE}"));
    }

    let text = std::fs::read_to_string(&accounts_file)
        .map_err(|e| format!("reading {accounts_file}: {e}"))?;
    let accounts =
        accounts::parse(&text, book.max_owners).map_err(|e| format!("{accounts_file}: {e}"))?;
    let config = EngineConfig {
        sync,
        snapshot_every: (snapshot_every > 0).then_some(snapshot_every),
        ..EngineConfig::new(book)
    };
    let (engine, report) =
        Engine::open(&dir, config, &mut Discard).map_err(|e| format!("opening {dir}: {e}"))?;
    eprintln!(
        "gateway: recovered {dir} up to command {} ({} replayed), {} orders on the book",
        engine.last_seq(),
        report.journal.replayed,
        engine.book().order_count()
    );
    let exchange = Exchange::new(
        engine.book(),
        engine.last_seq(),
        &accounts,
        Timing::default(),
    )
    .map_err(|e| e.to_string())?;
    if let Some(&core) = cores.first() {
        let found = core_affinity::get_core_ids()
            .unwrap_or_default()
            .into_iter()
            .find(|c| c.id == core);
        if !found.is_some_and(core_affinity::set_for_current) {
            return Err(format!("--cores: cannot pin to core {core}"));
        }
    }
    if pipelined {
        let config = PipelineConfig {
            wait,
            writer_core: cores.get(1).copied(),
            matcher_core: cores.get(2).copied(),
            ..PipelineConfig::default()
        };
        let pipeline = Pipeline::start(engine, config).map_err(|e| e.to_string())?;
        serve(exchange, pipeline, listen, server, accounts.len())
    } else {
        serve(exchange, engine, listen, server, accounts.len())
    }
}

/// Serves until the process is killed: nothing stops it otherwise.
fn serve<C: Core>(
    exchange: Exchange,
    core: C,
    listen: SocketAddr,
    config: ServerConfig,
    accounts: usize,
) -> Result<(), String> {
    let mut server =
        Server::bind(exchange, core, listen, config).map_err(|e| format!("{listen}: {e}"))?;
    eprintln!(
        "gateway: {accounts} accounts, listening on {}",
        server.local_addr().map_err(|e| e.to_string())?
    );
    let stop = AtomicBool::new(false);
    server.run(&stop).map_err(|e| e.to_string())
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
