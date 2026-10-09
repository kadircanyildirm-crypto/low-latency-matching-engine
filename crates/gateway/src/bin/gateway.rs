//! Runs the exchange: an engine on a directory, behind a TCP gateway.
//!
//! Usage: `gateway --dir <dir> --accounts <file> [--listen 127.0.0.1:9000] [--sync always|os]
//! [--min-price 1] [--max-price 100000] [--max-orders 1000000] [--max-owners 1024]
//! [--snapshot-every 1000000] [--max-sessions 1024] [--engine thread|pipeline]
//! [--wait spin|backoff] [--cores network,writer,matcher] [--web <addr>]
//! [--guests <file>] [--guest-ids 100..1024]`
//!
//! `--engine pipeline` runs the journal and the book on threads of their own, waiting as
//! `--wait` says; `--cores` pins the three threads to those logical cores.
//!
//! `--web` also serves browsers on `<addr>`: the exchange's page, and sessions over
//! WebSocket. Visitors may create accounts with ids from `--guest-ids`, which are saved to
//! the `--guests` file (`<dir>/guests.txt` by default) and loaded from it on the next start.
//! They trade paper money: `--guest-cash` (in price ticks times lots) and
//! `--guest-position` (in lots) to start with.
//!
//! The exchange's own state, paper money above all, is checkpointed in the data directory
//! every `--checkpoint-every` commands (10,000 by default, and at most `--snapshot-every`),
//! and rebuilt on the next start from the newest checkpoint and the journal after it.
//!
//! The accounts file holds one account per line: id, token, open-order limit and message
//! rate. The book's settings are part of the journal: a directory opens only with the ones
//! it was created with. Stop it with Ctrl+C or by killing it; the journal recovers either
//! way, and resting orders stay on the book.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::AtomicBool;

use engine::storage::FsStorage;
use engine::{EngineConfig, SyncPolicy};
use gateway::web::Guests;
use gateway::{
    Account, Core, Exchange, Funds, Pipeline, PipelineConfig, Server, ServerConfig, Timing,
    accounts, recovery,
};
use orderbook::BookConfig;
use ring::Wait;

const USAGE: &str = "usage: gateway --dir <dir> --accounts <file> [--listen <addr>] \
[--sync always|os] [--min-price <n>] [--max-price <n>] [--max-orders <n>] \
[--max-owners <n>] [--snapshot-every <n>] [--max-sessions <n>] [--engine thread|pipeline] \
[--wait spin|backoff] [--cores <network>,<writer>,<matcher>] [--web <addr>] \
[--guests <file>] [--guest-ids <from>..<to>] [--guest-cash <n>] [--guest-position <n>] \
[--checkpoint-every <n>]";

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
    let checkpoint_every: u64 = parse(take("checkpoint-every"), "10000", "checkpoint-every")?;
    if checkpoint_every == 0 || snapshot_every > 0 && checkpoint_every > snapshot_every {
        return Err(
            "--checkpoint-every must be at least one, and at most --snapshot-every: the journal \
             must reach back to the last checkpoint"
                .into(),
        );
    }
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
    let web: Option<SocketAddr> = match take("web") {
        None => None,
        Some(addr) => Some(
            addr.parse()
                .map_err(|_| format!("--web: cannot read {addr}"))?,
        ),
    };
    let guests_file = take("guests")
        .map(PathBuf::from)
        .unwrap_or_else(|| Path::new(&dir).join("guests.txt"));
    let guest_ids = take("guest-ids").unwrap_or_else(|| format!("100..{}", book.max_owners));
    let guest_ids = match guest_ids.split_once("..") {
        Some((from, to)) => match (from.parse::<u32>(), to.parse::<u32>()) {
            (Ok(from), Ok(to)) if from < to && to <= book.max_owners => from..to,
            _ => return Err(format!("--guest-ids: {guest_ids} is no range of owner ids")),
        },
        None => {
            return Err(format!(
                "--guest-ids: {guest_ids} is no range, such as 100..1024"
            ));
        }
    };
    let funds = Funds {
        cash: parse(take("guest-cash"), "10000000", "guest-cash")?,
        position: parse(take("guest-position"), "1000", "guest-position")?,
    };
    if let Some(name) = args.keys().next() {
        return Err(format!("unknown flag --{name}\n{USAGE}"));
    }

    let mut accounts = read_accounts(Path::new(&accounts_file), book.max_owners)?;
    if web.is_some() && guests_file.exists() {
        accounts.extend(read_accounts(&guests_file, book.max_owners)?);
    }
    let guests = Guests {
        file: Some(guests_file),
        ids: guest_ids,
        max_open_orders: 50,
        messages_per_second: 20,
        funds: Some(funds),
    };
    let web = web.map(|addr| (addr, guests));
    let config = EngineConfig {
        sync,
        snapshot_every: (snapshot_every > 0).then_some(snapshot_every),
        ..EngineConfig::new(book)
    };
    let (engine, exchange, recovered) = recovery::open(
        FsStorage,
        Path::new(&dir),
        config,
        &accounts,
        Timing::default(),
    )
    .map_err(|e| format!("opening {dir}: {e}"))?;
    eprintln!(
        "gateway: recovered {dir} up to command {}, {} orders on the book; the exchange from {}",
        engine.last_seq(),
        engine.book().order_count(),
        match recovered.checkpoint {
            Some(seq) => format!(
                "its checkpoint at {seq} and {} commands after it",
                recovered.replayed
            ),
            None => "the book alone".to_owned(),
        }
    );
    let checkpoints = (
        PathBuf::from(&dir),
        checkpoint_every,
        recovered.checkpoint.unwrap_or(0),
    );
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
        serve(
            exchange,
            pipeline,
            listen,
            server,
            accounts.len(),
            web,
            checkpoints,
        )
    } else {
        serve(
            exchange,
            engine,
            listen,
            server,
            accounts.len(),
            web,
            checkpoints,
        )
    }
}

/// The accounts in `path`, with ids below `max_owners`.
fn read_accounts(path: &Path, max_owners: u32) -> Result<Vec<Account>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("reading {}: {e}", path.display()))?;
    accounts::parse(&text, max_owners).map_err(|e| format!("{}: {e}", path.display()))
}

/// Serves until the process is killed: nothing stops it otherwise.
fn serve<C: Core>(
    exchange: Exchange,
    core: C,
    listen: SocketAddr,
    config: ServerConfig,
    accounts: usize,
    web: Option<(SocketAddr, Guests)>,
    (dir, every, since): (PathBuf, u64, u64),
) -> Result<(), String> {
    let mut server =
        Server::bind(exchange, core, listen, config).map_err(|e| format!("{listen}: {e}"))?;
    server.checkpoint_to(dir, every, since);
    eprintln!(
        "gateway: {accounts} accounts, listening on {}",
        server.local_addr().map_err(|e| e.to_string())?
    );
    if let Some((addr, guests)) = web {
        server
            .serve_web(addr, Some(guests))
            .map_err(|e| format!("{addr}: {e}"))?;
        eprintln!("gateway: the web page is on http://{addr}");
    }
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
