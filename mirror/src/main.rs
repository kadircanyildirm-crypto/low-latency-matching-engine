//! Mirrors a real venue's market into the exchange.
//!
//! Usage: `mirror --accounts <file> [--connect 127.0.0.1:9000] [--pair ethusd]
//! [--levels 20] [--price-decimals 2] [--lot-decimals 6] [--feed wss://ws.bitstamp.net]`
//!
//! The first account of the file holds the venue's resting orders, the second sends the
//! venue's trades again; use accounts without funds and with generous limits. The gateway
//! must take the venue's prices and quantities exactly: `--price-decimals` and
//! `--lot-decimals` must be the gateway's, and its price range must hold the venue's prices.
//! If the venue's connection drops, or it sends nothing for half a minute, the mirror
//! cancels its orders until the venue is back, rather than show a book that is no longer
//! real; if the gateway's drops, the exchange cancels them, and the mirror logs in again.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use gateway::accounts::{self, Account};
use gateway::client::Client;
use mirror::bitstamp::Book;
use mirror::book::{self, Mirror};
use mirror::feed::{self, Event};
use protocol::{Inbound, Outbound};

const USAGE: &str = "usage: mirror --accounts <file> [--connect <addr>] [--pair <pair>] \
[--levels <n>] [--price-decimals <n>] [--lot-decimals <n>] [--feed <url>]";

/// How often the mirror's orders are brought into line with the venue's book. Following a
/// book that has not changed sends nothing, unless the mirror's orders have: a modify sets
/// an order's total, so one computed before a fill was reported leaves too little, and the
/// next round puts that right.
const FOLLOW_EVERY: Duration = Duration::from_millis(250);

/// How long after sending a trade again the mirror waits before following the book, so
/// that the fills it causes are reported first.
const SETTLE: Duration = Duration::from_millis(100);

/// How long a book without news is shown before the mirror withdraws it.
const STALE: Duration = Duration::from_secs(30);

/// How long a connection to the gateway stays silent at most.
const HEARTBEAT: Duration = Duration::from_secs(1);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mirror: {error}");
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
    let levels: usize = parse(take("levels"), "20", "levels")?;
    let config = feed::Config {
        url: take("feed").unwrap_or_else(|| "wss://ws.bitstamp.net".to_owned()),
        pair: take("pair").unwrap_or_else(|| "ethusd".to_owned()),
        price_decimals: parse(take("price-decimals"), "2", "price-decimals")?,
        lot_decimals: parse(take("lot-decimals"), "6", "lot-decimals")?,
    };
    if let Some(name) = args.keys().next() {
        return Err(format!("unknown flag --{name}\n{USAGE}"));
    }
    let text = std::fs::read_to_string(&file).map_err(|e| format!("reading {file}: {e}"))?;
    let accounts = accounts::parse(&text, u32::MAX).map_err(|e| format!("{file}: {e}"))?;
    let [book_account, tape_account, ..] = accounts[..] else {
        return Err(format!(
            "{file} needs two accounts: one for the book, one for trades"
        ));
    };
    let events = feed::spawn(config);
    loop {
        if let Err(error) = mirror(addr, book_account, tape_account, levels, &events) {
            eprintln!("mirror: {error}");
        }
        thread::sleep(Duration::from_secs(3));
    }
}

/// Mirrors the venue through one pair of connections to the gateway, until one fails.
fn mirror(
    addr: SocketAddr,
    book_account: Account,
    tape_account: Account,
    levels: usize,
    events: &Receiver<Event>,
) -> Result<(), String> {
    let connect = |account: Account| {
        Client::login(addr, account.id, account.token)
            .map(|(client, _)| client)
            .map_err(|e| format!("logging in to {addr} as {}: {e}", account.id))
    };
    let mut book = connect(book_account)?;
    let mut tape = connect(tape_account)?;
    eprintln!("mirror: mirroring into {addr}");
    let mut mirror = Mirror::new(levels);
    let mut latest: Option<Book> = None;
    let mut followed = Instant::now();
    let mut retraded = Instant::now();
    let mut heard = Instant::now();
    let mut sent = [Instant::now(); 2];
    let mut next_ref = 0;
    loop {
        while let Some(message) = book.try_receive().map_err(|e| e.to_string())? {
            match message {
                Outbound::Report(report) => mirror.on_report(&report),
                Outbound::Reject { client_ref, .. } => mirror.on_refused(client_ref),
                Outbound::Logout { reason } => return Err(format!("logged out: {reason:?}")),
                _ => {}
            }
        }
        while let Some(message) = tape.try_receive().map_err(|e| e.to_string())? {
            if let Outbound::Logout { reason } = message {
                return Err(format!("trades logged out: {reason:?}"));
            }
        }
        match events.recv_timeout(Duration::from_millis(20)) {
            Ok(Event::Book(venue)) => {
                latest = Some(venue);
                heard = Instant::now();
            }
            // A trade is sent again only while the book it trades against is there.
            Ok(Event::Trade(trade)) if !mirror.is_empty() => {
                next_ref += 1;
                let order = book::retrade(next_ref, trade.taker, trade.price, trade.lots);
                tape.send(&order).map_err(|e| e.to_string())?;
                sent[1] = Instant::now();
                retraded = sent[1];
            }
            Ok(Event::Trade(_)) => {}
            Ok(Event::Down) => {
                withdraw(&mut book, &mut mirror)?;
                latest = None;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Err("the feed stopped".to_owned()),
        }
        if latest.is_some() && heard.elapsed() > STALE {
            eprintln!("mirror: nothing from the venue for {} s", STALE.as_secs());
            withdraw(&mut book, &mut mirror)?;
            latest = None;
        }
        if let Some(venue) = &latest {
            if followed.elapsed() >= FOLLOW_EVERY && retraded.elapsed() >= SETTLE {
                let messages = mirror.follow(venue);
                followed = Instant::now();
                if !messages.is_empty() {
                    for message in &messages {
                        book.queue(message);
                    }
                    book.flush().map_err(|e| e.to_string())?;
                    sent[0] = followed;
                }
            }
        }
        let [book_sent, tape_sent] = &mut sent;
        for (client, last) in [(&mut book, book_sent), (&mut tape, tape_sent)] {
            if last.elapsed() >= HEARTBEAT {
                client
                    .send(&Inbound::Heartbeat)
                    .map_err(|e| e.to_string())?;
                *last = Instant::now();
            }
        }
    }
}

/// Cancels every order of the mirror's, and forgets them.
fn withdraw(book: &mut Client, mirror: &mut Mirror) -> Result<(), String> {
    if !mirror.is_empty() {
        book.send(&Inbound::MassCancel).map_err(|e| e.to_string())?;
        mirror.clear();
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
