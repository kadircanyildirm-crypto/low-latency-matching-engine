//! The connection to the venue: a thread that reads its WebSocket and passes on its book
//! and its trades, and connects again, waiting longer each time, when the connection drops,
//! goes quiet, or the venue asks.

use std::io;
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::{Duration, Instant};

use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message as Frame, WebSocket};

use crate::bitstamp::{self, Book, Message, Trade};

/// What the feed passes on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The venue's best orders, now.
    Book(Book),
    /// A trade on the venue.
    Trade(Trade),
    /// The connection dropped: what was last passed on of the book may be stale.
    Down,
}

/// Where the feed connects, and what it follows.
#[derive(Clone, Debug)]
pub struct Config {
    /// The WebSocket's address, such as `wss://ws.bitstamp.net`.
    pub url: String,
    /// The pair, such as `ethusd`.
    pub pair: String,
    /// The decimals of the exchange's prices.
    pub price_decimals: u32,
    /// The decimals of the exchange's quantities.
    pub lot_decimals: u32,
}

/// How long the feed waits for a message before it calls the connection dead.
const QUIET: Duration = Duration::from_secs(30);

/// How often it tells the venue it is still there.
const HEARTBEAT: Duration = Duration::from_secs(10);

/// Starts the feed. It runs until the receiver is dropped.
pub fn spawn(config: Config) -> Receiver<Event> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut wait = Duration::from_secs(1);
        loop {
            let started = Instant::now();
            match session(&config, &tx) {
                Ok(()) => eprintln!("mirror: the venue closed the connection"),
                Err(error) => eprintln!("mirror: the venue's connection failed: {error}"),
            }
            if tx.send(Event::Down).is_err() {
                return;
            }
            // A connection that lasted starts the waits over.
            if started.elapsed() > Duration::from_secs(60) {
                wait = Duration::from_secs(1);
            }
            thread::sleep(wait);
            wait = (wait * 2).min(Duration::from_secs(30));
        }
    });
    rx
}

/// One connection, until it ends.
fn session(config: &Config, tx: &Sender<Event>) -> Result<(), String> {
    let (mut socket, _) = tungstenite::connect(config.url.as_str()).map_err(|e| e.to_string())?;
    // Reads give up now and then, so that heartbeats go out while the market is quiet.
    stream(&mut socket)
        .set_read_timeout(Some(Duration::from_secs(1)))
        .map_err(|e| e.to_string())?;
    for channel in bitstamp::channels(&config.pair) {
        socket
            .send(Frame::text(bitstamp::subscribe(&channel)))
            .map_err(|e| e.to_string())?;
    }
    eprintln!("mirror: following {} on {}", config.pair, config.url);
    let mut heard = Instant::now();
    let mut beat = Instant::now();
    loop {
        match socket.read() {
            Ok(Frame::Text(text)) => {
                heard = Instant::now();
                let parsed = bitstamp::parse(&text, config.price_decimals, config.lot_decimals);
                let event = match parsed {
                    Some(Message::Book(book)) => Event::Book(book),
                    Some(Message::Trade(trade)) => Event::Trade(trade),
                    Some(Message::Reconnect) => return Ok(()),
                    Some(Message::Other) => continue,
                    None => {
                        eprintln!("mirror: cannot read {}", &text[..text.len().min(200)]);
                        continue;
                    }
                };
                if tx.send(event).is_err() {
                    return Ok(());
                }
            }
            Ok(Frame::Close(_)) => return Ok(()),
            Ok(_) => heard = Instant::now(),
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(error) => return Err(error.to_string()),
        }
        if heard.elapsed() > QUIET {
            return Err(format!("nothing for {} s", QUIET.as_secs()));
        }
        if beat.elapsed() > HEARTBEAT {
            socket
                .send(Frame::text(bitstamp::heartbeat()))
                .map_err(|e| e.to_string())?;
            beat = Instant::now();
        }
    }
}

/// The TCP stream under the WebSocket, with or without TLS.
fn stream(socket: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> &mut TcpStream {
    match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => stream,
        MaybeTlsStream::Rustls(stream) => stream.get_mut(),
        _ => unreachable!("only rustls is built in"),
    }
}
