//! The connection to the venue: a thread that reads its WebSocket and passes on its book
//! and its trades, and connects again, waiting longer each time, when the connection drops,
//! goes quiet, or the venue asks.
//!
//! Whatever the venue, or something posing as it, sends, the feed holds little: connecting
//! and every write give up after ten seconds, a message may be a mebibyte at most, and the
//! events waiting for the mirror are a few hundred at most, the newest dropped if the
//! mirror falls that far behind.

use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::thread;
use std::time::{Duration, Instant};

use tungstenite::protocol::WebSocketConfig;
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

/// How long connecting, each handshake and each write may take.
const TIMEOUT: Duration = Duration::from_secs(10);

/// The longest message the venue may send: its book of a hundred orders a side takes a few
/// tens of kilobytes.
const MAX_MESSAGE: usize = 1 << 20;

/// Events waiting for the mirror at most.
const QUEUE: usize = 256;

/// Starts the feed. It runs until the receiver is dropped.
pub fn spawn(config: Config) -> Receiver<Event> {
    let (tx, rx) = mpsc::sync_channel(QUEUE);
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

/// Connects to `url`, giving up after [`TIMEOUT`] at each step.
fn connect(url: &str) -> Result<WebSocket<MaybeTlsStream<TcpStream>>, String> {
    let uri: tungstenite::http::Uri = url.parse().map_err(|e| format!("{url}: {e}"))?;
    let host = uri.host().ok_or_else(|| format!("{url}: no host"))?;
    let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("ws") {
        80
    } else {
        443
    });
    let mut failed = format!("{host}: no address");
    for addr in (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("{host}: {e}"))?
    {
        let tcp = match TcpStream::connect_timeout(&addr, TIMEOUT) {
            Ok(tcp) => tcp,
            Err(error) => {
                failed = format!("{addr}: {error}");
                continue;
            }
        };
        tcp.set_read_timeout(Some(TIMEOUT))
            .and_then(|()| tcp.set_write_timeout(Some(TIMEOUT)))
            .map_err(|e| e.to_string())?;
        let limits = WebSocketConfig::default()
            .max_message_size(Some(MAX_MESSAGE))
            .max_frame_size(Some(MAX_MESSAGE));
        let (socket, _) = tungstenite::client_tls_with_config(url, tcp, Some(limits), None)
            .map_err(|e| e.to_string())?;
        return Ok(socket);
    }
    Err(failed)
}

/// The start of `text`, for a log line: its first 200 characters at most.
fn excerpt(text: &str) -> &str {
    text.char_indices()
        .nth(200)
        .map_or(text, |(end, _)| &text[..end])
}

/// One connection, until it ends.
fn session(config: &Config, tx: &SyncSender<Event>) -> Result<(), String> {
    let mut socket = connect(&config.url)?;
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
                        eprintln!("mirror: cannot read {}", excerpt(&text));
                        continue;
                    }
                };
                // A mirror that far behind catches up with the books that follow.
                match tx.try_send(event) {
                    Ok(()) | Err(TrySendError::Full(_)) => {}
                    Err(TrySendError::Disconnected(_)) => return Ok(()),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A log line takes the start of a message the feed cannot read, cut between
    /// characters, never inside one.
    #[test]
    fn an_excerpt_is_cut_between_characters() {
        let text = format!("{}é{}", "a".repeat(199), "b".repeat(50));
        assert_eq!(excerpt(&text), format!("{}é", "a".repeat(199)));
        assert_eq!(excerpt("short"), "short");
        let wide = "€".repeat(300);
        assert_eq!(excerpt(&wide).chars().count(), 200);
    }
}
