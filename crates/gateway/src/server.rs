//! The network layer: one thread, one `mio` event loop, every connection non-blocking.
//!
//! Each round of the loop reads whatever the sockets hold and hands the decoded messages to
//! the [`Exchange`], then hands its batch to the [`Core`] that runs the engine, so the
//! commands of one round share a journal sync, delivers the events that have come back, and
//! writes the replies. A connection that sends bytes that do not decode is logged out; one
//! that falls too far behind reading its replies is dropped. Dropping or losing a
//! connection cancels its account's orders, unless it is a browser's.
//!
//! Stopping the server leaves the orders on the book, as a crash would: the next start
//! finds them.
//!
//! A second listener, if the server has one, serves browsers: HTTP for the exchange's page,
//! and sessions over WebSocket that speak JSON ([`web`]). A browser's session
//! is a session like any other.

use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use engine::storage::{FsStorage, Storage};
use engine::{Engine, Seq};
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use protocol::{LogoutReason, Outbound, decode_inbound, encode_outbound};

use crate::exchange::{Exchange, Logged, Mailbox, SessionId};
use crate::recovery;
use crate::web::json::{self, WebIn};
use crate::web::{self, Guests, http, ws};

/// Limits of the network layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerConfig {
    /// Connections served at once; more are closed as they arrive.
    pub max_sessions: usize,
    /// Bytes of replies a connection may leave unread before it is dropped.
    pub max_output: usize,
    /// How long a connection being logged out has to read its last messages.
    pub linger: Duration,
    /// How often heartbeats and idle sessions are checked.
    pub tick: Duration,
}

impl Default for ServerConfig {
    /// 1,024 sessions, 4 MiB of unread replies each, a second to linger, ticks every 10 ms.
    fn default() -> Self {
        ServerConfig {
            max_sessions: 1_024,
            max_output: 4 << 20,
            linger: Duration::from_secs(1),
            tick: Duration::from_millis(10),
        }
    }
}

/// Why the server stopped.
#[derive(Debug)]
pub enum ServerError {
    /// The event loop failed.
    Io(io::Error),
    /// The engine failed, and takes no more commands.
    Engine(engine::Error),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerError::Io(error) => write!(f, "event loop: {error}"),
            ServerError::Engine(error) => write!(f, "engine: {error}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<io::Error> for ServerError {
    fn from(error: io::Error) -> Self {
        ServerError::Io(error)
    }
}

/// What runs the engine behind a server.
pub trait Core {
    /// Takes the exchange's batch, as far as there is room, and delivers the events that
    /// have come back. An error means the engine takes no more commands.
    fn turn(
        &mut self,
        exchange: &mut Exchange,
        mail: &mut impl Mailbox,
    ) -> Result<(), engine::Error>;

    /// Whether events may still come back for commands taken: the server then turns again
    /// without waiting for its sockets.
    fn busy(&mut self) -> bool;

    /// Whether commands wait for room: the server then reads no more from its sockets.
    fn backed_up(&self) -> bool;
}

/// An engine on the server's own thread: each batch is journaled and applied at once.
impl<S: Storage> Core for Engine<S> {
    fn turn(
        &mut self,
        exchange: &mut Exchange,
        mail: &mut impl Mailbox,
    ) -> Result<(), engine::Error> {
        exchange.flush(self, mail)
    }

    fn busy(&mut self) -> bool {
        false
    }

    fn backed_up(&self) -> bool {
        false
    }
}

/// The binary listener's token, and the web listener's; a connection's is its session id
/// plus two.
const LISTENER: Token = Token(0);
const WEB_LISTENER: Token = Token(1);

/// The longest HTTP request a browser may send, headers and all.
const MAX_REQUEST: usize = 8 << 10;

/// The longest WebSocket message a browser may send.
const MAX_WEB_MESSAGE: usize = 4 << 10;

/// How long a browser has to send its HTTP request.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Bytes read from a socket at a time.
const READ_CHUNK: usize = 64 << 10;

/// Chunks read from one connection per round. A client that sends without pause would
/// otherwise keep the loop reading it, and its commands would fill one huge batch.
const READ_BUDGET: usize = 16;

/// How often subscribed browsers are shown the queues and the engine log, in nanoseconds.
const SHOW_EVERY: u64 = 200_000_000;

/// Levels of each side shown order by order.
const QUEUE_LEVELS: usize = 10;

/// Commands of the engine log shown each time at most: the latest.
const LOG_SHOWN: usize = 12;

/// Paper accounts on the leaderboard.
const LEADERS: usize = 10;

/// Seconds of statistics a browser that subscribes is sent: what the engine room's charts
/// show.
const STATS_KEPT: usize = 120;

/// What a connection speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// The binary protocol.
    Binary,
    /// HTTP, until it asks for a file or upgrades; accepted at `since`.
    Http { since: u64 },
    /// JSON over WebSocket; `registered` once it has created an account.
    WebSocket { registered: bool },
}

struct Connection {
    stream: TcpStream,
    kind: Kind,
    /// Bytes received but not yet decoded: at most one partial message.
    input: Vec<u8>,
    /// Encoded replies not yet written.
    output: Vec<u8>,
    /// Whether the socket is registered for writability.
    writing: bool,
    /// The peer is gone, or the socket failed.
    dead: bool,
    /// Replies exceeded `max_output`.
    slow: bool,
    /// The exchange closed the session; set to the time it did once the server notices.
    closing: bool,
    closed_at: Option<u64>,
}

/// The connections, as the exchange's [`Mailbox`].
struct Wires {
    slots: Vec<Option<Connection>>,
    max_output: usize,
}

impl Mailbox for Wires {
    fn send(&mut self, session: SessionId, message: Outbound) {
        let Some(connection) = self.slots.get_mut(session).and_then(Option::as_mut) else {
            return;
        };
        if connection.slow || connection.dead {
            return;
        }
        match connection.kind {
            Kind::Binary => encode_outbound(&message, &mut connection.output),
            Kind::WebSocket { .. } => {
                ws::encode_text(&json::outbound(&message), &mut connection.output);
            }
            Kind::Http { .. } => return,
        }
        if connection.output.len() > self.max_output {
            connection.slow = true;
        }
    }

    fn close(&mut self, session: SessionId) {
        if let Some(connection) = self.slots.get_mut(session).and_then(Option::as_mut) {
            if matches!(connection.kind, Kind::WebSocket { .. }) && !connection.closing {
                ws::encode_close(&mut connection.output);
            }
            connection.closing = true;
        }
    }
}

/// A gateway serving an [`Exchange`] on a TCP port, with a [`Core`] running its engine.
pub struct Server<C: Core> {
    exchange: Exchange,
    core: C,
    config: ServerConfig,
    poll: Poll,
    events: Events,
    listener: TcpListener,
    /// The listener for browsers, and where their accounts come from.
    web: Option<TcpListener>,
    guests: Option<Guests>,
    /// Where the exchange's checkpoints go, how many commands apart, and the last one's
    /// sequence number.
    checkpoints: Option<(PathBuf, u64, Seq)>,
    wires: Wires,
    /// Slots of closed connections, to reuse.
    free: Vec<SessionId>,
    /// Connections that may hold more to read than their last round's budget allowed.
    unread: Vec<SessionId>,
    /// Connections to read this round.
    ready: Vec<SessionId>,
    scratch: Box<[u8]>,
    started: Instant,
    stats: Stats,
    last_tick: u64,
    /// When browsers were last shown the queues and the engine log.
    last_show: u64,
    /// The queues as last shown.
    shown_queues: String,
    /// Scratch space for the engine log.
    logged: Vec<Logged>,
    /// The statistics of the last seconds, oldest first.
    recent_stats: VecDeque<json::Stats>,
}

impl<C: Core> Server<C> {
    /// A server for `exchange` and its `core`, listening on `addr`.
    pub fn bind(
        exchange: Exchange,
        core: C,
        addr: SocketAddr,
        config: ServerConfig,
    ) -> io::Result<Server<C>> {
        let poll = Poll::new()?;
        let mut listener = TcpListener::bind(addr)?;
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)?;
        // The server's clock reads zero now; the candles want wall-clock time.
        let mut exchange = exchange;
        exchange.set_epoch(unix_millis() / 1_000);
        let started = Instant::now();
        Ok(Server {
            exchange,
            core,
            config,
            poll,
            events: Events::with_capacity(1_024),
            listener,
            web: None,
            guests: None,
            checkpoints: None,
            wires: Wires {
                slots: Vec::new(),
                max_output: config.max_output,
            },
            free: Vec::new(),
            unread: Vec::new(),
            ready: Vec::new(),
            scratch: vec![0; READ_CHUNK].into_boxed_slice(),
            started,
            stats: Stats::default(),
            last_tick: 0,
            last_show: 0,
            shown_queues: String::new(),
            logged: Vec::new(),
            recent_stats: VecDeque::with_capacity(STATS_KEPT),
        })
    }

    /// The address the server listens on, with the port the system chose if `addr` asked
    /// for port 0.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Also serves browsers on `addr`: the exchange's page, and sessions over WebSocket.
    /// Visitors may create accounts from `guests`; without it, they log in to accounts that
    /// exist.
    pub fn serve_web(&mut self, addr: SocketAddr, guests: Option<Guests>) -> io::Result<()> {
        let mut listener = TcpListener::bind(addr)?;
        self.poll
            .registry()
            .register(&mut listener, WEB_LISTENER, Interest::READABLE)?;
        self.web = Some(listener);
        self.guests = guests;
        Ok(())
    }

    /// Saves the exchange's checkpoints in `dir` ([`recovery`]), once at least `every`
    /// commands have passed since the last, which was at `since`, and when the server
    /// stops: always at a moment when no command is in flight. A checkpoint that cannot be
    /// saved stops the server, since a restart could no longer rebuild the exchange.
    pub fn checkpoint_to(&mut self, dir: PathBuf, every: u64, since: Seq) {
        self.checkpoints = Some((dir, every.max(1), since));
    }

    /// Saves a checkpoint if one is due, or `now` if one can be taken.
    fn checkpoint(&mut self, now: bool) -> io::Result<()> {
        let Some((dir, every, last)) = &mut self.checkpoints else {
            return Ok(());
        };
        let seq = self.exchange.last_seq();
        let due = if now {
            seq > *last
        } else {
            seq >= *last + *every
        };
        if !due || self.core.busy() {
            return Ok(());
        }
        let Some(checkpoint) = self.exchange.checkpoint() else {
            return Ok(());
        };
        recovery::save(&mut FsStorage, dir, &checkpoint)?;
        *last = checkpoint.seq;
        Ok(())
    }

    /// The address the web listener listens on, if there is one.
    pub fn web_addr(&self) -> Option<io::Result<SocketAddr>> {
        self.web.as_ref().map(TcpListener::local_addr)
    }

    /// The exchange.
    pub fn exchange(&self) -> &Exchange {
        &self.exchange
    }

    /// The core.
    pub fn core(&self) -> &C {
        &self.core
    }

    /// Gives the exchange and the core back, to close the engine.
    pub fn into_parts(self) -> (Exchange, C) {
        (self.exchange, self.core)
    }

    /// Serves until `stop` is set, then logs every session out. An engine failure stops the
    /// server too, after telling every session.
    pub fn run(&mut self, stop: &AtomicBool) -> Result<(), ServerError> {
        while !stop.load(Ordering::Relaxed) {
            if let Err(error) = self.step() {
                self.shut_down();
                return Err(error);
            }
        }
        self.shut_down();
        self.checkpoint(true)?;
        Ok(())
    }

    /// One round of the event loop: waits up to one tick for sockets to become ready, reads
    /// them, applies what they sent, and writes the replies.
    pub fn step(&mut self) -> Result<(), ServerError> {
        // A batch left by dropped connections, input left unread, or events still to come
        // do not wait.
        let busy = self.exchange.has_batch() || !self.unread.is_empty() || self.core.busy();
        let timeout = if busy {
            Duration::ZERO
        } else {
            self.config.tick
        };
        match self.poll.poll(&mut self.events, Some(timeout)) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => result?,
        }
        let now = self.now();
        self.ready.clear();
        self.ready.append(&mut self.unread);
        let (mut accept, mut accept_web) = (false, false);
        for event in &self.events {
            match event.token() {
                LISTENER => accept = true,
                WEB_LISTENER => accept_web = true,
                Token(token) => self.ready.push(token - 2),
            }
        }
        if accept {
            self.accept(now, false);
        }
        if accept_web {
            self.accept(now, true);
        }
        // A connection both left unread and ready again is read once.
        self.ready.sort_unstable();
        self.ready.dedup();
        for index in 0..self.ready.len() {
            let session = self.ready[index];
            // While the engine is backed up, nothing more is read: the clients wait.
            if self.core.backed_up() || self.read(session, now) {
                self.unread.push(session);
            }
        }
        let handing = self.exchange.has_batch();
        let turned = Instant::now();
        if let Err(error) = self.core.turn(&mut self.exchange, &mut self.wires) {
            self.exchange.shut_down(&mut self.wires);
            return Err(ServerError::Engine(error));
        }
        if handing {
            let took = u64::try_from(turned.elapsed().as_nanos()).unwrap_or(u64::MAX);
            self.stats.turns.push(took);
        }
        if now.saturating_sub(self.stats.since) >= 1_000_000_000 {
            self.publish_stats(now);
        }
        self.exchange.publish(&mut self.wires);
        if now.saturating_sub(self.last_show) >= SHOW_EVERY {
            self.last_show = now;
            self.show();
        }
        if now.saturating_sub(self.last_tick) >= self.config.tick.as_nanos() as u64 {
            self.last_tick = now;
            self.exchange.tick(now, &mut self.wires);
        }
        self.write(now);
        self.checkpoint(false)?;
        Ok(())
    }

    fn now(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn accept(&mut self, now: u64, web: bool) {
        loop {
            let listener = match (web, &self.web) {
                (false, _) => &self.listener,
                (true, Some(listener)) => listener,
                (true, None) => return,
            };
            let mut stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                // Would block, or a connection that failed before it was accepted, or out
                // of file descriptors: try again on the next readiness.
                Err(_) => return,
            };
            let session = match self.free.pop() {
                Some(session) => session,
                None if self.wires.slots.len() < self.config.max_sessions => {
                    self.wires.slots.push(None);
                    self.wires.slots.len() - 1
                }
                None => continue,
            };
            // Replies are small and latency matters: no Nagle delay.
            let registered = stream.set_nodelay(true).and_then(|()| {
                self.poll
                    .registry()
                    .register(&mut stream, Token(session + 2), Interest::READABLE)
            });
            if registered.is_err() {
                self.free.push(session);
                continue;
            }
            // A browser's session starts once its connection is upgraded to a WebSocket.
            let kind = if web {
                Kind::Http { since: now }
            } else {
                self.exchange.connect(session, now);
                Kind::Binary
            };
            self.wires.slots[session] = Some(Connection {
                stream,
                kind,
                input: Vec::new(),
                output: Vec::new(),
                writing: false,
                dead: false,
                slow: false,
                closing: false,
                closed_at: None,
            });
        }
    }

    /// Reads what `session`'s socket holds, a chunk at a time, and hands each complete
    /// message to the exchange. Returns whether the budget ran out before the socket did.
    fn read(&mut self, session: SessionId, now: u64) -> bool {
        let mut budget = READ_BUDGET;
        loop {
            let Some(connection) = self.wires.slots.get_mut(session).and_then(Option::as_mut)
            else {
                return false;
            };
            if connection.dead {
                return false;
            }
            if budget == 0 {
                return true;
            }
            budget -= 1;
            let read = match connection.stream.read(&mut self.scratch) {
                Ok(0) => {
                    connection.dead = true;
                    return false;
                }
                Ok(read) => read,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return false,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    connection.dead = true;
                    return false;
                }
            };
            if connection.closing {
                // A session being logged out is not listened to; reading on notices when
                // the peer is gone.
                continue;
            }
            let mut input = std::mem::take(&mut connection.input);
            input.extend_from_slice(&self.scratch[..read]);
            let used = self.consume(session, &input, now);
            input.drain(..used);
            if let Some(connection) = self.wires.slots[session].as_mut() {
                connection.input = input;
            }
        }
    }

    /// Handles the complete messages at the start of `input`, in whatever the connection
    /// speaks, and returns how many bytes they took. Bytes that cannot be read end the
    /// session, and count as taken.
    fn consume(&mut self, session: SessionId, input: &[u8], now: u64) -> usize {
        let mut at = 0;
        while at < input.len() {
            let Some(connection) = self.wires.slots[session].as_mut() else {
                return input.len();
            };
            if connection.closing {
                return input.len();
            }
            let rest = &input[at..];
            match connection.kind {
                Kind::Binary => match decode_inbound(rest) {
                    Ok(Some((message, len))) => {
                        at += len;
                        self.exchange
                            .receive(session, message, now, &mut self.wires);
                    }
                    Ok(None) => return at,
                    Err(_) => {
                        let reason = LogoutReason::ProtocolError;
                        self.exchange.end(session, reason, &mut self.wires);
                        return input.len();
                    }
                },
                Kind::Http { .. } => match http::parse(rest, MAX_REQUEST) {
                    Ok(None) => return at,
                    Ok(Some((http::Request::Upgrade { key }, len))) => {
                        at += len;
                        connection
                            .output
                            .extend_from_slice(&http::upgrade_response(&key));
                        connection.kind = Kind::WebSocket { registered: false };
                        self.exchange.connect(session, now);
                    }
                    Ok(Some((http::Request::Get { path }, _))) => {
                        let response = match web::page(&path) {
                            Some((content_type, body)) => {
                                http::response("200 OK", content_type, body)
                            }
                            None => http::response("404 Not Found", "text/plain", b"not found"),
                        };
                        connection.output.extend_from_slice(&response);
                        connection.closing = true;
                        return input.len();
                    }
                    Err(error) => {
                        let body = error.to_string();
                        let response =
                            http::response("400 Bad Request", "text/plain", body.as_bytes());
                        connection.output.extend_from_slice(&response);
                        connection.closing = true;
                        return input.len();
                    }
                },
                Kind::WebSocket { .. } => match ws::decode(rest, MAX_WEB_MESSAGE) {
                    Ok(None) => return at,
                    Ok(Some((frame, len))) => {
                        at += len;
                        match frame {
                            ws::Frame::Text(text) => self.web_message(session, &text, now),
                            ws::Frame::Ping(payload) => {
                                ws::encode_pong(&payload, &mut connection.output);
                            }
                            ws::Frame::Pong => {}
                            // The browser is leaving: what it had is cancelled as it goes.
                            ws::Frame::Close => {
                                connection.dead = true;
                                return input.len();
                            }
                        }
                    }
                    Err(_) => {
                        let reason = LogoutReason::ProtocolError;
                        self.exchange.end(session, reason, &mut self.wires);
                        return input.len();
                    }
                },
            }
        }
        at
    }

    /// A browser's JSON message.
    fn web_message(&mut self, session: SessionId, text: &str, now: u64) {
        let message = match json::parse(text) {
            Ok(WebIn::Register) => return self.register(session),
            Ok(message) => message,
            Err(error) => {
                self.send_web(session, &json::error(&error.to_string()));
                let reason = LogoutReason::ProtocolError;
                return self.exchange.end(session, reason, &mut self.wires);
            }
        };
        match message.inbound() {
            Some(inbound) => {
                self.exchange
                    .receive(session, inbound, now, &mut self.wires);
                // A browser's chart starts from the last hour, and its queues from now.
                if message == WebIn::Subscribe && self.exchange.is_subscribed(session) {
                    let history =
                        json::history(crate::candles::INTERVAL, self.exchange.candles().iter());
                    self.send_web(session, &history);
                    let stats = json::stats_history(self.recent_stats.iter());
                    self.send_web(session, &stats);
                    let queues = json::queues(self.exchange.depth(), QUEUE_LEVELS);
                    self.send_web(session, &queues);
                }
            }
            None => {
                self.send_web(session, &json::error("a token is 16 hexadecimal digits"));
                let reason = LogoutReason::ProtocolError;
                self.exchange.end(session, reason, &mut self.wires);
            }
        }
    }

    /// Tells the browsers how the exchange is doing: commands per second over the last
    /// period, how long the turns that handed commands to the engine took, and how many
    /// sessions and orders there are.
    fn publish_stats(&mut self, now: u64) {
        let seconds = now.saturating_sub(self.stats.since) as f64 / 1e9;
        let commands = self.exchange.last_seq() - self.stats.seq;
        let mut turns = std::mem::take(&mut self.stats.turns);
        turns.sort_unstable();
        let at = |q: f64| {
            turns
                .get(((turns.len() as f64 - 1.0) * q).round() as usize)
                .copied()
        };
        let sessions = self
            .wires
            .slots
            .iter()
            .flatten()
            .filter(|c| !matches!(c.kind, Kind::Http { .. }))
            .count();
        let mut turn_buckets = [0; json::TURN_BUCKETS];
        for &turn in &turns {
            turn_buckets[json::turn_bucket(turn)] += 1;
        }
        let stats = json::Stats {
            turn_buckets,
            commands_per_second: (commands as f64 / seconds.max(1e-9)).round() as u64,
            turn_p50_ns: at(0.5).unwrap_or(0),
            turn_p99_ns: at(0.99).unwrap_or(0),
            turn_max_ns: turns.last().copied().unwrap_or(0),
            sessions: sessions as u64,
            orders: self.exchange.depth().order_count() as u64,
            time: unix_millis(),
        };
        if self.recent_stats.len() == STATS_KEPT {
            self.recent_stats.pop_front();
        }
        self.recent_stats.push_back(stats);
        let text = json::stats(&stats);
        for connection in self.wires.slots.iter_mut().flatten() {
            if matches!(connection.kind, Kind::WebSocket { .. }) && !connection.closing {
                ws::encode_text(&text, &mut connection.output);
            }
        }
        turns.clear();
        self.stats = Stats {
            since: now,
            seq: self.exchange.last_seq(),
            turns,
        };
        self.publish_leaders();
    }

    /// Shows subscribed browsers the most profitable paper accounts, and each logged-in
    /// browser whose account traded its own place.
    fn publish_leaders(&mut self) {
        let standings = self.exchange.standings();
        if standings.is_empty() {
            return;
        }
        let shown = standings.len().min(LEADERS);
        let text = json::leaders(&standings[..shown], standings.len());
        self.send_watchers(&text);
        for session in 0..self.wires.slots.len() {
            let Some(account) = self.exchange.account_of(session) else {
                continue;
            };
            if !self.watches(session) {
                continue;
            }
            if let Some(at) = standings.iter().position(|s| s.account == account) {
                self.send_web(session, &json::rank(at + 1, standings.len()));
            }
        }
    }

    /// Shows subscribed browsers the queues, if they changed, and the commands the engine
    /// has sequenced since the last time, the latest of them.
    fn show(&mut self) {
        let idle = !self.exchange.has_batch() && !self.core.busy();
        self.logged.clear();
        self.exchange.take_log(idle, &mut self.logged);
        if !(0..self.wires.slots.len()).any(|session| self.watches(session)) {
            return;
        }
        let queues = json::queues(self.exchange.depth(), QUEUE_LEVELS);
        if queues != self.shown_queues {
            self.send_watchers(&queues);
            self.shown_queues = queues;
        }
        if !self.logged.is_empty() {
            let skipped = self.logged.len().saturating_sub(LOG_SHOWN);
            let text = json::log(skipped, &self.logged[skipped..]);
            self.send_watchers(&text);
        }
    }

    /// Whether `session` is a browser that subscribed to the market data.
    fn watches(&self, session: SessionId) -> bool {
        self.wires.slots[session]
            .as_ref()
            .is_some_and(|c| matches!(c.kind, Kind::WebSocket { .. }) && !c.closing)
            && self.exchange.is_subscribed(session)
    }

    /// Sends `text` to every browser that subscribed to the market data.
    fn send_watchers(&mut self, text: &str) {
        for session in 0..self.wires.slots.len() {
            if self.watches(session) {
                self.send_web(session, text);
            }
        }
    }

    /// Creates an account for the browser on `session`: one per connection.
    fn register(&mut self, session: SessionId) {
        let registered = self.wires.slots[session]
            .as_ref()
            .is_some_and(|c| c.kind == Kind::WebSocket { registered: true });
        let reply = match (&mut self.guests, registered) {
            (_, true) => json::error("one account per connection"),
            (None, _) => json::error("registration is closed"),
            (Some(guests), false) => match guests.create(&mut self.exchange) {
                Ok(Some(account)) => {
                    if let Some(connection) = self.wires.slots[session].as_mut() {
                        connection.kind = Kind::WebSocket { registered: true };
                    }
                    json::registered(account.id, account.token)
                }
                Ok(None) => json::error("no accounts are left"),
                Err(_) => json::error("the account could not be saved"),
            },
        };
        self.send_web(session, &reply);
    }

    /// Sends a browser a message the exchange does not know of.
    fn send_web(&mut self, session: SessionId, text: &str) {
        if let Some(connection) = self.wires.slots[session].as_mut() {
            ws::encode_text(text, &mut connection.output);
        }
    }

    /// Writes the replies, and drops the connections that are done.
    fn write(&mut self, now: u64) {
        let linger = self.config.linger.as_nanos() as u64;
        let http_timeout = HTTP_TIMEOUT.as_nanos() as u64;
        for session in 0..self.wires.slots.len() {
            let Some(connection) = self.wires.slots[session].as_mut() else {
                continue;
            };
            // A browser that does not finish its request in time is dropped.
            if let Kind::Http { since } = connection.kind {
                if !connection.closing && now.saturating_sub(since) >= http_timeout {
                    connection.dead = true;
                }
            }
            if !connection.dead && !connection.slow && flush(connection).is_err() {
                connection.dead = true;
            }
            if connection.closing && connection.closed_at.is_none() {
                connection.closed_at = Some(now);
            }
            let done = connection.dead
                || connection.slow
                || connection.closing
                    && (connection.output.is_empty()
                        || connection
                            .closed_at
                            .is_some_and(|at| now.saturating_sub(at) >= linger));
            if done {
                self.drop_connection(session);
                continue;
            }
            let writing = !connection.output.is_empty();
            if writing != connection.writing {
                let interest = if writing {
                    Interest::READABLE | Interest::WRITABLE
                } else {
                    Interest::READABLE
                };
                let token = Token(session + 2);
                if self
                    .poll
                    .registry()
                    .reregister(&mut connection.stream, token, interest)
                    .is_ok()
                {
                    connection.writing = writing;
                } else {
                    self.drop_connection(session);
                }
            }
        }
    }

    fn drop_connection(&mut self, session: SessionId) {
        if let Some(mut connection) = self.wires.slots[session].take() {
            let _ = self.poll.registry().deregister(&mut connection.stream);
            // A program that loses its connection has its orders cancelled; a browser's
            // stay, since it may well come back, and is told them when it logs in.
            if matches!(connection.kind, Kind::WebSocket { .. }) {
                self.exchange.detach(session);
            } else {
                self.exchange.disconnect(session);
            }
            self.free.push(session);
        }
    }

    /// Logs every session out, gives them the linger time to read it, and closes them.
    /// Their orders stay on the book.
    fn shut_down(&mut self) {
        for session in 0..self.wires.slots.len() {
            self.exchange
                .end(session, LogoutReason::Shutdown, &mut self.wires);
        }
        let deadline = Instant::now() + self.config.linger;
        loop {
            let late = Instant::now() >= deadline;
            for session in 0..self.wires.slots.len() {
                let Some(connection) = self.wires.slots[session].as_mut() else {
                    continue;
                };
                let failed = connection.dead || flush(connection).is_err();
                if failed || connection.output.is_empty() || late {
                    let mut connection = self.wires.slots[session].take().expect("a connection");
                    let _ = self.poll.registry().deregister(&mut connection.stream);
                    self.exchange.detach(session);
                    self.free.push(session);
                }
            }
            if self.wires.slots.iter().all(Option::is_none) {
                break;
            }
            let _ = self.poll.poll(&mut self.events, Some(self.config.tick));
        }
        self.unread.clear();
    }
}

/// The wall clock, in milliseconds since the Unix epoch.
fn unix_millis() -> u64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
}

/// What the server measures for its statistics, since `since`.
#[derive(Debug, Default)]
struct Stats {
    since: u64,
    /// The last sequence number at `since`.
    seq: u64,
    /// How long each turn that handed commands to the engine took, in nanoseconds.
    turns: Vec<u64>,
}

/// Writes as much of the connection's output as the socket takes.
fn flush(connection: &mut Connection) -> io::Result<()> {
    let mut written = 0;
    while written < connection.output.len() {
        match connection.stream.write(&connection.output[written..]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => written += n,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    connection.output.drain(..written);
    Ok(())
}
