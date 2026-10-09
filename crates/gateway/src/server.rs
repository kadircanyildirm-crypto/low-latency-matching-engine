//! The network layer: one thread, one `mio` event loop, every connection non-blocking.
//!
//! Each round of the loop reads whatever the sockets hold and hands the decoded messages to
//! the [`Exchange`], then hands its batch to the [`Core`] that runs the engine, so the
//! commands of one round share a journal sync, delivers the events that have come back, and
//! writes the replies. A connection that sends bytes that do not decode is logged out; one
//! that falls too far behind reading its replies is dropped. Dropping or losing a
//! connection cancels its account's orders.
//!
//! Stopping the server leaves the orders on the book, as a crash would: the next start
//! finds them.

use std::fmt;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use engine::Engine;
use engine::storage::Storage;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use protocol::{LogoutReason, Outbound, decode_inbound, encode_outbound};

use crate::exchange::{Exchange, Mailbox, SessionId};

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

/// The listener's token; a connection's is its session id plus one.
const LISTENER: Token = Token(0);

/// Bytes read from a socket at a time.
const READ_CHUNK: usize = 64 << 10;

/// Chunks read from one connection per round. A client that sends without pause would
/// otherwise keep the loop reading it, and its commands would fill one huge batch.
const READ_BUDGET: usize = 16;

struct Connection {
    stream: TcpStream,
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
        encode_outbound(&message, &mut connection.output);
        if connection.output.len() > self.max_output {
            connection.slow = true;
        }
    }

    fn close(&mut self, session: SessionId) {
        if let Some(connection) = self.slots.get_mut(session).and_then(Option::as_mut) {
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
    wires: Wires,
    /// Slots of closed connections, to reuse.
    free: Vec<SessionId>,
    /// Connections that may hold more to read than their last round's budget allowed.
    unread: Vec<SessionId>,
    /// Connections to read this round.
    ready: Vec<SessionId>,
    scratch: Box<[u8]>,
    started: Instant,
    last_tick: u64,
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
        Ok(Server {
            exchange,
            core,
            config,
            poll,
            events: Events::with_capacity(1_024),
            listener,
            wires: Wires {
                slots: Vec::new(),
                max_output: config.max_output,
            },
            free: Vec::new(),
            unread: Vec::new(),
            ready: Vec::new(),
            scratch: vec![0; READ_CHUNK].into_boxed_slice(),
            started: Instant::now(),
            last_tick: 0,
        })
    }

    /// The address the server listens on, with the port the system chose if `addr` asked
    /// for port 0.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
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
        let mut accept = false;
        for event in &self.events {
            match event.token() {
                LISTENER => accept = true,
                Token(token) => self.ready.push(token - 1),
            }
        }
        if accept {
            self.accept(now);
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
        if let Err(error) = self.core.turn(&mut self.exchange, &mut self.wires) {
            self.exchange.shut_down(&mut self.wires);
            return Err(ServerError::Engine(error));
        }
        self.exchange.publish(&mut self.wires);
        if now.saturating_sub(self.last_tick) >= self.config.tick.as_nanos() as u64 {
            self.last_tick = now;
            self.exchange.tick(now, &mut self.wires);
        }
        self.write(now);
        Ok(())
    }

    fn now(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    fn accept(&mut self, now: u64) {
        loop {
            let mut stream = match self.listener.accept() {
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
                    .register(&mut stream, Token(session + 1), Interest::READABLE)
            });
            if registered.is_err() {
                self.free.push(session);
                continue;
            }
            self.wires.slots[session] = Some(Connection {
                stream,
                input: Vec::new(),
                output: Vec::new(),
                writing: false,
                dead: false,
                slow: false,
                closing: false,
                closed_at: None,
            });
            self.exchange.connect(session, now);
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
            let mut at = 0;
            loop {
                match decode_inbound(&input[at..]) {
                    Ok(Some((message, len))) => {
                        at += len;
                        self.exchange
                            .receive(session, message, now, &mut self.wires);
                    }
                    Ok(None) => break,
                    Err(_) => {
                        let reason = LogoutReason::ProtocolError;
                        self.exchange.end(session, reason, &mut self.wires);
                        at = input.len();
                        break;
                    }
                }
            }
            input.drain(..at);
            if let Some(connection) = self.wires.slots[session].as_mut() {
                connection.input = input;
            }
        }
    }

    /// Writes the replies, and drops the connections that are done.
    fn write(&mut self, now: u64) {
        let linger = self.config.linger.as_nanos() as u64;
        for session in 0..self.wires.slots.len() {
            let Some(connection) = self.wires.slots[session].as_mut() else {
                continue;
            };
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
                let token = Token(session + 1);
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
            self.exchange.disconnect(session);
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
