//! The gateway's logic, without sockets: sessions, pre-trade risk, order ids, and the
//! routing of the book's events to the sessions they concern.
//!
//! The network layer feeds it decoded messages with the current time and delivers what it
//! sends; nothing here blocks or reads a clock, so every rule can be tested, and fuzzed,
//! deterministically.
//!
//! Order-entry messages are not applied one by one: they collect in a batch, which
//! [`Exchange::flush`] journals and applies at once. Under `SyncPolicy::Always` the whole
//! batch then shares one sync, which is group commit: the network layer flushes once per
//! round of reading its sockets, so the more clients send at once, the more commands each
//! sync carries. Reports go out only after the batch is journaled.

use std::collections::HashMap;
use std::fmt;

use engine::storage::Storage;
use engine::{Engine, Error, Output, Seq};
use orderbook::{Command, Event, OrderBook, OrderId, Side};
use protocol::{
    Inbound, LoginError, LogoutReason, NewOrder, OrderKind, Outbound, RejectCode, Report,
    ReportKind, VERSION,
};

use crate::accounts::Account;

/// A connection's handle, chosen by the network layer.
pub type SessionId = usize;

/// Where the exchange's messages go: the network layer.
pub trait Mailbox {
    /// Sends `message` to `session`.
    fn send(&mut self, session: SessionId, message: Outbound);
    /// Closes `session` once what was sent to it is written. The network layer then calls
    /// [`Exchange::disconnect`].
    fn close(&mut self, session: SessionId);
}

/// Timing of sessions, in nanoseconds of whatever clock the caller passes in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timing {
    /// A logged-in session that has been sent nothing for this long is sent a heartbeat.
    pub heartbeat: u64,
    /// A session that has sent nothing for this long is logged out.
    pub idle_timeout: u64,
}

impl Default for Timing {
    /// Heartbeats after one second of silence, logout after five.
    fn default() -> Self {
        Timing {
            heartbeat: 1_000_000_000,
            idle_timeout: 5_000_000_000,
        }
    }
}

/// Why an exchange cannot be built around an engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SetupError {
    /// An account's id is not below the book's `max_owners`.
    AccountOutOfRange(u32),
    /// The recovered book holds an order whose id is above the journal's last sequence
    /// number: it was not placed through a gateway, and a new order could get its id.
    ForeignOrder(OrderId),
}

impl fmt::Display for SetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SetupError::AccountOutOfRange(id) => {
                write!(f, "account {id} is not below the book's max_owners")
            }
            SetupError::ForeignOrder(id) => write!(
                f,
                "the book holds order {id}, above the journal's last sequence number"
            ),
        }
    }
}

impl std::error::Error for SetupError {}

/// One message in a token bucket.
const TOKEN: u128 = 1_000_000_000;

/// One connection.
#[derive(Debug)]
struct Session {
    /// The logged-in account.
    account: Option<u32>,
    last_received: u64,
    last_sent: u64,
    /// The token bucket, in billionths of a message.
    tokens: u128,
    refilled: u64,
    /// Set once the session is being closed: nothing more is taken from or sent to it.
    closing: bool,
}

impl Session {
    /// Whether reports may go to the session.
    fn open(&self) -> bool {
        self.account.is_some() && !self.closing
    }
}

/// An account's limits and state.
#[derive(Debug)]
struct AccountState {
    account: Account,
    /// The session logged in for it.
    session: Option<SessionId>,
    /// Its orders and pending stops, counting those waiting in the batch.
    open: u32,
}

/// A live order: whose it is.
#[derive(Clone, Copy, Debug)]
struct Live {
    account: u32,
    client_ref: u64,
}

/// Marks commands the exchange issues itself, such as the mass cancel on a disconnect.
const SYSTEM: SessionId = SessionId::MAX;

/// The gateway's logic around an engine.
pub struct Exchange<S: Storage> {
    engine: Engine<S>,
    timing: Timing,
    accounts: Vec<Option<AccountState>>,
    sessions: Vec<Option<Session>>,
    /// Every order and pending stop on the book, and those in the batch.
    live: HashMap<OrderId, Live>,
    /// Commands waiting to be journaled, and the session each came from.
    batch: Vec<Command>,
    from: Vec<SessionId>,
    /// The time passed with the latest call.
    now: u64,
}

impl<S: Storage> Exchange<S> {
    /// An exchange around `engine`, for `accounts`. The orders on the recovered book are
    /// attributed to their accounts, without the client references they were placed with.
    pub fn new(
        engine: Engine<S>,
        accounts: &[Account],
        timing: Timing,
    ) -> Result<Exchange<S>, SetupError> {
        let max_owners = engine.config().book.max_owners;
        let mut states: Vec<Option<AccountState>> = (0..max_owners).map(|_| None).collect();
        for account in accounts {
            let state = states
                .get_mut(account.id as usize)
                .ok_or(SetupError::AccountOutOfRange(account.id))?;
            *state = Some(AccountState {
                account: *account,
                session: None,
                open: 0,
            });
        }
        let book = engine.book();
        let mut live = HashMap::new();
        for side in [Side::Buy, Side::Sell] {
            for level in book.depth(side) {
                for order in book.queue(side, level.price) {
                    live.insert(order.id, order.owner);
                }
            }
            for stop in book.stops(side) {
                live.insert(stop.id, stop.owner);
            }
        }
        let mut exchange = Exchange {
            engine,
            timing,
            accounts: states,
            sessions: Vec::new(),
            live: HashMap::with_capacity(live.len()),
            batch: Vec::new(),
            from: Vec::new(),
            now: 0,
        };
        for (id, account) in live {
            if id > exchange.engine.last_seq() {
                return Err(SetupError::ForeignOrder(id));
            }
            let client_ref = 0;
            exchange.live.insert(
                id,
                Live {
                    account,
                    client_ref,
                },
            );
            if let Some(state) = exchange.accounts[account as usize].as_mut() {
                state.open += 1;
            }
        }
        Ok(exchange)
    }

    /// The book.
    pub fn book(&self) -> &OrderBook {
        self.engine.book()
    }

    /// The engine.
    pub fn engine(&self) -> &Engine<S> {
        &self.engine
    }

    /// Gives the engine back, to close it.
    pub fn into_engine(self) -> Engine<S> {
        self.engine
    }

    /// The orders and pending stops `account` has, counting those waiting in the batch.
    pub fn open_orders(&self, account: u32) -> Option<u32> {
        let state = self.accounts.get(account as usize)?.as_ref()?;
        Some(state.open)
    }

    /// Whether commands are waiting for [`flush`](Self::flush).
    pub fn has_batch(&self) -> bool {
        !self.batch.is_empty()
    }

    /// A new connection. `session` must not be in use.
    pub fn connect(&mut self, session: SessionId, now: u64) {
        self.now = now;
        if self.sessions.len() <= session {
            self.sessions.resize_with(session + 1, || None);
        }
        debug_assert!(
            self.sessions[session].is_none(),
            "session {session} is in use"
        );
        self.sessions[session] = Some(Session {
            account: None,
            last_received: now,
            last_sent: now,
            tokens: 0,
            refilled: now,
            closing: false,
        });
    }

    /// A message from `session`. Order-entry messages wait in the batch until
    /// [`flush`](Self::flush).
    pub fn receive(
        &mut self,
        session: SessionId,
        message: Inbound,
        now: u64,
        mail: &mut impl Mailbox,
    ) {
        self.now = now;
        let Some(state) = self.sessions.get_mut(session).and_then(Option::as_mut) else {
            return;
        };
        if state.closing {
            return;
        }
        state.last_received = now;
        let Some(account) = state.account else {
            return self.login(session, message, mail);
        };
        match message {
            Inbound::Login { .. } => {
                let reason = LoginError::AlreadyInSession;
                self.send(session, Outbound::LoginRejected { reason }, mail);
            }
            Inbound::Logout => self.end(session, LogoutReason::Requested, mail),
            Inbound::Heartbeat => {}
            entry => {
                let client_ref = match entry {
                    Inbound::NewOrder(order) => order.client_ref,
                    _ => 0,
                };
                if !self.take_token(session, account) {
                    let reason = RejectCode::Throttled;
                    return self.send(session, Outbound::Reject { reason, client_ref }, mail);
                }
                self.enter(session, account, entry, mail);
            }
        }
    }

    /// The first message of a session, which must be a login.
    fn login(&mut self, session: SessionId, message: Inbound, mail: &mut impl Mailbox) {
        let Inbound::Login {
            version,
            account,
            token,
        } = message
        else {
            return self.end(session, LogoutReason::ProtocolError, mail);
        };
        let state = self
            .accounts
            .get_mut(account as usize)
            .and_then(Option::as_mut);
        let refusal = match state {
            _ if version != VERSION => Some(LoginError::UnsupportedVersion),
            None => Some(LoginError::BadCredentials),
            Some(state) if state.account.token != token => Some(LoginError::BadCredentials),
            Some(state) if state.session.is_some() => Some(LoginError::AlreadyLoggedIn),
            Some(state) => {
                state.session = Some(session);
                None
            }
        };
        if let Some(reason) = refusal {
            self.send(session, Outbound::LoginRejected { reason }, mail);
            return self.close(session, mail);
        }
        let rate = self.limits(account).messages_per_second;
        let state = self.sessions[session].as_mut().expect("a session");
        state.account = Some(account);
        state.tokens = u128::from(rate) * TOKEN;
        state.refilled = self.now;
        let last_seq = self.engine.last_seq() + self.batch.len() as u64;
        self.send(session, Outbound::LoginAccepted { account, last_seq }, mail);
    }

    fn limits(&self, account: u32) -> Account {
        self.accounts[account as usize]
            .as_ref()
            .expect("an account")
            .account
    }

    /// Takes one message from the session's token bucket, if it holds one.
    fn take_token(&mut self, session: SessionId, account: u32) -> bool {
        let rate = u128::from(self.limits(account).messages_per_second);
        let now = self.now;
        let state = self.sessions[session].as_mut().expect("a session");
        let elapsed = u128::from(now.saturating_sub(state.refilled));
        state.tokens = (state.tokens + elapsed * rate).min(rate * TOKEN);
        state.refilled = state.refilled.max(now);
        if state.tokens < TOKEN {
            return false;
        }
        state.tokens -= TOKEN;
        true
    }

    /// Puts an order-entry message into the batch, if the account's limits allow it.
    fn enter(
        &mut self,
        session: SessionId,
        account: u32,
        message: Inbound,
        mail: &mut impl Mailbox,
    ) {
        let command = match message {
            Inbound::NewOrder(NewOrder {
                client_ref,
                side,
                qty,
                kind,
            }) => {
                let state = self.accounts[account as usize]
                    .as_mut()
                    .expect("an account");
                if state.open >= state.account.max_open_orders {
                    let reason = RejectCode::TooManyOrders;
                    return self.send(session, Outbound::Reject { reason, client_ref }, mail);
                }
                state.open += 1;
                // An order's id is the sequence number of the command that places it:
                // unique, increasing, and recovered with the journal.
                let id = self.engine.last_seq() + self.batch.len() as u64 + 1;
                self.live.insert(
                    id,
                    Live {
                        account,
                        client_ref,
                    },
                );
                let owner = account;
                match kind {
                    OrderKind::Limit {
                        price,
                        tif,
                        display,
                    } => Command::Limit {
                        id,
                        owner,
                        side,
                        price,
                        qty,
                        tif,
                        display,
                    },
                    OrderKind::Market => Command::Market {
                        id,
                        owner,
                        side,
                        qty,
                    },
                    OrderKind::Stop { trigger, limit } => Command::Stop {
                        id,
                        owner,
                        side,
                        trigger,
                        limit,
                        qty,
                    },
                }
            }
            Inbound::Cancel { order_id } => Command::Cancel {
                id: order_id,
                owner: account,
            },
            Inbound::Modify {
                order_id,
                price,
                qty,
            } => Command::Modify {
                id: order_id,
                owner: account,
                price,
                qty,
            },
            Inbound::MassCancel => Command::CancelAll { owner: account },
            Inbound::Login { .. } | Inbound::Logout | Inbound::Heartbeat => {
                unreachable!("not an order-entry message")
            }
        };
        self.batch.push(command);
        self.from.push(session);
    }

    /// A connection has closed. A logged-in account's orders are cancelled: a client that
    /// is gone cannot manage them.
    pub fn disconnect(&mut self, session: SessionId) {
        if let Some(account) = self.detach(session) {
            self.batch.push(Command::CancelAll { owner: account });
            self.from.push(SYSTEM);
        }
    }

    /// A connection has closed because the gateway is stopping: the account's orders stay
    /// on the book, as they would if the gateway crashed. Returns the account that was
    /// logged in.
    pub fn detach(&mut self, session: SessionId) -> Option<u32> {
        let account = self
            .sessions
            .get_mut(session)
            .and_then(Option::take)?
            .account?;
        if let Some(state) = self.accounts[account as usize].as_mut() {
            state.session = None;
        }
        Some(account)
    }

    /// Journals and applies the batch, and sends the reports. An error means the engine
    /// takes no more commands: every session is logged out, and the gateway should stop.
    pub fn flush(&mut self, mail: &mut impl Mailbox) -> Result<(), Error> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.batch);
        let from = std::mem::take(&mut self.from);
        let mut router = Router {
            first: self.engine.last_seq() + 1,
            batch: &batch,
            from: &from,
            accounts: &mut self.accounts,
            sessions: &mut self.sessions,
            live: &mut self.live,
            now: self.now,
            mail,
        };
        let result = self.engine.submit_batch(&batch, &mut router);
        // The buffers are reused.
        self.batch = batch;
        self.batch.clear();
        self.from = from;
        self.from.clear();
        if let Err(error) = result {
            for session in 0..self.sessions.len() {
                self.end(session, LogoutReason::Shutdown, mail);
            }
            return Err(error);
        }
        Ok(())
    }

    /// Sends heartbeats to quiet sessions and logs out idle ones.
    pub fn tick(&mut self, now: u64, mail: &mut impl Mailbox) {
        self.now = now;
        for session in 0..self.sessions.len() {
            let Some(state) = self.sessions[session].as_ref() else {
                continue;
            };
            if state.closing {
                continue;
            }
            if now.saturating_sub(state.last_received) >= self.timing.idle_timeout {
                self.end(session, LogoutReason::Idle, mail);
            } else if state.account.is_some()
                && now.saturating_sub(state.last_sent) >= self.timing.heartbeat
            {
                self.send(session, Outbound::Heartbeat, mail);
            }
        }
    }

    /// Ends a session because its connection misbehaved: it sent bytes that do not decode,
    /// or does not read what it is sent. Also ends sessions on shutdown.
    pub fn end(&mut self, session: SessionId, reason: LogoutReason, mail: &mut impl Mailbox) {
        if self
            .sessions
            .get(session)
            .is_some_and(|s| s.as_ref().is_some_and(|s| !s.closing))
        {
            self.send(session, Outbound::Logout { reason }, mail);
            self.close(session, mail);
        }
    }

    fn send(&mut self, session: SessionId, message: Outbound, mail: &mut impl Mailbox) {
        if let Some(state) = self.sessions[session].as_mut() {
            state.last_sent = self.now;
        }
        mail.send(session, message);
    }

    fn close(&mut self, session: SessionId, mail: &mut impl Mailbox) {
        if let Some(state) = self.sessions[session].as_mut() {
            state.closing = true;
        }
        mail.close(session);
    }
}

/// Turns the book's events into reports for the sessions they concern.
struct Router<'a, M: Mailbox> {
    first: Seq,
    batch: &'a [Command],
    from: &'a [SessionId],
    accounts: &'a mut [Option<AccountState>],
    sessions: &'a mut [Option<Session>],
    live: &'a mut HashMap<OrderId, Live>,
    now: u64,
    mail: &'a mut M,
}

impl<M: Mailbox> Router<'_, M> {
    fn report(&mut self, session: SessionId, report: Report) {
        let Some(state) = self.sessions.get_mut(session).and_then(Option::as_mut) else {
            return;
        };
        if state.open() {
            state.last_sent = self.now;
            self.mail.send(session, Outbound::Report(report));
        }
    }

    /// Reports to the session of the account that owns order `id`, if it is logged in.
    fn tell_owner(&mut self, seq: Seq, id: OrderId, kind: ReportKind) {
        let Some(live) = self.live.get(&id).copied() else {
            return;
        };
        let session = self.accounts[live.account as usize]
            .as_ref()
            .and_then(|a| a.session);
        if let Some(session) = session {
            let report = Report {
                seq,
                order_id: id,
                client_ref: live.client_ref,
                kind,
            };
            self.report(session, report);
        }
    }

    /// The order has left the book.
    fn retire(&mut self, id: OrderId) {
        if let Some(live) = self.live.remove(&id) {
            if let Some(state) = self.accounts[live.account as usize].as_mut() {
                state.open -= 1;
            }
        }
    }
}

impl<M: Mailbox> Output for Router<'_, M> {
    fn on_event(&mut self, seq: Seq, event: Event) {
        match event {
            Event::Accepted { id } => self.tell_owner(seq, id, ReportKind::Accepted),
            Event::Rejected { id, reason } => {
                // Only a command's sender learns why it was refused: a cancel naming
                // someone else's order must not reach that order's owner.
                let index = (seq - self.first) as usize;
                let (owner, placed) = match self.batch[index] {
                    Command::Limit { id: new, owner, .. }
                    | Command::Market { id: new, owner, .. }
                    | Command::Stop { id: new, owner, .. } => (owner, new == id),
                    Command::Cancel { owner, .. }
                    | Command::Modify { owner, .. }
                    | Command::CancelAll { owner } => (owner, false),
                    Command::SetPhase { .. } => unreachable!("the gateway changes no phase"),
                };
                let client_ref = match self.live.get(&id) {
                    Some(live) if placed => live.client_ref,
                    _ => 0,
                };
                let report = Report {
                    seq,
                    order_id: id,
                    client_ref,
                    kind: ReportKind::Rejected(reason),
                };
                // The sender may have disconnected, and its connection's id gone to
                // another account's session since.
                let session = self.from[index];
                let sender = self.sessions.get(session).and_then(Option::as_ref);
                if sender.is_some_and(|s| s.account == Some(owner)) {
                    self.report(session, report);
                }
                if placed {
                    self.retire(id);
                }
            }
            Event::Trade {
                trade_id,
                taker,
                maker,
                taker_side,
                price,
                qty,
                taker_leaves,
                maker_leaves,
            } => {
                for (id, side, leaves) in [
                    (taker, taker_side, taker_leaves),
                    (maker, taker_side.opposite(), maker_leaves),
                ] {
                    let kind = ReportKind::Fill {
                        trade_id,
                        side,
                        price,
                        qty,
                        leaves,
                    };
                    self.tell_owner(seq, id, kind);
                    if leaves == 0 {
                        self.retire(id);
                    }
                }
            }
            Event::Rested {
                id,
                side,
                price,
                qty,
                visible,
            } => {
                let kind = ReportKind::Rested {
                    side,
                    price,
                    qty,
                    visible,
                };
                self.tell_owner(seq, id, kind);
            }
            Event::Replenished {
                id,
                side,
                price,
                visible,
            } => {
                let kind = ReportKind::Replenished {
                    side,
                    price,
                    visible,
                };
                self.tell_owner(seq, id, kind);
            }
            Event::Cancelled { id, qty, reason } => {
                self.tell_owner(seq, id, ReportKind::Cancelled { qty, reason });
                self.retire(id);
            }
            Event::Modified {
                id,
                price,
                qty,
                leaves,
            } => {
                self.tell_owner(seq, id, ReportKind::Modified { price, qty, leaves });
                if leaves == 0 {
                    self.retire(id);
                }
            }
            Event::StopPlaced {
                id,
                side,
                trigger,
                limit,
                qty,
            } => {
                let kind = ReportKind::StopPlaced {
                    side,
                    trigger,
                    limit,
                    qty,
                };
                self.tell_owner(seq, id, kind);
            }
            Event::Triggered { id } => self.tell_owner(seq, id, ReportKind::Triggered),
            Event::MassCancelled { owner, count } => {
                let session = self.accounts[owner as usize]
                    .as_ref()
                    .and_then(|a| a.session);
                if let Some(session) = session {
                    let report = Report {
                        seq,
                        order_id: 0,
                        client_ref: 0,
                        kind: ReportKind::MassCancelled { count },
                    };
                    self.report(session, report);
                }
            }
            Event::PhaseChanged { phase } => {
                for session in 0..self.sessions.len() {
                    let report = Report {
                        seq,
                        order_id: 0,
                        client_ref: 0,
                        kind: ReportKind::PhaseChanged(phase),
                    };
                    self.report(session, report);
                }
            }
        }
    }
}
