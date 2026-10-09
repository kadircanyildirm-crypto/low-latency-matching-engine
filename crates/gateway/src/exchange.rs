//! The gateway's logic, without sockets: sessions, pre-trade risk, order ids, and the
//! routing of the book's events to the sessions they concern.
//!
//! The network layer feeds it decoded messages with the current time and delivers what it
//! sends; nothing here blocks or reads a clock, so every rule can be tested, and fuzzed,
//! deterministically.
//!
//! The exchange holds no engine. Order-entry messages collect in a batch, numbered with the
//! sequence numbers they will be journaled under; whoever runs the engine takes the batch
//! ([`Exchange::take_batch`]) and hands each event of the book back
//! ([`Exchange::deliver`]), at once or later from another thread.
//! [`Exchange::flush`] does both with an engine on the same thread. Under
//! `SyncPolicy::Always` a batch shares one sync, which is group commit: the network layer
//! hands one batch over per round of reading its sockets, so the more clients send at once,
//! the more commands each sync carries. Reports go out only as events come back, which is
//! after their commands are journaled.

use std::collections::{HashMap, VecDeque};
use std::fmt;

use engine::storage::Storage;
use engine::{Engine, Error, Output, Seq};
use marketdata::Depth;
use orderbook::{Command, Event, OrderBook, OrderId, Side};
use protocol::{
    Inbound, LevelUpdate, LoginError, LogoutReason, NewOrder, OrderKind, Outbound, RejectCode,
    Report, ReportKind, TradeTick, VERSION,
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
    /// An account with this id exists already.
    AccountExists(u32),
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
            SetupError::AccountExists(id) => write!(f, "account {id} exists already"),
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
    /// Whether it gets market data.
    subscribed: bool,
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

/// A command handed over whose events may still come.
#[derive(Clone, Copy, Debug)]
struct InFlight {
    seq: Seq,
    /// The session that sent it, or `SYSTEM`.
    session: SessionId,
    /// The account it acts for.
    owner: u32,
    /// Whether it places a new order, whose id is `seq`.
    places: bool,
}

/// Marks commands the exchange issues itself, such as the mass cancel on a disconnect.
const SYSTEM: SessionId = SessionId::MAX;

/// The gateway's logic.
pub struct Exchange {
    timing: Timing,
    accounts: Vec<Option<AccountState>>,
    sessions: Vec<Option<Session>>,
    /// Every order and pending stop on the book, and those not yet applied.
    live: HashMap<OrderId, Live>,
    /// Commands not yet handed over.
    batch: Vec<Command>,
    /// Commands taken whose events may still come, in sequence order.
    in_flight: VecDeque<InFlight>,
    /// The sequence number of the last command numbered.
    last_seq: Seq,
    /// The time passed with the latest call.
    now: u64,
    /// The book's depth, kept from the events.
    depth: Depth,
    /// Trades delivered and not yet published.
    trades: Vec<TradeTick>,
    /// The sequence number of the last command whose events were delivered, and of the
    /// last one whose effect on the depth was published.
    delivered: Seq,
    published: Seq,
    /// Scratch space for the changed levels.
    updates: Vec<LevelUpdate>,
}

impl Exchange {
    /// An exchange for `accounts`, in front of an engine whose recovered `book` reflects
    /// the commands up to `last_seq`. The orders on the book are attributed to their
    /// accounts, without the client references they were placed with.
    pub fn new(
        book: &OrderBook,
        last_seq: Seq,
        accounts: &[Account],
        timing: Timing,
    ) -> Result<Exchange, SetupError> {
        let max_owners = book.config().max_owners;
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
            timing,
            accounts: states,
            sessions: Vec::new(),
            live: HashMap::with_capacity(live.len()),
            batch: Vec::new(),
            in_flight: VecDeque::new(),
            last_seq,
            now: 0,
            depth: Depth::of(book),
            trades: Vec::new(),
            delivered: last_seq,
            published: last_seq,
            updates: Vec::new(),
        };
        for (id, account) in live {
            if id > last_seq {
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

    /// The sequence number of the last command numbered: handed over, or still in the
    /// batch.
    pub fn last_seq(&self) -> Seq {
        self.last_seq
    }

    /// Adds an account while the exchange runs, such as a visitor's paper-trading account.
    pub fn add_account(&mut self, account: Account) -> Result<(), SetupError> {
        let state = self
            .accounts
            .get_mut(account.id as usize)
            .ok_or(SetupError::AccountOutOfRange(account.id))?;
        if state.is_some() {
            return Err(SetupError::AccountExists(account.id));
        }
        *state = Some(AccountState {
            account,
            session: None,
            open: 0,
        });
        Ok(())
    }

    /// Whether an account with id `id` exists.
    pub fn has_account(&self, id: u32) -> bool {
        self.accounts.get(id as usize).is_some_and(Option::is_some)
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
            subscribed: false,
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
            Inbound::Subscribe => self.subscribe(session, mail),
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
        let last_seq = self.last_seq;
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
                let id = self.last_seq + 1;
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
            Inbound::Login { .. } | Inbound::Logout | Inbound::Heartbeat | Inbound::Subscribe => {
                unreachable!("not an order-entry message")
            }
        };
        self.push(command, session, account);
    }

    /// Numbers `command` and puts it into the batch.
    fn push(&mut self, command: Command, session: SessionId, owner: u32) {
        self.last_seq += 1;
        let places = matches!(
            command,
            Command::Limit { .. } | Command::Market { .. } | Command::Stop { .. }
        );
        self.batch.push(command);
        self.in_flight.push_back(InFlight {
            seq: self.last_seq,
            session,
            owner,
            places,
        });
    }

    /// A connection has closed. A logged-in account's orders are cancelled: a client that
    /// is gone cannot manage them.
    pub fn disconnect(&mut self, session: SessionId) {
        if let Some(account) = self.detach(session) {
            self.push(Command::CancelAll { owner: account }, SYSTEM, account);
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

    /// Hands the batch over: swaps it with `into`, which must be empty. The commands are
    /// numbered from the last one handed over plus one, and their events must come back,
    /// through [`deliver`](Self::deliver), in sequence order.
    pub fn take_batch(&mut self, into: &mut Vec<Command>) {
        debug_assert!(into.is_empty(), "the previous batch was handed over");
        std::mem::swap(&mut self.batch, into);
    }

    /// Journals and applies the batch on `engine`, on this thread, and sends the reports.
    /// An error means the engine takes no more commands: every session is logged out, and
    /// the gateway should stop.
    pub fn flush<S: Storage>(
        &mut self,
        engine: &mut Engine<S>,
        mail: &mut impl Mailbox,
    ) -> Result<(), Error> {
        if self.batch.is_empty() {
            return Ok(());
        }
        debug_assert_eq!(engine.last_seq() + self.batch.len() as u64, self.last_seq);
        let batch = std::mem::take(&mut self.batch);
        let result = engine.submit_batch(
            &batch,
            &mut Deliver {
                exchange: self,
                mail,
            },
        );
        // The buffer is reused.
        self.batch = batch;
        self.batch.clear();
        result.map(|_| ()).inspect_err(|_| self.shut_down(mail))
    }

    /// Sends the subscribed sessions what changed in the market since the last call: the
    /// trades, then the levels that changed, as they are now, all under the sequence number
    /// of the last command delivered. The network layer calls it after delivering events.
    pub fn publish(&mut self, mail: &mut impl Mailbox) {
        let seq = self.delivered;
        self.updates.clear();
        self.updates
            .extend(self.depth.changes().map(|update| LevelUpdate {
                seq,
                side: update.side,
                price: update.price,
                qty: update.level.qty,
                orders: update.level.orders,
            }));
        self.published = seq;
        if self.trades.is_empty() && self.updates.is_empty() {
            return;
        }
        for session in 0..self.sessions.len() {
            if self.sessions[session]
                .as_ref()
                .is_some_and(|s| s.subscribed && s.open())
            {
                for &trade in &self.trades {
                    mail.send(session, Outbound::TradeTick(trade));
                }
                for &level in &self.updates {
                    mail.send(session, Outbound::LevelUpdate(level));
                }
                self.sessions[session]
                    .as_mut()
                    .expect("a session")
                    .last_sent = self.now;
            }
        }
        self.trades.clear();
    }

    /// Sends `session` the book as it was last published, and marks it for what follows.
    /// Subscribing again sends the book again: a client that lost track starts over.
    fn subscribe(&mut self, session: SessionId, mail: &mut impl Mailbox) {
        // What was delivered but not yet published goes out first, to the others, so the
        // book sent here is the one the next changes start from.
        self.publish(mail);
        let seq = self.published;
        let levels = [Side::Buy, Side::Sell]
            .iter()
            .map(|&side| self.depth.levels(side).count())
            .sum::<usize>();
        let levels = u32::try_from(levels).expect("fewer levels than u32::MAX");
        self.send(session, Outbound::BookSnapshot { seq, levels }, mail);
        for side in [Side::Buy, Side::Sell] {
            for (price, level) in self.depth.levels(side) {
                let update = LevelUpdate {
                    seq,
                    side,
                    price,
                    qty: level.qty,
                    orders: level.orders,
                };
                mail.send(session, Outbound::LevelUpdate(update));
            }
        }
        if let Some(state) = self.sessions[session].as_mut() {
            state.subscribed = true;
        }
    }

    /// The book's depth, as the delivered events left it.
    pub fn depth(&self) -> &Depth {
        &self.depth
    }

    /// The engine failed and takes no more commands: every session is logged out, and the
    /// gateway should stop.
    pub fn shut_down(&mut self, mail: &mut impl Mailbox) {
        for session in 0..self.sessions.len() {
            self.end(session, LogoutReason::Shutdown, mail);
        }
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

/// Hands an engine's events to an exchange.
struct Deliver<'a, M: Mailbox> {
    exchange: &'a mut Exchange,
    mail: &'a mut M,
}

impl<M: Mailbox> Output for Deliver<'_, M> {
    fn on_event(&mut self, seq: Seq, event: Event) {
        self.exchange.deliver(seq, event, self.mail);
    }
}

impl Exchange {
    fn report(&mut self, session: SessionId, report: Report, mail: &mut impl Mailbox) {
        let Some(state) = self.sessions.get_mut(session).and_then(Option::as_mut) else {
            return;
        };
        if state.open() {
            state.last_sent = self.now;
            mail.send(session, Outbound::Report(report));
        }
    }

    /// Reports to the session of the account that owns order `id`, if it is logged in.
    fn tell_owner(&mut self, seq: Seq, id: OrderId, kind: ReportKind, mail: &mut impl Mailbox) {
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
            self.report(session, report, mail);
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

    /// The command handed over under `seq`.
    ///
    /// # Panics
    ///
    /// If no command was handed over under `seq`, or its events came out of order.
    fn command(&mut self, seq: Seq) -> InFlight {
        // Every command has at least one event, so a command's events have all come once
        // a later command's arrive.
        while self.in_flight.front().is_some_and(|c| c.seq < seq) {
            self.in_flight.pop_front();
        }
        let command = self.in_flight.front().copied();
        command
            .filter(|c| c.seq == seq)
            .unwrap_or_else(|| panic!("an event of command {seq}, which was not handed over"))
    }

    /// An event of the book, about the command handed over under `seq`: reports it to the
    /// sessions it concerns. Events must come in sequence order.
    pub fn deliver(&mut self, seq: Seq, event: Event, mail: &mut impl Mailbox) {
        let command = self.command(seq);
        self.depth.apply(&event);
        self.delivered = seq;
        if let Event::Trade {
            trade_id,
            taker_side,
            price,
            qty,
            ..
        } = event
        {
            self.trades.push(TradeTick {
                seq,
                trade_id,
                side: taker_side,
                price,
                qty,
            });
        }
        match event {
            Event::Accepted { id } => self.tell_owner(seq, id, ReportKind::Accepted, mail),
            Event::Rejected { id, reason } => {
                // Only a command's sender learns why it was refused: a cancel naming
                // someone else's order must not reach that order's owner.
                let placed = command.places && id == seq;
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
                let sender = self.sessions.get(command.session).and_then(Option::as_ref);
                if sender.is_some_and(|s| s.account == Some(command.owner)) {
                    self.report(command.session, report, mail);
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
                    self.tell_owner(seq, id, kind, mail);
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
                self.tell_owner(seq, id, kind, mail);
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
                self.tell_owner(seq, id, kind, mail);
            }
            Event::Cancelled { id, qty, reason } => {
                self.tell_owner(seq, id, ReportKind::Cancelled { qty, reason }, mail);
                self.retire(id);
            }
            Event::Modified {
                id,
                price,
                qty,
                leaves,
            } => {
                self.tell_owner(seq, id, ReportKind::Modified { price, qty, leaves }, mail);
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
                self.tell_owner(seq, id, kind, mail);
            }
            Event::Triggered { id } => self.tell_owner(seq, id, ReportKind::Triggered, mail),
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
                    self.report(session, report, mail);
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
                    self.report(session, report, mail);
                }
            }
        }
    }
}
