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
use orderbook::{Command, Event, OrderBook, OrderId, Price, Qty, Side};
use protocol::{
    Inbound, LevelUpdate, LoginError, LogoutReason, NewOrder, OrderKind, Outbound, RejectCode,
    Report, ReportKind, TradeTick, VERSION,
};
use serde::{Deserialize, Serialize};

use crate::accounts::Account;
use crate::candles::Candles;
use crate::wallet::Wallet;

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

/// What the exchange knows that the book does not, as of a sequence number: the money of
/// paper-trading accounts, and every open order's account and client reference. Saved now
/// and then, it lets a restart rebuild the exchange from the journal after it
/// ([`recovery`](crate::recovery)), as a snapshot lets the engine rebuild the book.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The sequence number of the last command it reflects.
    pub seq: Seq,
    /// Paper-trading accounts' cash and positions; what their orders hold follows from the
    /// orders.
    pub wallets: Vec<SavedWallet>,
    /// The orders and pending stops on the book.
    pub orders: Vec<SavedOrder>,
}

/// A paper-trading account's cash and position, in a checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedWallet {
    /// The account.
    pub account: u32,
    /// Its cash.
    pub cash: i64,
    /// Its position.
    pub position: i64,
}

/// An open order, in a checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedOrder {
    /// Its id.
    pub id: OrderId,
    /// Its account.
    pub account: u32,
    /// The client's reference for it.
    pub client_ref: u64,
    /// Whether it buys.
    pub buy: bool,
    /// Its limit price, if it has one.
    pub price: Option<Price>,
    /// Its open quantity.
    pub leaves: Qty,
}

/// Drops what it is sent: the mailbox of a recovery, when nobody is logged in.
struct Nobody;

impl Mailbox for Nobody {
    fn send(&mut self, _: SessionId, _: Outbound) {}
    fn close(&mut self, _: SessionId) {}
}

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
    /// A paper-trading account's money and position.
    wallet: Option<Wallet>,
}

impl AccountState {
    fn new(account: Account) -> AccountState {
        AccountState {
            account,
            session: None,
            open: 0,
            wallet: account.funds.map(Wallet::new),
        }
    }
}

/// A live order: whose it is, and what it holds of its account's wallet.
#[derive(Clone, Copy, Debug)]
struct Live {
    account: u32,
    client_ref: u64,
    side: Side,
    /// Its limit price: none for a market or stop-market order.
    price: Option<Price>,
    /// Its open quantity.
    leaves: Qty,
    /// What it holds of a paper-trading account's wallet.
    held: i64,
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
    /// The last hour of trades, for charts.
    candles: Candles,
    /// Seconds since the Unix epoch when the clock passed to the exchange read zero.
    epoch: u64,
    /// Trades delivered and not yet published.
    trades: Vec<TradeTick>,
    /// The sequence number of the last command whose events were delivered, and of the
    /// last one whose effect on the depth was published.
    delivered: Seq,
    published: Seq,
    /// Scratch space for the changed levels.
    updates: Vec<LevelUpdate>,
    /// Accounts whose wallets changed since the last publication.
    changed_wallets: Vec<u32>,
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
        let mut exchange = Exchange::empty(book.config().max_owners, last_seq, accounts, timing)?;
        let mut live = HashMap::new();
        for side in [Side::Buy, Side::Sell] {
            for level in book.depth(side) {
                for order in book.queue(side, level.price) {
                    live.insert(
                        order.id,
                        (order.owner, side, Some(level.price), order.leaves),
                    );
                }
            }
            for stop in book.stops(side) {
                live.insert(stop.id, (stop.owner, side, stop.limit, stop.qty));
            }
        }
        for (id, (account, side, price, leaves)) in live {
            if id > last_seq {
                return Err(SetupError::ForeignOrder(id));
            }
            exchange.place(id, account, 0, side, price, 0);
            exchange.set_leaves(id, leaves);
        }
        exchange.depth = Depth::of(book);
        exchange.changed_wallets.clear();
        Ok(exchange)
    }

    /// An exchange for `accounts` with no orders, after the command `last_seq`.
    fn empty(
        max_owners: u32,
        last_seq: Seq,
        accounts: &[Account],
        timing: Timing,
    ) -> Result<Exchange, SetupError> {
        let mut states: Vec<Option<AccountState>> = (0..max_owners).map(|_| None).collect();
        for account in accounts {
            let state = states
                .get_mut(account.id as usize)
                .ok_or(SetupError::AccountOutOfRange(account.id))?;
            *state = Some(AccountState::new(*account));
        }
        Ok(Exchange {
            timing,
            accounts: states,
            sessions: Vec::new(),
            live: HashMap::new(),
            batch: Vec::new(),
            in_flight: VecDeque::new(),
            last_seq,
            now: 0,
            depth: Depth::new(),
            candles: Candles::default(),
            epoch: 0,
            trades: Vec::new(),
            delivered: last_seq,
            published: last_seq,
            updates: Vec::new(),
            changed_wallets: Vec::new(),
        })
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
        *state = Some(AccountState::new(account));
        Ok(())
    }

    /// A paper-trading account's wallet.
    pub fn wallet(&self, account: u32) -> Option<Wallet> {
        self.accounts.get(account as usize)?.as_ref()?.wallet
    }

    /// The exchange's state, if every command numbered has had all its events delivered:
    /// the caller knows, since it hands the batches over and delivers the events.
    pub fn checkpoint(&self) -> Option<Checkpoint> {
        if !self.batch.is_empty() || self.delivered != self.last_seq {
            return None;
        }
        let wallets = self
            .accounts
            .iter()
            .flatten()
            .filter_map(|state| {
                let wallet = state.wallet?;
                Some(SavedWallet {
                    account: state.account.id,
                    cash: wallet.cash,
                    position: wallet.position,
                })
            })
            .collect();
        let mut orders: Vec<SavedOrder> = self
            .live
            .iter()
            .map(|(&id, live)| SavedOrder {
                id,
                account: live.account,
                client_ref: live.client_ref,
                buy: live.side == Side::Buy,
                price: live.price,
                leaves: live.leaves,
            })
            .collect();
        orders.sort_unstable_by_key(|order| order.id);
        Some(Checkpoint {
            seq: self.last_seq,
            wallets,
            orders,
        })
    }

    /// The exchange as `checkpoint` saved it, for `accounts`, in front of a book configured
    /// as `book`. The commands and events after it follow through
    /// [`replay_command`](Self::replay_command) and [`replay_event`](Self::replay_event),
    /// and [`finish`](Self::finish) ends the recovery.
    pub fn restore(
        checkpoint: &Checkpoint,
        book: &orderbook::BookConfig,
        accounts: &[Account],
        timing: Timing,
    ) -> Result<Exchange, SetupError> {
        let mut exchange = Exchange::empty(book.max_owners, checkpoint.seq, accounts, timing)?;
        for saved in &checkpoint.wallets {
            if let Some(wallet) = exchange
                .accounts
                .get_mut(saved.account as usize)
                .and_then(Option::as_mut)
                .and_then(|state| state.wallet.as_mut())
            {
                wallet.cash = saved.cash;
                wallet.position = saved.position;
            }
        }
        for order in &checkpoint.orders {
            if order.id > checkpoint.seq {
                return Err(SetupError::ForeignOrder(order.id));
            }
            let side = if order.buy { Side::Buy } else { Side::Sell };
            exchange.place(
                order.id,
                order.account,
                order.client_ref,
                side,
                order.price,
                0,
            );
            exchange.set_leaves(order.id, order.leaves);
        }
        Ok(exchange)
    }

    /// A command after the checkpoint, as recovery replays it: numbered and tracked as if
    /// it had just been handed over. Its client reference was not journaled, and is lost.
    ///
    /// # Panics
    ///
    /// If `seq` does not follow the last command.
    pub fn replay_command(&mut self, seq: Seq, command: Command) {
        assert_eq!(seq, self.last_seq + 1, "commands are replayed in sequence");
        let (owner, places) = match command {
            Command::Limit {
                id,
                owner,
                side,
                price,
                qty,
                ..
            } => {
                self.place(id, owner, 0, side, Some(price), qty);
                (owner, true)
            }
            Command::Market {
                id,
                owner,
                side,
                qty,
            } => {
                self.place(id, owner, 0, side, None, qty);
                (owner, true)
            }
            Command::Stop {
                id,
                owner,
                side,
                limit,
                qty,
                ..
            } => {
                self.place(id, owner, 0, side, limit, qty);
                (owner, true)
            }
            Command::Cancel { owner, .. }
            | Command::Modify { owner, .. }
            | Command::CancelAll { owner } => (owner, false),
            Command::SetPhase { .. } => (0, false),
        };
        self.last_seq = seq;
        self.in_flight.push_back(InFlight {
            seq,
            session: SYSTEM,
            owner,
            places,
        });
    }

    /// An event after the checkpoint, as recovery replays it.
    pub fn replay_event(&mut self, seq: Seq, event: Event) {
        self.deliver(seq, event, &mut Nobody);
    }

    /// Ends a recovery: the depth is taken from `book`, and the orders the exchange knows
    /// must be exactly those on it, with the same open quantities.
    pub fn finish(&mut self, book: &OrderBook) -> Result<(), String> {
        self.depth = Depth::of(book);
        self.trades.clear();
        self.changed_wallets.clear();
        self.delivered = self.last_seq;
        self.published = self.last_seq;
        let mut on_book = 0;
        for side in [Side::Buy, Side::Sell] {
            for level in book.depth(side) {
                for order in book.queue(side, level.price) {
                    on_book += 1;
                    match self.live.get(&order.id) {
                        Some(live)
                            if live.leaves == order.leaves && live.account == order.owner => {}
                        other => {
                            return Err(format!(
                                "order {} is on the book with {} open, but the exchange has {other:?}",
                                order.id, order.leaves
                            ));
                        }
                    }
                }
            }
            for stop in book.stops(side) {
                on_book += 1;
                if !self.live.contains_key(&stop.id) {
                    return Err(format!(
                        "stop {} is on the book, not in the exchange",
                        stop.id
                    ));
                }
            }
        }
        if on_book != self.live.len() {
            return Err(format!(
                "the book holds {on_book} orders, the exchange knows of {}",
                self.live.len()
            ));
        }
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
        if let Some(wallet) = self.wallet(account) {
            self.send(session, Outbound::Balance(wallet.balance()), mail);
        }
        // The account's open orders, as if they had just rested, so that a client that
        // comes back, or a gateway that restarted, does not lose sight of them. Pending
        // stops have no price to rest at, and are not told; nor are orders whose command
        // is still in flight, which the book has not seen and which will be reported as
        // they are applied.
        let delivered = self.delivered;
        let mut orders: Vec<(OrderId, Live)> = self
            .live
            .iter()
            .filter(|&(&id, live)| {
                live.account == account && live.price.is_some() && id <= delivered
            })
            .map(|(&id, &live)| (id, live))
            .collect();
        orders.sort_unstable_by_key(|&(id, _)| id);
        for (id, live) in orders {
            let report = Report {
                seq: last_seq,
                order_id: id,
                client_ref: live.client_ref,
                kind: ReportKind::Rested {
                    side: live.side,
                    price: live.price.expect("a limit order"),
                    qty: live.leaves,
                    visible: live.leaves,
                },
            };
            self.send(session, Outbound::Report(report), mail);
        }
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
                    .as_ref()
                    .expect("an account");
                if state.open >= state.account.max_open_orders {
                    let reason = RejectCode::TooManyOrders;
                    return self.send(session, Outbound::Reject { reason, client_ref }, mail);
                }
                let price = match kind {
                    OrderKind::Limit { price, .. } => Some(price),
                    OrderKind::Stop { limit, .. } => limit,
                    OrderKind::Market => None,
                };
                // A paper-trading account places limit orders it can pay for in full.
                if let Some(wallet) = state.wallet {
                    let refusal = match (kind, price) {
                        (OrderKind::Limit { .. }, Some(price)) => {
                            match Wallet::hold_of(side, price, qty) {
                                Some(hold) if hold >= 0 && wallet.covers(side, hold) => None,
                                _ => Some(RejectCode::InsufficientFunds),
                            }
                        }
                        _ => Some(RejectCode::NotAllowed),
                    };
                    if let Some(reason) = refusal {
                        return self.send(session, Outbound::Reject { reason, client_ref }, mail);
                    }
                }
                // An order's id is the sequence number of the command that places it:
                // unique, increasing, and recovered with the journal.
                let id = self.last_seq + 1;
                self.place(id, account, client_ref, side, price, qty);
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
            // A paper-trading account cancels and places again instead: a modify could need
            // more than it holds while it waits to be applied.
            Inbound::Modify { .. } if self.wallet(account).is_some() => {
                let reason = RejectCode::NotAllowed;
                return self.send(
                    session,
                    Outbound::Reject {
                        reason,
                        client_ref: 0,
                    },
                    mail,
                );
            }
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
        for index in 0..self.changed_wallets.len() {
            let account = self.changed_wallets[index];
            let Some(state) = self.accounts[account as usize].as_ref() else {
                continue;
            };
            if let (Some(session), Some(wallet)) = (state.session, state.wallet) {
                let open = self.sessions[session].as_ref().is_some_and(Session::open);
                if open {
                    self.send(session, Outbound::Balance(wallet.balance()), mail);
                }
            }
        }
        self.changed_wallets.clear();
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

    /// Sets what the clock passed to the exchange reads in wall-clock time: `epoch` seconds
    /// since the Unix epoch when it reads zero. Only the candles use it.
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// The last hour of trades, as candles.
    pub fn candles(&self) -> &Candles {
        &self.candles
    }

    /// Whether `session` is logged in and told the market data.
    pub fn is_subscribed(&self, session: SessionId) -> bool {
        let state = self.sessions.get(session).and_then(Option::as_ref);
        state.is_some_and(|state| state.subscribed)
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

    /// A new order, holding `qty` at its price if its account trades paper money.
    fn place(
        &mut self,
        id: OrderId,
        account: u32,
        client_ref: u64,
        side: Side,
        price: Option<Price>,
        qty: Qty,
    ) {
        if let Some(state) = self.accounts[account as usize].as_mut() {
            state.open += 1;
        }
        let live = Live {
            account,
            client_ref,
            side,
            price,
            leaves: 0,
            held: 0,
        };
        self.live.insert(id, live);
        self.set_leaves(id, qty);
    }

    /// The order's open quantity is now `leaves`: what it holds follows.
    fn set_leaves(&mut self, id: OrderId, leaves: Qty) {
        let Some(live) = self.live.get_mut(&id) else {
            return;
        };
        live.leaves = leaves;
        let Some(state) = self.accounts[live.account as usize].as_mut() else {
            return;
        };
        let Some(wallet) = state.wallet.as_mut() else {
            return;
        };
        let held = live
            .price
            .and_then(|price| Wallet::hold_of(live.side, price, leaves))
            .unwrap_or(0);
        if held != live.held {
            wallet.hold(live.side, held - live.held);
            live.held = held;
            self.changed_wallets.push(live.account);
        }
    }

    /// The order traded `qty` at `price`, and has `leaves` open.
    fn traded(&mut self, id: OrderId, price: Price, qty: Qty, leaves: Qty) {
        let Some(live) = self.live.get(&id).copied() else {
            return;
        };
        if let Some(wallet) = self.accounts[live.account as usize]
            .as_mut()
            .and_then(|state| state.wallet.as_mut())
        {
            wallet.settle(live.side, price, qty);
            self.changed_wallets.push(live.account);
        }
        self.set_leaves(id, leaves);
    }

    /// The order has left the book: what it held goes back.
    fn retire(&mut self, id: OrderId) {
        self.set_leaves(id, 0);
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
            let time = self.epoch + self.now / 1_000_000_000;
            self.candles.record(time, price, qty);
        }
        match event {
            Event::Accepted { id } => self.tell_owner(seq, id, ReportKind::Accepted, mail),
            Event::Rejected { id, reason } => {
                // Only a command's sender learns why it was refused: a cancel naming
                // someone else's order must not reach that order's owner.
                let placed = command.places;
                debug_assert!(!placed || id == seq, "a new order's refusal names it");
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
                    self.traded(id, price, qty, leaves);
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
                self.set_leaves(id, qty);
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
                if let Some(live) = self.live.get_mut(&id) {
                    live.price = Some(price);
                }
                self.set_leaves(id, leaves);
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
