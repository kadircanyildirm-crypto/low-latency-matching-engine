//! The gateway's wire protocol: fixed-length, little-endian binary messages over TCP.
//!
//! Every message starts with a 4-byte header: its total length (`u16`, header included),
//! its type (`u8`) and a zero byte. Each type has one fixed length. Decoding is strict, as
//! in the journal's codec: unknown types, wrong lengths, out-of-range codes and non-zero
//! padding or unused fields are all errors, and a connection that sends one is closed. A
//! decoder that accepted variants would give two byte strings the same meaning, and turn a
//! client's bug into a guess.
//!
//! Client to exchange:
//!
//! | Type | Message | Length | Body |
//! |---:|---|---:|---|
//! | 1 | `Login` | 20 | version `u16`, zero `u16`, account `u32`, token `u64` |
//! | 2 | `Logout` | 4 | |
//! | 3 | `Heartbeat` | 4 | |
//! | 4 | `NewOrder` | 44 | client ref `u64`, side, order type, time in force, option flag (`u8` each), zero `u32`, price or trigger `i64`, quantity `u64`, display or stop limit `i64` |
//! | 5 | `Cancel` | 12 | order id `u64` |
//! | 6 | `Modify` | 28 | order id `u64`, price `i64`, total quantity `u64` |
//! | 7 | `MassCancel` | 4 | |
//! | 8 | `Subscribe` | 4 | |
//!
//! Exchange to client:
//!
//! | Type | Message | Length | Body |
//! |---:|---|---:|---|
//! | 101 | `LoginAccepted` | 20 | account `u32`, zero `u32`, last sequence number `u64` |
//! | 102 | `LoginRejected` | 12 | reason `u8`, zero ×7 |
//! | 103 | `Heartbeat` | 4 | |
//! | 104 | `Logout` | 12 | reason `u8`, zero ×7 |
//! | 105 | `Reject` | 20 | reason `u8`, zero ×7, client ref `u64` |
//! | 110 | `Report` | 76 | kind, side, code, flags (`u8` each), zero `u32`, sequence number, order id, client ref (`u64`), price `i64`, quantity, leaves, trade id (`u64`), aux `i64` |
//! | 120 | `BookSnapshot` | 20 | sequence number `u64`, levels `u32`, zero `u32` |
//! | 121 | `LevelUpdate` | 36 | sequence number `u64`, side `u8`, zero ×3, orders `u32`, price `i64`, quantity `u64` |
//! | 122 | `TradeTick` | 44 | sequence number, trade id (`u64`), side `u8`, zero ×7, price `i64`, quantity `u64` |
//!
//! A `Report` describes one event of the book about one order, as seen by the order's
//! owner; [`ReportKind`] says which fields each kind uses. The sequence number is that of
//! the command that caused the event, and an order's id is the sequence number of the
//! command that placed it.
//!
//! Market data is public. After `Subscribe`, a session gets a `BookSnapshot`, the number of
//! price levels that follow it as `LevelUpdate`s with the same sequence number, bids best
//! first and then asks; then, as the book changes, `TradeTick`s and `LevelUpdate`s with the
//! sequence number of the last command they reflect. A level with no orders is gone.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::fmt;

use orderbook::{CancelReason, Phase, RejectReason, Side, TimeInForce};

/// The protocol version this crate speaks, sent in `Login`.
pub const VERSION: u16 = 1;

/// Size of a message header.
pub const HEADER_SIZE: usize = 4;

/// Size of the longest message.
pub const MAX_MESSAGE_SIZE: usize = 76;

/// A message from a client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Inbound {
    /// Opens the session for `account`.
    Login {
        /// The protocol version the client speaks.
        version: u16,
        /// The account to trade for.
        account: u32,
        /// The account's secret.
        token: u64,
    },
    /// Closes the session; the exchange answers with `Logout` and closes the connection.
    Logout,
    /// Keeps an idle session alive.
    Heartbeat,
    /// Places an order.
    NewOrder(NewOrder),
    /// Cancels the order with this id.
    Cancel {
        /// The exchange's id of the order.
        order_id: u64,
    },
    /// Cancel/replace: a new price and a new total quantity.
    Modify {
        /// The exchange's id of the order.
        order_id: u64,
        /// The new limit price.
        price: i64,
        /// The new total quantity, filled quantity included.
        qty: u64,
    },
    /// Cancels every order of the session's account.
    MassCancel,
    /// Asks for market data: the book now, then its changes.
    Subscribe,
}

/// An order to place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NewOrder {
    /// The client's own reference, echoed in every report about the order.
    pub client_ref: u64,
    /// Buy or sell.
    pub side: Side,
    /// Quantity in lots.
    pub qty: u64,
    /// What kind of order.
    pub kind: OrderKind,
}

/// The kind of a new order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderKind {
    /// A limit order.
    Limit {
        /// The limit price.
        price: i64,
        /// Time in force.
        tif: TimeInForce,
        /// Iceberg display quantity.
        display: Option<u64>,
    },
    /// A market order.
    Market,
    /// A stop or stop-limit order.
    Stop {
        /// The trigger price.
        trigger: i64,
        /// The limit of a stop-limit order.
        limit: Option<i64>,
    },
}

/// A message to a client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outbound {
    /// The session is open.
    LoginAccepted {
        /// The account.
        account: u32,
        /// The sequence number of the last command the exchange has applied.
        last_seq: u64,
    },
    /// The session could not be opened; the connection is closed.
    LoginRejected {
        /// Why.
        reason: LoginError,
    },
    /// Keeps an idle session alive.
    Heartbeat,
    /// The session is closed, and so is the connection.
    Logout {
        /// Why.
        reason: LogoutReason,
    },
    /// The gateway refused a message without passing it to the book.
    Reject {
        /// Why.
        reason: RejectCode,
        /// The client ref of the refused `NewOrder`, or zero.
        client_ref: u64,
    },
    /// An event of the book about one of the client's orders, or about the market.
    Report(Report),
    /// The book as of `seq`: the `levels` `LevelUpdate`s that follow describe it.
    BookSnapshot {
        /// The sequence number of the last command the book reflects.
        seq: u64,
        /// How many levels follow.
        levels: u32,
    },
    /// A price level of the book, as it is after the command `seq`.
    LevelUpdate(LevelUpdate),
    /// A trade.
    TradeTick(TradeTick),
}

/// A price level, in market data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelUpdate {
    /// The sequence number of the last command the level reflects.
    pub seq: u64,
    /// The side.
    pub side: Side,
    /// The price.
    pub price: i64,
    /// The quantity its orders show; zero, with zero orders, once it is gone.
    pub qty: u64,
    /// How many orders rest there.
    pub orders: u32,
}

/// A trade, in market data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TradeTick {
    /// The sequence number of the command that caused it.
    pub seq: u64,
    /// The trade's id.
    pub trade_id: u64,
    /// The side of the order that took liquidity; the buy side in an uncross.
    pub side: Side,
    /// The price.
    pub price: i64,
    /// The quantity.
    pub qty: u64,
}

/// Why a login failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoginError {
    /// The client speaks another protocol version.
    UnsupportedVersion,
    /// No such account, or the token does not match: the two are not told apart.
    BadCredentials,
    /// Another session of the account is open.
    AlreadyLoggedIn,
    /// The session is already logged in.
    AlreadyInSession,
}

/// Why a session ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogoutReason {
    /// The client asked.
    Requested,
    /// The client sent nothing for too long.
    Idle,
    /// The client sent a message that does not decode, or one the session's state does not
    /// allow.
    ProtocolError,
    /// The client did not read its messages fast enough.
    SlowConsumer,
    /// The exchange is shutting down, or cannot continue.
    Shutdown,
}

/// Why the gateway refused a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejectCode {
    /// The account has as many open orders as it may.
    TooManyOrders,
    /// The session sent more messages than its rate allows.
    Throttled,
    /// The exchange cannot accept commands now.
    Unavailable,
}

/// An event of the book, as reported to a client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Report {
    /// The sequence number of the command that caused the event.
    pub seq: u64,
    /// The order the event is about, or zero for events about the market or the account.
    pub order_id: u64,
    /// The client ref the order was placed with, or zero if unknown.
    pub client_ref: u64,
    /// What happened.
    pub kind: ReportKind,
}

/// What a [`Report`] says, and which fields it uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReportKind {
    /// The order was accepted.
    Accepted,
    /// The order, cancel or modify was refused by the book.
    Rejected(RejectReason),
    /// The order traded.
    Fill {
        /// The trade's id.
        trade_id: u64,
        /// The order's side.
        side: Side,
        /// The price it traded at.
        price: i64,
        /// The quantity it traded.
        qty: u64,
        /// What remains open.
        leaves: u64,
    },
    /// The order rests on the book.
    Rested {
        /// Its side.
        side: Side,
        /// Its price.
        price: i64,
        /// Its open quantity.
        qty: u64,
        /// What it shows.
        visible: u64,
    },
    /// An iceberg showed its next tranche.
    Replenished {
        /// Its side.
        side: Side,
        /// Its price.
        price: i64,
        /// What it shows now.
        visible: u64,
    },
    /// Open quantity left the book without trading.
    Cancelled {
        /// The quantity cancelled.
        qty: u64,
        /// Why.
        reason: CancelReason,
    },
    /// The order was modified.
    Modified {
        /// Its price.
        price: i64,
        /// Its new total quantity.
        qty: u64,
        /// Its open quantity.
        leaves: u64,
    },
    /// The stop order waits for its trigger.
    StopPlaced {
        /// Its side.
        side: Side,
        /// Its trigger price.
        trigger: i64,
        /// Its limit, for a stop-limit order.
        limit: Option<i64>,
        /// Its quantity.
        qty: u64,
    },
    /// The stop order was triggered.
    Triggered,
    /// Every order of the account was cancelled.
    MassCancelled {
        /// How many.
        count: u32,
    },
    /// The market moved to another trading phase.
    PhaseChanged(Phase),
}

/// Why bytes are not a valid message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// No message has this type.
    UnknownType(u8),
    /// The length is not that of the message type.
    BadLength {
        /// The message type.
        kind: u8,
        /// The length the header gives.
        len: u16,
    },
    /// A field holds a code no message uses.
    InvalidField(&'static str),
    /// Every field is valid, but padding or an unused field is not zero.
    NonCanonical,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownType(kind) => write!(f, "unknown message type {kind}"),
            Self::BadLength { kind, len } => write!(f, "message type {kind} with length {len}"),
            Self::InvalidField(field) => write!(f, "invalid {field}"),
            Self::NonCanonical => f.write_str("not a canonical encoding"),
        }
    }
}

impl std::error::Error for ProtocolError {}

const LOGIN: u8 = 1;
const LOGOUT: u8 = 2;
const HEARTBEAT: u8 = 3;
const NEW_ORDER: u8 = 4;
const CANCEL: u8 = 5;
const MODIFY: u8 = 6;
const MASS_CANCEL: u8 = 7;
const SUBSCRIBE: u8 = 8;

const LOGIN_ACCEPTED: u8 = 101;
const LOGIN_REJECTED: u8 = 102;
const HEARTBEAT_OUT: u8 = 103;
const LOGOUT_OUT: u8 = 104;
const REJECT: u8 = 105;
const REPORT: u8 = 110;
const BOOK_SNAPSHOT: u8 = 120;
const LEVEL_UPDATE: u8 = 121;
const TRADE_TICK: u8 = 122;

/// The length of messages of type `kind`, if it is one.
fn length_of(kind: u8) -> Option<usize> {
    Some(match kind {
        LOGIN => 20,
        LOGOUT | HEARTBEAT | MASS_CANCEL | SUBSCRIBE | HEARTBEAT_OUT => 4,
        NEW_ORDER => 44,
        CANCEL => 12,
        MODIFY => 28,
        LOGIN_ACCEPTED | REJECT => 20,
        LOGIN_REJECTED | LOGOUT_OUT => 12,
        REPORT => 76,
        BOOK_SNAPSHOT => 20,
        LEVEL_UPDATE => 36,
        TRADE_TICK => 44,
        _ => return None,
    })
}

/// A message being written: the header, then fields in order.
struct Writer<'a> {
    out: &'a mut Vec<u8>,
    start: usize,
}

impl<'a> Writer<'a> {
    fn new(out: &'a mut Vec<u8>, kind: u8) -> Writer<'a> {
        let start = out.len();
        let len = length_of(kind).expect("a message type") as u16;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&[kind, 0]);
        Writer { out, start }
    }
    fn u8(&mut self, value: u8) -> &mut Self {
        self.out.push(value);
        self
    }
    fn u16(&mut self, value: u16) -> &mut Self {
        self.out.extend_from_slice(&value.to_le_bytes());
        self
    }
    fn u32(&mut self, value: u32) -> &mut Self {
        self.out.extend_from_slice(&value.to_le_bytes());
        self
    }
    fn u64(&mut self, value: u64) -> &mut Self {
        self.out.extend_from_slice(&value.to_le_bytes());
        self
    }
    fn i64(&mut self, value: i64) -> &mut Self {
        self.out.extend_from_slice(&value.to_le_bytes());
        self
    }
    fn zeros(&mut self, n: usize) -> &mut Self {
        self.out.resize(self.out.len() + n, 0);
        self
    }
    fn done(&mut self) {
        let kind = self.out[self.start + 2];
        debug_assert_eq!(self.out.len() - self.start, length_of(kind).unwrap());
    }
}

/// Fields of a message being read, after its header.
struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> [u8; N] {
        let (head, rest) = self.0.split_at(N);
        self.0 = rest;
        head.try_into().unwrap()
    }
    fn u8(&mut self) -> u8 {
        self.take::<1>()[0]
    }
    fn u16(&mut self) -> u16 {
        u16::from_le_bytes(self.take())
    }
    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take())
    }
    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take())
    }
    fn i64(&mut self) -> i64 {
        i64::from_le_bytes(self.take())
    }
    fn skip(&mut self, n: usize) {
        self.0 = &self.0[n..];
    }
}

/// Message types a client sends.
const INBOUND: std::ops::RangeInclusive<u8> = LOGIN..=SUBSCRIBE;

/// The type and length of the message at the start of `buf`, once the header is there.
/// Only message types of the direction being read, `inbound` or not, are known.
fn frame(buf: &[u8], inbound: bool) -> Result<Option<(u8, usize)>, ProtocolError> {
    if buf.len() < HEADER_SIZE {
        return Ok(None);
    }
    let len = u16::from_le_bytes([buf[0], buf[1]]);
    let kind = buf[2];
    let expected = length_of(kind)
        .filter(|_| INBOUND.contains(&kind) == inbound)
        .ok_or(ProtocolError::UnknownType(kind))?;
    if usize::from(len) != expected {
        return Err(ProtocolError::BadLength { kind, len });
    }
    if buf[3] != 0 {
        return Err(ProtocolError::NonCanonical);
    }
    Ok((buf.len() >= expected).then_some((kind, expected)))
}

/// Appends the encoding of `message` to `out`.
pub fn encode_inbound(message: &Inbound, out: &mut Vec<u8>) {
    match *message {
        Inbound::Login {
            version,
            account,
            token,
        } => Writer::new(out, LOGIN)
            .u16(version)
            .zeros(2)
            .u32(account)
            .u64(token)
            .done(),
        Inbound::Logout => Writer::new(out, LOGOUT).done(),
        Inbound::Heartbeat => Writer::new(out, HEARTBEAT).done(),
        Inbound::NewOrder(order) => {
            let (kind, tif, price, option) = match order.kind {
                OrderKind::Limit {
                    price,
                    tif,
                    display,
                } => (1, tif_code(tif), price, display.map(|d| d as i64)),
                OrderKind::Market => (2, 0, 0, None),
                OrderKind::Stop { trigger, limit } => (3, 0, trigger, limit),
            };
            Writer::new(out, NEW_ORDER)
                .u64(order.client_ref)
                .u8(side_code(order.side))
                .u8(kind)
                .u8(tif)
                .u8(u8::from(option.is_some()))
                .zeros(4)
                .i64(price)
                .u64(order.qty)
                .i64(option.unwrap_or(0))
                .done();
        }
        Inbound::Cancel { order_id } => Writer::new(out, CANCEL).u64(order_id).done(),
        Inbound::Modify {
            order_id,
            price,
            qty,
        } => Writer::new(out, MODIFY)
            .u64(order_id)
            .i64(price)
            .u64(qty)
            .done(),
        Inbound::MassCancel => Writer::new(out, MASS_CANCEL).done(),
        Inbound::Subscribe => Writer::new(out, SUBSCRIBE).done(),
    }
}

/// Decodes the message at the start of `buf`: `Ok(None)` if `buf` does not hold all of it
/// yet, otherwise the message and its length.
pub fn decode_inbound(buf: &[u8]) -> Result<Option<(Inbound, usize)>, ProtocolError> {
    let Some((kind, len)) = frame(buf, true)? else {
        return Ok(None);
    };
    let bytes = &buf[..len];
    let mut r = Reader(&bytes[HEADER_SIZE..]);
    let message = match kind {
        LOGIN => {
            let version = r.u16();
            r.skip(2);
            Inbound::Login {
                version,
                account: r.u32(),
                token: r.u64(),
            }
        }
        LOGOUT => Inbound::Logout,
        HEARTBEAT => Inbound::Heartbeat,
        NEW_ORDER => {
            let client_ref = r.u64();
            let side = side_of(r.u8())?;
            let (kind, tif, flag) = (r.u8(), r.u8(), r.u8());
            let present = flag_of(flag)?;
            r.skip(4);
            let (price, qty, option) = (r.i64(), r.u64(), r.i64());
            let kind = match kind {
                1 => OrderKind::Limit {
                    price,
                    tif: tif_of(tif)?,
                    display: present.then_some(option as u64),
                },
                2 => OrderKind::Market,
                3 => OrderKind::Stop {
                    trigger: price,
                    limit: present.then_some(option),
                },
                _ => return Err(ProtocolError::InvalidField("order type")),
            };
            Inbound::NewOrder(NewOrder {
                client_ref,
                side,
                qty,
                kind,
            })
        }
        CANCEL => Inbound::Cancel { order_id: r.u64() },
        MODIFY => Inbound::Modify {
            order_id: r.u64(),
            price: r.i64(),
            qty: r.u64(),
        },
        MASS_CANCEL => Inbound::MassCancel,
        SUBSCRIBE => Inbound::Subscribe,
        other => return Err(ProtocolError::UnknownType(other)),
    };
    let mut canonical = Vec::with_capacity(len);
    encode_inbound(&message, &mut canonical);
    if canonical != bytes {
        return Err(ProtocolError::NonCanonical);
    }
    Ok(Some((message, len)))
}

/// Appends the encoding of `message` to `out`.
pub fn encode_outbound(message: &Outbound, out: &mut Vec<u8>) {
    match *message {
        Outbound::LoginAccepted { account, last_seq } => Writer::new(out, LOGIN_ACCEPTED)
            .u32(account)
            .zeros(4)
            .u64(last_seq)
            .done(),
        Outbound::LoginRejected { reason } => Writer::new(out, LOGIN_REJECTED)
            .u8(login_error_code(reason))
            .zeros(7)
            .done(),
        Outbound::Heartbeat => Writer::new(out, HEARTBEAT_OUT).done(),
        Outbound::Logout { reason } => Writer::new(out, LOGOUT_OUT)
            .u8(logout_code(reason))
            .zeros(7)
            .done(),
        Outbound::Reject { reason, client_ref } => Writer::new(out, REJECT)
            .u8(reject_code(reason))
            .zeros(7)
            .u64(client_ref)
            .done(),
        Outbound::Report(report) => {
            let mut f = Fields::default();
            match report.kind {
                ReportKind::Accepted => f.kind = 1,
                ReportKind::Rejected(reason) => {
                    f.kind = 2;
                    f.code = reject_reason_code(reason);
                }
                ReportKind::Fill {
                    trade_id,
                    side,
                    price,
                    qty,
                    leaves,
                } => {
                    f.kind = 3;
                    f.side = side_code(side);
                    (f.price, f.qty, f.leaves, f.trade_id) = (price, qty, leaves, trade_id);
                }
                ReportKind::Rested {
                    side,
                    price,
                    qty,
                    visible,
                } => {
                    f.kind = 4;
                    f.side = side_code(side);
                    (f.price, f.qty, f.leaves) = (price, qty, visible);
                }
                ReportKind::Replenished {
                    side,
                    price,
                    visible,
                } => {
                    f.kind = 5;
                    f.side = side_code(side);
                    (f.price, f.leaves) = (price, visible);
                }
                ReportKind::Cancelled { qty, reason } => {
                    f.kind = 6;
                    f.code = cancel_reason_code(reason);
                    f.qty = qty;
                }
                ReportKind::Modified { price, qty, leaves } => {
                    f.kind = 7;
                    (f.price, f.qty, f.leaves) = (price, qty, leaves);
                }
                ReportKind::StopPlaced {
                    side,
                    trigger,
                    limit,
                    qty,
                } => {
                    f.kind = 8;
                    f.side = side_code(side);
                    (f.price, f.qty) = (trigger, qty);
                    f.flags = u8::from(limit.is_some());
                    f.aux = limit.unwrap_or(0);
                }
                ReportKind::Triggered => f.kind = 9,
                ReportKind::MassCancelled { count } => {
                    f.kind = 10;
                    f.qty = u64::from(count);
                }
                ReportKind::PhaseChanged(phase) => {
                    f.kind = 11;
                    f.code = phase_code(phase);
                }
            }
            Writer::new(out, REPORT)
                .u8(f.kind)
                .u8(f.side)
                .u8(f.code)
                .u8(f.flags)
                .zeros(4)
                .u64(report.seq)
                .u64(report.order_id)
                .u64(report.client_ref)
                .i64(f.price)
                .u64(f.qty)
                .u64(f.leaves)
                .u64(f.trade_id)
                .i64(f.aux)
                .done();
        }
        Outbound::BookSnapshot { seq, levels } => Writer::new(out, BOOK_SNAPSHOT)
            .u64(seq)
            .u32(levels)
            .zeros(4)
            .done(),
        Outbound::LevelUpdate(level) => Writer::new(out, LEVEL_UPDATE)
            .u64(level.seq)
            .u8(side_code(level.side))
            .zeros(3)
            .u32(level.orders)
            .i64(level.price)
            .u64(level.qty)
            .done(),
        Outbound::TradeTick(trade) => Writer::new(out, TRADE_TICK)
            .u64(trade.seq)
            .u64(trade.trade_id)
            .u8(side_code(trade.side))
            .zeros(7)
            .i64(trade.price)
            .u64(trade.qty)
            .done(),
    }
}

/// The fields of a report message.
#[derive(Default)]
struct Fields {
    kind: u8,
    side: u8,
    code: u8,
    flags: u8,
    price: i64,
    qty: u64,
    leaves: u64,
    trade_id: u64,
    aux: i64,
}

/// Decodes the message at the start of `buf`: `Ok(None)` if `buf` does not hold all of it
/// yet, otherwise the message and its length.
pub fn decode_outbound(buf: &[u8]) -> Result<Option<(Outbound, usize)>, ProtocolError> {
    let Some((kind, len)) = frame(buf, false)? else {
        return Ok(None);
    };
    let bytes = &buf[..len];
    let mut r = Reader(&bytes[HEADER_SIZE..]);
    let message = match kind {
        LOGIN_ACCEPTED => {
            let account = r.u32();
            r.skip(4);
            Outbound::LoginAccepted {
                account,
                last_seq: r.u64(),
            }
        }
        LOGIN_REJECTED => Outbound::LoginRejected {
            reason: login_error_of(r.u8())?,
        },
        HEARTBEAT_OUT => Outbound::Heartbeat,
        LOGOUT_OUT => Outbound::Logout {
            reason: logout_of(r.u8())?,
        },
        REJECT => {
            let reason = reject_of(r.u8())?;
            r.skip(7);
            Outbound::Reject {
                reason,
                client_ref: r.u64(),
            }
        }
        REPORT => {
            let (kind, side, code, flags) = (r.u8(), r.u8(), r.u8(), r.u8());
            r.skip(4);
            let (seq, order_id, client_ref) = (r.u64(), r.u64(), r.u64());
            let (price, qty, leaves, trade_id, aux) = (r.i64(), r.u64(), r.u64(), r.u64(), r.i64());
            let kind = match kind {
                1 => ReportKind::Accepted,
                2 => ReportKind::Rejected(reject_reason_of(code)?),
                3 => ReportKind::Fill {
                    trade_id,
                    side: side_of(side)?,
                    price,
                    qty,
                    leaves,
                },
                4 => ReportKind::Rested {
                    side: side_of(side)?,
                    price,
                    qty,
                    visible: leaves,
                },
                5 => ReportKind::Replenished {
                    side: side_of(side)?,
                    price,
                    visible: leaves,
                },
                6 => ReportKind::Cancelled {
                    qty,
                    reason: cancel_reason_of(code)?,
                },
                7 => ReportKind::Modified { price, qty, leaves },
                8 => ReportKind::StopPlaced {
                    side: side_of(side)?,
                    trigger: price,
                    limit: flag_of(flags)?.then_some(aux),
                    qty,
                },
                9 => ReportKind::Triggered,
                10 => ReportKind::MassCancelled {
                    count: u32::try_from(qty)
                        .map_err(|_| ProtocolError::InvalidField("mass cancel count"))?,
                },
                11 => ReportKind::PhaseChanged(phase_of(code)?),
                _ => return Err(ProtocolError::InvalidField("report kind")),
            };
            Outbound::Report(Report {
                seq,
                order_id,
                client_ref,
                kind,
            })
        }
        BOOK_SNAPSHOT => Outbound::BookSnapshot {
            seq: r.u64(),
            levels: r.u32(),
        },
        LEVEL_UPDATE => {
            let seq = r.u64();
            let side = side_of(r.u8())?;
            r.skip(3);
            let orders = r.u32();
            Outbound::LevelUpdate(LevelUpdate {
                seq,
                side,
                orders,
                price: r.i64(),
                qty: r.u64(),
            })
        }
        TRADE_TICK => {
            let (seq, trade_id) = (r.u64(), r.u64());
            let side = side_of(r.u8())?;
            r.skip(7);
            Outbound::TradeTick(TradeTick {
                seq,
                trade_id,
                side,
                price: r.i64(),
                qty: r.u64(),
            })
        }
        other => return Err(ProtocolError::UnknownType(other)),
    };
    let mut canonical = Vec::with_capacity(len);
    encode_outbound(&message, &mut canonical);
    if canonical != bytes {
        return Err(ProtocolError::NonCanonical);
    }
    Ok(Some((message, len)))
}

/// Defines a code for each value of an enum, both ways.
macro_rules! codes {
    ($code:ident, $of:ident, $ty:ty, $field:literal, [$($value:path => $n:literal),+ $(,)?]) => {
        fn $code(value: $ty) -> u8 {
            match value {
                $($value => $n,)+
            }
        }
        fn $of(code: u8) -> Result<$ty, ProtocolError> {
            match code {
                $($n => Ok($value),)+
                _ => Err(ProtocolError::InvalidField($field)),
            }
        }
    };
}

codes!(side_code, side_of, Side, "side", [Side::Buy => 0, Side::Sell => 1]);
codes!(tif_code, tif_of, TimeInForce, "time in force", [
    TimeInForce::Gtc => 0,
    TimeInForce::Ioc => 1,
    TimeInForce::Fok => 2,
    TimeInForce::PostOnly => 3,
]);
codes!(phase_code, phase_of, Phase, "phase", [
    Phase::Continuous => 0,
    Phase::Auction => 1,
    Phase::Halted => 2,
    Phase::Closed => 3,
]);
codes!(login_error_code, login_error_of, LoginError, "login error", [
    LoginError::UnsupportedVersion => 1,
    LoginError::BadCredentials => 2,
    LoginError::AlreadyLoggedIn => 3,
    LoginError::AlreadyInSession => 4,
]);
codes!(logout_code, logout_of, LogoutReason, "logout reason", [
    LogoutReason::Requested => 1,
    LogoutReason::Idle => 2,
    LogoutReason::ProtocolError => 3,
    LogoutReason::SlowConsumer => 4,
    LogoutReason::Shutdown => 5,
]);
codes!(reject_code, reject_of, RejectCode, "reject reason", [
    RejectCode::TooManyOrders => 1,
    RejectCode::Throttled => 2,
    RejectCode::Unavailable => 3,
]);
codes!(reject_reason_code, reject_reason_of, RejectReason, "reject reason", [
    RejectReason::InvalidQuantity => 1,
    RejectReason::PriceOutOfRange => 2,
    RejectReason::PriceOutsideProtection => 3,
    RejectReason::DuplicateOrderId => 4,
    RejectReason::UnknownOrder => 5,
    RejectReason::BookFull => 6,
    RejectReason::InvalidOwner => 7,
    RejectReason::PostOnlyWouldCross => 8,
    RejectReason::InvalidDisplay => 9,
    RejectReason::PriceOutsideBand => 10,
    RejectReason::StopWouldTrigger => 11,
    RejectReason::PendingStop => 12,
    RejectReason::AuctionCall => 13,
    RejectReason::TradingHalted => 14,
    RejectReason::MarketClosed => 15,
]);
codes!(cancel_reason_code, cancel_reason_of, CancelReason, "cancel reason", [
    CancelReason::Requested => 1,
    CancelReason::NoLiquidity => 2,
    CancelReason::PriceProtection => 3,
    CancelReason::SelfTrade => 4,
    CancelReason::MassCancel => 5,
    CancelReason::ImmediateOrCancel => 6,
    CancelReason::FillOrKill => 7,
    CancelReason::PriceBand => 8,
    CancelReason::TradingPhase => 9,
]);

fn flag_of(code: u8) -> Result<bool, ProtocolError> {
    match code {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(ProtocolError::InvalidField("option flag")),
    }
}
