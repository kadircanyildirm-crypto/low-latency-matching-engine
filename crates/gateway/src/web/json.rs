//! The JSON a browser speaks: one object per WebSocket text message, with a `"type"`.
//!
//! Client to exchange: `register`, `login` (`account`, `token`), `logout`, `heartbeat`,
//! `order` (`ref`, `side`, `qty`, and `price` for a limit order, with `tif` and `display`,
//! or `trigger` and an optional `price` for a stop; neither for a market order), `cancel`
//! (`id`), `modify` (`id`, `price`, `qty`), `cancel_all`, `subscribe`.
//!
//! Exchange to client: the binary protocol's messages under snake-case names
//! (`login_accepted`, `login_rejected`, `heartbeat`, `logout`, `reject`, `report`, `book`,
//! `level`, `trade`, `balance`), plus `registered` (`account`, `token`) and `error`
//! (`message`). A subscribed browser also gets `history` (the last hour as candles),
//! `market` (what is traded: its names, the decimals of its prices and quantities, and where
//! its orders come from), `started` (how the server recovered when it started),
//! `stats_history` (the statistics of
//! the last two minutes), `queues` (the best levels
//! order by order), `log` (the commands the engine sequenced, with
//! what came of them), `leaders` (the most profitable paper accounts) and `rank` (its own
//! place among them); every browser gets `stats`. Codes such as reasons and sides are
//! snake-case strings. Tokens are 16 hexadecimal digits.

use std::fmt::Debug;

use marketdata::Depth;
use orderbook::{Command, Side, TimeInForce};
use serde_json::{Value, json};

use crate::exchange::{Logged, Standing};
use protocol::{Inbound, NewOrder, OrderKind, Outbound, ReportKind, VERSION};
use serde::{Deserialize, Serialize};

/// A message from a browser.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WebIn {
    /// Asks for a new paper-trading account.
    Register,
    /// Opens the session.
    Login {
        /// The account.
        account: u32,
        /// Its token, in hexadecimal.
        token: String,
    },
    /// Closes the session.
    Logout,
    /// Keeps the session alive.
    Heartbeat,
    /// Places an order.
    Order {
        /// The client's reference.
        #[serde(rename = "ref")]
        client_ref: u64,
        /// `buy` or `sell`.
        side: WebSide,
        /// Quantity in lots.
        qty: u64,
        /// The limit price; none for a market or stop-market order.
        price: Option<i64>,
        /// Time in force of a limit order; `gtc` if absent.
        #[serde(default)]
        tif: WebTif,
        /// Iceberg display quantity.
        display: Option<u64>,
        /// The trigger price of a stop order.
        trigger: Option<i64>,
    },
    /// Cancels an order.
    Cancel {
        /// The order's id.
        id: u64,
    },
    /// Changes an order's price and total quantity.
    Modify {
        /// The order's id.
        id: u64,
        /// The new price.
        price: i64,
        /// The new total quantity.
        qty: u64,
    },
    /// Cancels every order of the account.
    CancelAll,
    /// Asks for market data.
    Subscribe,
}

/// A side, in JSON.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSide {
    /// Buy.
    Buy,
    /// Sell.
    Sell,
}

/// A time in force, in JSON.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebTif {
    /// Good till cancelled.
    #[default]
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Fill or kill.
    Fok,
    /// Post only.
    PostOnly,
}

impl From<WebSide> for Side {
    fn from(side: WebSide) -> Side {
        match side {
            WebSide::Buy => Side::Buy,
            WebSide::Sell => Side::Sell,
        }
    }
}

impl From<Side> for WebSide {
    fn from(side: Side) -> WebSide {
        match side {
            Side::Buy => WebSide::Buy,
            Side::Sell => WebSide::Sell,
        }
    }
}

/// Parses a browser's message.
pub fn parse(text: &str) -> Result<WebIn, serde_json::Error> {
    serde_json::from_str(text)
}

impl WebIn {
    /// The protocol message this is, if it is one: `register` is not. A token that is not
    /// 16 hexadecimal digits is `None` too, and ends the session.
    pub fn inbound(&self) -> Option<Inbound> {
        Some(match *self {
            WebIn::Register => return None,
            WebIn::Login { account, ref token } => Inbound::Login {
                version: VERSION,
                account,
                token: parse_token(token)?,
            },
            WebIn::Logout => Inbound::Logout,
            WebIn::Heartbeat => Inbound::Heartbeat,
            WebIn::Order {
                client_ref,
                side,
                qty,
                price,
                tif,
                display,
                trigger,
            } => {
                let kind = match (trigger, price) {
                    (Some(trigger), limit) => OrderKind::Stop { trigger, limit },
                    (None, Some(price)) => OrderKind::Limit {
                        price,
                        tif: match tif {
                            WebTif::Gtc => TimeInForce::Gtc,
                            WebTif::Ioc => TimeInForce::Ioc,
                            WebTif::Fok => TimeInForce::Fok,
                            WebTif::PostOnly => TimeInForce::PostOnly,
                        },
                        display,
                    },
                    (None, None) => OrderKind::Market,
                };
                Inbound::NewOrder(NewOrder {
                    client_ref,
                    side: side.into(),
                    qty,
                    kind,
                })
            }
            WebIn::Cancel { id } => Inbound::Cancel { order_id: id },
            WebIn::Modify { id, price, qty } => Inbound::Modify {
                order_id: id,
                price,
                qty,
            },
            WebIn::CancelAll => Inbound::MassCancel,
            WebIn::Subscribe => Inbound::Subscribe,
        })
    }
}

/// A token as a browser sends it.
pub fn format_token(token: u64) -> String {
    format!("{token:016x}")
}

fn parse_token(text: &str) -> Option<u64> {
    (text.len() == 16 && text.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| u64::from_str_radix(text, 16).ok())
        .flatten()
}

/// A message to a browser.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WebOut {
    LoginAccepted {
        account: u32,
        last_seq: u64,
    },
    LoginRejected {
        reason: String,
    },
    Heartbeat,
    Logout {
        reason: String,
    },
    Reject {
        reason: String,
        #[serde(rename = "ref")]
        client_ref: u64,
    },
    Report {
        seq: u64,
        id: u64,
        #[serde(rename = "ref")]
        client_ref: u64,
        kind: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        side: Option<WebSide>,
        #[serde(skip_serializing_if = "Option::is_none")]
        price: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        qty: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        leaves: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        visible: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        trade_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        trigger: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        count: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        phase: Option<String>,
    },
    Book {
        seq: u64,
        levels: u32,
    },
    Level {
        seq: u64,
        side: WebSide,
        price: i64,
        qty: u64,
        orders: u32,
    },
    Trade {
        seq: u64,
        trade_id: u64,
        side: WebSide,
        price: i64,
        qty: u64,
    },
    Balance {
        cash: i64,
        position: i64,
        cash_held: i64,
        position_held: i64,
    },
    Registered {
        account: u32,
        token: String,
    },
    Error {
        message: String,
    },
}

/// A code's name in snake case: `PriceOutOfRange` becomes `price_out_of_range`.
fn name(value: impl Debug) -> String {
    let debug = format!("{value:?}");
    let mut out = String::with_capacity(debug.len() + 4);
    for (i, c) in debug.chars().enumerate() {
        if c.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// The JSON for a protocol message.
pub fn outbound(message: &Outbound) -> String {
    let web = match *message {
        Outbound::LoginAccepted { account, last_seq } => {
            WebOut::LoginAccepted { account, last_seq }
        }
        Outbound::LoginRejected { reason } => WebOut::LoginRejected {
            reason: name(reason),
        },
        Outbound::Heartbeat => WebOut::Heartbeat,
        Outbound::Logout { reason } => WebOut::Logout {
            reason: name(reason),
        },
        Outbound::Reject { reason, client_ref } => WebOut::Reject {
            reason: name(reason),
            client_ref,
        },
        Outbound::Report(report) => {
            let mut out = WebOut::Report {
                seq: report.seq,
                id: report.order_id,
                client_ref: report.client_ref,
                kind: "",
                side: None,
                price: None,
                qty: None,
                leaves: None,
                visible: None,
                trade_id: None,
                trigger: None,
                limit: None,
                count: None,
                reason: None,
                phase: None,
            };
            let WebOut::Report {
                kind: k,
                side: s,
                price: p,
                qty: q,
                leaves: l,
                visible: v,
                trade_id: t,
                trigger: tr,
                limit: li,
                count: c,
                reason: r,
                phase: ph,
                ..
            } = &mut out
            else {
                unreachable!()
            };
            match report.kind {
                ReportKind::Accepted => *k = "accepted",
                ReportKind::Rejected(reason) => {
                    *k = "rejected";
                    *r = Some(name(reason));
                }
                ReportKind::Fill {
                    trade_id,
                    side,
                    price,
                    qty,
                    leaves,
                } => {
                    *k = "fill";
                    (*t, *s, *p, *q, *l) = (
                        Some(trade_id),
                        Some(side.into()),
                        Some(price),
                        Some(qty),
                        Some(leaves),
                    );
                }
                ReportKind::Rested {
                    side,
                    price,
                    qty,
                    visible,
                } => {
                    *k = "rested";
                    (*s, *p, *q, *v) = (Some(side.into()), Some(price), Some(qty), Some(visible));
                }
                ReportKind::Replenished {
                    side,
                    price,
                    visible,
                } => {
                    *k = "replenished";
                    (*s, *p, *v) = (Some(side.into()), Some(price), Some(visible));
                }
                ReportKind::Cancelled { qty, reason } => {
                    *k = "cancelled";
                    (*q, *r) = (Some(qty), Some(name(reason)));
                }
                ReportKind::Modified { price, qty, leaves } => {
                    *k = "modified";
                    (*p, *q, *l) = (Some(price), Some(qty), Some(leaves));
                }
                ReportKind::StopPlaced {
                    side,
                    trigger,
                    limit,
                    qty,
                } => {
                    *k = "stop_placed";
                    (*s, *tr, *li, *q) = (Some(side.into()), Some(trigger), limit, Some(qty));
                }
                ReportKind::Triggered => *k = "triggered",
                ReportKind::MassCancelled { count } => {
                    *k = "mass_cancelled";
                    *c = Some(count);
                }
                ReportKind::PhaseChanged(phase) => {
                    *k = "phase_changed";
                    *ph = Some(name(phase));
                }
            }
            out
        }
        Outbound::BookSnapshot { seq, levels } => WebOut::Book { seq, levels },
        Outbound::LevelUpdate(level) => WebOut::Level {
            seq: level.seq,
            side: level.side.into(),
            price: level.price,
            qty: level.qty,
            orders: level.orders,
        },
        Outbound::Balance(balance) => WebOut::Balance {
            cash: balance.cash,
            position: balance.position,
            cash_held: balance.cash_held,
            position_held: balance.position_held,
        },
        Outbound::TradeTick(trade) => WebOut::Trade {
            seq: trade.seq,
            trade_id: trade.trade_id,
            side: trade.side.into(),
            price: trade.price,
            qty: trade.qty,
        },
    };
    serde_json::to_string(&web).expect("a message serialises")
}

/// How the exchange is doing, as browsers are told every second.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Stats {
    /// Commands numbered per second.
    pub commands_per_second: u64,
    /// How long a turn of the server that handed commands to the engine took: journaling,
    /// syncing, matching and routing a round's commands with the engine on the server's
    /// thread, handing them over and routing what came back with a pipeline.
    pub turn_p50_ns: u64,
    /// See `turn_p50_ns`.
    pub turn_p99_ns: u64,
    /// See `turn_p50_ns`.
    pub turn_max_ns: u64,
    /// Connections with a session.
    pub sessions: u64,
    /// Orders resting on the book.
    pub orders: u64,
    /// The server's clock, in milliseconds since the Unix epoch, so a browser can place
    /// trades in the candles it has from the server.
    pub time: u64,
    /// How many of those turns took how long: the first bucket counts turns under a
    /// microsecond, bucket `i` from 1 to 16 those from 2^(i-1) up to 2^i microseconds, and
    /// the last the turns of 65.536 ms or more.
    pub turn_buckets: [u64; TURN_BUCKETS],
    /// Inside the engine, on average over the same period: nanoseconds appending a batch
    /// to the journal, ...
    pub write_ns: u64,
    /// ... syncing the journal per batch, ...
    pub sync_ns: u64,
    /// ... with this many commands in a batch, ...
    pub batch_commands: u64,
    /// ... applying a command, with the exchange's routing of its events, ...
    pub apply_ns: u64,
    /// ... and matching alone, the book's own time on the `matched` commands it measured:
    /// one in 64. Zero for nothing measured.
    pub match_ns: u64,
    /// See `match_ns`.
    pub matched: u64,
}

/// What is traded, as a browser is told when it subscribes: prices are integer ticks of
/// `10^-price_decimals` of the quote currency, quantities integer lots of
/// `10^-lot_decimals` of the base asset, and cash is ticks times lots.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Market {
    /// Such as `ETH/USD`.
    pub symbol: String,
    /// Such as `ETH`.
    pub base: String,
    /// Such as `USD`.
    pub quote: String,
    /// The decimals of a price.
    pub price_decimals: u32,
    /// The decimals of a quantity.
    pub lot_decimals: u32,
    /// Where the market's orders come from, if it mirrors another venue: its name, for
    /// attribution.
    pub source: Option<String>,
    /// See `source`: where to find it.
    pub source_url: Option<String>,
}

impl Default for Market {
    /// The bots' demo market: whole lots of DEMO, priced in cents.
    fn default() -> Market {
        Market {
            symbol: "DEMO/USD".to_owned(),
            base: "DEMO".to_owned(),
            quote: "USD".to_owned(),
            price_decimals: 2,
            lot_decimals: 0,
            source: None,
            source_url: None,
        }
    }
}

/// The JSON for `market`.
pub fn market(market: &Market) -> String {
    let mut value = serde_json::to_value(market).expect("a market serialises");
    value["type"] = Value::from("market");
    value.to_string()
}

/// How the server started: what it recovered, and how long that took.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Started {
    /// When it started, in milliseconds since the Unix epoch.
    pub started: u64,
    /// The sequence number of the last command it recovered.
    pub recovered: u64,
    /// The snapshot recovery started from: the commands after it were replayed from the
    /// journal. Zero for none.
    pub snapshot: u64,
    /// The orders back on the book.
    pub orders: u64,
    /// How long opening the engine and the exchange took, in milliseconds.
    pub recovery_ms: u64,
    /// The book's state digest after recovery.
    #[serde(serialize_with = "hex")]
    pub digest: u64,
}

fn hex<S: serde::Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&format!("{value:016x}"))
}

/// The JSON for `started`.
pub fn started(started: &Started) -> String {
    let mut value = serde_json::to_value(started).expect("a start serialises");
    value["type"] = Value::from("started");
    value.to_string()
}

/// The JSON for the statistics of the last seconds, oldest first, as `stats_history`.
pub fn stats_history<'a>(stats: impl Iterator<Item = &'a Stats>) -> String {
    let stats: Vec<&Stats> = stats.collect();
    json!({"type": "stats_history", "stats": stats}).to_string()
}

/// Buckets of [`Stats::turn_buckets`].
pub const TURN_BUCKETS: usize = 18;

/// The bucket of [`Stats::turn_buckets`] a turn of `ns` nanoseconds falls in.
pub fn turn_bucket(ns: u64) -> usize {
    let micros = ns / 1_000;
    let doublings = (u64::BITS - micros.leading_zeros()) as usize;
    doublings.min(TURN_BUCKETS - 1)
}

/// Orders shown at most at a level of [`queues`]; those behind them come summed.
pub const QUEUE_SHOWN: usize = 24;

/// The JSON for the best `levels` levels of each side, order by order: `buy` and `sell`,
/// best first, each level `[price, [id, qty, id, qty, ...]]` with its orders in the order
/// they trade and the quantity they show. Past [`QUEUE_SHOWN`] orders, the others come as
/// one entry with id 0 and their total.
pub fn queues(depth: &Depth, levels: usize) -> String {
    let half = |side| -> Vec<Value> {
        depth
            .levels(side)
            .take(levels)
            .map(|(price, _)| {
                let mut flat = Vec::new();
                let (mut behind, mut rest) = (0, 0);
                for entry in depth.queue(side, price) {
                    if flat.len() < 2 * QUEUE_SHOWN {
                        flat.extend([entry.id, entry.visible]);
                    } else {
                        behind += 1;
                        rest += entry.visible;
                    }
                }
                if behind > 0 {
                    flat.extend([0, rest]);
                }
                json!([price, flat])
            })
            .collect()
    };
    json!({"type": "queues", "buy": half(Side::Buy), "sell": half(Side::Sell)}).to_string()
}

/// The JSON for commands of the engine log, oldest first, after `skipped` older ones that
/// are not shown. Each has its `seq`, `cmd` and fields, whether a visitor sent it
/// (`paper`), and what came of it: `trades` and `traded`, `rested`, `cancelled` and
/// `cancelled_qty`, `rejected`, each only if there is something to say.
pub fn log(skipped: usize, entries: &[Logged]) -> String {
    let entries: Vec<Value> = entries.iter().map(logged).collect();
    json!({"type": "log", "skipped": skipped, "entries": entries}).to_string()
}

fn logged(logged: &Logged) -> Value {
    let mut entry = match logged.command {
        Command::Limit {
            owner,
            side,
            price,
            qty,
            tif,
            display,
            ..
        } => {
            let mut entry = json!({"cmd": "limit", "owner": owner, "side": name(side),
                "price": price, "qty": qty, "tif": name(tif)});
            if let Some(display) = display {
                entry["display"] = json!(display);
            }
            entry
        }
        Command::Market {
            owner, side, qty, ..
        } => json!({"cmd": "market", "owner": owner, "side": name(side), "qty": qty}),
        Command::Stop {
            owner,
            side,
            trigger,
            limit,
            qty,
            ..
        } => json!({"cmd": "stop", "owner": owner, "side": name(side), "trigger": trigger,
            "price": limit, "qty": qty}),
        Command::Cancel { id, owner } => json!({"cmd": "cancel", "owner": owner, "id": id}),
        Command::Modify {
            id,
            owner,
            price,
            qty,
        } => json!({"cmd": "modify", "owner": owner, "id": id, "price": price, "qty": qty}),
        Command::CancelAll { owner } => json!({"cmd": "cancel_all", "owner": owner}),
        Command::SetPhase { phase } => json!({"cmd": "set_phase", "phase": name(phase)}),
    };
    entry["seq"] = json!(logged.seq);
    entry["paper"] = json!(logged.paper);
    let outcome = logged.outcome;
    if outcome.trades > 0 {
        entry["trades"] = json!(outcome.trades);
        entry["traded"] = json!(outcome.traded);
    }
    if outcome.rested > 0 {
        entry["rested"] = json!(outcome.rested);
    }
    if outcome.cancelled > 0 {
        entry["cancelled"] = json!(outcome.cancelled);
        entry["cancelled_qty"] = json!(outcome.cancelled_qty);
    }
    if let Some(reason) = outcome.rejected {
        entry["rejected"] = json!(name(reason));
    }
    entry
}

/// The JSON for the best of `standings`, out of `total` paper accounts that have traded.
pub fn leaders(standings: &[Standing], total: usize) -> String {
    let leaders: Vec<Value> = standings
        .iter()
        .map(|s| json!({"account": s.account, "value": s.value, "profit": s.profit}))
        .collect();
    json!({"type": "leaders", "total": total, "leaders": leaders}).to_string()
}

/// The JSON telling a browser its account's place among `of` paper accounts that traded.
pub fn rank(rank: usize, of: usize) -> String {
    json!({"type": "rank", "rank": rank, "of": of}).to_string()
}

/// The JSON for `stats`, with `"type": "stats"`.
pub fn stats(stats: &Stats) -> String {
    let mut value = serde_json::to_value(stats).expect("statistics serialise");
    value["type"] = serde_json::Value::from("stats");
    value.to_string()
}

/// The JSON for the last hour of trades, as candles of `interval` seconds.
pub fn history<'a>(
    interval: u64,
    candles: impl Iterator<Item = &'a crate::candles::Candle>,
) -> String {
    let candles: Vec<_> = candles.collect();
    serde_json::json!({"type": "history", "interval": interval, "candles": candles}).to_string()
}

/// The JSON telling a browser its new account.
pub fn registered(account: u32, token: u64) -> String {
    let web = WebOut::Registered {
        account,
        token: format_token(token),
    };
    serde_json::to_string(&web).expect("a message serialises")
}

/// The JSON telling a browser why its request failed, before its session ends.
pub fn error(message: &str) -> String {
    let web = WebOut::Error {
        message: message.to_owned(),
    };
    serde_json::to_string(&web).expect("a message serialises")
}

#[cfg(test)]
mod tests {
    use super::*;
    use orderbook::{CancelReason, Phase, RejectReason};
    use protocol::{LoginError, LogoutReason, RejectCode, Report};

    #[test]
    fn browser_messages_become_protocol_messages() {
        let cases = [
            (r#"{"type":"logout"}"#, Some(Inbound::Logout)),
            (r#"{"type":"heartbeat"}"#, Some(Inbound::Heartbeat)),
            (r#"{"type":"cancel_all"}"#, Some(Inbound::MassCancel)),
            (r#"{"type":"subscribe"}"#, Some(Inbound::Subscribe)),
            (
                r#"{"type":"cancel","id":7}"#,
                Some(Inbound::Cancel { order_id: 7 }),
            ),
            (
                r#"{"type":"modify","id":7,"price":101,"qty":3}"#,
                Some(Inbound::Modify {
                    order_id: 7,
                    price: 101,
                    qty: 3,
                }),
            ),
            (
                r#"{"type":"login","account":3,"token":"00000000000000ff"}"#,
                Some(Inbound::Login {
                    version: VERSION,
                    account: 3,
                    token: 255,
                }),
            ),
            (r#"{"type":"login","account":3,"token":"ff"}"#, None),
            (r#"{"type":"register"}"#, None),
        ];
        for (text, inbound) in cases {
            assert_eq!(parse(text).unwrap().inbound(), inbound, "{text}");
        }
        let order = |text: &str| match parse(text).unwrap().inbound() {
            Some(Inbound::NewOrder(order)) => order,
            other => panic!("{other:?}"),
        };
        let limit = order(
            r#"{"type":"order","ref":1,"side":"buy","qty":5,"price":100,"tif":"ioc","display":2}"#,
        );
        assert_eq!((limit.client_ref, limit.side, limit.qty), (1, Side::Buy, 5));
        assert_eq!(
            limit.kind,
            OrderKind::Limit {
                price: 100,
                tif: TimeInForce::Ioc,
                display: Some(2)
            }
        );
        let market = order(r#"{"type":"order","ref":2,"side":"sell","qty":1}"#);
        assert_eq!(market.kind, OrderKind::Market);
        let stop = order(r#"{"type":"order","ref":3,"side":"sell","qty":1,"trigger":90}"#);
        assert_eq!(
            stop.kind,
            OrderKind::Stop {
                trigger: 90,
                limit: None
            }
        );
        let gtc = order(r#"{"type":"order","ref":4,"side":"buy","qty":1,"price":9}"#);
        assert!(matches!(
            gtc.kind,
            OrderKind::Limit {
                tif: TimeInForce::Gtc,
                ..
            }
        ));
        for bad in [
            "",
            "{}",
            r#"{"type":"unknown"}"#,
            r#"{"type":"order","ref":1,"side":"up","qty":1}"#,
            r#"{"type":"cancel","id":-1}"#,
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn protocol_messages_become_json() {
        let check = |message: Outbound, json: &str| {
            let got: serde_json::Value = serde_json::from_str(&outbound(&message)).unwrap();
            let want: serde_json::Value = serde_json::from_str(json).unwrap();
            assert_eq!(got, want);
        };
        check(
            Outbound::LoginAccepted {
                account: 3,
                last_seq: 9,
            },
            r#"{"type":"login_accepted","account":3,"last_seq":9}"#,
        );
        check(
            Outbound::LoginRejected {
                reason: LoginError::BadCredentials,
            },
            r#"{"type":"login_rejected","reason":"bad_credentials"}"#,
        );
        check(
            Outbound::Logout {
                reason: LogoutReason::SlowConsumer,
            },
            r#"{"type":"logout","reason":"slow_consumer"}"#,
        );
        check(
            Outbound::Reject {
                reason: RejectCode::TooManyOrders,
                client_ref: 4,
            },
            r#"{"type":"reject","reason":"too_many_orders","ref":4}"#,
        );
        let report = |kind| {
            Outbound::Report(Report {
                seq: 5,
                order_id: 2,
                client_ref: 8,
                kind,
            })
        };
        check(
            report(ReportKind::Fill {
                trade_id: 1,
                side: Side::Sell,
                price: 100,
                qty: 3,
                leaves: 0,
            }),
            r#"{"type":"report","seq":5,"id":2,"ref":8,"kind":"fill","trade_id":1,"side":"sell","price":100,"qty":3,"leaves":0}"#,
        );
        check(
            report(ReportKind::Rejected(RejectReason::PriceOutOfRange)),
            r#"{"type":"report","seq":5,"id":2,"ref":8,"kind":"rejected","reason":"price_out_of_range"}"#,
        );
        check(
            report(ReportKind::Cancelled {
                qty: 2,
                reason: CancelReason::SelfTrade,
            }),
            r#"{"type":"report","seq":5,"id":2,"ref":8,"kind":"cancelled","qty":2,"reason":"self_trade"}"#,
        );
        check(
            report(ReportKind::StopPlaced {
                side: Side::Buy,
                trigger: 105,
                limit: None,
                qty: 1,
            }),
            r#"{"type":"report","seq":5,"id":2,"ref":8,"kind":"stop_placed","side":"buy","trigger":105,"qty":1}"#,
        );
        check(
            report(ReportKind::PhaseChanged(Phase::Auction)),
            r#"{"type":"report","seq":5,"id":2,"ref":8,"kind":"phase_changed","phase":"auction"}"#,
        );
        check(
            Outbound::LevelUpdate(protocol::LevelUpdate {
                seq: 6,
                side: Side::Buy,
                price: 99,
                qty: 10,
                orders: 2,
            }),
            r#"{"type":"level","seq":6,"side":"buy","price":99,"qty":10,"orders":2}"#,
        );
        assert_eq!(
            registered(4, 255),
            r#"{"type":"registered","account":4,"token":"00000000000000ff"}"#
        );
        assert_eq!(error("full"), r#"{"type":"error","message":"full"}"#);
        let stats: serde_json::Value = serde_json::from_str(&super::stats(&Stats {
            commands_per_second: 3,
            turn_p50_ns: 4,
            turn_p99_ns: 5,
            turn_max_ns: 6,
            sessions: 7,
            orders: 8,
            time: 9,
            turn_buckets: [1; TURN_BUCKETS],
            write_ns: 10,
            sync_ns: 11,
            batch_commands: 12,
            apply_ns: 13,
            match_ns: 14,
            matched: 15,
        }))
        .unwrap();
        assert_eq!(
            stats,
            serde_json::json!({"type": "stats", "commands_per_second": 3, "turn_p50_ns": 4,
                "turn_p99_ns": 5, "turn_max_ns": 6, "sessions": 7, "orders": 8, "time": 9,
                "turn_buckets": vec![1; TURN_BUCKETS], "write_ns": 10, "sync_ns": 11,
                "batch_commands": 12, "apply_ns": 13, "match_ns": 14, "matched": 15})
        );
        let buckets = [
            0, 999, 1_000, 1_999, 2_000, 3_999, 4_000, 65_535_999, 65_536_000,
        ];
        assert_eq!(
            buckets.map(turn_bucket),
            [0, 0, 1, 1, 2, 2, 3, 16, TURN_BUCKETS - 1]
        );
        assert_eq!(turn_bucket(u64::MAX), TURN_BUCKETS - 1);
        let second = Stats {
            commands_per_second: 1,
            ..Stats::default()
        };
        let history: Value = serde_json::from_str(&stats_history([second, second].iter())).unwrap();
        assert_eq!(history["type"], "stats_history");
        assert_eq!(history["stats"].as_array().unwrap().len(), 2);
        assert_eq!(history["stats"][1]["commands_per_second"], 1);
        assert_eq!(
            serde_json::from_str::<Value>(&market(&Market::default())).unwrap(),
            json!({"type": "market", "symbol": "DEMO/USD", "base": "DEMO", "quote": "USD",
                "price_decimals": 2, "lot_decimals": 0, "source": null, "source_url": null})
        );
        let start = Started {
            started: 1,
            recovered: 2,
            snapshot: 3,
            orders: 4,
            recovery_ms: 5,
            digest: 0xab,
        };
        assert_eq!(
            serde_json::from_str::<Value>(&started(&start)).unwrap(),
            json!({"type": "started", "started": 1, "recovered": 2, "snapshot": 3,
                "orders": 4, "recovery_ms": 5, "digest": "00000000000000ab"})
        );
        let mut candles = crate::candles::Candles::default();
        candles.record(1_000, 100, 2);
        let history: serde_json::Value =
            serde_json::from_str(&super::history(5, candles.iter())).unwrap();
        assert_eq!(
            history,
            serde_json::json!({"type": "history", "interval": 5,
                "candles": [{"t": 1000, "o": 100, "h": 100, "l": 100, "c": 100, "v": 2}]})
        );
    }

    #[test]
    fn what_a_browser_is_shown_of_the_engine() {
        use crate::exchange::{Logged, Outcome, Standing};
        use orderbook::{Event, Phase};
        let parse = |text: String| serde_json::from_str::<Value>(&text).unwrap();
        // Two orders at 100 and one at 101 to buy, and more than are shown at 105 to sell.
        let mut depth = Depth::new();
        let rest = |id, side, price, qty| Event::Rested {
            id,
            side,
            price,
            qty,
            visible: qty,
        };
        for event in [
            rest(1, Side::Buy, 100, 5),
            rest(2, Side::Buy, 101, 3),
            rest(3, Side::Buy, 100, 7),
        ] {
            depth.apply(&event);
        }
        for id in 0..QUEUE_SHOWN as u64 + 2 {
            depth.apply(&rest(10 + id, Side::Sell, 105, 1 + id));
        }
        let mut sell: Vec<u64> = (0..QUEUE_SHOWN as u64)
            .flat_map(|id| [10 + id, 1 + id])
            .collect();
        let behind = (QUEUE_SHOWN as u64 + 1) + (QUEUE_SHOWN as u64 + 2);
        sell.extend([0, behind]);
        assert_eq!(
            parse(queues(&depth, 10)),
            json!({"type": "queues", "buy": [[101, [2, 3]], [100, [1, 5, 3, 7]]],
                "sell": [[105, sell]]})
        );
        assert_eq!(
            parse(queues(&depth, 1)),
            json!({"type": "queues", "buy": [[101, [2, 3]]], "sell": [[105, sell]]})
        );

        let entry = |seq, command, outcome| Logged {
            seq,
            command,
            paper: seq % 2 == 0,
            outcome,
        };
        let entries = [
            entry(
                7,
                Command::Limit {
                    id: 7,
                    owner: 3,
                    side: Side::Sell,
                    price: 105,
                    qty: 10,
                    tif: TimeInForce::PostOnly,
                    display: Some(2),
                },
                Outcome {
                    rested: 10,
                    ..Outcome::default()
                },
            ),
            entry(
                8,
                Command::Market {
                    id: 8,
                    owner: 4,
                    side: Side::Buy,
                    qty: 4,
                },
                Outcome {
                    trades: 2,
                    traded: 3,
                    cancelled: 1,
                    cancelled_qty: 1,
                    ..Outcome::default()
                },
            ),
            entry(
                9,
                Command::Stop {
                    id: 9,
                    owner: 3,
                    side: Side::Sell,
                    trigger: 90,
                    limit: None,
                    qty: 1,
                },
                Outcome::default(),
            ),
            entry(
                10,
                Command::Cancel { id: 1, owner: 4 },
                Outcome {
                    rejected: Some(RejectReason::UnknownOrder),
                    ..Outcome::default()
                },
            ),
            entry(
                11,
                Command::Modify {
                    id: 7,
                    owner: 3,
                    price: 104,
                    qty: 6,
                },
                Outcome::default(),
            ),
            entry(12, Command::CancelAll { owner: 3 }, Outcome::default()),
            entry(
                13,
                Command::SetPhase {
                    phase: Phase::Halted,
                },
                Outcome::default(),
            ),
        ];
        assert_eq!(
            parse(log(4, &entries)),
            json!({"type": "log", "skipped": 4, "entries": [
                {"seq": 7, "paper": false, "cmd": "limit", "owner": 3, "side": "sell",
                    "price": 105, "qty": 10, "tif": "post_only", "display": 2, "rested": 10},
                {"seq": 8, "paper": true, "cmd": "market", "owner": 4, "side": "buy", "qty": 4,
                    "trades": 2, "traded": 3, "cancelled": 1, "cancelled_qty": 1},
                {"seq": 9, "paper": false, "cmd": "stop", "owner": 3, "side": "sell",
                    "trigger": 90, "price": null, "qty": 1},
                {"seq": 10, "paper": true, "cmd": "cancel", "owner": 4, "id": 1,
                    "rejected": "unknown_order"},
                {"seq": 11, "paper": false, "cmd": "modify", "owner": 3, "id": 7,
                    "price": 104, "qty": 6},
                {"seq": 12, "paper": true, "cmd": "cancel_all", "owner": 3},
                {"seq": 13, "paper": false, "cmd": "set_phase", "phase": "halted"},
            ]})
        );

        let standing = Standing {
            account: 101,
            value: 500,
            profit: -20,
        };
        assert_eq!(
            parse(leaders(&[standing], 3)),
            json!({"type": "leaders", "total": 3,
                "leaders": [{"account": 101, "value": 500, "profit": -20}]})
        );
        assert_eq!(
            parse(rank(2, 3)),
            json!({"type": "rank", "rank": 2, "of": 3})
        );
    }
}
