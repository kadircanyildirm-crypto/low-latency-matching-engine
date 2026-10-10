//! The JSON a browser speaks: one object per WebSocket text message, with a `"type"`.
//!
//! Client to exchange: `register`, `login` (`account`, `token`), `logout`, `heartbeat`,
//! `order` (`ref`, `side`, `qty`, and `price` for a limit order, with `tif` and `display`,
//! or `trigger` and an optional `price` for a stop; neither for a market order), `cancel`
//! (`id`), `modify` (`id`, `price`, `qty`), `cancel_all`, `subscribe`.
//!
//! Exchange to client: the binary protocol's messages under snake-case names
//! (`login_accepted`, `login_rejected`, `heartbeat`, `logout`, `reject`, `report`, `book`,
//! `level`, `trade`, `balance`), plus `registered` (`account`, `token`) and `error` (`message`). Codes
//! such as reasons and sides are snake-case strings. Tokens are 16 hexadecimal digits.

use std::fmt::Debug;

use orderbook::{Side, TimeInForce};
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
        }))
        .unwrap();
        assert_eq!(
            stats,
            serde_json::json!({"type": "stats", "commands_per_second": 3, "turn_p50_ns": 4,
                "turn_p99_ns": 5, "turn_max_ns": 6, "sessions": 7, "orders": 8, "time": 9})
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
}
