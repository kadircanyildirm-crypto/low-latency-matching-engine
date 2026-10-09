//! The gateway's rules, without sockets: logins, order ids, risk limits, routing, timing,
//! cancel-on-disconnect, recovery, and engine failure.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::path::Path;

use engine::sim::SimStorage;
use engine::{Discard, Engine, EngineConfig};
use gateway::{Account, Exchange, Mailbox, SessionId, SetupError, Timing};
use orderbook::{
    BookConfig, CancelReason, Command, OrderBook, Phase, RejectReason, Side, TimeInForce,
};
use proptest::prelude::*;
use protocol::{
    Inbound, LoginError, LogoutReason, NewOrder, OrderKind, Outbound, RejectCode, Report,
    ReportKind, VERSION,
};

const DIR: &str = "data";
const SECOND: u64 = 1_000_000_000;

/// Collects what the exchange sends.
#[derive(Default)]
struct Mail {
    sent: Vec<(SessionId, Outbound)>,
    closed: Vec<SessionId>,
}

impl Mailbox for Mail {
    fn send(&mut self, session: SessionId, message: Outbound) {
        self.sent.push((session, message));
    }

    fn close(&mut self, session: SessionId) {
        self.closed.push(session);
    }
}

impl Mail {
    /// Takes what was sent to `session`.
    fn take(&mut self, session: SessionId) -> Vec<Outbound> {
        let (to, rest) = std::mem::take(&mut self.sent)
            .into_iter()
            .partition(|(to, _)| *to == session);
        self.sent = rest;
        to.into_iter().map(|(_, message)| message).collect()
    }

    /// Takes the reports sent to `session`.
    fn reports(&mut self, session: SessionId) -> Vec<Report> {
        self.take(session)
            .into_iter()
            .map(|message| match message {
                Outbound::Report(report) => report,
                other => panic!("not a report: {other:?}"),
            })
            .collect()
    }
}

/// An exchange with an engine on the same thread, as the single-threaded server runs them.
struct Direct {
    exchange: Exchange,
    engine: Engine<SimStorage>,
}

impl Direct {
    fn new(
        engine: Engine<SimStorage>,
        accounts: &[Account],
        timing: Timing,
    ) -> Result<Direct, SetupError> {
        let exchange = Exchange::new(engine.book(), engine.last_seq(), accounts, timing)?;
        Ok(Direct { exchange, engine })
    }

    fn flush(&mut self, mail: &mut Mail) -> Result<(), engine::Error> {
        self.exchange.flush(&mut self.engine, mail)
    }

    fn book(&self) -> &OrderBook {
        self.engine.book()
    }

    fn into_engine(self) -> Engine<SimStorage> {
        self.engine
    }
}

impl Deref for Direct {
    type Target = Exchange;

    fn deref(&self) -> &Exchange {
        &self.exchange
    }
}

impl DerefMut for Direct {
    fn deref_mut(&mut self) -> &mut Exchange {
        &mut self.exchange
    }
}

fn book() -> BookConfig {
    BookConfig {
        max_owners: 8,
        ..BookConfig::new(1, 1_000, 64)
    }
}

fn account(id: u32) -> Account {
    Account {
        id,
        token: 100 + u64::from(id),
        max_open_orders: 10,
        messages_per_second: 1_000,
    }
}

fn open(storage: &SimStorage, book: BookConfig) -> Engine<SimStorage> {
    let config = EngineConfig::new(book);
    Engine::open_with(storage.clone(), Path::new(DIR), config, &mut Discard)
        .unwrap()
        .0
}

fn exchange(accounts: &[Account]) -> Direct {
    Direct::new(
        open(&SimStorage::new(), book()),
        accounts,
        Timing::default(),
    )
    .unwrap()
}

fn login(account: u32) -> Inbound {
    Inbound::Login {
        version: VERSION,
        account,
        token: 100 + u64::from(account),
    }
}

/// Connects `session` and logs it in for `account`.
fn logged_in(exchange: &mut Exchange, session: SessionId, account: u32, mail: &mut Mail) {
    exchange.connect(session, 0);
    exchange.receive(session, login(account), 0, mail);
    assert!(matches!(
        mail.take(session)[..],
        [Outbound::LoginAccepted { .. }]
    ));
}

fn limit(client_ref: u64, side: Side, price: i64, qty: u64) -> Inbound {
    Inbound::NewOrder(NewOrder {
        client_ref,
        side,
        qty,
        kind: OrderKind::Limit {
            price,
            tif: TimeInForce::Gtc,
            display: None,
        },
    })
}

#[test]
fn logins_are_checked() {
    let mut exchange = exchange(&[account(1), account(2)]);
    let mut mail = Mail::default();
    let attempts = [
        (
            Inbound::Login {
                version: VERSION + 1,
                account: 1,
                token: 101,
            },
            LoginError::UnsupportedVersion,
        ),
        (
            Inbound::Login {
                version: VERSION,
                account: 1,
                token: 102,
            },
            LoginError::BadCredentials,
        ),
        // No such account, inside and outside the book's owners.
        (login(3), LoginError::BadCredentials),
        (login(9_999), LoginError::BadCredentials),
    ];
    for (session, (message, reason)) in attempts.into_iter().enumerate() {
        exchange.connect(session, 0);
        exchange.receive(session, message, 0, &mut mail);
        assert_eq!(mail.take(session), [Outbound::LoginRejected { reason }]);
        assert_eq!(mail.closed, [session]);
        mail.closed.clear();
        // A refused session is not listened to any more.
        exchange.receive(session, login(1), 0, &mut mail);
        assert!(mail.sent.is_empty());
        exchange.disconnect(session);
    }
    // Anything before a login ends the session.
    exchange.connect(10, 0);
    exchange.receive(10, Inbound::Heartbeat, 0, &mut mail);
    let reason = LogoutReason::ProtocolError;
    assert_eq!(mail.take(10), [Outbound::Logout { reason }]);
    assert_eq!(mail.closed, [10]);
    mail.closed.clear();

    // One session per account.
    exchange.connect(11, 0);
    exchange.receive(11, login(1), 0, &mut mail);
    let last_seq = 0;
    assert_eq!(
        mail.take(11),
        [Outbound::LoginAccepted {
            account: 1,
            last_seq
        }]
    );
    exchange.connect(12, 0);
    exchange.receive(12, login(1), 0, &mut mail);
    let reason = LoginError::AlreadyLoggedIn;
    assert_eq!(mail.take(12), [Outbound::LoginRejected { reason }]);
    // A second login in a session is refused, but the session goes on.
    exchange.receive(11, login(2), 0, &mut mail);
    let reason = LoginError::AlreadyInSession;
    assert_eq!(mail.take(11), [Outbound::LoginRejected { reason }]);
    assert_eq!(mail.closed, [12]);
    // Once the first session is gone, the account may log in again.
    exchange.disconnect(11);
    exchange.disconnect(12);
    exchange.connect(12, 0);
    exchange.receive(12, login(1), 0, &mut mail);
    assert!(matches!(
        mail.take(12)[..],
        [Outbound::LoginAccepted { account: 1, .. }]
    ));
}

/// A login tells the last sequence number the exchange has taken, counting commands still
/// waiting in the batch: every report the session gets after it carries a larger one.
#[test]
fn a_login_tells_the_last_sequence_number_taken() {
    let mut exchange = exchange(&[account(1), account(2)]);
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    for client_ref in 1..=3 {
        exchange.receive(0, limit(client_ref, Side::Buy, 10, 1), 1, &mut mail);
    }
    exchange.flush(&mut mail).unwrap();
    for client_ref in 4..=5 {
        exchange.receive(0, limit(client_ref, Side::Buy, 10, 1), 1, &mut mail);
    }
    exchange.connect(1, 1);
    exchange.receive(1, login(2), 1, &mut mail);
    assert_eq!(
        mail.take(1),
        [Outbound::LoginAccepted {
            account: 2,
            last_seq: 5
        }]
    );
}

#[test]
fn orders_get_sequential_ids_and_reports_reach_their_owners() {
    let mut exchange = exchange(&[account(1), account(2)]);
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    logged_in(&mut exchange, 1, 2, &mut mail);

    exchange.receive(0, limit(7, Side::Sell, 100, 10), 1, &mut mail);
    exchange.receive(0, limit(8, Side::Sell, 101, 5), 1, &mut mail);
    // Nothing happens before the batch is flushed.
    assert!(mail.sent.is_empty());
    assert_eq!(exchange.open_orders(1), Some(2));
    exchange.flush(&mut mail).unwrap();
    let rested = |seq, client_ref, price, qty| {
        [
            Report {
                seq,
                order_id: seq,
                client_ref,
                kind: ReportKind::Accepted,
            },
            Report {
                seq,
                order_id: seq,
                client_ref,
                kind: ReportKind::Rested {
                    side: Side::Sell,
                    price,
                    qty,
                    visible: qty,
                },
            },
        ]
    };
    assert_eq!(
        mail.reports(0),
        [rested(1, 7, 100, 10), rested(2, 8, 101, 5)].concat()
    );
    assert!(mail.sent.is_empty());

    // The buyer takes 12: all of the first order and 2 of the second.
    exchange.receive(1, limit(9, Side::Buy, 101, 12), 2, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let fill = |order_id, client_ref, trade_id, side, price, qty, leaves| Report {
        seq: 3,
        order_id,
        client_ref,
        kind: ReportKind::Fill {
            trade_id,
            side,
            price,
            qty,
            leaves,
        },
    };
    assert_eq!(
        mail.reports(1),
        [
            Report {
                seq: 3,
                order_id: 3,
                client_ref: 9,
                kind: ReportKind::Accepted
            },
            fill(3, 9, 1, Side::Buy, 100, 10, 2),
            fill(3, 9, 2, Side::Buy, 101, 2, 0),
        ]
    );
    assert_eq!(
        mail.reports(0),
        [
            fill(1, 7, 1, Side::Sell, 100, 10, 0),
            fill(2, 8, 2, Side::Sell, 101, 2, 3),
        ]
    );
    assert_eq!(exchange.open_orders(1), Some(1));
    assert_eq!(exchange.open_orders(2), Some(0));

    // Modify and cancel.
    exchange.receive(
        0,
        Inbound::Modify {
            order_id: 2,
            price: 105,
            qty: 4,
        },
        3,
        &mut mail,
    );
    exchange.receive(0, Inbound::Cancel { order_id: 2 }, 3, &mut mail);
    exchange.flush(&mut mail).unwrap();
    assert_eq!(
        mail.reports(0),
        [
            Report {
                seq: 4,
                order_id: 2,
                client_ref: 8,
                kind: ReportKind::Modified {
                    price: 105,
                    qty: 4,
                    leaves: 2
                }
            },
            Report {
                seq: 4,
                order_id: 2,
                client_ref: 8,
                kind: ReportKind::Rested {
                    side: Side::Sell,
                    price: 105,
                    qty: 2,
                    visible: 2
                }
            },
            Report {
                seq: 5,
                order_id: 2,
                client_ref: 8,
                kind: ReportKind::Cancelled {
                    qty: 2,
                    reason: CancelReason::Requested
                }
            },
        ]
    );
    assert_eq!(exchange.open_orders(1), Some(0));
    assert_eq!(exchange.book().order_count(), 0);
}

#[test]
fn refusals_go_to_the_sender_only() {
    let mut exchange = exchange(&[account(1), account(2)]);
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    logged_in(&mut exchange, 1, 2, &mut mail);
    exchange.receive(0, limit(7, Side::Sell, 100, 10), 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    mail.sent.clear();

    // Account 2 tries to cancel and modify account 1's order, and places a bad order.
    exchange.receive(1, Inbound::Cancel { order_id: 1 }, 1, &mut mail);
    let modify = Inbound::Modify {
        order_id: 1,
        price: 100,
        qty: 1,
    };
    exchange.receive(1, modify, 1, &mut mail);
    exchange.receive(1, limit(5, Side::Buy, 5_000, 1), 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let rejected = |seq, order_id, client_ref, reason| Report {
        seq,
        order_id,
        client_ref,
        kind: ReportKind::Rejected(reason),
    };
    assert_eq!(
        mail.reports(1),
        [
            rejected(2, 1, 0, RejectReason::UnknownOrder),
            rejected(3, 1, 0, RejectReason::UnknownOrder),
            rejected(4, 4, 5, RejectReason::PriceOutOfRange),
        ]
    );
    assert!(mail.sent.is_empty());
    assert_eq!(exchange.open_orders(1), Some(1));
    assert_eq!(exchange.open_orders(2), Some(0));
}

#[test]
fn open_orders_are_limited() {
    let limited = Account {
        max_open_orders: 2,
        ..account(1)
    };
    let mut exchange = exchange(&[limited, account(2)]);
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    logged_in(&mut exchange, 1, 2, &mut mail);
    // The limit counts orders still waiting in the batch.
    for client_ref in 1..=3 {
        exchange.receive(0, limit(client_ref, Side::Sell, 100, 1), 1, &mut mail);
    }
    let reason = RejectCode::TooManyOrders;
    assert_eq!(
        mail.take(0),
        [Outbound::Reject {
            reason,
            client_ref: 3
        }]
    );
    exchange.flush(&mut mail).unwrap();
    assert_eq!(exchange.book().order_count(), 2);
    // A stop counts too, and so does an order that will trade at once.
    exchange.receive(0, Inbound::Cancel { order_id: 1 }, 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let stop = Inbound::NewOrder(NewOrder {
        client_ref: 4,
        side: Side::Buy,
        qty: 1,
        kind: OrderKind::Stop {
            trigger: 200,
            limit: None,
        },
    });
    exchange.receive(0, stop, 1, &mut mail);
    exchange.receive(0, limit(5, Side::Buy, 1, 1), 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let reports = mail.take(0);
    assert!(reports.contains(&Outbound::Reject {
        reason,
        client_ref: 5
    }));
    assert_eq!(exchange.open_orders(1), Some(2));
    // Fills free the limit: account 2 takes the resting order.
    exchange.receive(1, limit(1, Side::Buy, 100, 1), 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    assert_eq!(exchange.open_orders(1), Some(1));
    // Orders that never rest free it at once: a market order without liquidity.
    let market = Inbound::NewOrder(NewOrder {
        client_ref: 6,
        side: Side::Buy,
        qty: 1,
        kind: OrderKind::Market,
    });
    exchange.receive(0, market, 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    assert_eq!(exchange.open_orders(1), Some(1));
}

#[test]
fn messages_are_throttled() {
    let slow = Account {
        messages_per_second: 4,
        ..account(1)
    };
    let mut exchange = exchange(&[slow]);
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    // A full bucket allows a burst of four.
    for client_ref in 1..=5 {
        exchange.receive(0, limit(client_ref, Side::Sell, 100, 1), 0, &mut mail);
    }
    let reason = RejectCode::Throttled;
    assert_eq!(
        mail.take(0),
        [Outbound::Reject {
            reason,
            client_ref: 5
        }]
    );
    // Heartbeats are not throttled; cancels are, and are refused with no client ref.
    exchange.receive(0, Inbound::Heartbeat, 0, &mut mail);
    exchange.receive(0, Inbound::Cancel { order_id: 1 }, 0, &mut mail);
    assert_eq!(
        mail.take(0),
        [Outbound::Reject {
            reason,
            client_ref: 0
        }]
    );
    // A quarter of a second refills one message.
    exchange.receive(0, Inbound::MassCancel, SECOND / 4 - 1, &mut mail);
    assert_eq!(mail.take(0).len(), 1);
    exchange.receive(0, Inbound::MassCancel, SECOND / 4, &mut mail);
    exchange.receive(0, Inbound::MassCancel, SECOND / 4, &mut mail);
    assert_eq!(mail.take(0).len(), 1);
    // The bucket holds no more than four, however long the pause.
    exchange.flush(&mut mail).unwrap();
    mail.sent.clear();
    for client_ref in 1..=5 {
        exchange.receive(
            0,
            limit(client_ref, Side::Sell, 100, 1),
            100 * SECOND,
            &mut mail,
        );
    }
    assert_eq!(mail.take(0).len(), 1);
}

#[test]
fn a_disconnect_cancels_the_accounts_orders() {
    let mut exchange = exchange(&[account(1), account(2)]);
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    logged_in(&mut exchange, 1, 2, &mut mail);
    exchange.receive(0, limit(1, Side::Sell, 100, 1), 1, &mut mail);
    exchange.receive(0, limit(2, Side::Sell, 101, 1), 1, &mut mail);
    exchange.receive(1, limit(3, Side::Buy, 90, 1), 1, &mut mail);
    let stop = Inbound::NewOrder(NewOrder {
        client_ref: 4,
        side: Side::Sell,
        qty: 1,
        kind: OrderKind::Stop {
            trigger: 50,
            limit: Some(40),
        },
    });
    exchange.receive(0, stop, 1, &mut mail);
    // The batch still holds account 1's orders when it goes.
    exchange.disconnect(0);
    assert!(exchange.has_batch());
    exchange.flush(&mut mail).unwrap();
    assert_eq!(exchange.book().order_count(), 1);
    assert_eq!(exchange.open_orders(1), Some(0));
    assert_eq!(exchange.open_orders(2), Some(1));
    // Nothing went to the departed session.
    assert!(mail.take(0).is_empty());
    // A new session for the account starts clean.
    logged_in(&mut exchange, 0, 1, &mut mail);
    exchange.receive(0, Inbound::MassCancel, 2, &mut mail);
    exchange.flush(&mut mail).unwrap();
    assert_eq!(
        mail.reports(0),
        [Report {
            seq: 6,
            order_id: 0,
            client_ref: 0,
            kind: ReportKind::MassCancelled { count: 0 }
        }]
    );
    // A mass cancel reports each order, then the count.
    mail.take(1);
    exchange.receive(1, Inbound::MassCancel, 2, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let kinds: Vec<ReportKind> = mail.reports(1).into_iter().map(|r| r.kind).collect();
    assert_eq!(
        kinds,
        [
            ReportKind::Cancelled {
                qty: 1,
                reason: CancelReason::MassCancel
            },
            ReportKind::MassCancelled { count: 1 }
        ]
    );
}

#[test]
fn quiet_sessions_get_heartbeats_and_silent_ones_are_logged_out() {
    let timing = Timing {
        heartbeat: 10,
        idle_timeout: 30,
    };
    let engine = open(&SimStorage::new(), book());
    let mut exchange = Direct::new(engine, &[account(1)], timing).unwrap();
    let mut mail = Mail::default();
    exchange.connect(0, 0);
    exchange.receive(0, login(1), 0, &mut mail);
    exchange.connect(1, 0);
    mail.sent.clear();
    exchange.tick(9, &mut mail);
    assert!(mail.sent.is_empty());
    // Only logged-in sessions get heartbeats.
    exchange.tick(10, &mut mail);
    assert_eq!(mail.sent, [(0, Outbound::Heartbeat)]);
    mail.sent.clear();
    exchange.tick(19, &mut mail);
    assert!(mail.sent.is_empty());
    // A message from the client keeps it alive; one to it postpones the heartbeat.
    exchange.receive(0, Inbound::Heartbeat, 20, &mut mail);
    exchange.receive(0, limit(1, Side::Buy, 10, 1), 25, &mut mail);
    exchange.flush(&mut mail).unwrap();
    mail.sent.clear();
    exchange.tick(29, &mut mail);
    assert!(mail.sent.is_empty());
    // The session that never logged in times out.
    exchange.tick(30, &mut mail);
    let reason = LogoutReason::Idle;
    assert_eq!(mail.take(1), [Outbound::Logout { reason }]);
    assert_eq!(mail.closed, [1]);
    exchange.tick(35, &mut mail);
    assert_eq!(mail.sent, [(0, Outbound::Heartbeat)]);
    mail.sent.clear();
    exchange.tick(55, &mut mail);
    assert_eq!(mail.take(0), [Outbound::Logout { reason }]);
    assert_eq!(mail.closed, [1, 0]);
    // A session being closed gets nothing more.
    exchange.tick(100, &mut mail);
    exchange.receive(0, Inbound::Heartbeat, 100, &mut mail);
    assert!(mail.sent.is_empty());
    // Its orders go once the connection does.
    exchange.disconnect(0);
    exchange.flush(&mut mail).unwrap();
    assert_eq!(exchange.book().order_count(), 0);
}

#[test]
fn stops_and_self_trades_are_reported() {
    let mut exchange = exchange(&[account(1), account(2)]);
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    logged_in(&mut exchange, 1, 2, &mut mail);
    let stop = Inbound::NewOrder(NewOrder {
        client_ref: 11,
        side: Side::Buy,
        qty: 2,
        kind: OrderKind::Stop {
            trigger: 100,
            limit: None,
        },
    });
    exchange.receive(0, stop, 1, &mut mail);
    exchange.receive(1, limit(21, Side::Sell, 100, 1), 1, &mut mail);
    exchange.receive(1, limit(22, Side::Sell, 101, 5), 1, &mut mail);
    exchange.receive(0, limit(12, Side::Buy, 100, 1), 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let mine: Vec<(u64, u64, ReportKind)> = mail
        .reports(0)
        .into_iter()
        .map(|r| (r.order_id, r.client_ref, r.kind))
        .collect();
    // The trade at 100 triggers the stop, which buys 2 at 101.
    assert_eq!(
        mine,
        [
            (1, 11, ReportKind::Accepted),
            (
                1,
                11,
                ReportKind::StopPlaced {
                    side: Side::Buy,
                    trigger: 100,
                    limit: None,
                    qty: 2
                }
            ),
            (4, 12, ReportKind::Accepted),
            (
                4,
                12,
                ReportKind::Fill {
                    trade_id: 1,
                    side: Side::Buy,
                    price: 100,
                    qty: 1,
                    leaves: 0
                }
            ),
            (1, 11, ReportKind::Triggered),
            (
                1,
                11,
                ReportKind::Fill {
                    trade_id: 2,
                    side: Side::Buy,
                    price: 101,
                    qty: 2,
                    leaves: 0
                }
            ),
        ]
    );
    assert_eq!(exchange.open_orders(1), Some(0));
    // A self-trade cancels the resting order, under the book's default policy.
    exchange.receive(1, limit(23, Side::Buy, 101, 1), 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let kinds: Vec<(u64, ReportKind)> = mail
        .reports(1)
        .into_iter()
        .filter(|r| r.seq == 5)
        .map(|r| (r.order_id, r.kind))
        .collect();
    assert!(kinds.contains(&(
        3,
        ReportKind::Cancelled {
            qty: 3,
            reason: CancelReason::SelfTrade
        }
    )));
    assert_eq!(exchange.open_orders(2), Some(1));
}

#[test]
fn phase_changes_reach_every_session() {
    // A market order stopped by the price band starts a volatility auction.
    let config = BookConfig {
        price_band: Some(5),
        reference_price: Some(100),
        auction_on_band: true,
        ..book()
    };
    let engine = open(&SimStorage::new(), config);
    let mut exchange = Direct::new(
        engine,
        &[account(1), account(2), account(3)],
        Timing::default(),
    )
    .unwrap();
    let mut mail = Mail::default();
    for (session, account) in [(0, 1), (1, 2), (2, 3)] {
        logged_in(&mut exchange, session, account, &mut mail);
    }
    exchange.receive(0, limit(1, Side::Sell, 100, 1), 1, &mut mail);
    exchange.receive(0, limit(2, Side::Sell, 110, 1), 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    // It trades at 100; the order at 110 lies beyond the band around that trade.
    let market = Inbound::NewOrder(NewOrder {
        client_ref: 9,
        side: Side::Buy,
        qty: 5,
        kind: OrderKind::Market,
    });
    exchange.receive(1, market, 2, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let phase = |session: SessionId, mail: &mut Mail| {
        mail.reports(session)
            .into_iter()
            .filter(|r| matches!(r.kind, ReportKind::PhaseChanged(_)))
            .map(|r| (r.order_id, r.kind))
            .collect::<Vec<_>>()
    };
    assert_eq!(exchange.book().phase(), Phase::Auction);
    for session in 0..3 {
        assert_eq!(
            phase(session, &mut mail),
            [(0, ReportKind::PhaseChanged(Phase::Auction))]
        );
    }
}

#[test]
fn recovered_orders_belong_to_their_accounts() {
    let storage = SimStorage::new();
    {
        let mut exchange = Direct::new(
            open(&storage, book()),
            &[account(1), account(2)],
            Timing::default(),
        )
        .unwrap();
        let mut mail = Mail::default();
        logged_in(&mut exchange, 0, 1, &mut mail);
        exchange.receive(0, limit(7, Side::Sell, 100, 5), 1, &mut mail);
        let stop = Inbound::NewOrder(NewOrder {
            client_ref: 8,
            side: Side::Buy,
            qty: 1,
            kind: OrderKind::Stop {
                trigger: 200,
                limit: Some(210),
            },
        });
        exchange.receive(0, stop, 1, &mut mail);
        exchange.flush(&mut mail).unwrap();
        // The gateway stops: the orders stay.
        assert_eq!(exchange.detach(0), Some(1));
        assert!(!exchange.has_batch());
        exchange.into_engine().close().unwrap();
    }
    let mut exchange = Direct::new(
        open(&storage, book()),
        &[account(1), account(2)],
        Timing::default(),
    )
    .unwrap();
    assert_eq!(exchange.open_orders(1), Some(2));
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    logged_in(&mut exchange, 1, 2, &mut mail);
    exchange.receive(1, limit(1, Side::Buy, 100, 2), 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    // New ids continue after the recovered ones; the recovered order's client ref is lost.
    assert_eq!(
        mail.reports(0),
        [Report {
            seq: 3,
            order_id: 1,
            client_ref: 0,
            kind: ReportKind::Fill {
                trade_id: 1,
                side: Side::Sell,
                price: 100,
                qty: 2,
                leaves: 3
            }
        }]
    );
    exchange.receive(0, Inbound::Cancel { order_id: 2 }, 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    assert_eq!(exchange.open_orders(1), Some(1));
    // A recovered order of an account that no longer exists counts for no one.
    exchange.into_engine().close().unwrap();
    let exchange = Direct::new(open(&storage, book()), &[account(2)], Timing::default()).unwrap();
    assert_eq!(exchange.open_orders(1), None);
    assert_eq!(exchange.book().order_count(), 1);
}

#[test]
fn foreign_books_and_accounts_out_of_range_are_refused() {
    let engine = open(&SimStorage::new(), book());
    let error = Direct::new(engine, &[account(8)], Timing::default()).err();
    assert_eq!(error, Some(SetupError::AccountOutOfRange(8)));

    // An order placed with its own id scheme could collide with the gateway's ids.
    let storage = SimStorage::new();
    let mut engine = open(&storage, book());
    let order = Command::Limit {
        id: 500,
        owner: 1,
        side: Side::Buy,
        price: 10,
        qty: 1,
        tif: TimeInForce::Gtc,
        display: None,
    };
    engine.submit(order, &mut Discard).unwrap();
    let error = Direct::new(engine, &[account(1)], Timing::default()).err();
    assert_eq!(error, Some(SetupError::ForeignOrder(500)));
    for error in [
        SetupError::AccountOutOfRange(8),
        SetupError::ForeignOrder(500),
    ] {
        assert!(!error.to_string().is_empty());
    }
}

#[test]
fn an_engine_failure_logs_everyone_out() {
    let storage = SimStorage::new();
    let mut exchange = Direct::new(
        open(&storage, book()),
        &[account(1), account(2)],
        Timing::default(),
    )
    .unwrap();
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    logged_in(&mut exchange, 1, 2, &mut mail);
    exchange.connect(2, 0);
    exchange.receive(0, limit(1, Side::Buy, 10, 1), 1, &mut mail);
    storage.set_failing(true);
    assert!(exchange.flush(&mut mail).is_err());
    let reason = LogoutReason::Shutdown;
    for session in 0..3 {
        assert_eq!(mail.take(session), [Outbound::Logout { reason }]);
    }
    assert_eq!(mail.closed, [0, 1, 2]);
}

#[derive(Clone, Debug)]
enum Step {
    Connect(SessionId),
    Login(SessionId, u32),
    Send(SessionId, Inbound),
    Disconnect(SessionId),
    Flush,
    Tick(u64),
}

fn step() -> impl Strategy<Value = Step> {
    let session = 0..4usize;
    let side = prop_oneof![Just(Side::Buy), Just(Side::Sell)];
    let tif = prop_oneof![
        Just(TimeInForce::Gtc),
        Just(TimeInForce::Ioc),
        Just(TimeInForce::Fok),
        Just(TimeInForce::PostOnly),
    ];
    let kind = prop_oneof![
        4 => (95..106i64, tif, prop::option::weighted(0.1, 1..4u64))
            .prop_map(|(price, tif, display)| OrderKind::Limit { price, tif, display }),
        1 => Just(OrderKind::Market),
        1 => (95..106i64, prop::option::of(95..106i64))
            .prop_map(|(trigger, limit)| OrderKind::Stop { trigger, limit }),
    ];
    let order = (any::<u64>(), side, 0..12u64, kind).prop_map(|(client_ref, side, qty, kind)| {
        Inbound::NewOrder(NewOrder {
            client_ref,
            side,
            qty,
            kind,
        })
    });
    let message = prop_oneof![
        6 => order,
        2 => (1..40u64).prop_map(|order_id| Inbound::Cancel { order_id }),
        2 => (1..40u64, 95..106i64, 0..12u64)
            .prop_map(|(order_id, price, qty)| Inbound::Modify { order_id, price, qty }),
        1 => Just(Inbound::MassCancel),
        1 => Just(Inbound::Heartbeat),
        1 => Just(Inbound::Logout),
        1 => (0..5u32).prop_map(login),
    ];
    prop_oneof![
        1 => session.clone().prop_map(Step::Connect),
        2 => (session.clone(), 0..5u32).prop_map(|(s, a)| Step::Login(s, a)),
        12 => (session.clone(), message).prop_map(|(s, m)| Step::Send(s, m)),
        1 => session.prop_map(Step::Disconnect),
        3 => Just(Step::Flush),
        1 => (0..3 * SECOND).prop_map(Step::Tick),
    ]
}

/// Orders and stops on the book, by owner.
fn open_on_book(book: &OrderBook, owner: u32) -> u32 {
    let mut count = 0;
    for side in [Side::Buy, Side::Sell] {
        for level in book.depth(side) {
            count += book
                .queue(side, level.price)
                .filter(|o| o.owner == owner)
                .count();
        }
        count += book.stops(side).filter(|s| s.owner == owner).count();
    }
    count as u32
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Whatever clients do, every report goes to the session of the order's owner, every
    /// refusal to the session that sent the command, and the open-order counts match the
    /// book after every flush.
    #[test]
    fn reports_reach_owners_and_counts_match_the_book(
        steps in prop::collection::vec(step(), 1..120),
    ) {
        let accounts: Vec<Account> = (1..4)
            .map(|id| Account { max_open_orders: 4, messages_per_second: 20, ..account(id) })
            .collect();
        let mut exchange = exchange(&accounts);
        let mut mail = Mail::default();
        let mut connected = [false; 4];
        // Which account each session is logged in for, as the replies say.
        let mut logged: [Option<u32>; 4] = [None; 4];
        // Whose each order id is, from the reports of its acceptance.
        let mut owners: HashMap<u64, u32> = HashMap::new();
        let mut now = 0;
        for step in steps {
            match step {
                Step::Connect(session) => {
                    if !connected[session] {
                        connected[session] = true;
                        exchange.connect(session, now);
                    }
                }
                Step::Login(session, account) => {
                    if connected[session] {
                        exchange.receive(session, login(account), now, &mut mail);
                    }
                }
                Step::Send(session, message) => {
                    if connected[session] {
                        exchange.receive(session, message, now, &mut mail);
                    }
                }
                Step::Disconnect(session) => {
                    if connected[session] {
                        connected[session] = false;
                        logged[session] = None;
                        exchange.disconnect(session);
                    }
                }
                Step::Flush => {
                    exchange.flush(&mut mail).unwrap();
                    for account in 1..4 {
                        prop_assert_eq!(
                            exchange.open_orders(account),
                            Some(open_on_book(exchange.book(), account))
                        );
                    }
                }
                Step::Tick(by) => {
                    now += by;
                    exchange.tick(now, &mut mail);
                }
            }
            for (session, message) in std::mem::take(&mut mail.sent) {
                prop_assert!(connected[session], "{:?} to a closed session", message);
                match message {
                    Outbound::LoginAccepted { account, .. } => {
                        prop_assert_eq!(logged[session], None);
                        prop_assert!(!logged.contains(&Some(account)));
                        logged[session] = Some(account);
                    }
                    Outbound::Report(report) => {
                        let account = logged[session];
                        prop_assert!(account.is_some());
                        match report.kind {
                            ReportKind::Accepted => {
                                owners.insert(report.order_id, account.unwrap());
                            }
                            ReportKind::MassCancelled { .. } | ReportKind::PhaseChanged(_) => {}
                            // A refused cancel names someone else's order, or none.
                            ReportKind::Rejected(_) => {}
                            _ => prop_assert_eq!(owners.get(&report.order_id).copied(), account),
                        }
                    }
                    _ => {}
                }
            }
            for session in std::mem::take(&mut mail.closed) {
                if connected[session] {
                    connected[session] = false;
                    logged[session] = None;
                    exchange.disconnect(session);
                }
            }
        }
        exchange.flush(&mut mail).unwrap();
        exchange.book().validate().unwrap();
    }
}

/// A session being closed is sent nothing more, not even the acceptance of an order it
/// placed; the account's next session hears what becomes of the order.
#[test]
fn a_closing_sessions_orders_are_reported_to_the_next_session() {
    let mut exchange = exchange(&[account(1)]);
    let mut mail = Mail::default();
    logged_in(&mut exchange, 0, 1, &mut mail);
    exchange.receive(0, limit(7, Side::Buy, 10, 1), 1, &mut mail);
    exchange.receive(0, Inbound::Logout, 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let reason = LogoutReason::Requested;
    assert_eq!(mail.take(0), [Outbound::Logout { reason }]);
    // The connection lingers while it reads its logout; the account stays logged in.
    exchange.connect(1, 2);
    exchange.receive(1, login(1), 2, &mut mail);
    let reason = LoginError::AlreadyLoggedIn;
    assert_eq!(mail.take(1), [Outbound::LoginRejected { reason }]);
    exchange.disconnect(1);
    exchange.disconnect(0);
    logged_in(&mut exchange, 1, 1, &mut mail);
    exchange.flush(&mut mail).unwrap();
    let kinds: Vec<(u64, u64, ReportKind)> = mail
        .reports(1)
        .into_iter()
        .map(|r| (r.order_id, r.client_ref, r.kind))
        .collect();
    assert_eq!(
        kinds,
        [
            (
                1,
                7,
                ReportKind::Cancelled {
                    qty: 1,
                    reason: CancelReason::MassCancel
                }
            ),
            (0, 0, ReportKind::MassCancelled { count: 1 }),
        ]
    );
}

/// A client that subscribes in the middle of trading, applying the snapshot and then every
/// update, holds exactly the book's depth after every publish; trades come as ticks.
#[test]
fn market_data_rebuilds_the_depth() {
    for seed in 0..20u64 {
        let accounts: Vec<Account> = (1..=3).map(account).collect();
        let mut exchange = exchange(&accounts);
        let mut mail = Mail::default();
        for (session, id) in [(0, 1), (1, 2), (2, 3)] {
            logged_in(&mut exchange, session, id, &mut mail);
        }
        let mut rng = orderbook::workload::SplitMix64::new(seed);
        let subscribe_at = rng.below(200);
        // The client's view: price levels by side and price.
        let mut view: std::collections::BTreeMap<(bool, i64), (u64, u32)> = Default::default();
        let mut subscribed = false;
        let mut last_seq = 0;
        let mut ticks = 0;
        for step in 0..400u64 {
            if step == subscribe_at {
                exchange.receive(2, Inbound::Subscribe, 1, &mut mail);
                subscribed = true;
            }
            let session = rng.below(3) as usize;
            let side = if rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = 95 + rng.below(11) as i64;
            let message = match rng.below(10) {
                0..=5 => Inbound::NewOrder(NewOrder {
                    client_ref: step,
                    side,
                    qty: 1 + rng.below(5),
                    kind: OrderKind::Limit {
                        price,
                        tif: TimeInForce::Gtc,
                        display: (rng.below(4) == 0).then_some(1),
                    },
                }),
                6 => Inbound::NewOrder(NewOrder {
                    client_ref: step,
                    side,
                    qty: 1 + rng.below(8),
                    kind: OrderKind::Market,
                }),
                7..=8 => Inbound::Cancel {
                    order_id: 1 + rng.below(step + 1),
                },
                _ => Inbound::Modify {
                    order_id: 1 + rng.below(step + 1),
                    price,
                    qty: 1 + rng.below(5),
                },
            };
            exchange.receive(session, message, 1, &mut mail);
            if rng.below(3) == 0 {
                exchange.flush(&mut mail).unwrap();
                exchange.publish(&mut mail);
            }
            // The client reads what it got.
            for message in mail.take(2) {
                match message {
                    Outbound::BookSnapshot { seq, levels } => {
                        view.clear();
                        last_seq = seq;
                        assert!(levels < 100);
                    }
                    Outbound::LevelUpdate(update) => {
                        assert!(update.seq >= last_seq, "seed {seed}: updates go forward");
                        last_seq = update.seq;
                        let key = (update.side == Side::Buy, update.price);
                        if update.orders == 0 {
                            assert_eq!(update.qty, 0);
                            view.remove(&key);
                        } else {
                            view.insert(key, (update.qty, update.orders));
                        }
                    }
                    Outbound::TradeTick(tick) => {
                        assert!(tick.seq >= last_seq);
                        ticks += 1;
                    }
                    _ => {}
                }
            }
            mail.sent.clear();
            if subscribed && !exchange.has_batch() {
                let book: std::collections::BTreeMap<(bool, i64), (u64, u32)> =
                    [Side::Buy, Side::Sell]
                        .into_iter()
                        .flat_map(|side| {
                            exchange
                                .book()
                                .depth(side)
                                .map(move |l| ((side == Side::Buy, l.price), (l.qty, l.orders)))
                                .collect::<Vec<_>>()
                        })
                        .collect();
                assert_eq!(view, book, "seed {seed}, step {step}");
            }
        }
        assert!(ticks > 0, "seed {seed}");
    }
}
