//! The gateway's sessions on byte streams the fuzzer chooses, read as the server reads its
//! sockets: connections come and go, send arbitrary bytes, time passes, and batches flush.
//!
//! - Nothing panics.
//! - Nothing goes to a session after it was closed.
//! - All reports about an order go to sessions of one account, the one that owns it on the
//!   book, and only logged-in sessions get reports.
//! - After every flush, each account's open-order count matches its orders on the book,
//!   the open-order limit holds, and the depth kept for market data is the book's.

#![no_main]

use std::collections::HashMap;
use std::path::Path;

use arbitrary::Arbitrary;
use engine::sim::SimStorage;
use engine::{Discard, Engine, EngineConfig};
use gateway::{Account, Exchange, Mailbox, SessionId, Timing};
use libfuzzer_sys::fuzz_target;
use orderbook::{BookConfig, OrderBook, Side, TimeInForce};
use protocol::{
    Inbound, LogoutReason, NewOrder, OrderKind, Outbound, ReportKind, VERSION, decode_inbound,
    encode_inbound,
};

const SESSIONS: usize = 4;
const MAX_STEPS: usize = 200;
const MAX_OPEN: u32 = 5;

#[derive(Arbitrary, Debug)]
enum Step {
    Connect(u8),
    /// Raw bytes, which may or may not decode.
    Bytes(u8, Vec<u8>),
    /// A well-formed message, so that logins and orders are easy to reach.
    Send(u8, Message),
    Disconnect(u8),
    Flush,
    Tick(u16),
}

#[derive(Arbitrary, Debug)]
enum Message {
    Login {
        account: u8,
        token: u8,
    },
    Logout,
    Heartbeat,
    Limit {
        client_ref: u8,
        buy: bool,
        price: u8,
        qty: u8,
        tif: u8,
        display: Option<u8>,
    },
    Market {
        client_ref: u8,
        buy: bool,
        qty: u8,
    },
    Stop {
        client_ref: u8,
        buy: bool,
        trigger: u8,
        limit: Option<u8>,
        qty: u8,
    },
    Cancel(u8),
    Modify {
        order_id: u8,
        price: u8,
        qty: u8,
    },
    MassCancel,
    Subscribe,
}

/// Prices near the reference price of 100, inside and outside the band of 20.
fn price(p: u8) -> i64 {
    70 + i64::from(p % 61)
}

impl Message {
    fn inbound(&self) -> Inbound {
        let side = |buy: bool| if buy { Side::Buy } else { Side::Sell };
        let order = |client_ref: u8, buy: bool, qty: u8, kind| {
            Inbound::NewOrder(NewOrder {
                client_ref: u64::from(client_ref),
                side: side(buy),
                qty: u64::from(qty % 12),
                kind,
            })
        };
        match *self {
            Message::Login { account, token } => Inbound::Login {
                version: VERSION,
                account: u32::from(account % 5),
                token: u64::from(token % 5),
            },
            Message::Logout => Inbound::Logout,
            Message::Heartbeat => Inbound::Heartbeat,
            Message::Limit {
                client_ref,
                buy,
                price: p,
                qty,
                tif,
                display,
            } => {
                let tif = [
                    TimeInForce::Gtc,
                    TimeInForce::Ioc,
                    TimeInForce::Fok,
                    TimeInForce::PostOnly,
                ][usize::from(tif % 4)];
                let display = display.map(|d| u64::from(d % 5));
                order(
                    client_ref,
                    buy,
                    qty,
                    OrderKind::Limit {
                        price: price(p),
                        tif,
                        display,
                    },
                )
            }
            Message::Market {
                client_ref,
                buy,
                qty,
            } => order(client_ref, buy, qty, OrderKind::Market),
            Message::Stop {
                client_ref,
                buy,
                trigger,
                limit,
                qty,
            } => order(
                client_ref,
                buy,
                qty,
                OrderKind::Stop {
                    trigger: price(trigger),
                    limit: limit.map(price),
                },
            ),
            Message::Cancel(order_id) => Inbound::Cancel {
                order_id: u64::from(order_id % 40),
            },
            Message::Modify {
                order_id,
                price: p,
                qty,
            } => Inbound::Modify {
                order_id: u64::from(order_id % 40),
                price: price(p),
                qty: u64::from(qty % 12),
            },
            Message::MassCancel => Inbound::MassCancel,
            Message::Subscribe => Inbound::Subscribe,
        }
    }
}

/// What the exchange did, in order: `None` closes the session.
#[derive(Default)]
struct Mail(Vec<(SessionId, Option<Outbound>)>);

impl Mailbox for Mail {
    fn send(&mut self, session: SessionId, message: Outbound) {
        self.0.push((session, Some(message)));
    }

    fn close(&mut self, session: SessionId) {
        self.0.push((session, None));
    }
}

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

fuzz_target!(|steps: Vec<Step>| {
    let book = BookConfig {
        max_owners: 4,
        price_band: Some(20),
        reference_price: Some(100),
        auction_on_band: true,
        ..BookConfig::new(1, 200, 32)
    };
    let engine = Engine::open_with(
        SimStorage::new(),
        Path::new("data"),
        EngineConfig {
            segment_capacity: 64,
            ..EngineConfig::new(book)
        },
        &mut Discard,
    )
    .unwrap()
    .0;
    // Tokens 1..=3, so that logins with small numbers succeed.
    let accounts: Vec<Account> = (1..4)
        .map(|id| Account {
            id,
            token: u64::from(id),
            max_open_orders: MAX_OPEN,
            messages_per_second: 50,
        })
        .collect();
    let timing = Timing {
        heartbeat: 1_000,
        idle_timeout: 5_000,
    };
    let mut engine = engine;
    let mut exchange = Exchange::new(engine.book(), engine.last_seq(), &accounts, timing).unwrap();
    let mut mail = Mail::default();
    let mut connected = [false; SESSIONS];
    let mut closing = [false; SESSIONS];
    let mut inputs: [Vec<u8>; SESSIONS] = Default::default();
    let mut logged: [Option<u32>; SESSIONS] = [None; SESSIONS];
    let mut owners: HashMap<u64, u32> = HashMap::new();
    let mut now = 0u64;
    for step in steps.into_iter().take(MAX_STEPS) {
        match step {
            Step::Connect(session) => {
                let session = usize::from(session) % SESSIONS;
                if !connected[session] {
                    connected[session] = true;
                    exchange.connect(session, now);
                }
            }
            Step::Bytes(..) | Step::Send(..) => {
                let (session, bytes) = match step {
                    Step::Bytes(session, bytes) => (session, bytes),
                    Step::Send(session, message) => {
                        let mut bytes = Vec::new();
                        encode_inbound(&message.inbound(), &mut bytes);
                        (session, bytes)
                    }
                    _ => unreachable!(),
                };
                let session = usize::from(session) % SESSIONS;
                if connected[session] && !closing[session] {
                    let input = &mut inputs[session];
                    input.extend_from_slice(&bytes);
                    let mut at = 0;
                    loop {
                        match decode_inbound(&input[at..]) {
                            Ok(Some((message, len))) => {
                                at += len;
                                exchange.receive(session, message, now, &mut mail);
                            }
                            Ok(None) => break,
                            Err(_) => {
                                exchange.end(session, LogoutReason::ProtocolError, &mut mail);
                                at = input.len();
                                break;
                            }
                        }
                    }
                    input.drain(..at);
                }
            }
            Step::Disconnect(session) => {
                let session = usize::from(session) % SESSIONS;
                if connected[session] {
                    connected[session] = false;
                    closing[session] = false;
                    logged[session] = None;
                    inputs[session].clear();
                    exchange.disconnect(session);
                }
            }
            Step::Flush => {
                exchange.flush(&mut engine, &mut mail).unwrap();
                exchange.publish(&mut mail);
                for account in 1..4 {
                    let open = exchange.open_orders(account).unwrap();
                    assert_eq!(open, open_on_book(engine.book(), account));
                    assert!(open <= MAX_OPEN);
                }
                // The depth kept from the events is the book's.
                for side in [Side::Buy, Side::Sell] {
                    let kept: Vec<_> = exchange
                        .depth()
                        .levels(side)
                        .map(|(price, level)| (price, level.qty, level.orders))
                        .collect();
                    let book: Vec<_> = engine
                        .book()
                        .depth(side)
                        .map(|level| (level.price, level.qty, level.orders))
                        .collect();
                    assert_eq!(kept, book);
                }
            }
            Step::Tick(by) => {
                now += u64::from(by);
                exchange.tick(now, &mut mail);
            }
        }
        for (session, message) in std::mem::take(&mut mail.0) {
            assert!(
                connected[session] && !closing[session],
                "{message:?} to a closed session"
            );
            let Some(message) = message else {
                closing[session] = true;
                continue;
            };
            match message {
                Outbound::LoginAccepted { account, .. } => {
                    assert_eq!(logged[session], None);
                    assert!(!logged.contains(&Some(account)));
                    logged[session] = Some(account);
                }
                Outbound::Report(report) => {
                    let account = logged[session].expect("a logged-in session");
                    match report.kind {
                        ReportKind::Accepted => {
                            assert_eq!(owners.insert(report.order_id, account), None);
                        }
                        ReportKind::MassCancelled { .. }
                        | ReportKind::PhaseChanged(_)
                        | ReportKind::Rejected(_) => {}
                        _ => {
                            // All of an order's reports go to one account. The first may
                            // come later than the acceptance, which a session being closed
                            // was not sent.
                            let owner = *owners.entry(report.order_id).or_insert(account);
                            assert_eq!(owner, account);
                            if let Some(order) = engine.book().order(report.order_id) {
                                assert_eq!(order.owner, account);
                            }
                        }
                    }
                }
                Outbound::BookSnapshot { .. }
                | Outbound::LevelUpdate(_)
                | Outbound::TradeTick(_) => {
                    assert!(logged[session].is_some(), "market data before a login");
                }
                _ => {}
            }
        }
    }
    exchange.flush(&mut engine, &mut mail).unwrap();
    engine.book().validate().unwrap();
});
