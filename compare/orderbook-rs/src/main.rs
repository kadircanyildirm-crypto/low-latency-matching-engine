//! OrderBook-rs (`orderbook-rs` 0.15.0 on crates.io, by Joaquín Béjar) replaying the
//! comparison streams.
//!
//! The book is built as in the crate's own latency benchmarks (`OrderBook::<()>`, default
//! features, no fee schedule, no risk limits, self-trade prevention off, which is its
//! default), with one change: a `StubClock`, the clock the crate provides for replay, so
//! no command pays for reading the wall clock. Trades are counted by a trade listener,
//! which receives every execution whatever the call returns.
//!
//! OrderBook-rs is built for concurrent access (lock-free skip lists of levels, a
//! concurrent map from id to location, atomics, a submit gate); a single-threaded replay
//! pays for that machinery without using it.
//!
//! Commands map one to one: `add_limit_order_with_user` (GTC),
//! `submit_market_order_with_user`, `cancel_order`, and `update_order(UpdatePrice)` for a
//! move, which re-adds the order with its remaining quantity at the back of the new level,
//! trading first if it crosses.
//!
//! Users: OrderBook-rs keeps, per user, a `Vec` of the user's order ids and removes from it
//! with a linear search and an order-preserving shift. Submitting everything under its
//! default user (what `add_limit_order` does) would make every cancel and fill scan the
//! whole book, about a million ids in the `deep` scenario. Each order therefore goes to the
//! participant our generator assigned it to, one of 64 (`harness::scenarios::participant`;
//! a few nanoseconds per order, inside the timed region). Self-trade prevention is off, so
//! users do not change the matching.
//!
//! Run: `cargo run --release --manifest-path compare/Cargo.toml --bin run-orderbook-rs`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use harness::run::{self, Engine};
use harness::scenarios::{PARTICIPANTS, participant};
use harness::stream::{Header, Kind, Record, Summary};
use orderbook_rs::prelude::{OrderBook, StubClock, TradeResult};
use pricelevel::{Hash32, Id, OrderUpdate, Price, Side, TimeInForce};

#[derive(Default)]
struct Counters {
    trades: AtomicU64,
    traded_qty: AtomicU64,
}

struct ObRs {
    book: OrderBook<()>,
    counters: Arc<Counters>,
    users: Vec<Hash32>,
    seed: u64,
}

impl ObRs {
    #[inline(always)]
    fn user(&self, id: u64) -> Hash32 {
        self.users[participant(id, self.seed) as usize]
    }
}

impl Engine for ObRs {
    const NAME: &'static str = "orderbook-rs";

    fn new(header: &Header) -> Self {
        let mut book = OrderBook::<()>::with_clock("CMP", Arc::new(StubClock::new()));
        let counters = Arc::new(Counters::default());
        let sink = Arc::clone(&counters);
        book.set_trade_listener(Arc::new(move |result: &TradeResult| {
            for trade in result.match_result.trades().as_vec() {
                sink.trades.fetch_add(1, Ordering::Relaxed);
                sink.traded_qty
                    .fetch_add(trade.quantity().as_u64(), Ordering::Relaxed);
            }
        }));
        // One non-zero tag byte per participant, like the crate's own benchmark owners.
        let users = (1..=PARTICIPANTS)
            .map(|tag| {
                let mut bytes = [0u8; 32];
                bytes[0] = u8::try_from(tag).expect("fewer than 256 participants");
                Hash32::new(bytes)
            })
            .collect();
        Self {
            book,
            counters,
            users,
            seed: header.seed,
        }
    }

    #[inline(always)]
    fn apply(&mut self, r: &Record) {
        let id = Id::from_u64(r.id);
        let side = if r.is_buy() { Side::Buy } else { Side::Sell };
        // Errors are part of normal flow here (a market order meeting an empty side); the
        // final book and trades are verified against the recorded ones instead.
        match r.kind() {
            Kind::Limit => {
                let price = u128::try_from(r.price).expect("non-negative price");
                let user = self.user(r.id);
                let _ = self.book.add_limit_order_with_user(
                    id,
                    price,
                    u64::from(r.qty),
                    side,
                    TimeInForce::Gtc,
                    user,
                    None,
                );
            }
            Kind::Market => {
                let user = self.user(r.id);
                let _ = self
                    .book
                    .submit_market_order_with_user(id, u64::from(r.qty), side, user);
            }
            Kind::Cancel => {
                let _ = self.book.cancel_order(id);
            }
            Kind::Move => {
                let price = u128::try_from(r.price).expect("non-negative price");
                let _ = self.book.update_order(OrderUpdate::UpdatePrice {
                    order_id: id,
                    new_price: Price::new(price),
                });
            }
        }
    }

    fn summary(&self) -> Summary {
        let orders = self.book.get_all_orders();
        Summary {
            trades: self.counters.trades.load(Ordering::Relaxed),
            traded_qty: self.counters.traded_qty.load(Ordering::Relaxed),
            resting_orders: orders.len() as u64,
            resting_qty: orders.iter().map(|o| o.visible_quantity().as_u64()).sum(),
            best_bid: self
                .book
                .best_bid()
                .map_or(Summary::NO_BID, |p| i64::try_from(p).expect("price")),
            best_ask: self
                .book
                .best_ask()
                .map_or(Summary::NO_ASK, |p| i64::try_from(p).expect("price")),
        }
    }

    fn describe() -> String {
        "(orderbook-rs 0.15.0; StubClock, 64 users, no fees, no risk limits, STP off; \
         trade listener)"
            .to_string()
    }
}

fn main() {
    run::main::<ObRs>();
}
