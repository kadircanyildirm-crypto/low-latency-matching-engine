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
//! Commands map one to one: `add_limit_order` (GTC), `submit_market_order`, `cancel_order`,
//! and `update_order(UpdatePrice)` for a move, which re-adds the order with its remaining
//! quantity at the back of the new level, trading first if it crosses.
//!
//! Run: `cargo run --release --manifest-path compare/Cargo.toml --bin run-orderbook-rs`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use harness::run::{self, Engine};
use harness::stream::{Header, Kind, Record, Summary};
use orderbook_rs::prelude::{OrderBook, StubClock, TradeResult};
use pricelevel::{Id, OrderUpdate, Price, Side, TimeInForce};

#[derive(Default)]
struct Counters {
    trades: AtomicU64,
    traded_qty: AtomicU64,
}

struct ObRs {
    book: OrderBook<()>,
    counters: Arc<Counters>,
}

impl Engine for ObRs {
    const NAME: &'static str = "orderbook-rs";

    fn new(_header: &Header) -> Self {
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
        Self { book, counters }
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
                let _ = self.book.add_limit_order(
                    id,
                    price,
                    u64::from(r.qty),
                    side,
                    TimeInForce::Gtc,
                    None,
                );
            }
            Kind::Market => {
                let _ = self.book.submit_market_order(id, u64::from(r.qty), side);
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
        "(orderbook-rs 0.15.0; StubClock, no fees, no risk limits, STP off; trade listener)"
            .to_string()
    }
}

fn main() {
    run::main::<ObRs>();
}
