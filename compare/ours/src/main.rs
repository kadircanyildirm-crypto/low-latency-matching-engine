//! Our engine, `crates/orderbook`, replaying the comparison streams.
//!
//! Configured as in the latency benchmark, minus everything outside the common subset:
//! the generator's price band, no price protection, no price band. Self-trade prevention
//! cannot be switched off, so the book still compares owners on every match, but the
//! streams give each side its own owner and it never fires.
//!
//! Every event is materialised into a `Vec<Event>`, as in the latency benchmark, and the
//! trades are counted from it afterwards, so the engine pays for emitting its full event
//! stream, not just for the trades.
//!
//! Run: `cargo run --release --manifest-path compare/Cargo.toml --bin run-ours`

use harness::run::{self, Engine};
use harness::stream::{Header, Kind, Record, Summary};
use orderbook::{BookConfig, Command, Event, OrderBook, SelfTradePolicy, Side, TimeInForce};

struct Ours {
    book: OrderBook,
    events: Vec<Event>,
    trades: u64,
    traded_qty: u64,
}

#[inline(always)]
fn side(record: &Record) -> Side {
    if record.is_buy() {
        Side::Buy
    } else {
        Side::Sell
    }
}

impl Engine for Ours {
    const NAME: &'static str = "ours";

    fn new(header: &Header) -> Self {
        let max_orders = u32::try_from(header.max_live).expect("capacity fits in u32");
        Self {
            book: OrderBook::new(BookConfig {
                min_price: header.min_price,
                max_price: header.max_price,
                max_orders,
                max_owners: 2,
                max_order_qty: 1_000_000,
                max_iceberg_tranches: BookConfig::DEFAULT_MAX_ICEBERG_TRANCHES,
                price_protection: None,
                price_band: None,
                reference_price: None,
                self_trade: SelfTradePolicy::CancelResting,
            }),
            events: Vec::with_capacity(4096),
            trades: 0,
            traded_qty: 0,
        }
    }

    #[inline(always)]
    fn apply(&mut self, r: &Record) {
        // Owner 0 buys, owner 1 sells: the side code is the owner id.
        let owner = u32::from(r.side);
        let command = match r.kind() {
            Kind::Limit => Command::Limit {
                id: r.id,
                owner,
                side: side(r),
                price: r.price,
                qty: u64::from(r.qty),
                tif: TimeInForce::Gtc,
                display: None,
            },
            Kind::Market => Command::Market {
                id: r.id,
                owner,
                side: side(r),
                qty: u64::from(r.qty),
            },
            Kind::Cancel => Command::Cancel { id: r.id, owner },
            Kind::Move => Command::Modify {
                id: r.id,
                owner,
                price: r.price,
                qty: u64::from(r.qty),
            },
        };
        self.events.clear();
        self.book.process(command, &mut self.events);
        for event in &self.events {
            if let Event::Trade { qty, .. } = *event {
                self.trades += 1;
                self.traded_qty += qty;
            }
        }
    }

    fn summary(&self) -> Summary {
        let mut summary = Summary {
            trades: self.trades,
            traded_qty: self.traded_qty,
            best_bid: self.book.best_bid().map_or(Summary::NO_BID, |l| l.price),
            best_ask: self.book.best_ask().map_or(Summary::NO_ASK, |l| l.price),
            ..Summary::default()
        };
        for side in [Side::Buy, Side::Sell] {
            for level in self.book.depth(side) {
                summary.resting_orders += u64::from(level.orders);
                summary.resting_qty += level.qty;
            }
        }
        summary
    }

    fn describe() -> String {
        "(crates/orderbook; no price protection or band; every event materialised)".to_string()
    }
}

fn main() {
    run::main::<Ours>();
}
