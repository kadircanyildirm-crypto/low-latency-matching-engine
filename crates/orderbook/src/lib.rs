//! Matching core of the exchange: a single-threaded, deterministic limit order book with
//! price-time priority.
//!
//! Design in brief (the full rationale is in `docs/DESIGN.md`):
//! - **Integer prices.** Prices are tick counts (`i64`); floating point never touches the
//!   book.
//! - **Dense price ladder.** Each side is an array of levels indexed by `price - min_price`,
//!   so finding a level is one subtraction. A two-level bitset finds the next best level
//!   when one empties.
//! - **Intrusive FIFO queues.** Orders live in a preallocated slab and link to their
//!   neighbours by `u32` slot, giving O(1) insert, cancel and fill.
//! - **No allocation after construction, no overflow by construction.** Every buffer is
//!   sized from [`BookConfig`], and `max_orders * max_order_qty` must fit in a `u64`.
//! - **Exchange semantics.** GTC, IOC, fill-or-kill and post-only limit orders, iceberg
//!   orders, market orders, stop and stop-limit orders, owner-checked cancels and
//!   modifies, FIX-style modifies on total quantity, mass cancel, self-trade prevention,
//!   and price protection and a price band against fat-finger orders.
//! - **Trading phases.** Continuous trading, call phases that end in an auction uncross at
//!   a single price, halts and the close; optionally, a volatility interruption when the
//!   price band stops a market order.
//! - **Determinism.** The event stream is a pure function of the command stream, which is
//!   the foundation for event sourcing and replay. Snapshots restore an identical book, and
//!   a platform-independent digest identifies its state.
//!
//! ```
//! use orderbook::{BookConfig, Command, Event, OrderBook, Side, TimeInForce};
//!
//! let mut book = OrderBook::new(BookConfig::new(1, 1_000, 64));
//! let mut events = Vec::new();
//! let (gtc, ioc) = (TimeInForce::Gtc, TimeInForce::Ioc);
//! book.process(Command::Limit { id: 1, owner: 7, side: Side::Sell, price: 101, qty: 5, tif: gtc, display: None }, &mut events);
//! book.process(Command::Limit { id: 2, owner: 8, side: Side::Buy, price: 102, qty: 3, tif: ioc, display: None }, &mut events);
//!
//! assert!(events.contains(&Event::Trade {
//!     trade_id: 1,
//!     taker: 2,
//!     maker: 1,
//!     taker_side: Side::Buy,
//!     price: 101,
//!     qty: 3,
//!     taker_leaves: 0,
//!     maker_leaves: 2,
//! }));
//! assert_eq!(book.best_ask().map(|level| level.qty), Some(2));
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod bitset;
mod book;
mod index;
mod owners;
mod pool;
mod types;
pub mod workload;

/// The version of the matching rules: what every command does to the book. It goes up
/// whenever some command, applied to some book, emits other events or leaves another
/// state than before. A journal of commands can be replayed only under the rules it was
/// written under; a snapshot is state, and stays valid across versions.
pub const RULES_VERSION: u32 = 1;

pub use book::{
    BookConfig, BookSnapshot, ConfigError, Depth, LevelInfo, OrderBook, OrderInfo, Queue,
    QueuedOrder, SnapshotError, SnapshotOrder, StopOrder, Stops,
};
pub use types::{
    CancelReason, Command, Event, EventSink, OrderId, OwnerId, Phase, Price, Qty, RejectReason,
    SelfTradePolicy, Side, TimeInForce, TradeId,
};
