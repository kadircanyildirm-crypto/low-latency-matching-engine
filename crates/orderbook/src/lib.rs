//! Matching core of the exchange: a single-threaded, deterministic limit order book with
//! price-time priority.
//!
//! Design in brief:
//! - **Integer prices.** Prices are tick counts (`i64`); floating point never touches the
//!   book.
//! - **Dense price ladder.** Each side is an array of levels indexed by `price - min_price`,
//!   so finding a level is one subtraction. A two-level bitset finds the next best level
//!   when one empties.
//! - **Intrusive FIFO queues.** Orders live in a preallocated slab and link to their
//!   neighbours by `u32` slot, giving O(1) insert, cancel and fill.
//! - **No allocation after construction.** Every buffer is sized from [`BookConfig`].
//! - **Determinism.** The event stream is a pure function of the command stream, which is
//!   the foundation for event sourcing and replay.
//!
//! ```
//! use orderbook::{BookConfig, Command, Event, OrderBook, Side};
//!
//! let mut book = OrderBook::new(BookConfig { min_price: 1, max_price: 1_000, max_orders: 64 });
//! let mut events = Vec::new();
//! book.process(Command::Limit { id: 1, side: Side::Sell, price: 101, qty: 5 }, &mut events);
//! book.process(Command::Limit { id: 2, side: Side::Buy, price: 102, qty: 3 }, &mut events);
//!
//! assert!(events.contains(&Event::Trade { taker: 2, maker: 1, taker_side: Side::Buy, price: 101, qty: 3 }));
//! assert_eq!(book.best_ask().map(|level| level.qty), Some(2));
//! ```

#![forbid(unsafe_code)]

mod bitset;
mod book;
mod pool;
mod types;
pub mod workload;

pub use book::{BookConfig, Depth, LevelInfo, OrderBook, OrderInfo, Queue};
pub use types::{CancelReason, Command, Event, EventSink, OrderId, Price, Qty, RejectReason, Side};
