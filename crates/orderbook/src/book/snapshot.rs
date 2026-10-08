//! Snapshots of a book's complete state, restoring a book from one, and a digest of that
//! state.
//!
//! A snapshot holds everything that decides how the book responds to future commands, so a
//! restored book is indistinguishable from the original: same events, same trade ids, same
//! queue positions. The digest is a platform-independent hash of the same state, cheap
//! enough to compare a replica or a replay against the live book without shipping the
//! whole snapshot.

use std::fmt;

use super::{BookConfig, OrderBook, OrderNode};
use crate::types::{OrderId, OwnerId, Price, Qty, SelfTradePolicy, Side};

/// A resting order as recorded in a [`BookSnapshot`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotOrder {
    /// Order id.
    pub id: OrderId,
    /// Owner of the order.
    pub owner: OwnerId,
    /// Side.
    pub side: Side,
    /// Limit price.
    pub price: Price,
    /// Open quantity.
    pub leaves: Qty,
    /// Quantity filled so far. Kept because a modify's new quantity is a total that
    /// includes it.
    pub filled: Qty,
}

/// The complete state of a book.
///
/// [`OrderBook::snapshot`] produces it in canonical order, so two books are in the same
/// state exactly when their snapshots are equal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BookSnapshot {
    /// The book's configuration.
    pub config: BookConfig,
    /// Trades executed so far; the next trade gets id `trade_count + 1`.
    pub trade_count: u64,
    /// Resting orders: bids best price first, then asks best price first, and within each
    /// price level in time priority.
    pub orders: Vec<SnapshotOrder>,
}

/// Why a snapshot cannot be restored. Each variant names a state the engine itself can
/// never reach, so it points at a corrupted or hand-edited snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// More orders than the configuration's `max_orders`.
    TooManyOrders,
    /// Two orders share an id.
    DuplicateOrderId(OrderId),
    /// The order's owner is not below `max_owners`, its price is outside the band, its open
    /// quantity is zero, or its open plus filled quantity exceeds `max_order_qty`.
    InvalidOrder(OrderId),
    /// The best bid is at or above the best ask.
    Crossed,
    /// The trade count leaves no room for another trade id.
    TradeCountExhausted,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyOrders => f.write_str("more orders than max_orders"),
            Self::DuplicateOrderId(id) => write!(f, "order id {id} appears twice"),
            Self::InvalidOrder(id) => write!(f, "order {id} cannot rest in this book"),
            Self::Crossed => f.write_str("the best bid is at or above the best ask"),
            Self::TradeCountExhausted => f.write_str("no trade ids left"),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl BookSnapshot {
    /// The digest of this state; equal to [`OrderBook::digest`] of the book it was taken
    /// from or restores to.
    pub fn digest(&self) -> u64 {
        let mut hash = Digest::new(&self.config, self.trade_count, self.orders.len());
        self.orders.iter().for_each(|order| hash.order(order));
        hash.finish()
    }
}

impl OrderBook {
    /// The book's complete state. See [`BookSnapshot`].
    pub fn snapshot(&self) -> BookSnapshot {
        let mut orders = Vec::with_capacity(self.order_count());
        self.for_each_order(|order| orders.push(order));
        BookSnapshot {
            config: self.config,
            trade_count: self.trade_count(),
            orders,
        }
    }

    /// Rebuilds a book from a snapshot. The result behaves exactly like the book the
    /// snapshot was taken from.
    ///
    /// # Panics
    ///
    /// If the snapshot's configuration is invalid; see [`OrderBook::new`].
    pub fn restore(snapshot: &BookSnapshot) -> Result<Self, SnapshotError> {
        let mut book = Self::new(snapshot.config);
        if snapshot.orders.len() > snapshot.config.max_orders as usize {
            return Err(SnapshotError::TooManyOrders);
        }
        book.next_trade_id = snapshot
            .trade_count
            .checked_add(1)
            .ok_or(SnapshotError::TradeCountExhausted)?;
        for order in &snapshot.orders {
            let invalid = SnapshotError::InvalidOrder(order.id);
            book.check_owner(order.owner).map_err(|_| invalid)?;
            let level = book.level_of(order.price).ok_or(invalid)?;
            let total = order
                .leaves
                .checked_add(order.filled)
                .filter(|&total| order.leaves > 0 && total <= snapshot.config.max_order_qty)
                .ok_or(invalid)?;
            if book.index.contains_key(&order.id) {
                return Err(SnapshotError::DuplicateOrderId(order.id));
            }
            let slot = book.pool.alloc(OrderNode::new(
                order.id,
                order.owner,
                order.side,
                level,
                order.leaves,
                total,
            ));
            // Orders arrive in book order, so each owner's list ends up in queue order
            // within every level, which is all that mass cancels depend on.
            book.place(slot);
        }
        if let (Some(bid), Some(ask)) = (book.bids.best, book.asks.best) {
            if bid >= ask {
                return Err(SnapshotError::Crossed);
            }
        }
        Ok(book)
    }

    /// A 64-bit digest of the book's complete state, identical on every platform and
    /// computed without allocating.
    ///
    /// Two books in the same state have the same digest. It is meant for detecting
    /// divergence between a primary and a replica, or between a live book and its replay;
    /// it is not a cryptographic hash.
    pub fn digest(&self) -> u64 {
        let mut hash = Digest::new(&self.config, self.trade_count(), self.order_count());
        self.for_each_order(|order| hash.order(&order));
        hash.finish()
    }

    /// Visits every resting order in snapshot order.
    fn for_each_order(&self, mut visit: impl FnMut(SnapshotOrder)) {
        for side in [Side::Buy, Side::Sell] {
            for level in self.depth(side) {
                for order in self.queue(side, level.price) {
                    visit(SnapshotOrder {
                        id: order.id,
                        owner: order.owner,
                        side,
                        price: level.price,
                        leaves: order.leaves,
                        filled: order.filled,
                    });
                }
            }
        }
    }
}

/// FNV-1a over a fixed little-endian encoding of the state. The encoding names every enum
/// value explicitly instead of relying on discriminants, so reordering a declaration can
/// never silently change a digest.
struct Digest(u64);

impl Digest {
    fn new(config: &BookConfig, trade_count: u64, orders: usize) -> Self {
        let mut hash = Self(0xcbf2_9ce4_8422_2325);
        hash.i64(config.min_price);
        hash.i64(config.max_price);
        hash.u64(u64::from(config.max_orders));
        hash.u64(u64::from(config.max_owners));
        hash.u64(config.max_order_qty);
        match config.price_protection {
            None => hash.u64(0),
            Some(ticks) => {
                hash.u64(1);
                hash.u64(u64::from(ticks));
            }
        }
        hash.u64(match config.self_trade {
            SelfTradePolicy::CancelResting => 0,
            SelfTradePolicy::CancelIncoming => 1,
        });
        hash.u64(trade_count);
        hash.u64(orders as u64);
        hash
    }

    fn order(&mut self, order: &SnapshotOrder) {
        self.u64(match order.side {
            Side::Buy => 0,
            Side::Sell => 1,
        });
        self.i64(order.price);
        self.u64(order.id);
        self.u64(u64::from(order.owner));
        self.u64(order.leaves);
        self.u64(order.filled);
    }

    fn i64(&mut self, value: i64) {
        self.u64(value as u64);
    }

    fn u64(&mut self, value: u64) {
        for byte in value.to_le_bytes() {
            self.0 ^= u64::from(byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish(&self) -> u64 {
        self.0
    }
}
