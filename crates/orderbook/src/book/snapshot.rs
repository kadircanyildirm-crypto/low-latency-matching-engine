//! Snapshots of a book's complete state, restoring a book from one, and a digest of that
//! state.
//!
//! A snapshot holds everything that decides how the book responds to future commands, so a
//! restored book is indistinguishable from the original: same events, same trade ids, same
//! queue positions. The digest is a platform-independent hash of the same state, cheap
//! enough to compare a replica or a replay against the live book without shipping the
//! whole snapshot.

use std::fmt;

use super::{BookConfig, OrderBook, OrderNode, StopOrder};
use crate::types::{OrderId, OwnerId, Phase, Price, Qty, SelfTradePolicy, Side};

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
    /// Whether the order was entered post-only; it still restricts the order's modifies.
    pub post_only: bool,
    /// Iceberg display quantity, if the order is an iceberg.
    pub display: Option<Qty>,
    /// The part of `leaves` on display: all of it for a plain order, the current tranche's
    /// remainder for an iceberg.
    pub visible: Qty,
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
    /// The price band's reference: the last trade price, or the configured reference
    /// price before the first trade.
    pub reference_price: Option<Price>,
    /// The trading phase.
    pub phase: Phase,
    /// Resting orders: bids best price first, then asks best price first, and within each
    /// price level in time priority.
    pub orders: Vec<SnapshotOrder>,
    /// Pending stops: buy stops in trigger order, then sell stops in trigger order.
    pub stops: Vec<StopOrder>,
}

impl BookSnapshot {
    /// The largest trade count [`OrderBook::restore`] accepts: 2⁶³ − 1. It leaves the
    /// restored book 2⁶³ trade ids, which at a billion trades a second last 292 years, so
    /// the counter can never run out. A book that starts empty has 2⁶⁴ − 1 ids.
    pub const MAX_TRADE_COUNT: u64 = i64::MAX as u64;
}

/// Why a snapshot cannot be restored. Each variant names a state the engine itself can
/// never reach, so it points at a corrupted or hand-edited snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// More orders and stops than the configuration's `max_orders`.
    TooManyOrders,
    /// Two orders share an id.
    DuplicateOrderId(OrderId),
    /// A resting order whose owner is not below `max_owners`, whose price is outside the
    /// band, or whose quantities do not fit; or a pending stop whose owner, prices or
    /// quantity do not fit, or whose trigger the last trade price has already reached. For a
    /// resting order: its price is outside the band, its open
    /// quantity is zero, its open plus filled quantity exceeds `max_order_qty`, or what it
    /// shows does not fit its quantity and display: a plain order shows everything, an
    /// iceberg between one lot and its display, and `max_iceberg_tranches` of its display
    /// cover its total.
    InvalidOrder(OrderId),
    /// The best bid is at or above the best ask outside a call phase.
    Crossed,
    /// The trade count is above [`BookSnapshot::MAX_TRADE_COUNT`], too close to the end of
    /// the trade id counter.
    TradeCountExhausted,
    /// The reference price lies outside the price band.
    InvalidReferencePrice,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyOrders => f.write_str("more orders than max_orders"),
            Self::DuplicateOrderId(id) => write!(f, "order id {id} appears twice"),
            Self::InvalidOrder(id) => write!(f, "order {id} cannot rest in this book"),
            Self::Crossed => {
                f.write_str("the best bid is at or above the best ask outside a call phase")
            }
            Self::TradeCountExhausted => f.write_str("too few trade ids left"),
            Self::InvalidReferencePrice => f.write_str("the reference price is outside the band"),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl BookSnapshot {
    /// The digest of this state; equal to [`OrderBook::digest`] of the book it was taken
    /// from or restores to.
    pub fn digest(&self) -> u64 {
        let mut hash = Digest::new(
            &self.config,
            self.trade_count,
            self.reference_price,
            self.phase,
            self.orders.len(),
        );
        self.orders.iter().for_each(|order| hash.order(order));
        hash.u64(self.stops.len() as u64);
        self.stops.iter().for_each(|stop| hash.stop(stop));
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
            reference_price: self.reference_price(),
            phase: self.phase,
            orders,
            stops: self.all_stops().collect(),
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
        if snapshot.orders.len() + snapshot.stops.len() > snapshot.config.max_orders as usize {
            return Err(SnapshotError::TooManyOrders);
        }
        if snapshot.trade_count > BookSnapshot::MAX_TRADE_COUNT {
            return Err(SnapshotError::TradeCountExhausted);
        }
        book.next_trade_id = snapshot.trade_count + 1;
        book.reference = match snapshot.reference_price {
            None => None,
            Some(price) => Some(
                book.level_of(price)
                    .ok_or(SnapshotError::InvalidReferencePrice)?,
            ),
        };
        for order in &snapshot.orders {
            let invalid = SnapshotError::InvalidOrder(order.id);
            book.check_owner(order.owner).map_err(|_| invalid)?;
            let level = book.level_of(order.price).ok_or(invalid)?;
            let total = order
                .leaves
                .checked_add(order.filled)
                .filter(|&total| order.leaves > 0 && total <= snapshot.config.max_order_qty)
                .ok_or(invalid)?;
            let shows_validly = match order.display {
                None => order.visible == order.leaves,
                Some(display) => {
                    order.visible > 0
                        && order.visible <= display.min(order.leaves)
                        && book.tranches_cover(display, total)
                }
            };
            if !shows_validly {
                return Err(invalid);
            }
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
                order.post_only,
            ));
            if let Some(display) = order.display {
                book.pool.make_iceberg(slot, display, order.visible);
            }
            // Orders arrive in book order, so each owner's list ends up in queue order
            // within every level, which is all that mass cancels depend on.
            book.place(slot);
        }
        for stop in &snapshot.stops {
            let invalid = SnapshotError::InvalidOrder(stop.id);
            book.check_owner(stop.owner).map_err(|_| invalid)?;
            book.check_qty(stop.qty).map_err(|_| invalid)?;
            let trigger = book.level_of(stop.trigger).ok_or(invalid)?;
            let limit = match stop.limit {
                None => None,
                Some(price) => Some(book.level_of(price).ok_or(invalid)?),
            };
            if book.reached(stop.side, trigger) {
                return Err(invalid);
            }
            if book.index.contains_key(&stop.id) {
                return Err(SnapshotError::DuplicateOrderId(stop.id));
            }
            let slot = book.pool.alloc(OrderNode::stop(
                stop.id, stop.owner, stop.side, trigger, limit, stop.qty,
            ));
            book.place(slot);
        }
        book.phase = snapshot.phase;
        if let (Some(bid), Some(ask)) = (book.bids.best, book.asks.best) {
            if bid >= ask && snapshot.phase != Phase::Auction {
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
        // `order_count` counts pending stops too; the encoding counts them separately.
        let stops = self.all_stops().count();
        let mut hash = Digest::new(
            &self.config,
            self.trade_count(),
            self.reference_price(),
            self.phase,
            self.order_count() - stops,
        );
        self.for_each_order(|order| hash.order(&order));
        hash.u64(stops as u64);
        self.all_stops().for_each(|stop| hash.stop(&stop));
        hash.finish()
    }

    /// Every pending stop in snapshot order.
    fn all_stops(&self) -> impl Iterator<Item = StopOrder> + '_ {
        self.stops(Side::Buy).chain(self.stops(Side::Sell))
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
                        post_only: order.post_only,
                        display: order.display,
                        visible: order.visible,
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
    fn new(
        config: &BookConfig,
        trade_count: u64,
        reference_price: Option<Price>,
        phase: Phase,
        orders: usize,
    ) -> Self {
        let mut hash = Self(0xcbf2_9ce4_8422_2325);
        hash.i64(config.min_price);
        hash.i64(config.max_price);
        hash.u64(u64::from(config.max_orders));
        hash.u64(u64::from(config.max_owners));
        hash.u64(config.max_order_qty);
        hash.u64(u64::from(config.max_iceberg_tranches));
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
        hash.optional_u64(config.price_band.map(u64::from));
        hash.optional_u64(config.reference_price.map(|price| price as u64));
        hash.u64(u64::from(config.auction_on_band));
        hash.u64(trade_count);
        hash.optional_u64(reference_price.map(|price| price as u64));
        hash.u64(match phase {
            Phase::Continuous => 0,
            Phase::Auction => 1,
            Phase::Halted => 2,
            Phase::Closed => 3,
        });
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
        self.u64(u64::from(order.post_only));
        match order.display {
            None => self.u64(0),
            Some(display) => {
                self.u64(1);
                self.u64(display);
            }
        }
        self.u64(order.visible);
    }

    fn stop(&mut self, stop: &StopOrder) {
        self.u64(match stop.side {
            Side::Buy => 0,
            Side::Sell => 1,
        });
        self.i64(stop.trigger);
        self.u64(stop.id);
        self.u64(u64::from(stop.owner));
        self.optional_u64(stop.limit.map(|price| price as u64));
        self.u64(stop.qty);
    }

    fn optional_u64(&mut self, value: Option<u64>) {
        match value {
            None => self.u64(0),
            Some(value) => {
                self.u64(1);
                self.u64(value);
            }
        }
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
