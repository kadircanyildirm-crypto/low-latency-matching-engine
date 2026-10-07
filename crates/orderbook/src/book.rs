//! The limit order book and its matching logic.

use rustc_hash::FxHashMap;

use crate::bitset::LevelBitset;
use crate::pool::{NIL, OrderNode, OrderPool};
use crate::types::{
    CancelReason, Command, Event, EventSink, OrderId, Price, Qty, RejectReason, Side,
};

/// Static limits of a book.
///
/// All memory is reserved from these at construction, which is what keeps
/// [`OrderBook::process`] free of heap allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BookConfig {
    /// Lowest accepted price in ticks (inclusive).
    pub min_price: Price,
    /// Highest accepted price in ticks (inclusive). Orders outside the band are rejected,
    /// like an exchange's static price collar.
    pub max_price: Price,
    /// Maximum number of resting orders.
    pub max_orders: u32,
}

/// Aggregated view of one price level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelInfo {
    pub price: Price,
    pub qty: Qty,
    pub orders: u32,
}

/// A resting order as seen from outside the book.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderInfo {
    pub side: Side,
    pub price: Price,
    pub qty: Qty,
}

#[derive(Clone, Copy, Debug)]
struct Level {
    head: u32,
    tail: u32,
    total_qty: Qty,
    order_count: u32,
}

impl Level {
    const EMPTY: Level = Level {
        head: NIL,
        tail: NIL,
        total_qty: 0,
        order_count: 0,
    };
}

/// One side of the book: a dense ladder of price levels indexed by `price - min_price`, each
/// holding an intrusive FIFO queue of orders.
struct HalfBook {
    side: Side,
    levels: Vec<Level>,
    /// Which levels hold orders; finds the next best level without scanning empty ones.
    occupied: LevelBitset,
    /// Best level: highest index for bids, lowest for asks.
    best: Option<u32>,
}

impl HalfBook {
    fn new(side: Side, levels: usize) -> Self {
        Self {
            side,
            levels: vec![Level::EMPTY; levels],
            occupied: LevelBitset::new(levels),
            best: None,
        }
    }

    #[inline]
    fn is_better(&self, a: u32, b: u32) -> bool {
        match self.side {
            Side::Buy => a > b,
            Side::Sell => a < b,
        }
    }

    /// Appends the order in `slot` to the back of its level's queue.
    #[inline]
    fn push_back(&mut self, pool: &mut OrderPool, slot: u32) {
        let OrderNode {
            level, remaining, ..
        } = *pool.get(slot);
        let lvl = &mut self.levels[level as usize];
        let tail = lvl.tail;
        let node = pool.get_mut(slot);
        node.prev = tail;
        node.next = NIL;
        if tail == NIL {
            lvl.head = slot;
        } else {
            pool.get_mut(tail).next = slot;
        }
        lvl.tail = slot;
        lvl.total_qty += remaining;
        lvl.order_count += 1;
        if lvl.order_count == 1 {
            self.occupied.insert(level as usize);
            if self.best.is_none_or(|best| self.is_better(level, best)) {
                self.best = Some(level);
            }
        }
    }

    /// Unlinks the order in `slot` from anywhere in its level's queue. The slot itself stays
    /// allocated; freeing it is the caller's job.
    #[inline]
    fn unlink(&mut self, pool: &mut OrderPool, slot: u32) {
        let OrderNode {
            level,
            remaining,
            prev,
            next,
            ..
        } = *pool.get(slot);
        let lvl = &mut self.levels[level as usize];
        if prev == NIL {
            lvl.head = next;
        } else {
            pool.get_mut(prev).next = next;
        }
        if next == NIL {
            lvl.tail = prev;
        } else {
            pool.get_mut(next).prev = prev;
        }
        lvl.total_qty -= remaining;
        lvl.order_count -= 1;
        if lvl.order_count == 0 {
            self.level_emptied(level);
        }
    }

    #[inline]
    fn level_emptied(&mut self, level: u32) {
        self.occupied.remove(level as usize);
        if self.best == Some(level) {
            let next = match self.side {
                Side::Buy => self.occupied.prev_at_or_before(level as usize),
                Side::Sell => self.occupied.next_at_or_after(level as usize),
            };
            self.best = next.map(|i| i as u32);
        }
    }
}

/// A single instrument's limit order book with price-time priority.
///
/// Single-threaded and deterministic by design: commands go in one at a time and the same
/// command stream always produces the same event stream. After construction, processing a
/// command performs no heap allocation.
pub struct OrderBook {
    config: BookConfig,
    bids: HalfBook,
    asks: HalfBook,
    pool: OrderPool,
    /// Order id -> pool slot. Reserved at twice `max_orders` so that clearing out deleted
    /// entries is always an in-place rehash, never a reallocation.
    index: FxHashMap<OrderId, u32>,
}

impl OrderBook {
    /// Builds an empty book, reserving all memory it will ever use.
    ///
    /// # Panics
    ///
    /// If `max_price < min_price`, the band spans `u32::MAX` levels or more, or `max_orders`
    /// is zero or `u32::MAX`.
    pub fn new(config: BookConfig) -> Self {
        let levels = i128::from(config.max_price) - i128::from(config.min_price) + 1;
        assert!(levels >= 1, "max_price must be >= min_price");
        assert!(levels < i128::from(u32::MAX), "price band too wide");
        assert!(
            config.max_orders > 0 && config.max_orders < NIL,
            "max_orders out of range"
        );
        let levels = levels as usize;
        let mut index = FxHashMap::default();
        index.reserve((config.max_orders as usize).saturating_mul(2));
        Self {
            config,
            bids: HalfBook::new(Side::Buy, levels),
            asks: HalfBook::new(Side::Sell, levels),
            pool: OrderPool::with_capacity(config.max_orders),
            index,
        }
    }

    /// Applies one command, reporting its outcome to `sink`.
    pub fn process<S: EventSink>(&mut self, command: Command, sink: &mut S) {
        let result = match command {
            Command::Limit {
                id,
                side,
                price,
                qty,
            } => self.new_limit(id, side, price, qty, sink),
            Command::Market { id, side, qty } => self.new_market(id, side, qty, sink),
            Command::Cancel { id } => self.cancel(id, sink),
            Command::Modify { id, price, qty } => self.modify(id, price, qty, sink),
        };
        if let Err(reason) = result {
            sink.on_event(Event::Rejected {
                id: command.id(),
                reason,
            });
        }
    }

    // Each handler validates everything before emitting its first event, so a rejected
    // command leaves no trace besides the `Rejected` event.

    fn new_limit<S: EventSink>(
        &mut self,
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        if qty == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        let level = self.level_of(price).ok_or(RejectReason::PriceOutOfRange)?;
        if self.index.contains_key(&id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        // Checked up front even if the order would trade in full: a remainder must never
        // find the pool exhausted after `Accepted` has gone out.
        if self.pool.is_full() {
            return Err(RejectReason::BookFull);
        }
        sink.on_event(Event::Accepted { id });
        self.execute_limit(id, side, level, qty, sink);
        Ok(())
    }

    fn new_market<S: EventSink>(
        &mut self,
        id: OrderId,
        side: Side,
        qty: Qty,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        if qty == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        if self.index.contains_key(&id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        sink.on_event(Event::Accepted { id });
        let unfilled = self.match_incoming(id, side, qty, None, sink);
        if unfilled > 0 {
            sink.on_event(Event::Cancelled {
                id,
                qty: unfilled,
                reason: CancelReason::NoLiquidity,
            });
        }
        Ok(())
    }

    fn cancel<S: EventSink>(&mut self, id: OrderId, sink: &mut S) -> Result<(), RejectReason> {
        let slot = self.index.remove(&id).ok_or(RejectReason::UnknownOrder)?;
        let OrderNode {
            side, remaining, ..
        } = *self.pool.get(slot);
        let (half, pool) = self.half_and_pool(side);
        half.unlink(pool, slot);
        pool.free(slot);
        sink.on_event(Event::Cancelled {
            id,
            qty: remaining,
            reason: CancelReason::Requested,
        });
        Ok(())
    }

    fn modify<S: EventSink>(
        &mut self,
        id: OrderId,
        price: Price,
        qty: Qty,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        if qty == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        let level = self.level_of(price).ok_or(RejectReason::PriceOutOfRange)?;
        let slot = *self.index.get(&id).ok_or(RejectReason::UnknownOrder)?;
        let node = *self.pool.get(slot);
        let (half, pool) = self.half_and_pool(node.side);

        if level == node.level && qty <= node.remaining {
            // Same price, less (or equal) size: shrink in place and keep queue position.
            half.levels[level as usize].total_qty -= node.remaining - qty;
            pool.get_mut(slot).remaining = qty;
            sink.on_event(Event::Modified { id, price, qty });
            return Ok(());
        }

        // Anything else is a cancel/replace: the order goes to the back of the queue at its
        // new price and may trade on the way in.
        half.unlink(pool, slot);
        pool.free(slot);
        self.index.remove(&id);
        sink.on_event(Event::Modified { id, price, qty });
        self.execute_limit(id, node.side, level, qty, sink);
        Ok(())
    }

    /// Matches a validated limit order and rests whatever is left.
    #[inline]
    fn execute_limit<S: EventSink>(
        &mut self,
        id: OrderId,
        side: Side,
        level: u32,
        qty: Qty,
        sink: &mut S,
    ) {
        let remaining = self.match_incoming(id, side, qty, Some(level), sink);
        if remaining == 0 {
            return;
        }
        let slot = self.pool.alloc(OrderNode {
            id,
            remaining,
            level,
            prev: NIL,
            next: NIL,
            side,
        });
        self.index.insert(id, slot);
        let (half, pool) = self.half_and_pool(side);
        half.push_back(pool, slot);
        sink.on_event(Event::Rested {
            id,
            side,
            price: self.price_of(level),
            qty: remaining,
        });
    }

    /// The matching loop: trades the incoming order against the opposite side, best price
    /// first and oldest order first within a price, until it is filled or no longer crosses
    /// `limit` (`None` = any price). Returns the unfilled quantity.
    #[inline]
    fn match_incoming<S: EventSink>(
        &mut self,
        taker: OrderId,
        taker_side: Side,
        mut qty: Qty,
        limit: Option<u32>,
        sink: &mut S,
    ) -> Qty {
        let min_price = self.config.min_price;
        let Self {
            bids,
            asks,
            pool,
            index,
            ..
        } = self;
        let resting = match taker_side {
            Side::Buy => asks,
            Side::Sell => bids,
        };

        while qty > 0 {
            let Some(level) = resting.best else { break };
            if let Some(limit) = limit {
                let crosses = match taker_side {
                    Side::Buy => level <= limit,
                    Side::Sell => level >= limit,
                };
                if !crosses {
                    break;
                }
            }
            let price = min_price + Price::from(level);
            let lvl = &mut resting.levels[level as usize];

            while qty > 0 && lvl.head != NIL {
                let maker_slot = lvl.head;
                let maker = pool.get_mut(maker_slot);
                let fill = qty.min(maker.remaining);
                maker.remaining -= fill;
                lvl.total_qty -= fill;
                qty -= fill;
                sink.on_event(Event::Trade {
                    taker,
                    maker: maker.id,
                    taker_side,
                    price,
                    qty: fill,
                });
                if maker.remaining == 0 {
                    let (maker_id, next) = (maker.id, maker.next);
                    lvl.head = next;
                    if next == NIL {
                        lvl.tail = NIL;
                    } else {
                        pool.get_mut(next).prev = NIL;
                    }
                    lvl.order_count -= 1;
                    index.remove(&maker_id);
                    pool.free(maker_slot);
                }
            }

            if lvl.head == NIL {
                resting.level_emptied(level);
            }
        }
        qty
    }

    #[inline]
    fn half_and_pool(&mut self, side: Side) -> (&mut HalfBook, &mut OrderPool) {
        let half = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        (half, &mut self.pool)
    }

    #[inline]
    fn half(&self, side: Side) -> &HalfBook {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    #[inline]
    fn level_of(&self, price: Price) -> Option<u32> {
        let offset = price.checked_sub(self.config.min_price)?;
        (0..self.bids.levels.len() as i64)
            .contains(&offset)
            .then_some(offset as u32)
    }

    #[inline]
    fn price_of(&self, level: u32) -> Price {
        self.config.min_price + Price::from(level)
    }

    // ----------------------------------------------------------------------------------
    // Read-only queries. None of these are needed for matching; they serve market data,
    // tests and debugging.

    /// The configuration this book was built with.
    pub fn config(&self) -> BookConfig {
        self.config
    }

    /// Number of resting orders.
    pub fn order_count(&self) -> usize {
        self.pool.live()
    }

    /// Highest bid level.
    pub fn best_bid(&self) -> Option<LevelInfo> {
        self.depth(Side::Buy).next()
    }

    /// Lowest ask level.
    pub fn best_ask(&self) -> Option<LevelInfo> {
        self.depth(Side::Sell).next()
    }

    /// A resting order, if `id` is on the book.
    pub fn order(&self, id: OrderId) -> Option<OrderInfo> {
        let node = self.pool.get(*self.index.get(&id)?);
        Some(OrderInfo {
            side: node.side,
            price: self.price_of(node.level),
            qty: node.remaining,
        })
    }

    /// Occupied levels of one side, best price first.
    pub fn depth(&self, side: Side) -> Depth<'_> {
        let half = self.half(side);
        Depth {
            half,
            min_price: self.config.min_price,
            next: half.best,
        }
    }

    /// Orders resting at `price` on one side, in time priority.
    pub fn queue(&self, side: Side, price: Price) -> Queue<'_> {
        let head = self
            .level_of(price)
            .map_or(NIL, |level| self.half(side).levels[level as usize].head);
        Queue {
            pool: &self.pool,
            next: head,
        }
    }

    /// Exhaustively checks the book's internal invariants: queue links, per-level
    /// aggregates, occupancy bits, best-price pointers, the id index, and that the book is
    /// not crossed.
    ///
    /// `O(levels + orders)`; for tests and debugging, never for the hot path.
    pub fn validate(&self) -> Result<(), String> {
        let mut resting = 0usize;
        for half in [&self.bids, &self.asks] {
            let side = half.side;
            let mut expected_best: Option<u32> = None;
            for (i, lvl) in half.levels.iter().enumerate() {
                let level = i as u32;
                let price = self.price_of(level);
                if (lvl.order_count > 0) != half.occupied.contains(i) {
                    return Err(format!(
                        "{side:?} {price}: occupancy bit disagrees with {} orders",
                        lvl.order_count
                    ));
                }
                if lvl.order_count == 0 {
                    if lvl.head != NIL || lvl.tail != NIL || lvl.total_qty != 0 {
                        return Err(format!("{side:?} {price}: empty level has stale state"));
                    }
                    continue;
                }

                let (mut count, mut total, mut prev, mut cur) = (0u32, 0 as Qty, NIL, lvl.head);
                while cur != NIL {
                    let node = self.pool.get(cur);
                    if node.prev != prev {
                        return Err(format!(
                            "{side:?} {price}: broken back link at #{}",
                            node.id
                        ));
                    }
                    if node.level != level || node.side != side {
                        return Err(format!("{side:?} {price}: #{} is misfiled", node.id));
                    }
                    if node.remaining == 0 {
                        return Err(format!(
                            "{side:?} {price}: #{} rests with zero qty",
                            node.id
                        ));
                    }
                    if self.index.get(&node.id) != Some(&cur) {
                        return Err(format!("{side:?} {price}: index disagrees on #{}", node.id));
                    }
                    count += 1;
                    total += node.remaining;
                    prev = cur;
                    cur = node.next;
                    if count as usize > self.pool.capacity() {
                        return Err(format!("{side:?} {price}: cycle in queue"));
                    }
                }
                if prev != lvl.tail {
                    return Err(format!(
                        "{side:?} {price}: tail does not point at last order"
                    ));
                }
                if count != lvl.order_count || total != lvl.total_qty {
                    return Err(format!(
                        "{side:?} {price}: aggregates say {}/{} but queue holds {count}/{total}",
                        lvl.order_count, lvl.total_qty
                    ));
                }
                resting += count as usize;
                if expected_best.is_none_or(|best| half.is_better(level, best)) {
                    expected_best = Some(level);
                }
            }
            if half.best != expected_best {
                return Err(format!(
                    "{side:?}: best is {:?}, expected {expected_best:?}",
                    half.best
                ));
            }
        }
        if resting != self.pool.live() || resting != self.index.len() {
            return Err(format!(
                "{resting} orders in queues, {} in pool, {} in index",
                self.pool.live(),
                self.index.len()
            ));
        }
        if let (Some(bid), Some(ask)) = (self.bids.best, self.asks.best) {
            if bid >= ask {
                return Err(format!(
                    "book is crossed: bid {} >= ask {}",
                    self.price_of(bid),
                    self.price_of(ask)
                ));
            }
        }
        Ok(())
    }
}

/// Iterator over the occupied levels of one side, best price first.
pub struct Depth<'a> {
    half: &'a HalfBook,
    min_price: Price,
    next: Option<u32>,
}

impl Iterator for Depth<'_> {
    type Item = LevelInfo;

    fn next(&mut self) -> Option<LevelInfo> {
        let level = self.next?;
        let i = level as usize;
        self.next = match self.half.side {
            Side::Buy => i
                .checked_sub(1)
                .and_then(|j| self.half.occupied.prev_at_or_before(j)),
            Side::Sell => self.half.occupied.next_at_or_after(i + 1),
        }
        .map(|j| j as u32);
        let lvl = &self.half.levels[i];
        Some(LevelInfo {
            price: self.min_price + Price::from(level),
            qty: lvl.total_qty,
            orders: lvl.order_count,
        })
    }
}

/// Iterator over the orders at one price level as `(id, open qty)`, in time priority.
pub struct Queue<'a> {
    pool: &'a OrderPool,
    next: u32,
}

impl Iterator for Queue<'_> {
    type Item = (OrderId, Qty);

    fn next(&mut self) -> Option<(OrderId, Qty)> {
        if self.next == NIL {
            return None;
        }
        let node = self.pool.get(self.next);
        self.next = node.next;
        Some((node.id, node.remaining))
    }
}
