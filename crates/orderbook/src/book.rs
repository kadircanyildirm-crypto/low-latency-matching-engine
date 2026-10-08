//! The limit order book and its matching logic.

mod snapshot;

pub use snapshot::{BookSnapshot, SnapshotError, SnapshotOrder};

use rustc_hash::FxHashMap;

use crate::bitset::LevelBitset;
use crate::owners::Owners;
use crate::pool::{NIL, OrderNode, OrderPool};
use crate::types::{
    CancelReason, Command, Event, EventSink, OrderId, OwnerId, Price, Qty, RejectReason,
    SelfTradePolicy, Side, TradeId,
};

/// Static limits and policies of a book.
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
    /// Owner ids run from 0 to `max_owners - 1`. The gateway assigns these dense indices to
    /// participants, so the book can keep per-owner state in plain arrays. New orders from
    /// any other owner id are rejected.
    pub max_owners: u32,
    /// Largest accepted order quantity. `max_orders * max_order_qty` must fit in a `u64`,
    /// which makes every quantity sum inside the book overflow-free by construction.
    pub max_order_qty: Qty,
    /// Price protection in ticks, measured from the opposite best price when an order
    /// arrives. Market orders stop trading beyond it; limit orders priced further through
    /// it are rejected. `None` disables protection.
    pub price_protection: Option<u32>,
    /// What happens when an order would trade against an order of the same owner.
    pub self_trade: SelfTradePolicy,
}

impl BookConfig {
    /// Owners allowed by [`BookConfig::new`].
    pub const DEFAULT_MAX_OWNERS: u32 = 1_024;

    /// A config with the given band and capacity, [`Self::DEFAULT_MAX_OWNERS`] owners, the
    /// largest `max_order_qty` the capacity allows, no price protection, and
    /// `CancelResting` self-trade prevention.
    pub const fn new(min_price: Price, max_price: Price, max_orders: u32) -> Self {
        let max_orders_nonzero = if max_orders == 0 { 1 } else { max_orders };
        Self {
            min_price,
            max_price,
            max_orders,
            max_owners: Self::DEFAULT_MAX_OWNERS,
            max_order_qty: u64::MAX / max_orders_nonzero as u64,
            price_protection: None,
            self_trade: SelfTradePolicy::CancelResting,
        }
    }
}

/// Aggregated view of one price level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelInfo {
    /// Level price.
    pub price: Price,
    /// Total open quantity at the level.
    pub qty: Qty,
    /// Number of orders at the level.
    pub orders: u32,
}

/// A resting order as seen from outside the book.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderInfo {
    /// Owner of the order.
    pub owner: OwnerId,
    /// Side.
    pub side: Side,
    /// Limit price.
    pub price: Price,
    /// Open quantity.
    pub leaves: Qty,
    /// Quantity filled so far.
    pub filled: Qty,
}

/// One order in a level's queue, as yielded by [`OrderBook::queue`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueuedOrder {
    /// Order id.
    pub id: OrderId,
    /// Owner of the order.
    pub owner: OwnerId,
    /// Open quantity.
    pub leaves: Qty,
    /// Quantity filled so far.
    pub filled: Qty,
}

#[derive(Clone, Copy, Debug)]
struct Level {
    head: u32,
    tail: u32,
    total_qty: Qty,
    order_count: u32,
}

// The ladder's memory is `2 * levels * size_of::<Level>()`; see docs/DESIGN.md.
const _: () = assert!(size_of::<Level>() == 24);

impl Level {
    const EMPTY: Level = Level {
        head: NIL,
        tail: NIL,
        total_qty: 0,
        order_count: 0,
    };

    /// Removes the order at the head of the queue and frees its slot. The caller has
    /// already taken its quantity out of `total_qty`.
    #[inline]
    fn pop_front(
        &mut self,
        pool: &mut OrderPool,
        index: &mut FxHashMap<OrderId, u32>,
        owners: &mut Owners,
    ) {
        let slot = self.head;
        let OrderNode {
            id, next, owner, ..
        } = *pool.get(slot);
        self.head = next;
        if next == NIL {
            self.tail = NIL;
        } else {
            pool.get_mut(next).prev = NIL;
        }
        self.order_count -= 1;
        index.remove(&id);
        owners.unlink(slot, owner);
        pool.free(slot);
    }
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
            self.best = Some(match (self.side, self.best) {
                (_, None) => level,
                (Side::Buy, Some(best)) => best.max(level),
                (Side::Sell, Some(best)) => best.min(level),
            });
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

/// Why the matching loop stopped with quantity left over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Halt {
    /// Nothing left to match: the taker is filled.
    Filled,
    /// The opposite side has no more orders.
    Empty,
    /// The opposite best no longer crosses the taker's limit (or protection cap).
    Limit,
    /// Self-trade prevention cancelled the rest of the taker.
    SelfTrade,
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
    /// Each owner's resting orders, for mass cancels.
    owners: Owners,
    /// Sort buffer for mass cancels, sized for every resting order up front.
    scratch: Vec<MassCancelKey>,
    next_trade_id: TradeId,
}

/// Sort key that puts an owner's orders in book order: side (bids first), price priority,
/// then position in the owner's list, which within a level is queue order. The last field
/// is the slot.
type MassCancelKey = (u8, u32, u32, u32);

impl OrderBook {
    /// Builds an empty book, reserving all memory it will ever use.
    ///
    /// # Panics
    ///
    /// If `max_price < min_price`, the band spans `u32::MAX` levels or more, `max_orders`
    /// is zero or `u32::MAX`, `max_order_qty` is zero, or `max_orders * max_order_qty`
    /// does not fit in a `u64`.
    pub fn new(config: BookConfig) -> Self {
        let levels = i128::from(config.max_price) - i128::from(config.min_price) + 1;
        assert!(levels >= 1, "max_price must be >= min_price");
        assert!(levels < i128::from(u32::MAX), "price band too wide");
        assert!(
            config.max_orders > 0 && config.max_orders < NIL,
            "max_orders out of range"
        );
        assert!(config.max_order_qty > 0, "max_order_qty must be positive");
        assert!(config.max_owners > 0, "max_owners must be positive");
        assert!(
            u128::from(config.max_orders) * u128::from(config.max_order_qty)
                <= u128::from(u64::MAX),
            "max_orders * max_order_qty must fit in a u64"
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
            owners: Owners::new(config.max_owners, config.max_orders),
            scratch: Vec::with_capacity(config.max_orders as usize),
            next_trade_id: 1,
        }
    }

    /// Applies one command, reporting its outcome to `sink`.
    pub fn process<S: EventSink>(&mut self, command: Command, sink: &mut S) {
        let (id, result) = match command {
            Command::Limit {
                id,
                owner,
                side,
                price,
                qty,
            } => (id, self.new_limit(id, owner, side, price, qty, sink)),
            Command::Market {
                id,
                owner,
                side,
                qty,
            } => (id, self.new_market(id, owner, side, qty, sink)),
            Command::Cancel { id, owner } => (id, self.cancel(id, owner, sink)),
            Command::Modify {
                id,
                owner,
                price,
                qty,
            } => (id, self.modify(id, owner, price, qty, sink)),
            Command::CancelAll { owner } => return self.cancel_all(owner, sink),
        };
        if let Err(reason) = result {
            sink.on_event(Event::Rejected { id, reason });
        }
    }

    // Each handler validates everything before emitting its first event, so a rejected
    // command leaves no trace besides the `Rejected` event. Stateless checks (owner,
    // quantity, price band) come before stateful ones (ids, protection, capacity).

    fn new_limit<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        price: Price,
        qty: Qty,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        self.check_owner(owner)?;
        self.check_qty(qty)?;
        let level = self.level_of(price).ok_or(RejectReason::PriceOutOfRange)?;
        if self.index.contains_key(&id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if self.outside_protection(side, level) {
            return Err(RejectReason::PriceOutsideProtection);
        }
        // A crossing order always finds room to rest: its first match either fills it or
        // removes a resting order (by filling it or by self-trade prevention), freeing a
        // slot. So only an order that can do nothing but rest is refused when full.
        if self.pool.is_full() && !self.crosses(side, level) {
            return Err(RejectReason::BookFull);
        }
        sink.on_event(Event::Accepted { id });
        self.execute_limit(id, owner, side, level, qty, qty, sink);
        Ok(())
    }

    fn new_market<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        qty: Qty,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        self.check_owner(owner)?;
        self.check_qty(qty)?;
        if self.index.contains_key(&id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        sink.on_event(Event::Accepted { id });
        let cap = self.protection_cap(side);
        let (unfilled, halt) = self.match_incoming(id, owner, side, qty, cap, sink);
        if unfilled > 0 {
            let reason = match halt {
                Halt::SelfTrade => CancelReason::SelfTrade,
                Halt::Limit => CancelReason::PriceProtection,
                Halt::Empty | Halt::Filled => CancelReason::NoLiquidity,
            };
            sink.on_event(Event::Cancelled {
                id,
                qty: unfilled,
                reason,
            });
        }
        Ok(())
    }

    fn cancel<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        let slot = self.owned_slot(id, owner)?;
        let remaining = self.pool.get(slot).remaining;
        self.remove(slot);
        sink.on_event(Event::Cancelled {
            id,
            qty: remaining,
            reason: CancelReason::Requested,
        });
        Ok(())
    }

    /// Cancels all of `owner`'s resting orders in book order. The owner's list holds them in
    /// the order they started resting; sorting by side, price priority and list position
    /// turns that into book order, because within one level list order is queue order.
    fn cancel_all<S: EventSink>(&mut self, owner: OwnerId, sink: &mut S) {
        let mut keys = std::mem::take(&mut self.scratch);
        keys.clear();
        let mut slot = self.owners.list(owner).head;
        let mut position = 0;
        while slot != NIL {
            let node = self.pool.get(slot);
            let key = match node.side {
                Side::Buy => (0, u32::MAX - node.level, position, slot),
                Side::Sell => (1, node.level, position, slot),
            };
            keys.push(key);
            position += 1;
            slot = self.owners.link_of(slot).next;
        }
        keys.sort_unstable();
        for &(.., slot) in &keys {
            let OrderNode { id, remaining, .. } = *self.pool.get(slot);
            self.remove(slot);
            sink.on_event(Event::Cancelled {
                id,
                qty: remaining,
                reason: CancelReason::MassCancel,
            });
        }
        sink.on_event(Event::MassCancelled {
            owner,
            count: position,
        });
        self.scratch = keys;
    }

    fn modify<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        price: Price,
        qty: Qty,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        self.check_qty(qty)?;
        let level = self.level_of(price).ok_or(RejectReason::PriceOutOfRange)?;
        let slot = self.owned_slot(id, owner)?;
        let node = *self.pool.get(slot);
        let filled = node.total - node.remaining;

        if qty <= filled {
            // The new total is already filled: nothing is left to work.
            self.remove(slot);
            sink.on_event(Event::Modified {
                id,
                price,
                qty,
                leaves: 0,
            });
            return Ok(());
        }
        let leaves = qty - filled;

        if level == node.level && leaves <= node.remaining {
            // Same price, total not increased: shrink in place and keep queue position.
            let (half, pool) = self.half_and_pool(node.side);
            half.levels[level as usize].total_qty -= node.remaining - leaves;
            let n = pool.get_mut(slot);
            n.remaining = leaves;
            n.total = qty;
            sink.on_event(Event::Modified {
                id,
                price,
                qty,
                leaves,
            });
            return Ok(());
        }

        // Cancel/replace: back of the queue at the new price, trading on the way in. The
        // order's own slot is freed first, so it can always rest again.
        if self.outside_protection(node.side, level) {
            return Err(RejectReason::PriceOutsideProtection);
        }
        self.remove(slot);
        sink.on_event(Event::Modified {
            id,
            price,
            qty,
            leaves,
        });
        self.execute_limit(id, owner, node.side, level, leaves, qty, sink);
        Ok(())
    }

    /// Matches a validated limit order and rests whatever is left. `open` is the quantity
    /// to work, `total` the order's total quantity including earlier fills.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn execute_limit<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        level: u32,
        open: Qty,
        total: Qty,
        sink: &mut S,
    ) {
        let (remaining, halt) = self.match_incoming(id, owner, side, open, Some(level), sink);
        if remaining == 0 {
            return;
        }
        if halt == Halt::SelfTrade {
            sink.on_event(Event::Cancelled {
                id,
                qty: remaining,
                reason: CancelReason::SelfTrade,
            });
            return;
        }
        let slot = self
            .pool
            .alloc(OrderNode::new(id, owner, side, level, remaining, total));
        self.place(slot);
        sink.on_event(Event::Rested {
            id,
            side,
            price: self.price_of(level),
            qty: remaining,
        });
    }

    /// The matching loop: trades the incoming order against the opposite side, best price
    /// first and oldest order first within a price, until it is filled or the opposite best
    /// no longer crosses `limit` (`None` = any price). Returns the unfilled quantity and
    /// why matching stopped.
    #[inline]
    fn match_incoming<S: EventSink>(
        &mut self,
        taker: OrderId,
        owner: OwnerId,
        taker_side: Side,
        mut qty: Qty,
        limit: Option<u32>,
        sink: &mut S,
    ) -> (Qty, Halt) {
        let min_price = self.config.min_price;
        let policy = self.config.self_trade;
        let Self {
            bids,
            asks,
            pool,
            index,
            owners,
            next_trade_id,
            ..
        } = self;
        let resting = match taker_side {
            Side::Buy => asks,
            Side::Sell => bids,
        };

        while qty > 0 {
            let Some(level) = resting.best else {
                return (qty, Halt::Empty);
            };
            if let Some(limit) = limit {
                let crosses = match taker_side {
                    Side::Buy => level <= limit,
                    Side::Sell => level >= limit,
                };
                if !crosses {
                    return (qty, Halt::Limit);
                }
            }
            let price = min_price + Price::from(level);
            let lvl = &mut resting.levels[level as usize];

            while qty > 0 && lvl.head != NIL {
                let maker = pool.get_mut(lvl.head);
                if maker.owner == owner {
                    // Self-trade prevention.
                    if policy == SelfTradePolicy::CancelIncoming {
                        return (qty, Halt::SelfTrade);
                    }
                    let (maker_id, maker_leaves) = (maker.id, maker.remaining);
                    lvl.total_qty -= maker_leaves;
                    lvl.pop_front(pool, index, owners);
                    sink.on_event(Event::Cancelled {
                        id: maker_id,
                        qty: maker_leaves,
                        reason: CancelReason::SelfTrade,
                    });
                    continue;
                }

                let fill = qty.min(maker.remaining);
                maker.remaining -= fill;
                lvl.total_qty -= fill;
                qty -= fill;
                let trade_id = *next_trade_id;
                *next_trade_id += 1;
                sink.on_event(Event::Trade {
                    trade_id,
                    taker,
                    maker: maker.id,
                    taker_side,
                    price,
                    qty: fill,
                    taker_leaves: qty,
                    maker_leaves: maker.remaining,
                });
                if maker.remaining == 0 {
                    lvl.pop_front(pool, index, owners);
                }
            }

            if lvl.head == NIL {
                resting.level_emptied(level);
            }
        }
        (0, Halt::Filled)
    }

    /// Puts an allocated order on the book: into the id index, at the back of its level's
    /// queue, and at the back of its owner's list.
    #[inline]
    fn place(&mut self, slot: u32) {
        let OrderNode {
            id, owner, side, ..
        } = *self.pool.get(slot);
        self.index.insert(id, slot);
        let (half, pool) = self.half_and_pool(side);
        half.push_back(pool, slot);
        self.owners.link(slot, owner);
    }

    /// Unlinks and frees a resting order.
    #[inline]
    fn remove(&mut self, slot: u32) {
        let OrderNode {
            id, side, owner, ..
        } = *self.pool.get(slot);
        let (half, pool) = self.half_and_pool(side);
        half.unlink(pool, slot);
        self.owners.unlink(slot, owner);
        self.pool.free(slot);
        self.index.remove(&id);
    }

    /// The slot of `id` if it rests on the book and belongs to `owner`.
    #[inline]
    fn owned_slot(&self, id: OrderId, owner: OwnerId) -> Result<u32, RejectReason> {
        match self.index.get(&id) {
            Some(&slot) if self.pool.get(slot).owner == owner => Ok(slot),
            _ => Err(RejectReason::UnknownOrder),
        }
    }

    #[inline]
    fn check_owner(&self, owner: OwnerId) -> Result<(), RejectReason> {
        if owner < self.config.max_owners {
            Ok(())
        } else {
            Err(RejectReason::InvalidOwner)
        }
    }

    #[inline]
    fn check_qty(&self, qty: Qty) -> Result<(), RejectReason> {
        if qty == 0 || qty > self.config.max_order_qty {
            Err(RejectReason::InvalidQuantity)
        } else {
            Ok(())
        }
    }

    /// Whether a `side` order at `level` would trade with the opposite best.
    #[inline]
    fn crosses(&self, side: Side, level: u32) -> bool {
        match side {
            Side::Buy => self.asks.best.is_some_and(|ask| level >= ask),
            Side::Sell => self.bids.best.is_some_and(|bid| level <= bid),
        }
    }

    /// Whether a limit price lies more than `price_protection` ticks through the opposite
    /// best price.
    #[inline]
    fn outside_protection(&self, side: Side, level: u32) -> bool {
        let Some(ticks) = self.config.price_protection else {
            return false;
        };
        let (level, ticks) = (u64::from(level), u64::from(ticks));
        match side {
            Side::Buy => self
                .asks
                .best
                .is_some_and(|ask| level > u64::from(ask) + ticks),
            Side::Sell => self
                .bids
                .best
                .is_some_and(|bid| level + ticks < u64::from(bid)),
        }
    }

    /// The furthest level a market order may trade at, if protection is on and the
    /// opposite side has orders.
    #[inline]
    fn protection_cap(&self, side: Side) -> Option<u32> {
        let ticks = self.config.price_protection?;
        match side {
            Side::Buy => self.asks.best.map(|ask| ask.saturating_add(ticks)),
            Side::Sell => self.bids.best.map(|bid| bid.saturating_sub(ticks)),
        }
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

    /// Number of trades executed so far; the next trade gets id `trade_count() + 1`.
    pub fn trade_count(&self) -> u64 {
        self.next_trade_id - 1
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
            owner: node.owner,
            side: node.side,
            price: self.price_of(node.level),
            leaves: node.remaining,
            filled: node.total - node.remaining,
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

    /// Checks the book's internal invariants: queue links, per-order and per-level
    /// quantities, occupancy bits, best-price pointers, the id index, and that the book is
    /// not crossed. All arithmetic is checked, so an overflow is reported rather than
    /// wrapped into a plausible-looking number.
    ///
    /// Walks the occupied levels via the bitset, so it costs `O(occupied levels + orders)`
    /// regardless of the band width. An order sitting in a level whose bit is missing is
    /// still caught: the orders reached that way would not add up to the pool's count.
    /// Meant for tests and debugging, never for the hot path.
    pub fn validate(&self) -> Result<(), String> {
        let mut resting = 0usize;
        for half in [&self.bids, &self.asks] {
            let side = half.side;
            let mut next = match side {
                Side::Buy => half.occupied.prev_at_or_before(usize::MAX),
                Side::Sell => half.occupied.next_at_or_after(0),
            };
            if half.best != next.map(|i| i as u32) {
                return Err(format!(
                    "{side:?}: best is {:?}, but the best occupied level is {next:?}",
                    half.best
                ));
            }
            while let Some(i) = next {
                next = match side {
                    Side::Buy => i
                        .checked_sub(1)
                        .and_then(|j| half.occupied.prev_at_or_before(j)),
                    Side::Sell => half.occupied.next_at_or_after(i + 1),
                };
                let level = i as u32;
                let price = self.price_of(level);
                let lvl = &half.levels[i];
                if lvl.order_count == 0 || lvl.head == NIL {
                    return Err(format!("{side:?} {price}: occupied bit on an empty level"));
                }

                let (mut count, mut total, mut prev, mut cur) = (0u32, 0 as Qty, NIL, lvl.head);
                while cur != NIL {
                    let node = self.pool.get(cur);
                    let id = node.id;
                    if node.prev != prev {
                        return Err(format!("{side:?} {price}: broken back link at #{id}"));
                    }
                    if node.level != level || node.side != side {
                        return Err(format!("{side:?} {price}: #{id} is misfiled"));
                    }
                    if node.remaining == 0
                        || node.remaining > node.total
                        || node.total > self.config.max_order_qty
                    {
                        return Err(format!(
                            "{side:?} {price}: #{id} has leaves {} of total {}",
                            node.remaining, node.total
                        ));
                    }
                    if self.index.get(&id) != Some(&cur) {
                        return Err(format!("{side:?} {price}: index disagrees on #{id}"));
                    }
                    count += 1;
                    total = total
                        .checked_add(node.remaining)
                        .ok_or_else(|| format!("{side:?} {price}: level quantity overflows"))?;
                    // No separate cycle check is needed: the first node a cycle revisits is
                    // reached from a different predecessor than the first time, so its back
                    // link fails the check above.
                    prev = cur;
                    cur = node.next;
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
            }
        }
        if resting != self.pool.live() || resting != self.index.len() {
            return Err(format!(
                "{resting} orders in queues, {} in pool, {} in index",
                self.pool.live(),
                self.index.len()
            ));
        }
        self.validate_owners()?;
        self.validate_free_list()?;
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

    /// The owner lists: links, membership and counts, and that together they hold every
    /// resting order exactly once.
    fn validate_owners(&self) -> Result<(), String> {
        let mut listed = 0usize;
        for (owner, list) in self.owners.lists() {
            let (mut count, mut prev, mut cur) = (0u32, NIL, list.head);
            while cur != NIL {
                let node = self.pool.get(cur);
                let link = self.owners.link_of(cur);
                let id = node.id;
                if link.prev != prev {
                    return Err(format!("owner {owner}: broken back link at #{id}"));
                }
                if node.owner != owner {
                    return Err(format!("owner {owner}: #{id} is misfiled"));
                }
                count += 1;
                prev = cur;
                cur = link.next;
            }
            if prev != list.tail {
                return Err(format!("owner {owner}: tail does not point at last order"));
            }
            if count != list.count {
                return Err(format!(
                    "owner {owner}: count says {} but list holds {count}",
                    list.count
                ));
            }
            listed += count as usize;
        }
        if listed != self.pool.live() {
            return Err(format!(
                "{listed} orders in owner lists, {} in pool",
                self.pool.live()
            ));
        }
        Ok(())
    }

    /// The order pool's free list holds exactly the unused slots, ends in `NIL`, and points
    /// only inside the slab. Allocation trusts all three.
    fn validate_free_list(&self) -> Result<(), String> {
        let unused = self.pool.capacity() - self.pool.live();
        let (mut listed, mut cur) = (0usize, self.pool.free_head());
        while cur != NIL {
            if cur as usize >= self.pool.capacity() {
                return Err("free list points outside the order pool".to_string());
            }
            if listed == unused {
                return Err(format!(
                    "free list holds more than the {unused} unused slots"
                ));
            }
            let node = self.pool.get(cur);
            if node.remaining != 0 {
                return Err(format!("free slot {cur} still holds #{}", node.id));
            }
            listed += 1;
            cur = node.next;
        }
        if listed != unused {
            return Err(format!(
                "free list holds {listed} of the {unused} unused slots"
            ));
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

/// Iterator over the orders at one price level, in time priority.
pub struct Queue<'a> {
    pool: &'a OrderPool,
    next: u32,
}

impl Iterator for Queue<'_> {
    type Item = QueuedOrder;

    fn next(&mut self) -> Option<QueuedOrder> {
        if self.next == NIL {
            return None;
        }
        let node = self.pool.get(self.next);
        self.next = node.next;
        Some(QueuedOrder {
            id: node.id,
            owner: node.owner,
            leaves: node.remaining,
            filled: node.total - node.remaining,
        })
    }
}

/// Tests for the invariant checker itself: every other test relies on `validate()` saying
/// `Ok` for a healthy book, so here each test corrupts one invariant of a healthy book and
/// requires `validate()` to name it.
#[cfg(test)]
mod validate_tests {
    use super::*;
    use crate::types::Side::{Buy, Sell};

    /// Bids 100: #1 (5), #2 (7); bids 99: #3 (1); asks 105: #4 (3); asks 106: #5 (4).
    /// Owner 1 has #1 and #3; every other order has its own owner.
    fn healthy() -> OrderBook {
        let mut book = OrderBook::new(BookConfig::new(1, 1_000, 16));
        let mut events = Vec::new();
        for (id, side, price, qty) in [
            (1, Buy, 100, 5),
            (2, Buy, 100, 7),
            (3, Buy, 99, 1),
            (4, Sell, 105, 3),
            (5, Sell, 106, 4),
        ] {
            let owner = if id == 3 { 1 } else { id as OwnerId };
            book.process(
                Command::Limit {
                    id,
                    owner,
                    side,
                    price,
                    qty,
                },
                &mut events,
            );
        }
        book.validate().expect("the fixture must start out healthy");
        book
    }

    fn level(book: &OrderBook, price: Price) -> usize {
        book.level_of(price).unwrap() as usize
    }

    fn slot(book: &OrderBook, id: OrderId) -> u32 {
        book.index[&id]
    }

    #[track_caller]
    fn assert_detects(corrupt: impl FnOnce(&mut OrderBook), expected: &str) {
        let mut book = healthy();
        corrupt(&mut book);
        let error = book.validate().expect_err("corruption went unnoticed");
        assert!(
            error.contains(expected),
            "expected an error about {expected:?}, got {error:?}"
        );
    }

    #[test]
    fn missing_occupancy_bit() {
        assert_detects(
            |b| {
                let l = level(b, 99);
                b.bids.occupied.remove(l);
            },
            "orders in queues",
        );
    }

    #[test]
    fn stray_occupancy_bit() {
        assert_detects(
            |b| {
                let l = level(b, 200);
                b.asks.occupied.insert(l);
            },
            "occupied bit on an empty level",
        );
    }

    #[test]
    fn occupied_level_without_head() {
        assert_detects(
            |b| {
                let l = level(b, 99);
                b.bids.levels[l].head = NIL;
            },
            "occupied bit on an empty level",
        );
    }

    #[test]
    fn wrong_best_price() {
        assert_detects(|b| b.bids.best = Some(level(b, 99) as u32), "best is");
        assert_detects(|b| b.asks.best = None, "best is");
    }

    #[test]
    fn wrong_level_aggregates() {
        assert_detects(
            |b| {
                let l = level(b, 100);
                b.bids.levels[l].total_qty += 1;
            },
            "aggregates",
        );
        assert_detects(
            |b| {
                let l = level(b, 100);
                b.bids.levels[l].order_count += 1;
            },
            "aggregates",
        );
    }

    #[test]
    fn broken_links() {
        assert_detects(
            |b| {
                let s = slot(b, 2);
                b.pool.get_mut(s).prev = NIL;
            },
            "broken back link",
        );
        assert_detects(
            |b| {
                let l = level(b, 100);
                b.bids.levels[l].tail = b.bids.levels[l].head;
            },
            "tail does not point",
        );
    }

    #[test]
    fn misfiled_order() {
        assert_detects(
            |b| {
                let s = slot(b, 1);
                b.pool.get_mut(s).side = Sell;
            },
            "misfiled",
        );
        assert_detects(
            |b| {
                let s = slot(b, 1);
                b.pool.get_mut(s).level += 1;
            },
            "misfiled",
        );
    }

    #[test]
    fn impossible_order_quantities() {
        assert_detects(
            |b| {
                let s = slot(b, 1);
                b.pool.get_mut(s).remaining = 0;
            },
            "has leaves",
        );
        assert_detects(
            |b| {
                let s = slot(b, 2);
                b.pool.get_mut(s).total = 6;
            },
            "has leaves",
        );
        assert_detects(
            |b| {
                let max = b.config.max_order_qty;
                let s = slot(b, 3);
                let node = b.pool.get_mut(s);
                node.remaining = max;
                node.total = max + 1;
            },
            "has leaves",
        );
    }

    #[test]
    fn index_out_of_sync() {
        assert_detects(
            |b| {
                b.index.remove(&1);
            },
            "index disagrees",
        );
        assert_detects(
            |b| {
                let s = slot(b, 2);
                b.index.insert(1, s);
            },
            "index disagrees",
        );
        assert_detects(
            |b| {
                b.index.insert(999, 0);
            },
            "in index",
        );
    }

    #[test]
    fn crossed_book() {
        // A bid placed straight into the ladder at the best ask's price, bypassing matching:
        // every other invariant holds.
        assert_detects(
            |b| {
                let l = level(b, 105) as u32;
                let s = b.pool.alloc(OrderNode::new(9, 9, Buy, l, 1, 1));
                b.place(s);
            },
            "crossed",
        );
    }

    /// The fixture has 16 slots, 5 of them in use.
    #[test]
    fn corrupted_free_list() {
        assert_detects(
            |b| {
                let s = b.pool.free_head();
                b.pool.get_mut(s).next = NIL;
            },
            "free list holds 1 of the 11 unused slots",
        );
        assert_detects(
            |b| {
                let s = b.pool.free_head();
                b.pool.get_mut(s).next = 16;
            },
            "free list points outside the order pool",
        );
        assert_detects(
            |b| {
                let s = b.pool.free_head();
                b.pool.get_mut(s).next = s;
            },
            "free list holds more than the 11 unused slots",
        );
        assert_detects(
            |b| {
                let s = b.pool.free_head();
                b.pool.get_mut(s).next = slot(b, 2);
            },
            "free slot 1 still holds #2",
        );
    }

    #[test]
    fn broken_owner_links() {
        assert_detects(
            |b| {
                let s = slot(b, 3);
                b.owners.link_mut(s).prev = NIL;
            },
            "owner 1: broken back link",
        );
        assert_detects(
            |b| {
                let s = slot(b, 1);
                b.owners.list_mut(1).tail = s;
            },
            "owner 1: tail does not point",
        );
        // An empty list whose tail still points somewhere.
        assert_detects(
            |b| {
                let s = slot(b, 1);
                b.owners.list_mut(9).tail = s;
            },
            "owner 9: tail does not point",
        );
    }

    #[test]
    fn misfiled_owner_membership() {
        assert_detects(
            |b| {
                let s = slot(b, 3);
                b.pool.get_mut(s).owner = 2;
            },
            "owner 1: #3 is misfiled",
        );
    }

    #[test]
    fn owner_lists_out_of_step_with_the_book() {
        assert_detects(
            |b| b.owners.list_mut(1).count += 1,
            "owner 1: count says 3 but list holds 2",
        );
        // #3 dropped from its owner's list, consistently: only the totals can tell.
        assert_detects(
            |b| {
                let first = slot(b, 1);
                b.owners.link_mut(first).next = NIL;
                let list = b.owners.list_mut(1);
                list.tail = first;
                list.count = 1;
            },
            "4 orders in owner lists, 5 in pool",
        );
    }
}
