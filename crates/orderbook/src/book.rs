//! The limit order book and its matching logic.

mod auction;
mod snapshot;
mod stops;

pub use snapshot::{BookSnapshot, SnapshotError, SnapshotOrder};
pub use stops::{StopOrder, Stops};

use crate::bitset::LevelBitset;
use crate::index::IdIndex;
use crate::owners::Owners;
use crate::pool::{NIL, OrderKind, OrderNode, OrderPool};
use crate::types::{
    CancelReason, Command, Event, EventSink, OrderId, OwnerId, Phase, Price, Qty, RejectReason,
    SelfTradePolicy, Side, TimeInForce, TradeId,
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
    /// Maximum number of resting orders and pending stops together; each holds one slot.
    pub max_orders: u32,
    /// Owner ids run from 0 to `max_owners - 1`. The gateway assigns these dense indices to
    /// participants, so the book can keep per-owner state in plain arrays. New orders from
    /// any other owner id are rejected.
    pub max_owners: u32,
    /// Largest accepted order quantity. `max_orders * max_order_qty` must fit in a `u64`,
    /// which makes every quantity sum inside the book overflow-free by construction.
    pub max_order_qty: Qty,
    /// Most tranches an iceberg may be cut into: its display times this must cover its
    /// total quantity. Each tranche is a separate trade, so this bounds the work and the
    /// events one command can cause per resting order. Zero disallows icebergs.
    pub max_iceberg_tranches: u32,
    /// Price protection in ticks, measured from the opposite best price when an order
    /// arrives. Market orders stop trading beyond it; limit orders priced further through
    /// it are rejected. `None` disables protection.
    pub price_protection: Option<u32>,
    /// Dynamic price band in ticks around the reference price: the last trade, or
    /// `reference_price` before the first one. A limit order or modify priced more than
    /// this through the reference is rejected, and a market order stops at the band. Unlike
    /// price protection, a stale order far from the market cannot move it. `None` disables
    /// the band.
    pub price_band: Option<u32>,
    /// Reference price before the first trade, such as the previous close. `None` leaves
    /// the band inactive until something trades.
    pub reference_price: Option<Price>,
    /// A volatility interruption: when the price band stops a market order short of
    /// liquidity it would have taken, the book also switches to [`Phase::Auction`], whose
    /// uncross sets a new reference price. The sequencer decides when the call ends. Without
    /// it, the market order is just cut short.
    pub auction_on_band: bool,
    /// What happens when an order would trade against an order of the same owner.
    pub self_trade: SelfTradePolicy,
}

impl BookConfig {
    /// Owners allowed by [`BookConfig::new`].
    pub const DEFAULT_MAX_OWNERS: u32 = 1_024;

    /// Iceberg tranches allowed by [`BookConfig::new`]: an iceberg shows at least a tenth of
    /// its quantity, a rule some exchanges use.
    pub const DEFAULT_MAX_ICEBERG_TRANCHES: u32 = 10;

    /// A config with the given band and capacity, [`Self::DEFAULT_MAX_OWNERS`] owners, the
    /// largest `max_order_qty` the capacity allows, [`Self::DEFAULT_MAX_ICEBERG_TRANCHES`],
    /// no price protection or price band, and `CancelResting` self-trade prevention.
    pub const fn new(min_price: Price, max_price: Price, max_orders: u32) -> Self {
        let max_orders_nonzero = if max_orders == 0 { 1 } else { max_orders };
        Self {
            min_price,
            max_price,
            max_orders,
            max_owners: Self::DEFAULT_MAX_OWNERS,
            max_order_qty: u64::MAX / max_orders_nonzero as u64,
            max_iceberg_tranches: Self::DEFAULT_MAX_ICEBERG_TRANCHES,
            price_protection: None,
            price_band: None,
            reference_price: None,
            auction_on_band: false,
            self_trade: SelfTradePolicy::CancelResting,
        }
    }
}

/// Aggregated view of one price level.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelInfo {
    /// Level price.
    pub price: Price,
    /// Quantity the level shows. Hidden iceberg quantity is not included.
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
    /// Whether the order was entered post-only, which still restricts its modifies.
    pub post_only: bool,
    /// Iceberg display quantity, if the order is an iceberg.
    pub display: Option<Qty>,
    /// The part of `leaves` on display; all of it for a plain order.
    pub visible: Qty,
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
    /// Whether the order was entered post-only, which still restricts its modifies.
    pub post_only: bool,
    /// Iceberg display quantity, if the order is an iceberg.
    pub display: Option<Qty>,
    /// The part of `leaves` on display; all of it for a plain order.
    pub visible: Qty,
}

#[derive(Clone, Copy, Debug)]
struct Level {
    head: u32,
    tail: u32,
    /// What the level's orders show.
    total_qty: Qty,
    order_count: u32,
    /// How many of them are icebergs, whose hidden quantity `total_qty` leaves out. Where
    /// there are none, an auction reads the level's open quantity from `total_qty` alone.
    /// It fills what would otherwise be padding.
    icebergs: u32,
}

// The ladder's memory is `2 * levels * size_of::<Level>()`; see docs/DESIGN.md.
const _: () = assert!(size_of::<Level>() == 24);

impl Level {
    const EMPTY: Level = Level {
        head: NIL,
        tail: NIL,
        total_qty: 0,
        order_count: 0,
        icebergs: 0,
    };

    /// Removes the order at the head of the queue and frees its slot. The caller has
    /// already taken its quantity out of `total_qty`.
    #[inline]
    fn pop_front(&mut self, pool: &mut OrderPool, index: &mut IdIndex, owners: &mut Owners) {
        let slot = self.head;
        let OrderNode {
            id,
            next,
            owner,
            iceberg,
            ..
        } = *pool.get(slot);
        self.head = next;
        if next == NIL {
            self.tail = NIL;
        } else {
            pool.get_mut(next).prev = NIL;
        }
        self.order_count -= 1;
        self.icebergs -= u32::from(iceberg);
        index.remove(&id);
        owners.unlink(slot, owner);
        pool.free(slot);
    }

    /// Moves the order at the head of the queue to its back, as an iceberg's new tranche
    /// loses time priority.
    #[inline]
    fn requeue_head(&mut self, pool: &mut OrderPool) {
        let slot = self.head;
        let next = pool.get(slot).next;
        if next == NIL {
            return;
        }
        self.head = next;
        pool.get_mut(next).prev = NIL;
        pool.get_mut(self.tail).next = slot;
        let node = pool.get_mut(slot);
        node.prev = self.tail;
        node.next = NIL;
        self.tail = slot;
    }
}

/// One side of the book: a dense ladder of price levels indexed by `price - min_price`, each
/// holding an intrusive FIFO queue of orders. Pending stops use the same structure, keyed by
/// trigger level.
struct HalfBook {
    /// Which way the ladder's priority runs: like bids (highest level first) or like asks
    /// (lowest first). Buy stops run like asks, since rising prices trigger the lowest first;
    /// sell stops run like bids.
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
        let OrderNode { level, iceberg, .. } = *pool.get(slot);
        let visible = pool.visible(slot);
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
        lvl.total_qty += visible;
        lvl.order_count += 1;
        lvl.icebergs += u32::from(iceberg);
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
            prev,
            next,
            iceberg,
            ..
        } = *pool.get(slot);
        let visible = pool.visible(slot);
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
        lvl.total_qty -= visible;
        lvl.order_count -= 1;
        lvl.icebergs -= u32::from(iceberg);
        if lvl.order_count == 0 {
            self.level_emptied(level);
        }
    }

    /// The next occupied level after `level` in priority order: the next lower bid or the
    /// next higher ask.
    #[inline]
    fn after(&self, level: u32) -> Option<u32> {
        let i = level as usize;
        match self.side {
            Side::Buy => i
                .checked_sub(1)
                .and_then(|j| self.occupied.prev_at_or_before(j)),
            Side::Sell => self.occupied.next_at_or_after(i + 1),
        }
        .map(|j| j as u32)
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
    /// The opposite best no longer crosses the taker's limit (or market cap).
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
    /// Pending buy stops by trigger level, lowest trigger first.
    buy_stops: HalfBook,
    /// Pending sell stops by trigger level, highest trigger first.
    sell_stops: HalfBook,
    pool: OrderPool,
    /// Order id -> pool slot, sized for `max_orders` up front.
    index: IdIndex,
    /// Each owner's resting orders, for mass cancels.
    owners: Owners,
    /// Sort buffer for mass cancels, sized for every resting order up front.
    scratch: Vec<MassCancelKey>,
    next_trade_id: TradeId,
    /// Level of the last trade, or of the configured reference price before the first one.
    reference: Option<u32>,
    /// Lowest and highest level traded at during the current command; it decides which
    /// stops trigger.
    traded: Option<(u32, u32)>,
    /// The trading phase. Continuous trading checks it once per order; the other phases
    /// take the slow paths.
    phase: Phase,
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
    /// is zero or `u32::MAX`, `max_order_qty` is zero, `max_orders * max_order_qty`
    /// does not fit in a `u64`, or `reference_price` lies outside the price band.
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
        let reference = config.reference_price.map(|price| {
            let offset = i128::from(price) - i128::from(config.min_price);
            assert!(
                (0..levels as i128).contains(&offset),
                "reference_price outside the price band"
            );
            offset as u32
        });
        Self {
            config,
            bids: HalfBook::new(Side::Buy, levels),
            asks: HalfBook::new(Side::Sell, levels),
            buy_stops: HalfBook::new(Side::Sell, levels),
            sell_stops: HalfBook::new(Side::Buy, levels),
            pool: OrderPool::with_capacity(config.max_orders),
            index: IdIndex::with_capacity(config.max_orders),
            owners: Owners::new(config.max_owners, config.max_orders),
            scratch: Vec::with_capacity(config.max_orders as usize),
            next_trade_id: 1,
            reference,
            traded: None,
            phase: Phase::Continuous,
        }
    }

    /// Applies one command, reporting its outcome to `sink`, then releases the stops its
    /// trades triggered. A new book is in continuous trading.
    pub fn process<S: EventSink>(&mut self, command: Command, sink: &mut S) {
        self.traded = None;
        let (id, result) = match command {
            Command::Limit {
                id,
                owner,
                side,
                price,
                qty,
                tif,
                display,
            } => (
                id,
                self.new_limit(id, owner, side, price, qty, tif, display, sink),
            ),
            Command::Market {
                id,
                owner,
                side,
                qty,
            } => (id, self.new_market(id, owner, side, qty, sink)),
            Command::Stop {
                id,
                owner,
                side,
                trigger,
                limit,
                qty,
            } => (
                id,
                self.new_stop(id, owner, side, trigger, limit, qty, sink),
            ),
            Command::Cancel { id, owner } => (id, self.cancel(id, owner, sink)),
            Command::Modify {
                id,
                owner,
                price,
                qty,
            } => (id, self.modify(id, owner, price, qty, sink)),
            Command::CancelAll { owner } => return self.cancel_all(owner, sink),
            Command::SetPhase { phase } => {
                self.set_phase(phase, sink);
                return self.release_stops(sink);
            }
        };
        if let Err(reason) = result {
            sink.on_event(Event::Rejected { id, reason });
        }
        self.release_stops(sink);
    }

    // Each handler validates everything before emitting its first event, so a rejected
    // command leaves no trace besides the `Rejected` event. Stateless checks (owner,
    // quantity, price band) come before stateful ones: the phase first, then ids,
    // protection and capacity.

    #[allow(clippy::too_many_arguments)]
    fn new_limit<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        price: Price,
        qty: Qty,
        tif: TimeInForce,
        display: Option<Qty>,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        self.check_owner(owner)?;
        self.check_qty(qty)?;
        let may_rest = matches!(tif, TimeInForce::Gtc | TimeInForce::PostOnly);
        if display.is_some_and(|display| {
            display == 0 || display >= qty || !may_rest || !self.tranches_cover(display, qty)
        }) {
            return Err(RejectReason::InvalidDisplay);
        }
        let level = self.level_of(price).ok_or(RejectReason::PriceOutOfRange)?;
        // Continuous trading pays this one branch for phases.
        if self.phase != Phase::Continuous {
            return self
                .new_limit_outside_continuous(id, owner, side, level, qty, tif, display, sink);
        }
        if self.index.contains_key(&id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if self.outside_protection(side, level) {
            return Err(RejectReason::PriceOutsideProtection);
        }
        if self.outside_band(side, level) {
            return Err(RejectReason::PriceOutsideBand);
        }
        if tif == TimeInForce::PostOnly && self.crosses(side, level) {
            return Err(RejectReason::PostOnlyWouldCross);
        }
        // A crossing order always finds room to rest: its first match either fills it or
        // removes a resting order (by filling it or by self-trade prevention), freeing a
        // slot. So only an order that can do nothing but rest is refused when full, and
        // orders that never rest are never refused.
        if may_rest && self.pool.is_full() && !self.crosses(side, level) {
            return Err(RejectReason::BookFull);
        }
        sink.on_event(Event::Accepted { id });
        match tif {
            TimeInForce::Gtc | TimeInForce::PostOnly => {
                let post_only = tif == TimeInForce::PostOnly;
                self.execute_limit(id, owner, side, level, qty, qty, post_only, display, sink);
            }
            TimeInForce::Fok if !self.can_fill(owner, side, level, qty) => {
                sink.on_event(Event::Cancelled {
                    id,
                    qty,
                    reason: CancelReason::FillOrKill,
                });
            }
            TimeInForce::Ioc | TimeInForce::Fok => {
                let (unfilled, halt) = self.match_incoming(id, owner, side, qty, Some(level), sink);
                debug_assert!(
                    tif == TimeInForce::Ioc || unfilled == 0,
                    "a fill-or-kill order that could fill did not"
                );
                if unfilled > 0 {
                    let reason = match halt {
                        Halt::SelfTrade => CancelReason::SelfTrade,
                        _ => CancelReason::ImmediateOrCancel,
                    };
                    sink.on_event(Event::Cancelled {
                        id,
                        qty: unfilled,
                        reason,
                    });
                }
            }
        }
        Ok(())
    }

    /// A limit order outside continuous trading. A halt or the close refuses it, and a call
    /// phase refuses one that must trade at once. Otherwise it rests without matching, and
    /// without the price controls, which guard against what an order trades on arrival: in
    /// a call nothing does, and the uncross finds the new price. Since nothing matches,
    /// nothing frees a slot either, so a full book refuses every order.
    #[cold]
    #[allow(clippy::too_many_arguments)]
    fn new_limit_outside_continuous<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        level: u32,
        qty: Qty,
        tif: TimeInForce,
        display: Option<Qty>,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        let post_only = tif == TimeInForce::PostOnly;
        self.check_phase(!matches!(tif, TimeInForce::Gtc | TimeInForce::PostOnly))?;
        if self.index.contains_key(&id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if post_only && self.crosses(side, level) {
            return Err(RejectReason::PostOnlyWouldCross);
        }
        if self.pool.is_full() {
            return Err(RejectReason::BookFull);
        }
        sink.on_event(Event::Accepted { id });
        self.rest(id, owner, side, level, qty, qty, post_only, display, sink);
        Ok(())
    }

    /// Whether an order of `owner` on `side`, limited at `limit`, would fill `qty`
    /// completely if it matched now. Walks the opposite side in the order `match_incoming`
    /// would, without changing anything. Under `CancelResting` the owner's own orders would
    /// be cancelled rather than traded, so they add nothing; under `CancelIncoming` matching
    /// would stop at the first of them.
    ///
    /// Icebergs show one tranche at a time, and each new tranche goes to the back of the
    /// queue. A taker that clears a level without meeting its own order therefore cycles
    /// through every tranche and gets the hidden quantity too. But under `CancelIncoming`,
    /// new tranches land behind the owner's own order, so at a level that holds one, only
    /// what shows in front of it counts.
    fn can_fill(&self, owner: OwnerId, side: Side, limit: u32, mut qty: Qty) -> bool {
        let resting = self.half(side.opposite());
        let mut next = resting.best;
        while let Some(level) = next {
            let crosses = match side {
                Side::Buy => level <= limit,
                Side::Sell => level >= limit,
            };
            if !crosses {
                return false;
            }
            let mut slot = resting.levels[level as usize].head;
            let mut hidden: Qty = 0;
            while slot != NIL {
                let node = self.pool.get(slot);
                if node.owner != owner {
                    let visible = self.pool.visible(slot);
                    if visible >= qty {
                        return true;
                    }
                    qty -= visible;
                    hidden += node.remaining - visible;
                } else if self.config.self_trade == SelfTradePolicy::CancelIncoming {
                    return false;
                }
                slot = node.next;
            }
            if hidden >= qty {
                return true;
            }
            qty -= hidden;
            next = resting.after(level);
        }
        false
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
        self.check_phase(true)?;
        if self.index.contains_key(&id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        sink.on_event(Event::Accepted { id });
        self.execute_market(id, owner, side, qty, sink);
        Ok(())
    }

    /// Matches a market order up to its cap and cancels whatever is left.
    #[inline]
    fn execute_market<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        qty: Qty,
        sink: &mut S,
    ) {
        // A market order stops at the tighter of its two caps, and says which one it was.
        let (protection, band) = (self.protection_cap(side), self.band_cap(side));
        let band_is_tighter = match (protection, band) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(protection), Some(band)) => match side {
                Side::Buy => band < protection,
                Side::Sell => band > protection,
            },
        };
        let cap = if band_is_tighter { band } else { protection };
        let (unfilled, halt) = self.match_incoming(id, owner, side, qty, cap, sink);
        if unfilled > 0 {
            let reason = match halt {
                Halt::SelfTrade => CancelReason::SelfTrade,
                Halt::Limit if band_is_tighter => CancelReason::PriceBand,
                Halt::Limit => CancelReason::PriceProtection,
                Halt::Empty | Halt::Filled => CancelReason::NoLiquidity,
            };
            sink.on_event(Event::Cancelled {
                id,
                qty: unfilled,
                reason,
            });
            // The band stopped the order short of liquidity beyond it: a volatility
            // interruption, if the book is set up for one.
            if reason == CancelReason::PriceBand && self.config.auction_on_band {
                self.set_phase(Phase::Auction, sink);
            }
        }
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

    /// Cancels all of `owner`'s resting orders in book order, then its pending stops in
    /// trigger order. The owner's list holds them in the order they joined their queues;
    /// sorting by ladder, priority and list position turns that into book order, because
    /// within one level list order is queue order.
    fn cancel_all<S: EventSink>(&mut self, owner: OwnerId, sink: &mut S) {
        let mut keys = std::mem::take(&mut self.scratch);
        keys.clear();
        let mut slot = self.owners.list(owner).head;
        let mut position = 0;
        while slot != NIL {
            let node = self.pool.get(slot);
            let key = match (node.is_stop(), node.side) {
                (false, Side::Buy) => (0, u32::MAX - node.level, position, slot),
                (false, Side::Sell) => (1, node.level, position, slot),
                (true, Side::Buy) => (2, node.level, position, slot),
                (true, Side::Sell) => (3, u32::MAX - node.level, position, slot),
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
        self.check_phase(false)?;
        let slot = self.owned_slot(id, owner)?;
        let node = *self.pool.get(slot);
        if node.is_stop() {
            return Err(RejectReason::PendingStop);
        }
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
            let shown_less = pool.shrink(slot, leaves, qty);
            half.levels[level as usize].total_qty -= shown_less;
            sink.on_event(Event::Modified {
                id,
                price,
                qty,
                leaves,
            });
            return Ok(());
        }

        // Cancel/replace: back of the queue at the new price, trading on the way in. The
        // order's own slot is freed first, so it can always rest again. In a call phase it
        // does not trade, and so the price controls do not apply.
        let continuous = self.phase == Phase::Continuous;
        if continuous && self.outside_protection(node.side, level) {
            return Err(RejectReason::PriceOutsideProtection);
        }
        if continuous && self.outside_band(node.side, level) {
            return Err(RejectReason::PriceOutsideBand);
        }
        if node.post_only && self.crosses(node.side, level) {
            return Err(RejectReason::PostOnlyWouldCross);
        }
        let display = self.pool.iceberg(slot).map(|part| part.display);
        if display.is_some_and(|display| !self.tranches_cover(display, qty)) {
            return Err(RejectReason::InvalidDisplay);
        }
        self.remove(slot);
        sink.on_event(Event::Modified {
            id,
            price,
            qty,
            leaves,
        });
        let (side, post_only) = (node.side, node.post_only);
        if continuous {
            self.execute_limit(
                id, owner, side, level, leaves, qty, post_only, display, sink,
            );
        } else {
            self.rest(
                id, owner, side, level, leaves, qty, post_only, display, sink,
            );
        }
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
        post_only: bool,
        display: Option<Qty>,
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
        self.rest(
            id, owner, side, level, remaining, total, post_only, display, sink,
        );
    }

    /// Rests `remaining` of a validated limit order at the back of its level, in a free
    /// slot, showing its first tranche if it is an iceberg.
    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    fn rest<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        level: u32,
        remaining: Qty,
        total: Qty,
        post_only: bool,
        display: Option<Qty>,
        sink: &mut S,
    ) {
        let slot = self.pool.alloc(OrderNode::new(
            id, owner, side, level, remaining, total, post_only,
        ));
        if let Some(display) = display {
            self.pool
                .make_iceberg(slot, display, display.min(remaining));
        }
        self.place(slot);
        sink.on_event(Event::Rested {
            id,
            side,
            price: self.price_of(level),
            qty: remaining,
            visible: self.pool.visible(slot),
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
            reference,
            traded,
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
                let head = lvl.head;
                let maker = *pool.get(head);
                if maker.owner == owner {
                    // Self-trade prevention.
                    if policy == SelfTradePolicy::CancelIncoming {
                        return (qty, Halt::SelfTrade);
                    }
                    lvl.total_qty -= pool.visible(head);
                    lvl.pop_front(pool, index, owners);
                    sink.on_event(Event::Cancelled {
                        id: maker.id,
                        qty: maker.remaining,
                        reason: CancelReason::SelfTrade,
                    });
                    continue;
                }

                let (fill, maker_leaves) = pool.fill(head, qty);
                lvl.total_qty -= fill;
                qty -= fill;
                let trade_id = *next_trade_id;
                *next_trade_id += 1;
                *reference = Some(level);
                *traded = Some(match *traded {
                    None => (level, level),
                    Some((low, high)) => (low.min(level), high.max(level)),
                });
                sink.on_event(Event::Trade {
                    trade_id,
                    taker,
                    maker: maker.id,
                    taker_side,
                    price,
                    qty: fill,
                    taker_leaves: qty,
                    maker_leaves,
                });
                if maker_leaves == 0 {
                    lvl.pop_front(pool, index, owners);
                } else if pool.visible(head) == 0 {
                    // An iceberg's tranche is used up: the next one shows at the back of
                    // the queue, and at the back of the owner's list, which must stay in
                    // queue order within the level.
                    let visible = pool.replenish(head);
                    lvl.total_qty += visible;
                    lvl.requeue_head(pool);
                    owners.unlink(head, maker.owner);
                    owners.link(head, maker.owner);
                    sink.on_event(Event::Replenished {
                        id: maker.id,
                        side: maker.side,
                        price,
                        visible,
                    });
                }
            }

            if lvl.head == NIL {
                resting.level_emptied(level);
            }
        }
        (0, Halt::Filled)
    }

    /// Puts an allocated order or stop in its place: into the id index, at the back of its
    /// level's queue in its ladder, and at the back of its owner's list.
    #[inline]
    fn place(&mut self, slot: u32) {
        let OrderNode { id, owner, .. } = *self.pool.get(slot);
        self.index.insert(id, slot);
        let (ladder, pool) = self.ladder_and_pool(slot);
        ladder.push_back(pool, slot);
        self.owners.link(slot, owner);
    }

    /// Unlinks and frees a resting order or pending stop.
    #[inline]
    fn remove(&mut self, slot: u32) {
        let (ladder, pool) = self.ladder_and_pool(slot);
        ladder.unlink(pool, slot);
        self.retire(slot);
    }

    /// Frees the slot of an order that is in no queue any more: out of the id index, out of
    /// its owner's list, back to the pool.
    #[inline]
    fn retire(&mut self, slot: u32) {
        let OrderNode { id, owner, .. } = *self.pool.get(slot);
        self.index.remove(&id);
        self.owners.unlink(slot, owner);
        self.pool.free(slot);
    }

    /// The ladder the order or stop in `slot` belongs to, with the pool.
    #[inline]
    fn ladder_and_pool(&mut self, slot: u32) -> (&mut HalfBook, &mut OrderPool) {
        let node = self.pool.get(slot);
        let ladder = match (node.kind, node.side) {
            (OrderKind::Resting, Side::Buy) => &mut self.bids,
            (OrderKind::Resting, Side::Sell) => &mut self.asks,
            (_, Side::Buy) => &mut self.buy_stops,
            (_, Side::Sell) => &mut self.sell_stops,
        };
        (ladder, &mut self.pool)
    }

    /// The slot of `id` if it rests on the book and belongs to `owner`.
    #[inline]
    fn owned_slot(&self, id: OrderId, owner: OwnerId) -> Result<u32, RejectReason> {
        match self.index.get(&id) {
            Some(&slot) if self.pool.get(slot).owner == owner => Ok(slot),
            _ => Err(RejectReason::UnknownOrder),
        }
    }

    /// Whether `max_iceberg_tranches` tranches of `display` cover a total of `qty`.
    #[inline]
    fn tranches_cover(&self, display: Qty, qty: Qty) -> bool {
        u128::from(display) * u128::from(self.config.max_iceberg_tranches) >= u128::from(qty)
    }

    #[inline]
    fn check_owner(&self, owner: OwnerId) -> Result<(), RejectReason> {
        if owner < self.config.max_owners {
            Ok(())
        } else {
            Err(RejectReason::InvalidOwner)
        }
    }

    /// Whether the phase accepts a new order or modify: everything in continuous trading,
    /// all but orders that must trade at once (`immediate`) in a call phase, nothing while
    /// halted or closed. Cancels and mass cancels are accepted in every phase.
    #[inline]
    fn check_phase(&self, immediate: bool) -> Result<(), RejectReason> {
        match self.phase {
            Phase::Continuous => Ok(()),
            Phase::Auction if immediate => Err(RejectReason::AuctionCall),
            Phase::Auction => Ok(()),
            Phase::Halted => Err(RejectReason::TradingHalted),
            Phase::Closed => Err(RejectReason::MarketClosed),
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

    /// Whether a limit price lies more than `price_band` ticks through the reference price.
    #[inline]
    fn outside_band(&self, side: Side, level: u32) -> bool {
        let (Some(ticks), Some(reference)) = (self.config.price_band, self.reference) else {
            return false;
        };
        let (level, reference, ticks) = (u64::from(level), u64::from(reference), u64::from(ticks));
        match side {
            Side::Buy => level > reference + ticks,
            Side::Sell => level + ticks < reference,
        }
    }

    /// The furthest level a market order may trade at under the price band, if the band is
    /// on and there is a reference price.
    #[inline]
    fn band_cap(&self, side: Side) -> Option<u32> {
        let ticks = self.config.price_band?;
        let reference = self.reference?;
        Some(match side {
            Side::Buy => reference.saturating_add(ticks),
            Side::Sell => reference.saturating_sub(ticks),
        })
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

    /// Number of resting orders and pending stops: the slots in use out of `max_orders`.
    pub fn order_count(&self) -> usize {
        self.pool.live()
    }

    /// The trading phase.
    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// The price the band is measured from: the last trade, or the configured reference
    /// price before the first trade.
    pub fn reference_price(&self) -> Option<Price> {
        self.reference.map(|level| self.price_of(level))
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

    /// A resting order, if `id` is on the book. Pending stops are not on the book until
    /// they trigger; [`OrderBook::stop`] reports them.
    pub fn order(&self, id: OrderId) -> Option<OrderInfo> {
        let slot = *self.index.get(&id)?;
        let node = self.pool.get(slot);
        if node.is_stop() {
            return None;
        }
        Some(OrderInfo {
            owner: node.owner,
            side: node.side,
            price: self.price_of(node.level),
            leaves: node.remaining,
            filled: node.total - node.remaining,
            post_only: node.post_only,
            display: self.pool.iceberg(slot).map(|part| part.display),
            visible: self.pool.visible(slot),
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
    /// quantities (a level's total counts what its orders show), iceberg tranches,
    /// occupancy bits, best-price pointers, the id index, owner lists, the free list, and
    /// that the book is not crossed outside a call phase. All arithmetic is checked, so an
    /// overflow is reported rather than wrapped into a plausible-looking number.
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
                let mut icebergs = 0u32;
                while cur != NIL {
                    let node = self.pool.get(cur);
                    let id = node.id;
                    icebergs += u32::from(node.iceberg);
                    if node.prev != prev {
                        return Err(format!("{side:?} {price}: broken back link at #{id}"));
                    }
                    if node.level != level || node.side != side || node.is_stop() {
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
                    if let Some(part) = self.pool.iceberg(cur) {
                        let visible = part.visible;
                        if visible == 0 || visible > node.remaining || visible > part.display {
                            return Err(format!(
                                "{side:?} {price}: iceberg #{id} shows {visible} of {} with display {}",
                                node.remaining, part.display
                            ));
                        }
                        if !self.tranches_cover(part.display, node.total) {
                            return Err(format!(
                                "{side:?} {price}: iceberg #{id} needs more than {} tranches",
                                self.config.max_iceberg_tranches
                            ));
                        }
                    }
                    count += 1;
                    total = total
                        .checked_add(self.pool.visible(cur))
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
                if icebergs != lvl.icebergs {
                    return Err(format!(
                        "{side:?} {price}: level counts {} icebergs but holds {icebergs}",
                        lvl.icebergs
                    ));
                }
                resting += count as usize;
            }
        }
        let stops = self.validate_stops()?;
        let held = resting + stops;
        if held != self.pool.live() || held != self.index.len() {
            return Err(format!(
                "{resting} orders in queues and {stops} stops, {} in pool, {} in index",
                self.pool.live(),
                self.index.len()
            ));
        }
        self.validate_owners()?;
        self.validate_free_list()?;
        // Only a call phase collects orders without matching them; every way out of it
        // uncrosses the book.
        if let (Some(bid), Some(ask)) = (self.bids.best, self.asks.best) {
            if bid >= ask && self.phase != Phase::Auction {
                return Err(format!(
                    "book is crossed in phase {:?}: bid {} >= ask {}",
                    self.phase,
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
        self.next = self.half.after(level);
        let lvl = &self.half.levels[level as usize];
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
        let slot = self.next;
        let node = self.pool.get(slot);
        self.next = node.next;
        Some(QueuedOrder {
            id: node.id,
            owner: node.owner,
            leaves: node.remaining,
            filled: node.total - node.remaining,
            post_only: node.post_only,
            display: self.pool.iceberg(slot).map(|part| part.display),
            visible: self.pool.visible(slot),
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
                    tif: TimeInForce::Gtc,
                    display: None,
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
                let s = b.pool.alloc(OrderNode::new(9, 9, Buy, l, 1, 1, false));
                b.place(s);
            },
            "book is crossed in phase Continuous: bid 105 >= ask 105",
        );
    }

    /// The healthy fixture moved into a call phase, where a bid at 106 crosses both asks.
    fn crossed_call(b: &mut OrderBook) {
        let mut events = Vec::new();
        for command in [
            Command::SetPhase {
                phase: Phase::Auction,
            },
            Command::Limit {
                id: 9,
                owner: 9,
                side: Buy,
                price: 106,
                qty: 1,
                tif: TimeInForce::Gtc,
                display: None,
            },
        ] {
            b.process(command, &mut events);
        }
        b.validate()
            .expect("a crossed book is healthy in a call phase");
    }

    #[test]
    fn a_crossed_book_outside_a_call_phase() {
        for (phase, name) in [
            (Phase::Continuous, "Continuous"),
            (Phase::Halted, "Halted"),
            (Phase::Closed, "Closed"),
        ] {
            assert_detects(
                |b| {
                    crossed_call(b);
                    b.phase = phase;
                },
                &format!("book is crossed in phase {name}: bid 106 >= ask 105"),
            );
        }
    }

    /// Adds #9, an iceberg of 10 showing 3, behind the bids at 99 of the healthy fixture.
    fn add_iceberg(b: &mut OrderBook) -> u32 {
        let mut events = Vec::new();
        b.process(
            Command::Limit {
                id: 9,
                owner: 9,
                side: Buy,
                price: 99,
                qty: 10,
                tif: TimeInForce::Gtc,
                display: Some(3),
            },
            &mut events,
        );
        b.validate()
            .expect("the iceberg fixture must start out healthy");
        slot(b, 9)
    }

    #[test]
    fn impossible_iceberg_tranches() {
        assert_detects(
            |b| {
                add_iceberg(b);
                b.config.max_iceberg_tranches = 3;
            },
            "iceberg #9 needs more than 3 tranches",
        );
        assert_detects(
            |b| {
                let s = add_iceberg(b);
                b.pool.iceberg_mut(s).visible = 0;
            },
            "iceberg #9 shows 0 of 10 with display 3",
        );
        assert_detects(
            |b| {
                let s = add_iceberg(b);
                b.pool.iceberg_mut(s).display = 2;
            },
            "iceberg #9 shows 3 of 10 with display 2",
        );
        assert_detects(
            |b| {
                let s = add_iceberg(b);
                b.pool.get_mut(s).remaining = 2;
            },
            "iceberg #9 shows 3 of 2",
        );
        // A level's total is what its orders show, not their hidden quantity.
        assert_detects(
            |b| {
                let s = add_iceberg(b);
                b.pool.iceberg_mut(s).visible = 2;
            },
            "aggregates say 2/4 but queue holds 2/3",
        );
        // An auction trusts the count to know where hidden quantity rests.
        assert_detects(
            |b| {
                add_iceberg(b);
                let l = level(b, 99);
                b.bids.levels[l].icebergs = 0;
            },
            "Buy 99: level counts 0 icebergs but holds 1",
        );
        assert_detects(
            |b| {
                let l = level(b, 100);
                b.bids.levels[l].icebergs = 1;
            },
            "Buy 100: level counts 1 icebergs but holds 0",
        );
    }

    /// Adds pending stops to the healthy fixture, which has no last price yet: buy stops #20
    /// (market) and #21 (limit 205) at 200, and a sell stop #22 at 50.
    fn add_stops(b: &mut OrderBook) {
        let mut events = Vec::new();
        for (id, side, trigger, limit) in [
            (20, Buy, 200, None),
            (21, Buy, 200, Some(205)),
            (22, Sell, 50, None),
        ] {
            b.process(
                Command::Stop {
                    id,
                    owner: 20,
                    side,
                    trigger,
                    limit,
                    qty: 2,
                },
                &mut events,
            );
        }
        b.validate()
            .expect("the stop fixture must start out healthy");
    }

    #[test]
    fn broken_stop_ladders() {
        assert_detects(
            |b| {
                add_stops(b);
                b.buy_stops.best = None;
            },
            "Buy stops: best is None",
        );
        assert_detects(
            |b| {
                add_stops(b);
                let l = level(b, 50);
                b.sell_stops.levels[l].head = NIL;
            },
            "Sell stops 50: occupied bit on an empty level",
        );
        assert_detects(
            |b| {
                add_stops(b);
                let s = slot(b, 21);
                b.pool.get_mut(s).prev = NIL;
            },
            "Buy stops 200: broken back link at #21",
        );
        assert_detects(
            |b| {
                add_stops(b);
                let l = level(b, 200);
                b.buy_stops.levels[l].tail = b.buy_stops.levels[l].head;
            },
            "Buy stops 200: tail does not point",
        );
        assert_detects(
            |b| {
                add_stops(b);
                let l = level(b, 200);
                b.buy_stops.levels[l].order_count += 1;
            },
            "Buy stops 200: aggregates say 3/4 but queue holds 2/4",
        );
    }

    #[test]
    fn impossible_stops() {
        assert_detects(
            |b| {
                add_stops(b);
                let s = slot(b, 22);
                b.pool.get_mut(s).side = Buy;
            },
            "Sell stops 50: #22 is misfiled",
        );
        assert_detects(
            |b| {
                add_stops(b);
                let s = slot(b, 22);
                b.pool.get_mut(s).kind = OrderKind::Resting;
            },
            "Sell stops 50: #22 is misfiled",
        );
        assert_detects(
            |b| {
                add_stops(b);
                let s = slot(b, 20);
                b.pool.get_mut(s).limit = 0;
            },
            "Buy stops 200: #20 has a bad limit level",
        );
        assert_detects(
            |b| {
                add_stops(b);
                let s = slot(b, 21);
                b.pool.get_mut(s).limit = 1_000;
            },
            "Buy stops 200: #21 has a bad limit level",
        );
        assert_detects(
            |b| {
                add_stops(b);
                let s = slot(b, 20);
                b.pool.get_mut(s).remaining = 1;
            },
            "Buy stops 200: #20 has quantity 1 of 2",
        );
        assert_detects(
            |b| {
                add_stops(b);
                b.index.remove(&22);
            },
            "Sell stops 50: index disagrees on #22",
        );
        // A trade at 200 or above should have released the buy stops at 200.
        assert_detects(
            |b| {
                add_stops(b);
                b.reference = Some(level(b, 200) as u32);
            },
            "Buy stops 200: #20 should have triggered",
        );
        assert_detects(
            |b| {
                add_stops(b);
                b.reference = Some(level(b, 50) as u32);
            },
            "Sell stops 50: #22 should have triggered",
        );
        // A stop that left its ladder but kept its slot.
        assert_detects(
            |b| {
                add_stops(b);
                let s = slot(b, 22);
                let (ladder, pool) = b.ladder_and_pool(s);
                ladder.unlink(pool, s);
            },
            "5 orders in queues and 2 stops, 8 in pool",
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
