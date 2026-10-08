//! Deterministic synthetic order flow, plus a counting [`EventSink`], for benchmarks and
//! soak tests.
//!
//! [`Workload`] behaves like a set of participants trading against the book: it generates
//! commands and learns from the engine's events (their execution reports) which orders are
//! still resting, so cancels and modifies target live orders and carry the right owner.
//! Neither generating a command nor observing an event allocates, so the generator can run
//! inside allocation-checked loops.

use rustc_hash::FxHashMap;

use crate::{
    BookConfig, CancelReason, Command, Event, EventSink, OrderId, OwnerId, Price, Qty,
    RejectReason, SelfTradePolicy, Side, TimeInForce,
};

/// SplitMix64: tiny, fast and identical on every platform, so a seed fully determines a run.
#[derive(Clone, Debug)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    /// A generator starting from `seed`.
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// Next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (multiply-shift; the bias is negligible for small `n`).
    pub fn below(&mut self, n: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }
}

/// Command mix in percent. The fields must add up to 100.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Mix {
    /// New limit orders placed behind the mid; they usually rest without trading.
    pub passive_limit: u8,
    /// New limit orders priced through the mid; they usually trade.
    pub aggressive_limit: u8,
    /// Market orders.
    pub market: u8,
    /// Cancels of resting orders.
    pub cancel: u8,
    /// Mass cancels of a random participant's orders, as on a session disconnect.
    pub mass_cancel: u8,
    /// Modifies of resting orders.
    pub modify: u8,
}

/// Time in force of generated limit orders, in percent; the rest are GTC. All zero by
/// default, in which case the generator draws no extra random numbers, so adding this
/// left every existing stream unchanged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TifMix {
    /// Share of aggressive limits sent immediate-or-cancel.
    pub ioc: u8,
    /// Share of aggressive limits sent fill-or-kill.
    pub fok: u8,
    /// Share of passive limits sent post-only.
    pub post_only: u8,
}

/// Shape of the generated order flow.
#[derive(Clone, Copy, Debug)]
pub struct WorkloadConfig {
    /// Seed; the same seed yields the same stream.
    pub seed: u64,
    /// Lowest price of the book's band.
    pub min_price: Price,
    /// Highest price of the book's band.
    pub max_price: Price,
    /// Starting mid price; it then random-walks by one tick at a time.
    pub initial_mid: Price,
    /// Passive orders land 1..=`passive_depth` ticks from the mid, skewed toward the touch.
    pub passive_depth: Price,
    /// Order sizes are uniform in `1..=max_qty`.
    pub max_qty: Qty,
    /// Number of distinct participants; orders get a uniformly random owner.
    pub owners: u32,
    /// Upper bound on resting orders. At the bound, a random resting order is cancelled
    /// before a new one is placed, so the book reaches a steady state instead of growing.
    pub max_live: u32,
    /// Command mix.
    pub mix: Mix,
    /// Time in force of limit orders.
    pub tif: TifMix,
    /// Share of passive limits, in percent, sent as icebergs that show a quarter of their
    /// size. Zero by default, and no random numbers are drawn then.
    pub iceberg: u8,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            seed: 0x5EED,
            min_price: 0,
            max_price: 200_000,
            initial_mid: 100_000,
            passive_depth: 50,
            max_qty: 100,
            owners: 64,
            max_live: 10_000,
            mix: Mix {
                passive_limit: 55,
                aggressive_limit: 10,
                market: 5,
                cancel: 25,
                mass_cancel: 0,
                modify: 5,
            },
            tif: TifMix::default(),
            iceberg: 0,
        }
    }
}

impl WorkloadConfig {
    /// A book that can hold everything this workload generates, with price protection and
    /// self-trade prevention switched on.
    pub fn book_config(&self) -> BookConfig {
        BookConfig {
            min_price: self.min_price,
            max_price: self.max_price,
            max_orders: self.max_live,
            max_owners: self.owners,
            max_order_qty: 1_000_000,
            max_iceberg_tranches: BookConfig::DEFAULT_MAX_ICEBERG_TRANCHES,
            price_protection: Some((self.passive_depth * 4) as u32),
            price_band: None,
            reference_price: None,
            self_trade: SelfTradePolicy::CancelResting,
        }
    }
}

/// A resting order, as last reported by the engine.
#[derive(Clone, Copy, Debug)]
struct Resting {
    id: OrderId,
    owner: OwnerId,
    side: Side,
    price: Price,
    leaves: Qty,
}

/// Endless, reproducible stream of [`Command`]s.
///
/// Feed every event the engine emits back through [`Workload::observe`] before asking for
/// the next command; given the same seed and the same engine, the stream is then identical
/// on every run.
pub struct Workload {
    cfg: WorkloadConfig,
    rng: SplitMix64,
    mid: Price,
    next_id: OrderId,
    /// Resting orders in no particular order, for O(1) random picks.
    live: Vec<Resting>,
    /// Order id -> index into `live`. Reserved at twice `max_live` so that it never grows.
    position: FxHashMap<OrderId, u32>,
    /// Order deferred while the cancel that makes room for it goes out first.
    pending: Option<Command>,
}

impl Workload {
    /// A generator for `cfg`.
    ///
    /// # Panics
    ///
    /// If the mix does not add up to 100, a size or count is zero, or the price band is too
    /// narrow for the mid to move in.
    pub fn new(cfg: WorkloadConfig) -> Self {
        let m = cfg.mix;
        let total: u32 = [
            m.passive_limit,
            m.aggressive_limit,
            m.market,
            m.cancel,
            m.mass_cancel,
            m.modify,
        ]
        .iter()
        .map(|&p| u32::from(p))
        .sum();
        assert_eq!(total, 100, "mix must add up to 100");
        let t = cfg.tif;
        assert!(
            u16::from(t.ioc) + u16::from(t.fok) <= 100 && t.post_only <= 100,
            "time-in-force shares are percentages"
        );
        assert!(cfg.iceberg <= 100, "the iceberg share is a percentage");
        assert!(cfg.max_live > 0 && cfg.max_qty > 0 && cfg.owners > 0 && cfg.passive_depth > 0);
        let margin = cfg.passive_depth + 8;
        assert!(
            cfg.min_price + margin <= cfg.max_price - margin,
            "price band too narrow"
        );
        let mut position = FxHashMap::default();
        position.reserve(cfg.max_live as usize * 2);
        Self {
            rng: SplitMix64::new(cfg.seed),
            mid: cfg.initial_mid,
            next_id: 1,
            live: Vec::with_capacity(cfg.max_live as usize),
            position,
            pending: None,
            cfg,
        }
    }

    /// Number of orders the participants believe are resting.
    pub fn live_orders(&self) -> usize {
        self.live.len()
    }

    /// The next command to send.
    pub fn next_command(&mut self) -> Command {
        if let Some(command) = self.pending.take() {
            return command;
        }
        self.walk_mid();
        let m = self.cfg.mix;
        let mut roll = self.rng.below(100) as u8;
        if roll < m.passive_limit {
            return self.new_limit(false);
        }
        roll -= m.passive_limit;
        if roll < m.aggressive_limit {
            return self.new_limit(true);
        }
        roll -= m.aggressive_limit;
        if roll < m.market {
            return self.new_market();
        }
        roll -= m.market;
        if roll < m.cancel {
            return self.cancel();
        }
        roll -= m.cancel;
        if roll < m.mass_cancel {
            return Command::CancelAll {
                owner: self.rng.below(u64::from(self.cfg.owners)) as OwnerId,
            };
        }
        self.modify()
    }

    /// Updates the participants' view of their resting orders from one engine event.
    pub fn observe(&mut self, event: &Event) {
        match *event {
            Event::Rested {
                id,
                side,
                price,
                qty,
                ..
            } => {
                let owner = self.owner_of_new(id);
                self.position.insert(id, self.live.len() as u32);
                self.live.push(Resting {
                    id,
                    owner,
                    side,
                    price,
                    leaves: qty,
                });
            }
            Event::Trade {
                maker,
                maker_leaves,
                ..
            } => {
                if maker_leaves == 0 {
                    self.forget(maker);
                } else if let Some(&i) = self.position.get(&maker) {
                    self.live[i as usize].leaves = maker_leaves;
                }
            }
            Event::Cancelled { id, .. } => self.forget(id),
            Event::Modified {
                id, price, leaves, ..
            } => {
                if let Some(&i) = self.position.get(&id) {
                    let order = &mut self.live[i as usize];
                    if leaves > 0 && order.price == price && leaves <= order.leaves {
                        order.leaves = leaves;
                    } else {
                        // Done, or lost priority: in the latter case the engine re-enters
                        // the order and a `Rested` follows if anything is left.
                        self.forget(id);
                    }
                }
            }
            // A new iceberg tranche changes what shows, not what is open.
            Event::Accepted { .. }
            | Event::Rejected { .. }
            | Event::MassCancelled { .. }
            | Event::Replenished { .. } => {}
        }
    }

    /// Owners are a pure function of the order id, so the generator needs no extra state to
    /// remember who placed an order that has just rested.
    fn owner_of_new(&self, id: OrderId) -> OwnerId {
        (SplitMix64::new(id ^ self.cfg.seed).next_u64() % u64::from(self.cfg.owners)) as OwnerId
    }

    fn forget(&mut self, id: OrderId) {
        if let Some(i) = self.position.remove(&id) {
            self.live.swap_remove(i as usize);
            if let Some(moved) = self.live.get(i as usize) {
                self.position.insert(moved.id, i);
            }
        }
    }

    /// About one command in eight moves the mid a tick, staying clear of the band edges.
    fn walk_mid(&mut self) {
        if self.rng.below(8) == 0 {
            let step = if self.rng.below(2) == 0 { -1 } else { 1 };
            let margin = self.cfg.passive_depth + 8;
            self.mid =
                (self.mid + step).clamp(self.cfg.min_price + margin, self.cfg.max_price - margin);
        }
    }

    fn random_side(&mut self) -> Side {
        if self.rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        }
    }

    fn random_qty(&mut self) -> Qty {
        1 + self.rng.below(self.cfg.max_qty)
    }

    fn random_live(&mut self) -> Option<Resting> {
        if self.live.is_empty() {
            return None;
        }
        Some(self.live[self.rng.below(self.live.len() as u64) as usize])
    }

    /// 1..=`passive_depth` ticks behind the mid; the minimum of two uniform draws skews
    /// prices toward the touch, as in real books.
    fn passive_price(&mut self, side: Side) -> Price {
        let depth = self.cfg.passive_depth as u64;
        let offset = 1 + self.rng.below(depth).min(self.rng.below(depth)) as Price;
        match side {
            Side::Buy => self.mid - offset,
            Side::Sell => self.mid + offset,
        }
    }

    /// 1..=4 ticks through the mid, into the opposite side's best levels.
    fn aggressive_price(&mut self, side: Side) -> Price {
        let through = 1 + self.rng.below(4) as Price;
        match side {
            Side::Buy => self.mid + through,
            Side::Sell => self.mid - through,
        }
    }

    /// Draws only when the relevant shares are non-zero, so a GTC-only flow is unchanged.
    fn random_tif(&mut self, aggressive: bool) -> TimeInForce {
        let t = self.cfg.tif;
        if aggressive && t.ioc + t.fok > 0 {
            let roll = self.rng.below(100) as u8;
            if roll < t.ioc {
                TimeInForce::Ioc
            } else if roll < t.ioc + t.fok {
                TimeInForce::Fok
            } else {
                TimeInForce::Gtc
            }
        } else if !aggressive && t.post_only > 0 && (self.rng.below(100) as u8) < t.post_only {
            TimeInForce::PostOnly
        } else {
            TimeInForce::Gtc
        }
    }

    /// A quarter of the size, for the configured share of passive orders big enough to hide
    /// something. Draws only when the share is non-zero.
    fn random_display(&mut self, aggressive: bool, qty: Qty) -> Option<Qty> {
        let iceberg = !aggressive
            && self.cfg.iceberg > 0
            && (self.rng.below(100) as u8) < self.cfg.iceberg
            && qty >= 2;
        iceberg.then(|| (qty / 4).max(1))
    }

    fn take_id(&mut self) -> OrderId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn new_limit(&mut self, aggressive: bool) -> Command {
        let side = self.random_side();
        let price = if aggressive {
            self.aggressive_price(side)
        } else {
            self.passive_price(side)
        };
        let qty = self.random_qty();
        let id = self.take_id();
        let tif = self.random_tif(aggressive);
        let display = self.random_display(aggressive, qty);
        let order = Command::Limit {
            id,
            owner: self.owner_of_new(id),
            side,
            price,
            qty,
            tif,
            display,
        };
        if self.live.len() < self.cfg.max_live as usize {
            return order;
        }
        // At the bound: pull a resting order first, like a participant cancelling stale
        // quotes.
        let victim = self.random_live().expect("max_live > 0");
        self.pending = Some(order);
        Command::Cancel {
            id: victim.id,
            owner: victim.owner,
        }
    }

    fn new_market(&mut self) -> Command {
        let side = self.random_side();
        let qty = self.random_qty();
        let id = self.take_id();
        Command::Market {
            id,
            owner: self.owner_of_new(id),
            side,
            qty,
        }
    }

    fn cancel(&mut self) -> Command {
        match self.random_live() {
            Some(order) => Command::Cancel {
                id: order.id,
                owner: order.owner,
            },
            None => self.new_limit(false),
        }
    }

    /// Half the time keeps the price (a size change), half the time moves it. The new total
    /// quantity is random, so some modifies shrink in place, some lose priority, and some
    /// end the order because the new total is already filled.
    fn modify(&mut self) -> Command {
        let Some(order) = self.random_live() else {
            return self.new_limit(false);
        };
        let price = if self.rng.below(2) == 0 {
            order.price
        } else {
            self.passive_price(order.side)
        };
        Command::Modify {
            id: order.id,
            owner: order.owner,
            price,
            qty: self.random_qty(),
        }
    }
}

/// An [`EventSink`] that only counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventCounts {
    /// `Accepted` events.
    pub accepted: u64,
    /// `Rejected` events.
    pub rejected: u64,
    /// `Rejected` events with reason `BookFull`.
    pub rejected_book_full: u64,
    /// `Trade` events.
    pub trades: u64,
    /// Total traded quantity.
    pub traded_qty: u64,
    /// `Rested` events.
    pub rested: u64,
    /// `Cancelled` events.
    pub cancelled: u64,
    /// `Cancelled` events caused by self-trade prevention.
    pub self_trade_cancels: u64,
    /// `Cancelled` events caused by price protection.
    pub protection_cancels: u64,
    /// `Cancelled` events caused by the price band.
    pub band_cancels: u64,
    /// `Cancelled` events caused by a mass cancel.
    pub mass_cancelled_orders: u64,
    /// Unfilled remainders of immediate-or-cancel orders.
    pub ioc_cancels: u64,
    /// Fill-or-kill orders that could not fill.
    pub fok_kills: u64,
    /// `Replenished` events: new iceberg tranches.
    pub replenishes: u64,
    /// `Modified` events.
    pub modified: u64,
    /// `MassCancelled` events.
    pub mass_cancels: u64,
}

impl EventSink for EventCounts {
    #[inline]
    fn on_event(&mut self, event: Event) {
        match event {
            Event::Accepted { .. } => self.accepted += 1,
            Event::Rejected { reason, .. } => {
                self.rejected += 1;
                if reason == RejectReason::BookFull {
                    self.rejected_book_full += 1;
                }
            }
            Event::Trade { qty, .. } => {
                self.trades += 1;
                self.traded_qty += qty;
            }
            Event::Rested { .. } => self.rested += 1,
            Event::Cancelled { reason, .. } => {
                self.cancelled += 1;
                match reason {
                    CancelReason::SelfTrade => self.self_trade_cancels += 1,
                    CancelReason::PriceProtection => self.protection_cancels += 1,
                    CancelReason::PriceBand => self.band_cancels += 1,
                    CancelReason::MassCancel => self.mass_cancelled_orders += 1,
                    CancelReason::ImmediateOrCancel => self.ioc_cancels += 1,
                    CancelReason::FillOrKill => self.fok_kills += 1,
                    CancelReason::Requested | CancelReason::NoLiquidity => {}
                }
            }
            Event::Modified { .. } => self.modified += 1,
            Event::MassCancelled { .. } => self.mass_cancels += 1,
            Event::Replenished { .. } => self.replenishes += 1,
        }
    }
}
