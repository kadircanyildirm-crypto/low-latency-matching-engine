//! Deterministic synthetic order flow, plus a counting [`EventSink`], for benchmarks and
//! soak tests.
//!
//! [`Workload`] behaves like a single client trading against the book: it generates commands
//! and learns from the engine's events (its execution reports) which of its orders are still
//! resting, so its cancels and modifies target live orders. Neither generating a command nor
//! observing an event allocates, so the generator can run inside allocation-checked loops.

use rustc_hash::FxHashMap;

use crate::{BookConfig, Command, Event, EventSink, OrderId, Price, Qty, RejectReason, Side};

/// SplitMix64: tiny, fast and identical on every platform, so a seed fully determines a run.
#[derive(Clone, Debug)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

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
    pub market: u8,
    pub cancel: u8,
    pub modify: u8,
}

/// Shape of the generated order flow.
#[derive(Clone, Copy, Debug)]
pub struct WorkloadConfig {
    pub seed: u64,
    pub min_price: Price,
    pub max_price: Price,
    /// Starting mid price; it then random-walks by one tick at a time.
    pub initial_mid: Price,
    /// Passive orders land 1..=`passive_depth` ticks from the mid, skewed toward the touch.
    pub passive_depth: Price,
    pub max_qty: Qty,
    /// Upper bound on resting orders. At the bound, a random resting order is cancelled
    /// before a new one is placed, so the book reaches a steady state instead of growing.
    pub max_live: u32,
    pub mix: Mix,
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
            max_live: 10_000,
            mix: Mix {
                passive_limit: 55,
                aggressive_limit: 10,
                market: 5,
                cancel: 25,
                modify: 5,
            },
        }
    }
}

impl WorkloadConfig {
    /// A book that can hold everything this workload generates.
    pub fn book_config(&self) -> BookConfig {
        BookConfig {
            min_price: self.min_price,
            max_price: self.max_price,
            max_orders: self.max_live,
        }
    }
}

/// One of the client's resting orders, as last reported by the engine.
#[derive(Clone, Copy, Debug)]
struct Resting {
    id: OrderId,
    side: Side,
    price: Price,
    qty: Qty,
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
    /// # Panics
    ///
    /// If the mix does not add up to 100 or the price band is too narrow for the mid to
    /// move in.
    pub fn new(cfg: WorkloadConfig) -> Self {
        let m = cfg.mix;
        let total: u32 = [
            m.passive_limit,
            m.aggressive_limit,
            m.market,
            m.cancel,
            m.modify,
        ]
        .iter()
        .map(|&p| u32::from(p))
        .sum();
        assert_eq!(total, 100, "mix must add up to 100");
        assert!(cfg.max_live > 0 && cfg.max_qty > 0 && cfg.passive_depth > 0);
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

    /// Number of orders the client believes are resting.
    pub fn live_orders(&self) -> usize {
        self.live.len()
    }

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
        self.modify()
    }

    /// Updates the client's view of its resting orders from one engine event.
    pub fn observe(&mut self, event: &Event) {
        match *event {
            Event::Rested {
                id,
                side,
                price,
                qty,
            } => {
                self.position.insert(id, self.live.len() as u32);
                self.live.push(Resting {
                    id,
                    side,
                    price,
                    qty,
                });
            }
            Event::Trade { maker, qty, .. } => {
                if let Some(&i) = self.position.get(&maker) {
                    let order = &mut self.live[i as usize];
                    order.qty -= qty;
                    if order.qty == 0 {
                        self.forget(maker);
                    }
                }
            }
            Event::Cancelled { id, .. } => self.forget(id),
            Event::Modified { id, price, qty } => {
                if let Some(&i) = self.position.get(&id) {
                    let order = &mut self.live[i as usize];
                    if order.price == price && qty <= order.qty {
                        order.qty = qty;
                    } else {
                        // Lost priority: the engine re-enters it, and a `Rested` follows if
                        // anything is left after trading.
                        self.forget(id);
                    }
                }
            }
            Event::Accepted { .. } | Event::Rejected { .. } => {}
        }
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
        let order = Command::Limit {
            id: self.take_id(),
            side,
            price,
            qty,
        };
        if self.live.len() < self.cfg.max_live as usize {
            return order;
        }
        // At the bound: pull a resting order first, like a client cancelling stale quotes.
        let victim = self.random_live().expect("max_live > 0");
        self.pending = Some(order);
        Command::Cancel { id: victim.id }
    }

    fn new_market(&mut self) -> Command {
        let side = self.random_side();
        let qty = self.random_qty();
        Command::Market {
            id: self.take_id(),
            side,
            qty,
        }
    }

    fn cancel(&mut self) -> Command {
        match self.random_live() {
            Some(order) => Command::Cancel { id: order.id },
            None => self.new_limit(false),
        }
    }

    /// Half the time keeps the price (a size change), half the time moves it.
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
            price,
            qty: self.random_qty(),
        }
    }
}

/// An [`EventSink`] that only counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventCounts {
    pub accepted: u64,
    pub rejected: u64,
    pub rejected_book_full: u64,
    pub trades: u64,
    pub traded_qty: u64,
    pub rested: u64,
    pub cancelled: u64,
    pub modified: u64,
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
            Event::Cancelled { .. } => self.cancelled += 1,
            Event::Modified { .. } => self.modified += 1,
        }
    }
}
