//! Market data: the book's depth by price level, kept from the book's events alone, and the
//! levels each batch of events changed.
//!
//! A [`Depth`] follows every resting order: its side, price, and how much of it shows and
//! is left. Each event updates the orders it names and the levels they rest at, so the
//! depth needs neither the book nor a thread that holds it: it runs wherever the events go.
//! What shows is what the book shows: an iceberg counts with its visible tranche only.
//!
//! The rules it follows are the book's (DESIGN.md §4 and §5): an order rests with
//! `Rested`; a trade takes its quantity from what the resting orders it names show; an
//! iceberg whose tranche ran out shows the next one with `Replenished`; a cancel removes the
//! rest; a modify that keeps the price and does not add quantity shrinks the order in
//! place, its hidden part first, and any other modify takes it off the book, to rest again
//! with a `Rested` of its own if anything is left.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::{BTreeMap, HashMap};

use orderbook::{Event, OrderBook, OrderId, Price, Qty, Side};

/// One price level: what its orders show, and how many there are.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Level {
    /// The quantity the level's orders show; hidden iceberg quantity is not included.
    pub qty: Qty,
    /// The number of orders at the level.
    pub orders: u32,
}

/// The new state of a price level; zero orders means it is gone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LevelUpdate {
    /// The side.
    pub side: Side,
    /// The price.
    pub price: Price,
    /// The level now.
    pub level: Level,
}

/// A resting order, as far as depth goes.
#[derive(Clone, Copy, Debug)]
struct Resting {
    side: Side,
    price: Price,
    /// What it shows.
    visible: Qty,
    /// What is left of it, hidden part included.
    leaves: Qty,
}

/// The depth of a book, kept from its events.
#[derive(Clone, Debug, Default)]
pub struct Depth {
    orders: HashMap<OrderId, Resting>,
    bids: BTreeMap<Price, Level>,
    asks: BTreeMap<Price, Level>,
    /// Levels changed since the last [`changes`](Depth::changes), possibly repeated.
    changed: Vec<(Side, Price)>,
}

impl Depth {
    /// An empty depth.
    pub fn new() -> Depth {
        Depth::default()
    }

    /// The depth of `book` as it stands, such as a book recovered from a journal.
    pub fn of(book: &OrderBook) -> Depth {
        let mut depth = Depth::new();
        for side in [Side::Buy, Side::Sell] {
            for level in book.depth(side) {
                for order in book.queue(side, level.price) {
                    depth.add(order.id, side, level.price, order.visible, order.leaves);
                }
            }
        }
        depth.changed.clear();
        depth
    }

    /// How many orders rest on the book.
    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    /// The levels of `side`, best price first.
    pub fn levels(&self, side: Side) -> Box<dyn Iterator<Item = (Price, Level)> + '_> {
        match side {
            Side::Buy => Box::new(self.bids.iter().rev().map(|(&p, &l)| (p, l))),
            Side::Sell => Box::new(self.asks.iter().map(|(&p, &l)| (p, l))),
        }
    }

    /// The level of `side` at `price`; an empty one if no order rests there.
    pub fn level(&self, side: Side, price: Price) -> Level {
        self.half(side).get(&price).copied().unwrap_or_default()
    }

    /// Takes the levels changed since the last call, each once, in the order they first
    /// changed, with their state now.
    pub fn changes(&mut self) -> impl Iterator<Item = LevelUpdate> + '_ {
        let mut seen = std::collections::HashSet::with_capacity(self.changed.len());
        let changed = std::mem::take(&mut self.changed);
        changed
            .into_iter()
            .filter(move |key| seen.insert(*key))
            .map(|(side, price)| LevelUpdate {
                side,
                price,
                level: self.level(side, price),
            })
    }

    /// Follows one event of the book.
    pub fn apply(&mut self, event: &Event) {
        match *event {
            Event::Rested {
                id,
                side,
                price,
                qty,
                visible,
            } => self.add(id, side, price, visible, qty),
            Event::Trade {
                taker,
                maker,
                qty,
                taker_leaves,
                maker_leaves,
                ..
            } => {
                // In continuous trading only the maker rests; in an uncross both do.
                for (id, leaves) in [(maker, maker_leaves), (taker, taker_leaves)] {
                    if let Some(order) = self.orders.get_mut(&id) {
                        let (side, price, before) = (order.side, order.price, order.visible);
                        order.visible -= qty;
                        order.leaves = leaves;
                        self.show(side, price, before, before - qty);
                        if leaves == 0 {
                            self.remove(id);
                        }
                    }
                }
            }
            Event::Replenished { id, visible, .. } => {
                if let Some(order) = self.orders.get_mut(&id) {
                    let (side, price, before) = (order.side, order.price, order.visible);
                    order.visible = visible;
                    self.show(side, price, before, visible);
                }
            }
            Event::Cancelled { id, .. } => self.remove(id),
            Event::Modified {
                id, price, leaves, ..
            } => {
                let Some(order) = self.orders.get(&id).copied() else {
                    return;
                };
                if leaves > 0 && price == order.price && leaves <= order.leaves {
                    // Shrunk in place: the cut comes out of the hidden part first.
                    let visible = order.visible.min(leaves);
                    self.show(order.side, order.price, order.visible, visible);
                    let order = self.orders.get_mut(&id).expect("a resting order");
                    order.visible = visible;
                    order.leaves = leaves;
                } else {
                    // Gone, or taken off to rest again elsewhere with a `Rested` of its own.
                    self.remove(id);
                }
            }
            Event::Accepted { .. }
            | Event::Rejected { .. }
            | Event::StopPlaced { .. }
            | Event::Triggered { .. }
            | Event::MassCancelled { .. }
            | Event::PhaseChanged { .. } => {}
        }
    }

    fn half(&self, side: Side) -> &BTreeMap<Price, Level> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn half_mut(&mut self, side: Side) -> &mut BTreeMap<Price, Level> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn add(&mut self, id: OrderId, side: Side, price: Price, visible: Qty, leaves: Qty) {
        let previous = self.orders.insert(
            id,
            Resting {
                side,
                price,
                visible,
                leaves,
            },
        );
        debug_assert!(previous.is_none(), "order {id} rests twice");
        let level = self.half_mut(side).entry(price).or_default();
        level.qty += visible;
        level.orders += 1;
        self.changed.push((side, price));
    }

    /// An order at `side` and `price` now shows `after` instead of `before`.
    fn show(&mut self, side: Side, price: Price, before: Qty, after: Qty) {
        let level = self
            .half_mut(side)
            .get_mut(&price)
            .expect("a resting order's level");
        level.qty = level.qty - before + after;
        self.changed.push((side, price));
    }

    fn remove(&mut self, id: OrderId) {
        let Some(order) = self.orders.remove(&id) else {
            return;
        };
        let half = self.half_mut(order.side);
        let level = half.get_mut(&order.price).expect("a resting order's level");
        level.qty -= order.visible;
        level.orders -= 1;
        if level.orders == 0 {
            debug_assert_eq!(level.qty, 0, "an empty level shows nothing");
            half.remove(&order.price);
        }
        self.changed.push((order.side, order.price));
    }
}
