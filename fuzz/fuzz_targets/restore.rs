//! `restore` on arbitrary snapshots, valid or not.
//!
//! A snapshot starts as that of a live book the fuzzer builds, and the fuzzer then edits
//! it: inserts orders and stops made from scratch, changes any field, removes, duplicates
//! and reorders entries, and sets the trade count, reference price and phase. So inputs
//! range from valid snapshots through near misses to garbage.
//!
//! - `restore` never panics: an impossible snapshot is an error.
//! - It accepts exactly the snapshots that keep the rules `SnapshotError` documents, and an
//!   error names a rule the snapshot actually breaks. The rules are restated here from the
//!   documentation, independently of the implementation.
//! - An accepted snapshot yields a healthy book (`validate()`) holding exactly its orders
//!   and stops, each price level's queue and each trigger's stops in snapshot order, with
//!   its trade count, reference price and phase. The book's own snapshot restores to the same
//!   state, and its digest equals that snapshot's.
//! - The restored book then runs commands the fuzzer chooses with `validate()` after each,
//!   and, unless the band lies too close to the ends of `i64` for the reference book, it
//!   must match the reference book loaded with the same snapshot, event for event.

#![no_main]

use std::collections::{BTreeMap, HashSet};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use orderbook::{
    BookConfig, BookSnapshot, OrderBook, OrderId, Phase, Price, Side, SnapshotError, SnapshotOrder,
    StopOrder,
};
use orderbook_fuzz::reference::ReferenceBook;
use orderbook_fuzz::{
    CommandInput, ConfigInput, DisplayInput, IdInput, MAX_COMMANDS, OwnerInput, PhaseInput,
    PriceInput, Prices, QtyInput, assert_matches, reference_can_follow, side,
};

/// Commands that build the live book the snapshot starts from.
const MAX_SETUP: usize = 64;

/// Edits applied to the live book's snapshot.
const MAX_EDITS: usize = 16;

#[derive(Arbitrary, Debug)]
struct Input {
    config: ConfigInput,
    setup: Vec<CommandInput>,
    edits: Vec<Edit>,
    /// Commands for the restored book, if the snapshot is accepted.
    after: Vec<CommandInput>,
}

/// One change to a snapshot. Indices are taken modulo the number of entries; an edit of an
/// empty list does nothing.
#[derive(Arbitrary, Debug)]
enum Edit {
    InsertOrder {
        at: u8,
        order: OrderInput,
    },
    InsertStop {
        at: u8,
        stop: StopInput,
    },
    ChangeOrder {
        index: u8,
        change: OrderChange,
    },
    ChangeStop {
        index: u8,
        change: StopChange,
    },
    RemoveOrder(u8),
    RemoveStop(u8),
    DuplicateOrder(u8),
    DuplicateStop(u8),
    /// Moving an order within its level changes its time priority; moving it past other
    /// levels changes nothing.
    MoveOrder {
        from: u8,
        to: u8,
    },
    MoveStop {
        from: u8,
        to: u8,
    },
    TradeCount(TradeCountInput),
    ReferencePrice(Option<PriceInput>),
    Phase(PhaseInput),
}

/// An id for an inserted or changed entry: one no live order uses, or one from the pool the
/// commands draw from, which may collide.
#[derive(Arbitrary, Clone, Copy, Debug)]
enum NewIdInput {
    Fresh(u16),
    Pooled(IdInput),
}

impl NewIdInput {
    fn id(self) -> OrderId {
        match self {
            NewIdInput::Fresh(id) => 1_000 + OrderId::from(id),
            NewIdInput::Pooled(id) => id.id(),
        }
    }
}

#[derive(Arbitrary, Debug)]
struct OrderInput {
    id: NewIdInput,
    owner: OwnerInput,
    buy: bool,
    price: PriceInput,
    leaves: QtyInput,
    filled: Option<QtyInput>,
    post_only: bool,
    display: DisplayInput,
    /// What the order shows; `None` for what a consistent order shows.
    visible: Option<QtyInput>,
}

#[derive(Arbitrary, Debug)]
struct StopInput {
    id: NewIdInput,
    owner: OwnerInput,
    buy: bool,
    trigger: PriceInput,
    limit: Option<PriceInput>,
    qty: QtyInput,
}

#[derive(Arbitrary, Debug)]
enum OrderChange {
    Id(NewIdInput),
    Owner(OwnerInput),
    Side,
    Price(PriceInput),
    Leaves(QtyInput),
    Filled(QtyInput),
    PostOnly,
    Display(DisplayInput),
    Visible(QtyInput),
}

#[derive(Arbitrary, Debug)]
enum StopChange {
    Id(NewIdInput),
    Owner(OwnerInput),
    Side,
    Trigger(PriceInput),
    Limit(Option<PriceInput>),
    Qty(QtyInput),
}

#[derive(Arbitrary, Debug)]
enum TradeCountInput {
    Small(u16),
    /// `BookSnapshot::MAX_TRADE_COUNT` minus 128 to plus 127.
    NearLimit(i8),
    /// `u64::MAX` minus up to 255.
    NearEnd(u8),
    Any(u64),
}

impl OrderInput {
    fn order(&self, config: &BookConfig) -> SnapshotOrder {
        let leaves = self.leaves.qty(config);
        let display = self.display.display();
        SnapshotOrder {
            id: self.id.id(),
            owner: self.owner.owner(),
            side: side(self.buy),
            price: self.price.price(config),
            leaves,
            filled: self.filled.map_or(0, |filled| filled.qty(config)),
            post_only: self.post_only,
            display,
            visible: match self.visible {
                Some(visible) => visible.qty(config),
                None => display.map_or(leaves, |display| display.min(leaves)),
            },
        }
    }
}

impl StopInput {
    fn stop(&self, config: &BookConfig) -> StopOrder {
        StopOrder {
            id: self.id.id(),
            owner: self.owner.owner(),
            side: side(self.buy),
            trigger: self.trigger.price(config),
            limit: self.limit.map(|limit| limit.price(config)),
            qty: self.qty.qty(config),
        }
    }
}

impl Edit {
    fn apply(&self, snapshot: &mut BookSnapshot) {
        let config = snapshot.config;
        let (orders, stops) = (&mut snapshot.orders, &mut snapshot.stops);
        match self {
            Edit::InsertOrder { at, order } => {
                let at = usize::from(*at) % (orders.len() + 1);
                orders.insert(at, order.order(&config));
            }
            Edit::InsertStop { at, stop } => {
                let at = usize::from(*at) % (stops.len() + 1);
                stops.insert(at, stop.stop(&config));
            }
            Edit::ChangeOrder { index, change } => {
                let Some(order) = pick(orders, *index) else {
                    return;
                };
                match change {
                    OrderChange::Id(id) => order.id = id.id(),
                    OrderChange::Owner(owner) => order.owner = owner.owner(),
                    OrderChange::Side => order.side = order.side.opposite(),
                    OrderChange::Price(price) => order.price = price.price(&config),
                    OrderChange::Leaves(qty) => order.leaves = qty.qty(&config),
                    OrderChange::Filled(qty) => order.filled = qty.qty(&config),
                    OrderChange::PostOnly => order.post_only = !order.post_only,
                    OrderChange::Display(display) => order.display = display.display(),
                    OrderChange::Visible(qty) => order.visible = qty.qty(&config),
                }
            }
            Edit::ChangeStop { index, change } => {
                let Some(stop) = pick(stops, *index) else {
                    return;
                };
                match change {
                    StopChange::Id(id) => stop.id = id.id(),
                    StopChange::Owner(owner) => stop.owner = owner.owner(),
                    StopChange::Side => stop.side = stop.side.opposite(),
                    StopChange::Trigger(price) => stop.trigger = price.price(&config),
                    StopChange::Limit(limit) => {
                        stop.limit = limit.map(|limit| limit.price(&config));
                    }
                    StopChange::Qty(qty) => stop.qty = qty.qty(&config),
                }
            }
            Edit::RemoveOrder(index) => remove(orders, *index),
            Edit::RemoveStop(index) => remove(stops, *index),
            Edit::DuplicateOrder(index) => duplicate(orders, *index),
            Edit::DuplicateStop(index) => duplicate(stops, *index),
            Edit::MoveOrder { from, to } => relocate(orders, *from, *to),
            Edit::MoveStop { from, to } => relocate(stops, *from, *to),
            Edit::TradeCount(count) => {
                snapshot.trade_count = match *count {
                    TradeCountInput::Small(count) => u64::from(count),
                    TradeCountInput::NearLimit(offset) => {
                        BookSnapshot::MAX_TRADE_COUNT.wrapping_add_signed(i64::from(offset))
                    }
                    TradeCountInput::NearEnd(below) => u64::MAX - u64::from(below),
                    TradeCountInput::Any(count) => count,
                };
            }
            Edit::ReferencePrice(price) => {
                snapshot.reference_price = price.map(|price| price.price(&config));
            }
            Edit::Phase(phase) => snapshot.phase = phase.phase(),
        }
    }
}

fn pick<T>(list: &mut [T], index: u8) -> Option<&mut T> {
    let len = list.len();
    (len > 0).then(|| &mut list[usize::from(index) % len])
}

fn remove<T>(list: &mut Vec<T>, index: u8) {
    if !list.is_empty() {
        let index = usize::from(index) % list.len();
        list.remove(index);
    }
}

fn duplicate<T: Copy>(list: &mut Vec<T>, index: u8) {
    if !list.is_empty() {
        let index = usize::from(index) % list.len();
        list.insert(index, list[index]);
    }
}

fn relocate<T>(list: &mut Vec<T>, from: u8, to: u8) {
    if !list.is_empty() {
        let item = list.remove(usize::from(from) % list.len());
        let to = usize::from(to) % (list.len() + 1);
        list.insert(to, item);
    }
}

/// Every rule of `SnapshotError`'s documentation that `snapshot` breaks, as the error that
/// names it; empty if the snapshot keeps them all.
fn broken_rules(snapshot: &BookSnapshot) -> Vec<SnapshotError> {
    let config = &snapshot.config;
    let in_band = |price: Price| (config.min_price..=config.max_price).contains(&price);
    let mut broken = Vec::new();
    if snapshot.orders.len() + snapshot.stops.len() > config.max_orders as usize {
        broken.push(SnapshotError::TooManyOrders);
    }
    if snapshot.trade_count > BookSnapshot::MAX_TRADE_COUNT {
        broken.push(SnapshotError::TradeCountExhausted);
    }
    if snapshot
        .reference_price
        .is_some_and(|price| !in_band(price))
    {
        broken.push(SnapshotError::InvalidReferencePrice);
    }
    let mut ids = HashSet::new();
    for order in &snapshot.orders {
        let total = order.leaves.checked_add(order.filled);
        let quantities_fit = order.leaves > 0 && total.is_some_and(|t| t <= config.max_order_qty);
        let shows_validly = match order.display {
            None => order.visible == order.leaves,
            Some(display) => {
                order.visible > 0
                    && order.visible <= display.min(order.leaves)
                    && total.is_some_and(|total| {
                        u128::from(display) * u128::from(config.max_iceberg_tranches)
                            >= u128::from(total)
                    })
            }
        };
        if order.owner >= config.max_owners
            || !in_band(order.price)
            || !quantities_fit
            || !shows_validly
        {
            broken.push(SnapshotError::InvalidOrder(order.id));
        }
        if !ids.insert(order.id) {
            broken.push(SnapshotError::DuplicateOrderId(order.id));
        }
    }
    for stop in &snapshot.stops {
        let reached = snapshot
            .reference_price
            .is_some_and(|last| match stop.side {
                Side::Buy => stop.trigger <= last,
                Side::Sell => stop.trigger >= last,
            });
        if stop.owner >= config.max_owners
            || stop.qty == 0
            || stop.qty > config.max_order_qty
            || !in_band(stop.trigger)
            || stop.limit.is_some_and(|limit| !in_band(limit))
            || reached
        {
            broken.push(SnapshotError::InvalidOrder(stop.id));
        }
        if !ids.insert(stop.id) {
            broken.push(SnapshotError::DuplicateOrderId(stop.id));
        }
    }
    let prices = |side: Side| {
        snapshot
            .orders
            .iter()
            .filter(move |order| order.side == side)
            .map(|order| order.price)
    };
    let (best_bid, best_ask) = (prices(Side::Buy).max(), prices(Side::Sell).min());
    if let (Some(bid), Some(ask)) = (best_bid, best_ask) {
        if bid >= ask && snapshot.phase != Phase::Auction {
            broken.push(SnapshotError::Crossed);
        }
    }
    broken
}

/// Orders by side and price, and stops by side and trigger, each group in snapshot order.
/// `restore` must keep each group's order; the order of the groups is free.
type Groups = (
    BTreeMap<(bool, Price), Vec<SnapshotOrder>>,
    BTreeMap<(bool, Price), Vec<StopOrder>>,
);

fn groups(snapshot: &BookSnapshot) -> Groups {
    let mut orders: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for order in &snapshot.orders {
        let key = (order.side == Side::Buy, order.price);
        orders.entry(key).or_default().push(*order);
    }
    let mut stops: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for stop in &snapshot.stops {
        let key = (stop.side == Side::Buy, stop.trigger);
        stops.entry(key).or_default().push(*stop);
    }
    (orders, stops)
}

fuzz_target!(|input: Input| {
    let config = input.config.config(Prices::Unrestricted);
    let mut live = OrderBook::new(config);
    let mut events = Vec::new();
    for command in input.setup.iter().take(MAX_SETUP) {
        live.process(command.command(&config), &mut events);
    }
    let mut snapshot = live.snapshot();
    for edit in input.edits.iter().take(MAX_EDITS) {
        edit.apply(&mut snapshot);
    }

    let broken = broken_rules(&snapshot);
    let mut book = match OrderBook::restore(&snapshot) {
        Err(error) => {
            assert!(
                broken.contains(&error),
                "refused with {error:?}, but the snapshot breaks only {broken:?}"
            );
            return;
        }
        Ok(book) => book,
    };
    assert!(
        broken.is_empty(),
        "accepted, but the snapshot breaks {broken:?}"
    );
    if let Err(violation) = book.validate() {
        panic!("restored book is broken: {violation}");
    }
    let canonical = book.snapshot();
    assert_eq!(canonical.config, snapshot.config);
    assert_eq!(canonical.trade_count, snapshot.trade_count);
    assert_eq!(canonical.reference_price, snapshot.reference_price);
    assert_eq!(canonical.phase, snapshot.phase);
    assert_eq!(groups(&canonical), groups(&snapshot));
    assert_eq!(book.digest(), canonical.digest());
    match OrderBook::restore(&canonical) {
        Ok(again) => assert_eq!(again.snapshot(), canonical),
        Err(error) => panic!("a restored book's own snapshot was refused: {error}"),
    }

    let mut reference = reference_can_follow(&config).then(|| ReferenceBook::restore(&snapshot));
    let (mut got, mut want) = (Vec::new(), Vec::new());
    for (step, command) in input.after.iter().take(MAX_COMMANDS).enumerate() {
        let command = command.command(&config);
        got.clear();
        book.process(command, &mut got);
        if let Some(reference) = &mut reference {
            want.clear();
            reference.process(command, &mut want);
            assert_eq!(got, want, "events differ at step {step}: {command:?}");
            assert_matches(&book, reference, step, &command);
        } else if let Err(violation) = book.validate() {
            panic!("invariant broken at step {step} ({command:?}): {violation}");
        }
    }
});
