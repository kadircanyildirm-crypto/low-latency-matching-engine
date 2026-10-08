//! Specification checker: after every command, verify the engine's output and resulting book
//! against the rules of a price-time priority exchange, using only the engine's public
//! queries.
//!
//! The differential test compares the engine with a second implementation of the rules; if
//! both share a misunderstanding, it passes anyway. These checks are phrased as properties
//! of the outcome instead: each trade is with the next order in price-time priority, never
//! through the taker's limit or the protection cap, never between the same owner; fills are
//! as large as possible; quantity is conserved; rejected commands change nothing; orders the
//! command did not touch are left exactly as they were; a command is rejected exactly when a
//! rule requires it, with the reason that rule gives; immediate-or-cancel and fill-or-kill
//! orders never rest, a fill-or-kill order fills completely exactly when the book could fill
//! it, post-only orders never trade, icebergs trade only what they show and show their next
//! tranche at the back of the queue, and exactly the stops a command's trades reach trigger,
//! in the order the rules release them.
//!
//! The checker works out, from the rules alone, the state each part of a command must leave:
//! the command's own effect, then each stop it releases, one after the other. The engine's
//! book must end up in exactly the state at the end of that chain.

mod common;

use std::collections::{BTreeMap, VecDeque};

use common::strategies::scenario;
use common::{Snapshot, snapshot};
use orderbook::CancelReason::{
    FillOrKill, ImmediateOrCancel, MassCancel, NoLiquidity, PriceBand, PriceProtection, Requested,
    SelfTrade,
};
use orderbook::{
    BookConfig, CancelReason, Command, Event, OrderBook, OrderId, OwnerId, Price, Qty, QueuedOrder,
    RejectReason, SelfTradePolicy, Side, StopOrder, TimeInForce,
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1_000))]

    #[test]
    fn every_command_meets_the_specification((cfg, commands) in scenario(300)) {
        let mut book = OrderBook::new(cfg);
        let mut events = Vec::new();
        for (step, &command) in commands.iter().enumerate() {
            let before = State::capture(&book);
            events.clear();
            book.process(command, &mut events);
            if let Err(violation) = check(&cfg, &before, command, &events, &book) {
                prop_assert!(
                    false,
                    "step {}: {:?}\n  events: {:?}\n  violation: {}",
                    step, command, events, violation
                );
            }
        }
    }
}

type Check = Result<(), String>;

/// The state a part of a command leaves, and how many of its events that part accounts for.
type Effect = Result<(State, usize), String>;

fn ensure(condition: bool, message: impl Into<String>) -> Check {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

/// Everything the rules can see of a book.
#[derive(Clone, Debug, PartialEq)]
struct State {
    snapshot: Snapshot,
    /// Pending stops: buy stops in trigger order, then sell stops in trigger order.
    stops: [Vec<StopOrder>; 2],
    trade_count: u64,
    reference: Option<Price>,
}

impl State {
    fn capture(book: &OrderBook) -> Self {
        Self {
            snapshot: snapshot(book),
            stops: [
                book.stops(Side::Buy).collect(),
                book.stops(Side::Sell).collect(),
            ],
            trade_count: book.trade_count(),
            reference: book.reference_price(),
        }
    }

    fn side_index(side: Side) -> usize {
        match side {
            Side::Buy => 0,
            Side::Sell => 1,
        }
    }

    fn best(&self, side: Side) -> Option<Price> {
        self.snapshot[Self::side_index(side)]
            .first()
            .map(|(p, _)| *p)
    }

    /// Resting orders and pending stops: each holds one of `max_orders`.
    fn order_count(&self) -> usize {
        let resting: usize = self
            .snapshot
            .iter()
            .flatten()
            .map(|(_, queue)| queue.len())
            .sum();
        resting + self.stops.iter().map(Vec::len).sum::<usize>()
    }

    fn stop_count(&self) -> usize {
        self.stops.iter().map(Vec::len).sum()
    }

    fn find(&self, id: OrderId) -> Option<(Side, Price, QueuedOrder)> {
        for side in [Side::Buy, Side::Sell] {
            for (price, queue) in &self.snapshot[Self::side_index(side)] {
                if let Some(order) = queue.iter().find(|o| o.id == id) {
                    return Some((side, *price, *order));
                }
            }
        }
        None
    }

    fn find_stop(&self, id: OrderId) -> Option<StopOrder> {
        self.stops.iter().flatten().find(|s| s.id == id).copied()
    }

    /// Resting orders of `side` in the order an incoming order must reach them: best price
    /// first, then time priority.
    fn priority(&self, side: Side) -> Vec<(Price, QueuedOrder)> {
        self.snapshot[Self::side_index(side)]
            .iter()
            .flat_map(|(price, queue)| queue.iter().map(move |o| (*price, *o)))
            .collect()
    }

    /// The state without the resting order or pending stop `id`.
    fn without(&self, id: OrderId) -> State {
        let mut books = to_books(&self.snapshot);
        for book in &mut books {
            for queue in book.values_mut() {
                queue.retain(|o| o.id != id);
            }
            book.retain(|_, queue| !queue.is_empty());
        }
        let mut stops = self.stops.clone();
        stops
            .iter_mut()
            .for_each(|side| side.retain(|s| s.id != id));
        State {
            snapshot: from_books(books),
            stops,
            ..self.clone()
        }
    }

    /// The state with `stop` added behind the pending stops with the same trigger.
    fn with_stop(&self, stop: StopOrder) -> State {
        let mut state = self.clone();
        let list = &mut state.stops[Self::side_index(stop.side)];
        let at = list
            .iter()
            .position(|s| match stop.side {
                Side::Buy => s.trigger > stop.trigger,
                Side::Sell => s.trigger < stop.trigger,
            })
            .unwrap_or(list.len());
        list.insert(at, stop);
        state
    }
}

fn to_books(snapshot: &Snapshot) -> [BTreeMap<Price, Vec<QueuedOrder>>; 2] {
    [
        snapshot[0].iter().cloned().collect(),
        snapshot[1].iter().cloned().collect(),
    ]
}

fn from_books(books: [BTreeMap<Price, Vec<QueuedOrder>>; 2]) -> Snapshot {
    let [bids, asks] = books;
    [bids.into_iter().rev().collect(), asks.into_iter().collect()]
}

/// `price` is acceptable for a `side` order limited at `limit`.
fn within(side: Side, price: Price, limit: Price) -> bool {
    match side {
        Side::Buy => price <= limit,
        Side::Sell => price >= limit,
    }
}

/// Whether a `side` limit at `price` lies more than the protection through the opposite best.
fn beyond_protection(cfg: &BookConfig, state: &State, side: Side, price: Price) -> bool {
    cfg.price_protection.is_some_and(|ticks| {
        let ticks = i128::from(ticks);
        match side {
            Side::Buy => state
                .best(Side::Sell)
                .is_some_and(|ask| i128::from(price) > i128::from(ask) + ticks),
            Side::Sell => state
                .best(Side::Buy)
                .is_some_and(|bid| i128::from(price) < i128::from(bid) - ticks),
        }
    })
}

/// Whether a `side` limit at `price` lies more than the band through the reference price.
fn beyond_band(cfg: &BookConfig, state: &State, side: Side, price: Price) -> bool {
    cfg.price_band
        .zip(state.reference)
        .is_some_and(|(ticks, reference)| {
            let (price, reference, ticks) =
                (i128::from(price), i128::from(reference), i128::from(ticks));
            match side {
                Side::Buy => price > reference + ticks,
                Side::Sell => price < reference - ticks,
            }
        })
}

fn crosses(state: &State, side: Side, price: Price) -> bool {
    match side {
        Side::Buy => state.best(Side::Sell).is_some_and(|ask| price >= ask),
        Side::Sell => state.best(Side::Buy).is_some_and(|bid| price <= bid),
    }
}

/// Whether a trade has already reached a `side` stop's trigger.
fn reached(state: &State, side: Side, trigger: Price) -> bool {
    state.reference.is_some_and(|last| match side {
        Side::Buy => trigger <= last,
        Side::Sell => trigger >= last,
    })
}

/// Lowest and highest trade price among `events`.
fn trade_range(events: &[Event]) -> Option<(Price, Price)> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Trade { price, .. } => Some(*price),
            _ => None,
        })
        .fold(None, |range, price| widen(range, Some((price, price))))
}

fn widen(a: Option<(Price, Price)>, b: Option<(Price, Price)>) -> Option<(Price, Price)> {
    match (a, b) {
        (None, range) | (range, None) => range,
        (Some((a_low, a_high)), Some((b_low, b_high))) => {
            Some((a_low.min(b_low), a_high.max(b_high)))
        }
    }
}

/// The stop the rules release next, given the range traded at so far in this command: the
/// first buy stop if a trade reached its trigger, else the first sell stop if one did.
fn next_release(state: &State, traded: Option<(Price, Price)>) -> Option<StopOrder> {
    let (low, high) = traded?;
    state.stops[0]
        .first()
        .filter(|stop| stop.trigger <= high)
        .or_else(|| state.stops[1].first().filter(|stop| stop.trigger >= low))
        .copied()
}

fn check(
    cfg: &BookConfig,
    before: &State,
    command: Command,
    events: &[Event],
    book: &OrderBook,
) -> Check {
    book.validate()?;
    let after = State::capture(book);
    ensure(
        after.order_count() == book.order_count(),
        "order_count disagrees with the book's contents",
    )?;
    let first = *events.first().ok_or("no events")?;
    // No command may run away. Each execution, the command's own and each stop it releases,
    // yields at most a trade and a new tranche per tranche of every resting order it reaches,
    // or one cancel, plus a few events of its own.
    let per_order = 2 * cfg.max_iceberg_tranches.max(1) as usize;
    let executions = 1 + before.stop_count();
    ensure(
        events.len() <= executions * (4 + per_order * before.order_count()),
        format!(
            "{} events from a book of {} orders",
            events.len(),
            before.order_count()
        ),
    )?;
    // The band's reference is the last trade's price, whatever the command.
    let last_trade = events.iter().rev().find_map(|event| match event {
        Event::Trade { price, .. } => Some(*price),
        _ => None,
    });
    ensure(
        after.reference == last_trade.or(before.reference),
        "the reference price must be the last trade's",
    )?;
    let required = required_rejection(cfg, before, command);

    if let Event::Rejected { id, reason } = first {
        ensure(events.len() == 1, "a rejection must be the only event")?;
        ensure(Some(id) == command.id(), "rejection carries the wrong id")?;
        ensure(after == *before, "a rejected command changed the book")?;
        return ensure(
            required == Some(reason),
            format!("rejected as {reason:?}, but the rules require {required:?}"),
        );
    }
    if let Some(reason) = required {
        return Err(format!("accepted, but the rules require {reason:?}"));
    }

    let (mut state, mut used) = command_effect(cfg, before, command, events)?;

    // Then the stops its trades reached, one at a time, in the order the rules release them.
    let mut traded = trade_range(&events[..used]);
    while let Some(stop) = next_release(&state, traded) {
        ensure(
            events.get(used) == Some(&Event::Triggered { id: stop.id }),
            format!("stop #{} must trigger next", stop.id),
        )?;
        used += 1;
        state = state.without(stop.id);
        let (next, n) = release(cfg, &state, stop, &events[used..])?;
        traded = widen(traded, trade_range(&events[used..used + n]));
        state = next;
        used += n;
    }
    ensure(
        used == events.len(),
        format!("events the rules do not account for: {:?}", &events[used..]),
    )?;
    ensure(after == state, "the book is not what the rules make of it")
}

/// The effect of the command itself, before any stop it triggers.
fn command_effect(cfg: &BookConfig, before: &State, command: Command, events: &[Event]) -> Effect {
    let first = events[0];
    match command {
        Command::Cancel { id, owner } => {
            let (order_owner, qty) = match before.find(id) {
                Some((_, _, order)) => (order.owner, order.leaves),
                None => {
                    let stop = before
                        .find_stop(id)
                        .ok_or("cancelled an order that was not resting")?;
                    (stop.owner, stop.qty)
                }
            };
            ensure(order_owner == owner, "cancelled another owner's order")?;
            ensure(
                first
                    == Event::Cancelled {
                        id,
                        qty,
                        reason: Requested,
                    },
                "cancel must emit Cancelled with the open quantity",
            )?;
            Ok((before.without(id), 1))
        }
        Command::Limit {
            id,
            owner,
            side,
            price,
            qty,
            tif,
            display,
        } => {
            ensure(first == Event::Accepted { id }, "must start with Accepted")?;
            let taker = Taker {
                id,
                owner,
                side,
                limit: Some(price),
                cap: None,
                stop: None,
                tif: Some(tif),
                post_only: tif == TimeInForce::PostOnly,
                display,
                qty,
                filled_before: 0,
            };
            let (state, n) = check_execution(cfg, before, taker, &events[1..])?;
            Ok((state, n + 1))
        }
        Command::Market {
            id,
            owner,
            side,
            qty,
        } => {
            ensure(first == Event::Accepted { id }, "must start with Accepted")?;
            let (state, n) = check_execution(
                cfg,
                before,
                market_taker(cfg, before, id, owner, side, qty),
                &events[1..],
            )?;
            Ok((state, n + 1))
        }
        Command::Stop {
            id,
            owner,
            side,
            trigger,
            limit,
            qty,
        } => {
            let placed = [
                Event::Accepted { id },
                Event::StopPlaced {
                    id,
                    side,
                    trigger,
                    limit,
                    qty,
                },
            ];
            ensure(
                events.get(..2) == Some(&placed[..]),
                "a stop is accepted and placed, and does nothing else",
            )?;
            let stop = StopOrder {
                id,
                owner,
                side,
                trigger,
                limit,
                qty,
            };
            Ok((before.with_stop(stop), 2))
        }
        Command::Modify {
            id,
            owner,
            price,
            qty,
        } => {
            let (side, old_price, order) = before
                .find(id)
                .ok_or("modified an order that was not resting")?;
            ensure(order.owner == owner, "modified another owner's order")?;
            let leaves = qty.saturating_sub(order.filled);
            ensure(
                first
                    == Event::Modified {
                        id,
                        price,
                        qty,
                        leaves,
                    },
                "Modified must report the new total and new total minus filled as leaves",
            )?;
            if leaves == 0 {
                return Ok((before.without(id), 1));
            }
            if price == old_price && leaves <= order.leaves {
                let mut state = before.clone();
                for (_, queue) in state.snapshot.iter_mut().flatten() {
                    for o in queue.iter_mut().filter(|o| o.id == id) {
                        o.leaves = leaves;
                        o.visible = o.visible.min(leaves);
                    }
                }
                return Ok((state, 1));
            }
            // Lost priority: the same as a new GTC order for `leaves` arriving at a book
            // without the old order, keeping its post-only restriction and display.
            let taker = Taker {
                id,
                owner,
                side,
                limit: Some(price),
                cap: None,
                stop: None,
                tif: Some(TimeInForce::Gtc),
                post_only: order.post_only,
                display: order.display,
                qty: leaves,
                filled_before: order.filled,
            };
            let (state, n) = check_execution(cfg, &before.without(id), taker, &events[1..])?;
            Ok((state, n + 1))
        }
        Command::CancelAll { owner } => {
            // Exactly the owner's orders in book order, then its stops in trigger order, then
            // the count.
            let orders = [Side::Buy, Side::Sell]
                .into_iter()
                .flat_map(|side| before.priority(side))
                .map(|(_, order)| (order.id, order.owner, order.leaves));
            let stops = before
                .stops
                .iter()
                .flatten()
                .map(|stop| (stop.id, stop.owner, stop.qty));
            let mine: Vec<(OrderId, Qty)> = orders
                .chain(stops)
                .filter(|(_, o, _)| *o == owner)
                .map(|(id, _, qty)| (id, qty))
                .collect();
            let mut expected: Vec<Event> = mine
                .iter()
                .map(|&(id, qty)| Event::Cancelled {
                    id,
                    qty,
                    reason: MassCancel,
                })
                .collect();
            expected.push(Event::MassCancelled {
                owner,
                count: mine.len() as u32,
            });
            ensure(
                events.get(..expected.len()) == Some(&expected[..]),
                "a mass cancel must cancel exactly the owner's orders and stops, in book order, then report the count",
            )?;
            let state = mine
                .iter()
                .fold(before.clone(), |state, &(id, _)| state.without(id));
            Ok((state, expected.len()))
        }
    }
}

fn market_taker(
    cfg: &BookConfig,
    state: &State,
    id: OrderId,
    owner: OwnerId,
    side: Side,
    qty: Qty,
) -> Taker {
    let (cap, stop) = market_cap(cfg, state, side);
    Taker {
        id,
        owner,
        side,
        limit: None,
        cap,
        stop,
        tif: None,
        post_only: false,
        display: None,
        qty,
        filled_before: 0,
    }
}

/// A released stop: it becomes a market order, or a GTC limit order that must first pass
/// the price controls as they stand now, and is cancelled if it does not.
fn release(cfg: &BookConfig, state: &State, stop: StopOrder, events: &[Event]) -> Effect {
    let StopOrder {
        id,
        owner,
        side,
        qty,
        ..
    } = stop;
    let Some(limit) = stop.limit else {
        return check_execution(
            cfg,
            state,
            market_taker(cfg, state, id, owner, side, qty),
            events,
        );
    };
    let refused = if beyond_protection(cfg, state, side, limit) {
        Some(PriceProtection)
    } else if beyond_band(cfg, state, side, limit) {
        Some(PriceBand)
    } else {
        None
    };
    if let Some(reason) = refused {
        ensure(
            events.first() == Some(&Event::Cancelled { id, qty, reason }),
            "a triggered stop-limit outside the price controls must be cancelled",
        )?;
        return Ok((state.clone(), 1));
    }
    let taker = Taker {
        id,
        owner,
        side,
        limit: Some(limit),
        cap: None,
        stop: None,
        tif: Some(TimeInForce::Gtc),
        post_only: false,
        display: None,
        qty,
        filled_before: 0,
    };
    check_execution(cfg, state, taker, events)
}

/// The furthest price a market order may trade at, and the reason it gives if it stops
/// there: the tighter of price protection (from the opposite best) and the price band (from
/// the reference price); price protection when they are equal.
fn market_cap(
    cfg: &BookConfig,
    state: &State,
    side: Side,
) -> (Option<Price>, Option<CancelReason>) {
    let through = |from: Price, ticks: u32| match side {
        Side::Buy => from + i64::from(ticks),
        Side::Sell => from - i64::from(ticks),
    };
    let protection = cfg
        .price_protection
        .zip(state.best(side.opposite()))
        .map(|(ticks, best)| through(best, ticks));
    let band = cfg
        .price_band
        .zip(state.reference)
        .map(|(ticks, reference)| through(reference, ticks));
    match (protection, band) {
        (None, None) => (None, None),
        (Some(p), None) => (Some(p), Some(PriceProtection)),
        (None, Some(b)) => (Some(b), Some(PriceBand)),
        (Some(p), Some(b)) => {
            if within(side, b, p) && b != p {
                (Some(b), Some(PriceBand))
            } else {
                (Some(p), Some(PriceProtection))
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Taker {
    id: OrderId,
    owner: OwnerId,
    side: Side,
    /// Limit price; `None` for market orders.
    limit: Option<Price>,
    /// Cap of a market order: the tighter of price protection and the price band.
    cap: Option<Price>,
    /// The reason a market order gives when it stops at `cap`.
    stop: Option<CancelReason>,
    /// Time in force; `None` for market orders.
    tif: Option<TimeInForce>,
    /// A post-only order, or a modify of one: it may not trade, and rests post-only.
    post_only: bool,
    /// Iceberg display quantity the remainder rests with.
    display: Option<Qty>,
    /// Quantity to work.
    qty: Qty,
    /// Filled before this command (modifies keep their history).
    filled_before: Qty,
}

/// One side of the book, best price first, each level's queue in time priority.
type Levels = Vec<(Price, VecDeque<QueuedOrder>)>;

impl Taker {
    fn may_trade_at(&self, price: Price) -> bool {
        self.limit.is_none_or(|l| within(self.side, price, l))
            && self.cap.is_none_or(|c| within(self.side, price, c))
    }

    /// Whether matching against `levels` (the opposite side) would fill the whole quantity.
    /// Under `CancelResting` the owner's own orders would be cancelled, not traded; under
    /// `CancelIncoming` matching would stop at the first of them. An iceberg shows one
    /// tranche at a time and each new tranche goes to the back of its level, so a level
    /// cleared without meeting an own order yields its hidden quantity too, but new
    /// tranches end up behind an own order that would stop the match.
    fn could_fill(&self, cfg: &BookConfig, levels: &Levels) -> bool {
        let mut need = u128::from(self.qty);
        for (price, queue) in levels {
            if !self.may_trade_at(*price) {
                break;
            }
            let mut hidden = 0u128;
            for order in queue {
                if order.owner == self.owner {
                    if cfg.self_trade == SelfTradePolicy::CancelIncoming {
                        return false;
                    }
                    continue;
                }
                need = need.saturating_sub(u128::from(order.visible));
                if need == 0 {
                    return true;
                }
                hidden += u128::from(order.leaves - order.visible);
            }
            need = need.saturating_sub(hidden);
            if need == 0 {
                return true;
            }
        }
        false
    }

    /// What the order shows when `leaves` of it rests.
    fn shows(&self, leaves: Qty) -> Qty {
        self.display.map_or(leaves, |display| display.min(leaves))
    }
}

/// Checks the events of one order's execution, from its first trade to its resting or
/// cancelled remainder, and works out the state it leaves.
///
/// The events are replayed against a copy of the opposite side, each checked against the
/// rules as it comes: the next order in priority, at its price, within the limit, never the
/// same owner, as much as it shows. An iceberg whose tranche runs out must show its next one
/// at once, at the back of its level, which is where the copy moves it too.
fn check_execution(cfg: &BookConfig, before: &State, taker: Taker, events: &[Event]) -> Effect {
    let opposite = taker.side.opposite();
    let mut levels: Levels = before.snapshot[State::side_index(opposite)]
        .iter()
        .map(|(price, queue)| (*price, queue.iter().copied().collect()))
        .collect();
    if taker.tif == Some(TimeInForce::Fok) && !taker.could_fill(cfg, &levels) {
        ensure(
            events.first()
                == Some(&Event::Cancelled {
                    id: taker.id,
                    qty: taker.qty,
                    reason: FillOrKill,
                }),
            "a fill-or-kill order the book cannot fill must be killed without trading",
        )?;
        return Ok((before.clone(), 1));
    }

    let mut left = taker.qty;
    let mut next_trade_id = before.trade_count + 1;
    let mut last_price = None;
    // The level being worked, always one with orders unless the side is exhausted.
    let mut current = 0usize;
    let mut i = 0;
    loop {
        while levels
            .get(current)
            .is_some_and(|(_, queue)| queue.is_empty())
        {
            current += 1;
        }
        let Some(&event) = events.get(i) else {
            break;
        };
        match event {
            Event::Trade {
                trade_id,
                taker: t,
                maker,
                taker_side,
                price,
                qty,
                taker_leaves,
                maker_leaves,
            } => {
                ensure(
                    t == taker.id && taker_side == taker.side,
                    "trade names the wrong taker",
                )?;
                ensure(!taker.post_only, "a post-only order traded")?;
                ensure(trade_id == next_trade_id, "trade ids must be consecutive")?;
                next_trade_id += 1;
                last_price = Some(price);
                let (maker_price, queue) = levels
                    .get_mut(current)
                    .ok_or("traded although no resting order was left")?;
                let m = queue.front_mut().expect("empty levels are skipped");
                ensure(
                    m.id == maker,
                    format!(
                        "price-time priority: traded with #{maker}, next in line was #{}",
                        m.id
                    ),
                )?;
                ensure(
                    price == *maker_price,
                    "trade price must be the maker's price",
                )?;
                ensure(m.owner != taker.owner, "self-trade")?;
                ensure(
                    taker.may_trade_at(price),
                    "traded through the limit or protection cap",
                )?;
                ensure(
                    qty > 0 && qty == left.min(m.visible),
                    "fill must be as large as the maker shows and the taker needs",
                )?;
                left -= qty;
                m.leaves -= qty;
                m.visible -= qty;
                m.filled += qty;
                ensure(taker_leaves == left, "taker_leaves is wrong")?;
                ensure(maker_leaves == m.leaves, "maker_leaves is wrong")?;
                if m.leaves == 0 {
                    queue.pop_front();
                } else if m.visible == 0 {
                    let display = m.display.ok_or("a plain order showed less than it had")?;
                    let replenished = Event::Replenished {
                        id: maker,
                        side: opposite,
                        price,
                        visible: display.min(m.leaves),
                    };
                    ensure(
                        events.get(i + 1) == Some(&replenished),
                        "an iceberg's used-up tranche must be replenished at once",
                    )?;
                    i += 1;
                    let mut iceberg = queue.pop_front().expect("it was the front");
                    iceberg.visible = display.min(iceberg.leaves);
                    queue.push_back(iceberg);
                } else {
                    ensure(
                        left == 0,
                        "a maker was left partially filled while the taker was not",
                    )?;
                }
            }
            Event::Cancelled {
                id,
                qty,
                reason: SelfTrade,
            } if id != taker.id => {
                ensure(
                    cfg.self_trade == SelfTradePolicy::CancelResting,
                    "resting order cancelled under CancelIncoming",
                )?;
                let (price, queue) = levels
                    .get_mut(current)
                    .ok_or("self-trade cancel with no resting order left")?;
                let m = queue.front().expect("empty levels are skipped");
                ensure(m.id == id, "self-trade cancel skipped the queue")?;
                ensure(
                    m.owner == taker.owner,
                    "self-trade cancel of another owner's order",
                )?;
                ensure(
                    qty == m.leaves,
                    "self-trade cancel must remove all open quantity",
                )?;
                ensure(
                    taker.may_trade_at(*price),
                    "self-trade cancel beyond the limit",
                )?;
                queue.pop_front();
            }
            _ => break,
        }
        i += 1;
    }

    // The next resting order the taker would reach, if it may trade with it.
    let next_reachable = levels
        .get(current)
        .filter(|(price, _)| taker.may_trade_at(*price))
        .and_then(|(_, queue)| queue.front().copied());
    let exhausted = current == levels.len();
    let mut rested = None;
    match events.get(i) {
        Some(&Event::Rested {
            id,
            side,
            price,
            qty,
            visible,
        }) if id == taker.id => {
            i += 1;
            ensure(
                side == taker.side && Some(price) == taker.limit,
                "rested with the wrong side or price",
            )?;
            ensure(
                matches!(taker.tif, Some(TimeInForce::Gtc | TimeInForce::PostOnly)),
                "only GTC and post-only orders rest",
            )?;
            ensure(
                qty == left && left > 0,
                "rested quantity must be the unfilled quantity",
            )?;
            ensure(
                visible == taker.shows(left),
                "a resting order shows its display quantity, or everything",
            )?;
            ensure(
                next_reachable.is_none(),
                "rested while it could still trade",
            )?;
            rested = Some((price, qty, visible));
        }
        Some(&Event::Cancelled { id, qty, reason }) if id == taker.id => {
            i += 1;
            ensure(
                qty == left && left > 0,
                "cancelled quantity must be the unfilled quantity",
            )?;
            match reason {
                SelfTrade => {
                    ensure(
                        cfg.self_trade == SelfTradePolicy::CancelIncoming,
                        "incoming order cancelled under CancelResting",
                    )?;
                    let m = next_reachable.ok_or("self-trade cancel with nothing to trade")?;
                    ensure(
                        m.owner == taker.owner,
                        "self-trade cancel against another owner",
                    )?;
                }
                NoLiquidity => {
                    ensure(
                        taker.limit.is_none(),
                        "only market orders run out of liquidity",
                    )?;
                    ensure(exhausted, "NoLiquidity while orders remain")?;
                }
                PriceProtection | PriceBand => {
                    ensure(
                        taker.stop == Some(reason),
                        "a market order must name the tighter of its caps",
                    )?;
                    ensure(
                        !exhausted && next_reachable.is_none(),
                        "a market order stopped although the next order is within its cap",
                    )?;
                }
                ImmediateOrCancel => {
                    ensure(
                        taker.tif == Some(TimeInForce::Ioc),
                        "only immediate-or-cancel orders expire",
                    )?;
                    ensure(
                        next_reachable.is_none(),
                        "an immediate-or-cancel order expired although it could still trade",
                    )?;
                }
                FillOrKill => {
                    return Err("a fill-or-kill order that could fill was killed".into());
                }
                Requested | MassCancel => return Err("a Cancel reason on a new order".into()),
            }
        }
        _ => ensure(left == 0, "unfilled quantity vanished")?,
    }

    // The state: `before` with the opposite side as replayed and the taker appended to its
    // level.
    let mut books = to_books(&before.snapshot);
    books[State::side_index(opposite)] = levels
        .into_iter()
        .filter(|(_, queue)| !queue.is_empty())
        .map(|(price, queue)| (price, queue.into_iter().collect()))
        .collect();
    if let Some((price, qty, visible)) = rested {
        books[State::side_index(taker.side)]
            .entry(price)
            .or_default()
            .push(QueuedOrder {
                id: taker.id,
                owner: taker.owner,
                leaves: qty,
                filled: taker.filled_before + (taker.qty - qty),
                post_only: taker.post_only,
                display: taker.display,
                visible,
            });
    }
    let state = State {
        snapshot: from_books(books),
        stops: before.stops.clone(),
        trade_count: next_trade_id - 1,
        reference: last_price.or(before.reference),
    };
    Ok((state, i))
}

/// The rejection the rules require for `command` against the book before it, or `None` if
/// the command must be accepted. When several rules apply, the first one listed for the
/// command wins: stateless checks (owner, quantity, price band) before stateful ones (ids,
/// ownership, protection, capacity). An owner outside the table owns nothing, so its
/// cancels and modifies fail as `UnknownOrder` and its mass cancels find nothing.
///
/// Checking acceptance as well as rejection matters: a command that should have been
/// refused but was not can leave a book that looks perfectly healthy, such as a limit order
/// priced through the protection band that simply trades.
fn required_rejection(cfg: &BookConfig, before: &State, command: Command) -> Option<RejectReason> {
    use RejectReason::*;
    let owner_invalid = |owner: OwnerId| owner >= cfg.max_owners;
    let covers = |display: Qty, qty: Qty| {
        u128::from(display) * u128::from(cfg.max_iceberg_tranches) >= u128::from(qty)
    };
    let qty_invalid = |qty: Qty| qty == 0 || qty > cfg.max_order_qty;
    let out_of_band = |price: Price| price < cfg.min_price || price > cfg.max_price;
    let taken = |id: OrderId| before.find(id).is_some() || before.find_stop(id).is_some();
    let full = before.order_count() == cfg.max_orders as usize;
    let owned = |id: OrderId, owner: OwnerId| before.find(id).filter(|(_, _, o)| o.owner == owner);
    let owned_stop =
        |id: OrderId, owner: OwnerId| before.find_stop(id).filter(|s| s.owner == owner);

    match command {
        Command::Limit {
            id,
            owner,
            side,
            price,
            qty,
            tif,
            display,
        } => {
            let may_rest = matches!(tif, TimeInForce::Gtc | TimeInForce::PostOnly);
            if owner_invalid(owner) {
                Some(InvalidOwner)
            } else if qty_invalid(qty) {
                Some(InvalidQuantity)
            } else if display.is_some_and(|d| d == 0 || d >= qty || !may_rest || !covers(d, qty)) {
                Some(InvalidDisplay)
            } else if out_of_band(price) {
                Some(PriceOutOfRange)
            } else if taken(id) {
                Some(DuplicateOrderId)
            } else if beyond_protection(cfg, before, side, price) {
                Some(PriceOutsideProtection)
            } else if beyond_band(cfg, before, side, price) {
                Some(PriceOutsideBand)
            } else if tif == TimeInForce::PostOnly && crosses(before, side, price) {
                Some(PostOnlyWouldCross)
            } else if may_rest && full && !crosses(before, side, price) {
                // A crossing order frees a slot by its first match, and orders that never
                // rest need none, so neither is refused.
                Some(BookFull)
            } else {
                None
            }
        }
        Command::Market { id, owner, qty, .. } => {
            if owner_invalid(owner) {
                Some(InvalidOwner)
            } else if qty_invalid(qty) {
                Some(InvalidQuantity)
            } else if taken(id) {
                Some(DuplicateOrderId)
            } else {
                None
            }
        }
        Command::Stop {
            id,
            owner,
            side,
            trigger,
            limit,
            qty,
        } => {
            if owner_invalid(owner) {
                Some(InvalidOwner)
            } else if qty_invalid(qty) {
                Some(InvalidQuantity)
            } else if out_of_band(trigger) || limit.is_some_and(out_of_band) {
                Some(PriceOutOfRange)
            } else if taken(id) {
                Some(DuplicateOrderId)
            } else if reached(before, side, trigger) {
                Some(StopWouldTrigger)
            } else if full {
                Some(BookFull)
            } else {
                None
            }
        }
        Command::Cancel { id, owner } => {
            (owned(id, owner).is_none() && owned_stop(id, owner).is_none()).then_some(UnknownOrder)
        }
        Command::Modify {
            id,
            owner,
            price,
            qty,
        } => {
            if qty_invalid(qty) {
                return Some(InvalidQuantity);
            }
            if out_of_band(price) {
                return Some(PriceOutOfRange);
            }
            if owned_stop(id, owner).is_some() {
                return Some(PendingStop);
            }
            let Some((side, old_price, order)) = owned(id, owner) else {
                return Some(UnknownOrder);
            };
            // Only the cancel/replace path re-enters the book, so only it is subject to
            // protection; ending the order or shrinking it in place never is.
            let leaves = qty.saturating_sub(order.filled);
            let in_place = price == old_price && leaves <= order.leaves;
            let replaces = leaves > 0 && !in_place;
            if replaces && beyond_protection(cfg, before, side, price) {
                Some(PriceOutsideProtection)
            } else if replaces && beyond_band(cfg, before, side, price) {
                Some(PriceOutsideBand)
            } else if replaces && order.post_only && crosses(before, side, price) {
                Some(PostOnlyWouldCross)
            } else if replaces && order.display.is_some_and(|d| !covers(d, qty)) {
                Some(InvalidDisplay)
            } else {
                None
            }
        }
        Command::CancelAll { .. } => None,
    }
}
