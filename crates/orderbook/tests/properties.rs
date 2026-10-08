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
//! it, and post-only orders never trade.

mod common;

use std::collections::{BTreeMap, HashMap};

use common::strategies::scenario;
use common::{Snapshot, snapshot};
use orderbook::CancelReason::{
    FillOrKill, ImmediateOrCancel, MassCancel, NoLiquidity, PriceProtection, Requested, SelfTrade,
};
use orderbook::{
    BookConfig, Command, Event, OrderBook, OrderId, OwnerId, Price, Qty, QueuedOrder, RejectReason,
    SelfTradePolicy, Side, TimeInForce,
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

fn ensure(condition: bool, message: impl Into<String>) -> Check {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

#[derive(Clone)]
struct State {
    snapshot: Snapshot,
    trade_count: u64,
}

impl State {
    fn capture(book: &OrderBook) -> Self {
        Self {
            snapshot: snapshot(book),
            trade_count: book.trade_count(),
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

    fn order_count(&self) -> usize {
        self.snapshot
            .iter()
            .flatten()
            .map(|(_, queue)| queue.len())
            .sum()
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

    /// Resting orders of `side` in the order an incoming order must reach them: best price
    /// first, then time priority.
    fn priority(&self, side: Side) -> Vec<(Price, QueuedOrder)> {
        self.snapshot[Self::side_index(side)]
            .iter()
            .flat_map(|(price, queue)| queue.iter().map(move |o| (*price, *o)))
            .collect()
    }

    fn without(&self, id: OrderId) -> State {
        let mut books = to_books(&self.snapshot);
        for book in &mut books {
            for queue in book.values_mut() {
                queue.retain(|o| o.id != id);
            }
            book.retain(|_, queue| !queue.is_empty());
        }
        State {
            snapshot: from_books(books),
            trade_count: self.trade_count,
        }
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
    let required = required_rejection(cfg, before, command);

    if let Event::Rejected { id, reason } = first {
        ensure(events.len() == 1, "a rejection must be the only event")?;
        ensure(Some(id) == command.id(), "rejection carries the wrong id")?;
        ensure(
            after.snapshot == before.snapshot && after.trade_count == before.trade_count,
            "a rejected command changed the book",
        )?;
        return ensure(
            required == Some(reason),
            format!("rejected as {reason:?}, but the rules require {required:?}"),
        );
    }
    if let Some(reason) = required {
        return Err(format!("accepted, but the rules require {reason:?}"));
    }

    match command {
        Command::Cancel { id, owner } => {
            let (_, _, order) = before
                .find(id)
                .ok_or("cancelled an order that was not resting")?;
            ensure(order.owner == owner, "cancelled another owner's order")?;
            ensure(
                events
                    == [Event::Cancelled {
                        id,
                        qty: order.leaves,
                        reason: Requested,
                    }],
                "cancel must emit exactly Cancelled with the open quantity",
            )?;
            ensure(
                after.snapshot == before.without(id).snapshot,
                "cancel changed more than the cancelled order",
            )
        }
        Command::Limit {
            id,
            owner,
            side,
            price,
            qty,
            tif,
        } => {
            ensure(first == Event::Accepted { id }, "must start with Accepted")?;
            let taker = Taker {
                id,
                owner,
                side,
                limit: Some(price),
                cap: None,
                tif: Some(tif),
                post_only: tif == TimeInForce::PostOnly,
                qty,
                filled_before: 0,
            };
            check_execution(cfg, before, &after, taker, &events[1..])
        }
        Command::Market {
            id,
            owner,
            side,
            qty,
        } => {
            ensure(first == Event::Accepted { id }, "must start with Accepted")?;
            let taker = Taker {
                id,
                owner,
                side,
                limit: None,
                cap: protection_cap(cfg, before, side),
                tif: None,
                post_only: false,
                qty,
                filled_before: 0,
            };
            check_execution(cfg, before, &after, taker, &events[1..])
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
                ensure(events.len() == 1, "a completed order emits nothing else")?;
                return ensure(
                    after.snapshot == before.without(id).snapshot,
                    "an order modified below its filled quantity must disappear",
                );
            }
            if price == old_price && leaves <= order.leaves {
                ensure(events.len() == 1, "an in-place modify emits nothing else")?;
                let mut expected = before.snapshot.clone();
                for (_, queue) in expected.iter_mut().flatten() {
                    for o in queue.iter_mut().filter(|o| o.id == id) {
                        o.leaves = leaves;
                    }
                }
                return ensure(
                    after.snapshot == expected,
                    "an in-place modify must keep queue position and change only leaves",
                );
            }
            // Lost priority: the same as a new GTC order for `leaves` arriving at a book
            // without the old order, keeping its post-only restriction.
            let taker = Taker {
                id,
                owner,
                side,
                limit: Some(price),
                cap: None,
                tif: Some(TimeInForce::Gtc),
                post_only: order.post_only,
                qty: leaves,
                filled_before: order.filled,
            };
            check_execution(cfg, &before.without(id), &after, taker, &events[1..])
        }
        Command::CancelAll { owner } => {
            // Exactly the owner's orders, in book order, then the count; nothing else moves.
            let mine: Vec<QueuedOrder> = [Side::Buy, Side::Sell]
                .into_iter()
                .flat_map(|side| before.priority(side))
                .map(|(_, order)| order)
                .filter(|order| order.owner == owner)
                .collect();
            let mut expected: Vec<Event> = mine
                .iter()
                .map(|order| Event::Cancelled {
                    id: order.id,
                    qty: order.leaves,
                    reason: MassCancel,
                })
                .collect();
            expected.push(Event::MassCancelled {
                owner,
                count: mine.len() as u32,
            });
            ensure(
                events == expected,
                "a mass cancel must cancel exactly the owner's orders, in book order, then report the count",
            )?;
            let rest = mine
                .iter()
                .fold(before.clone(), |state, order| state.without(order.id));
            ensure(
                after.snapshot == rest.snapshot,
                "a mass cancel changed more than the owner's orders",
            )
        }
    }
}

fn protection_cap(cfg: &BookConfig, before: &State, side: Side) -> Option<Price> {
    let ticks = i128::from(cfg.price_protection?);
    let best = i128::from(before.best(side.opposite())?);
    Some(match side {
        Side::Buy => (best + ticks).min(i128::from(cfg.max_price)) as Price,
        Side::Sell => (best - ticks).max(i128::from(cfg.min_price)) as Price,
    })
}

#[derive(Clone, Copy)]
struct Taker {
    id: OrderId,
    owner: OwnerId,
    side: Side,
    /// Limit price; `None` for market orders.
    limit: Option<Price>,
    /// Price protection cap for market orders.
    cap: Option<Price>,
    /// Time in force; `None` for market orders.
    tif: Option<TimeInForce>,
    /// A post-only order, or a modify of one: it may not trade, and rests post-only.
    post_only: bool,
    /// Quantity to work.
    qty: Qty,
    /// Filled before this command (modifies keep their history).
    filled_before: Qty,
}

impl Taker {
    fn may_trade_at(&self, price: Price) -> bool {
        self.limit.is_none_or(|l| within(self.side, price, l))
            && self.cap.is_none_or(|c| within(self.side, price, c))
    }

    /// Whether matching against `line` (the opposite side in priority order) would fill the
    /// whole quantity. Under `CancelResting` the owner's own orders would be cancelled, not
    /// traded; under `CancelIncoming` matching would stop at the first of them.
    fn could_fill(&self, cfg: &BookConfig, line: &[(Price, QueuedOrder)]) -> bool {
        let mut available = 0u128;
        for &(price, order) in line {
            if !self.may_trade_at(price) {
                break;
            }
            if order.owner == self.owner {
                if cfg.self_trade == SelfTradePolicy::CancelIncoming {
                    break;
                }
                continue;
            }
            available += u128::from(order.leaves);
        }
        available >= u128::from(self.qty)
    }
}

/// Checks the events after `Accepted` / `Modified` of an order that matches and then rests or
/// is cancelled, and that the resulting book is exactly `before` with those effects applied.
fn check_execution(
    cfg: &BookConfig,
    before: &State,
    after: &State,
    taker: Taker,
    events: &[Event],
) -> Check {
    let line = before.priority(taker.side.opposite());
    if taker.tif == Some(TimeInForce::Fok) && !taker.could_fill(cfg, &line) {
        ensure(
            events
                == [Event::Cancelled {
                    id: taker.id,
                    qty: taker.qty,
                    reason: FillOrKill,
                }],
            "a fill-or-kill order the book cannot fill must be killed without trading",
        )?;
        return ensure(
            after.snapshot == before.snapshot && after.trade_count == before.trade_count,
            "a killed fill-or-kill order changed the book",
        );
    }
    let mut next_in_line = 0usize;
    let mut left = taker.qty;
    let mut next_trade_id = before.trade_count + 1;
    // Resting orders this command reached: new (leaves, filled), or None if removed.
    let mut touched: HashMap<OrderId, Option<(Qty, Qty)>> = HashMap::new();

    let mut i = 0;
    while let Some(&event) = events.get(i) {
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
                let (maker_price, m) = *line
                    .get(next_in_line)
                    .ok_or("traded although no resting order was left")?;
                ensure(
                    m.id == maker,
                    format!(
                        "price-time priority: traded with #{maker}, next in line was #{}",
                        m.id
                    ),
                )?;
                ensure(
                    price == maker_price,
                    "trade price must be the maker's price",
                )?;
                ensure(m.owner != taker.owner, "self-trade")?;
                ensure(
                    taker.may_trade_at(price),
                    "traded through the limit or protection cap",
                )?;
                ensure(
                    qty > 0 && qty == left.min(m.leaves),
                    "fill must be as large as possible",
                )?;
                left -= qty;
                ensure(taker_leaves == left, "taker_leaves is wrong")?;
                ensure(maker_leaves == m.leaves - qty, "maker_leaves is wrong")?;
                touched.insert(
                    maker,
                    (maker_leaves > 0).then_some((maker_leaves, m.filled + qty)),
                );
                if maker_leaves > 0 {
                    ensure(
                        left == 0,
                        "a maker was left partially filled while the taker was not",
                    )?;
                } else {
                    next_in_line += 1;
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
                let (price, m) = *line
                    .get(next_in_line)
                    .ok_or("self-trade cancel with no resting order left")?;
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
                    taker.may_trade_at(price),
                    "self-trade cancel beyond the limit",
                )?;
                touched.insert(id, None);
                next_in_line += 1;
            }
            _ => break,
        }
        i += 1;
    }
    ensure(
        after.trade_count == next_trade_id - 1,
        "trade counter disagrees with the trades emitted",
    )?;

    // The order of the next resting order the taker would reach, if it may trade with it.
    let next_reachable = line
        .get(next_in_line)
        .filter(|(price, _)| taker.may_trade_at(*price))
        .map(|(_, o)| *o);
    let mut rested = None;
    match &events[i..] {
        [] => ensure(left == 0, "unfilled quantity vanished")?,
        [
            Event::Rested {
                id,
                side,
                price,
                qty,
            },
        ] => {
            ensure(
                *id == taker.id && *side == taker.side && Some(*price) == taker.limit,
                "rested with the wrong id, side or price",
            )?;
            ensure(
                matches!(taker.tif, Some(TimeInForce::Gtc | TimeInForce::PostOnly)),
                "only GTC and post-only orders rest",
            )?;
            ensure(
                *qty == left && left > 0,
                "rested quantity must be the unfilled quantity",
            )?;
            ensure(
                next_reachable.is_none(),
                "rested while it could still trade",
            )?;
            rested = Some((*price, *qty));
        }
        [Event::Cancelled { id, qty, reason }] if *id == taker.id => {
            ensure(
                *qty == left && left > 0,
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
                    ensure(
                        next_in_line == line.len(),
                        "NoLiquidity while orders remain",
                    )?;
                }
                PriceProtection => {
                    ensure(taker.cap.is_some(), "PriceProtection without protection")?;
                    ensure(
                        next_in_line < line.len() && next_reachable.is_none(),
                        "PriceProtection although the next order is within the cap",
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
        rest => return Err(format!("unexpected trailing events: {rest:?}")),
    }

    // The book must be exactly `before`, with the reached orders updated and the taker
    // appended to its level.
    let mut books = to_books(&before.snapshot);
    for book in &mut books {
        for queue in book.values_mut() {
            queue.retain_mut(|o| match touched.get(&o.id) {
                None => true,
                Some(None) => false,
                Some(Some((leaves, filled))) => {
                    o.leaves = *leaves;
                    o.filled = *filled;
                    true
                }
            });
        }
        book.retain(|_, queue| !queue.is_empty());
    }
    if let Some((price, qty)) = rested {
        books[State::side_index(taker.side)]
            .entry(price)
            .or_default()
            .push(QueuedOrder {
                id: taker.id,
                owner: taker.owner,
                leaves: qty,
                filled: taker.filled_before + (taker.qty - qty),
                post_only: taker.post_only,
            });
    }
    ensure(
        after.snapshot == from_books(books),
        "the book changed beyond the effects of this command",
    )
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
    let qty_invalid = |qty: Qty| qty == 0 || qty > cfg.max_order_qty;
    let out_of_band = |price: Price| price < cfg.min_price || price > cfg.max_price;
    let owned = |id: OrderId, owner: OwnerId| before.find(id).filter(|(_, _, o)| o.owner == owner);
    let beyond_protection = |side: Side, price: Price| {
        cfg.price_protection.is_some_and(|ticks| {
            let ticks = i128::from(ticks);
            match side {
                Side::Buy => before
                    .best(Side::Sell)
                    .is_some_and(|ask| i128::from(price) > i128::from(ask) + ticks),
                Side::Sell => before
                    .best(Side::Buy)
                    .is_some_and(|bid| i128::from(price) < i128::from(bid) - ticks),
            }
        })
    };
    let crosses = |side: Side, price: Price| match side {
        Side::Buy => before.best(Side::Sell).is_some_and(|ask| price >= ask),
        Side::Sell => before.best(Side::Buy).is_some_and(|bid| price <= bid),
    };

    match command {
        Command::Limit {
            id,
            owner,
            side,
            price,
            qty,
            tif,
        } => {
            let may_rest = matches!(tif, TimeInForce::Gtc | TimeInForce::PostOnly);
            let full = before.order_count() == cfg.max_orders as usize;
            if owner_invalid(owner) {
                Some(InvalidOwner)
            } else if qty_invalid(qty) {
                Some(InvalidQuantity)
            } else if out_of_band(price) {
                Some(PriceOutOfRange)
            } else if before.find(id).is_some() {
                Some(DuplicateOrderId)
            } else if beyond_protection(side, price) {
                Some(PriceOutsideProtection)
            } else if tif == TimeInForce::PostOnly && crosses(side, price) {
                Some(PostOnlyWouldCross)
            } else if may_rest && full && !crosses(side, price) {
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
            } else if before.find(id).is_some() {
                Some(DuplicateOrderId)
            } else {
                None
            }
        }
        Command::Cancel { id, owner } => owned(id, owner).is_none().then_some(UnknownOrder),
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
            let Some((side, old_price, order)) = owned(id, owner) else {
                return Some(UnknownOrder);
            };
            // Only the cancel/replace path re-enters the book, so only it is subject to
            // protection; ending the order or shrinking it in place never is.
            let leaves = qty.saturating_sub(order.filled);
            let in_place = price == old_price && leaves <= order.leaves;
            let replaces = leaves > 0 && !in_place;
            if replaces && beyond_protection(side, price) {
                Some(PriceOutsideProtection)
            } else if replaces && order.post_only && crosses(side, price) {
                Some(PostOnlyWouldCross)
            } else {
                None
            }
        }
        Command::CancelAll { .. } => None,
    }
}
