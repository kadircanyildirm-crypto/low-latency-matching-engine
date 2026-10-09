//! Stop orders: they wait off the book, invisible to the market, until a trade reaches their
//! trigger price, and then work as market or limit orders.
//!
//! Pending stops sit in two more ladders of the same shape as the book's sides, keyed by
//! trigger level. Buy stops run like asks, lowest trigger first, because rising prices reach
//! the lowest first; sell stops run like bids. A pending stop holds a pool slot and counts
//! against `max_orders`, and it is in the id index and its owner's list, so cancels and mass
//! cancels find it.
//!
//! Which stops trigger: every stop whose trigger a trade of the current command reached, at
//! any price that command traded at, not only the last. A released stop trades too, which can
//! reach more triggers, so release repeats until none is left. When both sides have stops to
//! release, buy stops go first; within a side, the trigger prices reached first go first,
//! and at one trigger the oldest stop.
//!
//! The trades of an uncross reach triggers like any others, and the stops they reach are
//! released once the new phase is in force. A released stop becomes the order it was
//! waiting to become, under that phase's rules: in continuous trading it trades; in a call
//! phase a stop-limit rests without trading; anything else cannot work and is cancelled. A
//! halt or the close leaves pending stops pending: nothing trades, so nothing triggers.

use super::{HalfBook, Halt, OrderBook};
use crate::pool::{NIL, OrderKind, OrderNode, OrderPool};
use crate::types::{
    CancelReason, Event, EventSink, OrderId, OwnerId, Phase, Price, Qty, RejectReason, Side,
};

/// A pending stop order as seen from outside the book.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StopOrder {
    /// Order id.
    pub id: OrderId,
    /// Owner of the order.
    pub owner: OwnerId,
    /// Side of the order it becomes.
    pub side: Side,
    /// Trigger price.
    pub trigger: Price,
    /// Limit price of a stop-limit order; `None` for a stop-market order.
    pub limit: Option<Price>,
    /// Quantity.
    pub qty: Qty,
}

/// Iterator over one side's pending stops in trigger order, as yielded by
/// [`OrderBook::stops`].
pub struct Stops<'a> {
    pool: &'a OrderPool,
    ladder: &'a HalfBook,
    min_price: Price,
    /// Level whose queue comes next.
    level: Option<u32>,
    /// Next stop in the current level's queue.
    slot: u32,
}

impl Iterator for Stops<'_> {
    type Item = StopOrder;

    fn next(&mut self) -> Option<StopOrder> {
        while self.slot == NIL {
            let level = self.level?;
            self.slot = self.ladder.levels[level as usize].head;
            self.level = self.ladder.after(level);
        }
        let node = self.pool.get(self.slot);
        self.slot = node.next;
        Some(stop_order(node, self.min_price))
    }
}

fn stop_order(node: &OrderNode, min_price: Price) -> StopOrder {
    StopOrder {
        id: node.id,
        owner: node.owner,
        side: node.side,
        trigger: min_price + Price::from(node.level),
        limit: (node.kind == OrderKind::StopLimit).then(|| min_price + Price::from(node.limit)),
        qty: node.remaining,
    }
}

impl OrderBook {
    /// A pending stop, if `id` is one.
    pub fn stop(&self, id: OrderId) -> Option<StopOrder> {
        let node = self.pool.get(*self.index.get(&id)?);
        node.is_stop()
            .then(|| stop_order(node, self.config.min_price))
    }

    /// One side's pending stops in trigger order: buy stops lowest trigger first, sell stops
    /// highest first, each trigger level oldest first.
    pub fn stops(&self, side: Side) -> Stops<'_> {
        let ladder = self.stop_ladder(side);
        Stops {
            pool: &self.pool,
            ladder,
            min_price: self.config.min_price,
            level: ladder.best,
            slot: NIL,
        }
    }

    fn stop_ladder(&self, side: Side) -> &HalfBook {
        match side {
            Side::Buy => &self.buy_stops,
            Side::Sell => &self.sell_stops,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_stop<S: EventSink>(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        trigger: Price,
        limit: Option<Price>,
        qty: Qty,
        sink: &mut S,
    ) -> Result<(), RejectReason> {
        self.check_owner(owner)?;
        self.check_qty(qty)?;
        let trigger_level = self
            .level_of(trigger)
            .ok_or(RejectReason::PriceOutOfRange)?;
        let limit_level = match limit {
            None => None,
            Some(price) => Some(self.level_of(price).ok_or(RejectReason::PriceOutOfRange)?),
        };
        self.check_phase(false)?;
        if self.index.contains_key(&id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if self.reached(side, trigger_level) {
            return Err(RejectReason::StopWouldTrigger);
        }
        if self.pool.is_full() {
            return Err(RejectReason::BookFull);
        }
        sink.on_event(Event::Accepted { id });
        let slot = self.pool.alloc(OrderNode::stop(
            id,
            owner,
            side,
            trigger_level,
            limit_level,
            qty,
        ));
        self.place(slot);
        sink.on_event(Event::StopPlaced {
            id,
            side,
            trigger,
            limit,
            qty,
        });
        Ok(())
    }

    /// Whether the last trade price has already reached a `side` stop's trigger level.
    pub(super) fn reached(&self, side: Side, trigger: u32) -> bool {
        self.reference.is_some_and(|last| match side {
            Side::Buy => trigger <= last,
            Side::Sell => trigger >= last,
        })
    }

    /// Releases, one at a time, every stop the current command's trades reached, including
    /// those reached by the trades of stops released before it.
    pub(super) fn release_stops<S: EventSink>(&mut self, sink: &mut S) {
        while let Some((low, high)) = self.traded {
            let slot = if let Some(level) = self.buy_stops.best.filter(|&level| level <= high) {
                self.buy_stops.levels[level as usize].head
            } else if let Some(level) = self.sell_stops.best.filter(|&level| level >= low) {
                self.sell_stops.levels[level as usize].head
            } else {
                return;
            };
            self.trigger(slot, sink);
        }
    }

    /// Turns the pending stop in `slot` into the order it was waiting to become, under the
    /// current phase's rules. A stop-limit that rests keeps its slot, so it never needs a
    /// free one.
    fn trigger<S: EventSink>(&mut self, slot: u32, sink: &mut S) {
        let node = *self.pool.get(slot);
        let (ladder, pool) = self.ladder_and_pool(slot);
        ladder.unlink(pool, slot);
        let OrderNode {
            id,
            owner,
            side,
            remaining: qty,
            ..
        } = node;
        sink.on_event(Event::Triggered { id });

        match (self.phase, node.kind) {
            (Phase::Continuous, OrderKind::StopMarket) => {
                self.execute_market(id, owner, side, qty, sink);
                self.retire(slot);
                return;
            }
            (Phase::Continuous, _) => {}
            // A call phase collects limit orders without matching or price controls.
            (Phase::Auction, OrderKind::StopLimit) => {
                return self.rest_stop(slot, node.limit, qty, sink);
            }
            // A market order cannot work outside continuous trading, nor can anything
            // while trading is halted or closed. The stop was accepted long ago, so it is
            // cancelled rather than rejected.
            _ => {
                self.retire(slot);
                sink.on_event(Event::Cancelled {
                    id,
                    qty,
                    reason: CancelReason::TradingPhase,
                });
                return;
            }
        }
        // A stop-limit is checked against the price controls when it triggers, as a new
        // limit order would be; it was accepted long ago, so it is cancelled, not rejected.
        let level = node.limit;
        let refused = if self.outside_protection(side, level) {
            Some(CancelReason::PriceProtection)
        } else if self.outside_band(side, level) {
            Some(CancelReason::PriceBand)
        } else {
            None
        };
        if let Some(reason) = refused {
            self.retire(slot);
            sink.on_event(Event::Cancelled { id, qty, reason });
            return;
        }
        let (remaining, halt) = self.match_incoming(id, owner, side, qty, Some(level), sink);
        if remaining == 0 {
            self.retire(slot);
            return;
        }
        if halt == Halt::SelfTrade {
            self.retire(slot);
            sink.on_event(Event::Cancelled {
                id,
                qty: remaining,
                reason: CancelReason::SelfTrade,
            });
            return;
        }
        self.rest_stop(slot, level, remaining, sink);
    }

    /// Rests `remaining` of a triggered stop-limit as a GTC order at `level`, in the stop's
    /// own slot, at the back of its level's queue and of its owner's list.
    fn rest_stop<S: EventSink>(&mut self, slot: u32, level: u32, remaining: Qty, sink: &mut S) {
        let resting = self.pool.get_mut(slot);
        resting.kind = OrderKind::Resting;
        resting.level = level;
        resting.limit = NIL;
        resting.remaining = remaining;
        let OrderNode {
            id, owner, side, ..
        } = *resting;
        self.owners.unlink(slot, owner);
        self.place(slot);
        sink.on_event(Event::Rested {
            id,
            side,
            price: self.price_of(level),
            qty: remaining,
            visible: remaining,
        });
    }

    /// Checks both stop ladders the way `validate` checks the book's sides, and that no
    /// pending stop's trigger has been reached already. Returns how many stops there are.
    pub(super) fn validate_stops(&self) -> Result<usize, String> {
        let mut stops = 0usize;
        for side in [Side::Buy, Side::Sell] {
            let ladder = self.stop_ladder(side);
            let first = match ladder.side {
                Side::Buy => ladder.occupied.prev_at_or_before(usize::MAX),
                Side::Sell => ladder.occupied.next_at_or_after(0),
            };
            if ladder.best != first.map(|i| i as u32) {
                return Err(format!(
                    "{side:?} stops: best is {:?}, but the first occupied level is {first:?}",
                    ladder.best
                ));
            }
            let mut next = ladder.best;
            while let Some(level) = next {
                next = ladder.after(level);
                let trigger = self.price_of(level);
                let lvl = &ladder.levels[level as usize];
                if lvl.order_count == 0 || lvl.head == NIL {
                    return Err(format!(
                        "{side:?} stops {trigger}: occupied bit on an empty level"
                    ));
                }
                let (mut count, mut total, mut prev, mut cur) = (0u32, 0 as Qty, NIL, lvl.head);
                while cur != NIL {
                    let node = self.pool.get(cur);
                    let id = node.id;
                    if node.prev != prev {
                        return Err(format!(
                            "{side:?} stops {trigger}: broken back link at #{id}"
                        ));
                    }
                    if !node.is_stop() || node.side != side || node.level != level {
                        return Err(format!("{side:?} stops {trigger}: #{id} is misfiled"));
                    }
                    let limit_fits = match node.kind {
                        OrderKind::StopLimit => (node.limit as usize) < ladder.levels.len(),
                        _ => node.limit == NIL,
                    };
                    if !limit_fits {
                        return Err(format!(
                            "{side:?} stops {trigger}: #{id} has a bad limit level"
                        ));
                    }
                    if node.remaining == 0
                        || node.remaining != node.total
                        || node.total > self.config.max_order_qty
                    {
                        return Err(format!(
                            "{side:?} stops {trigger}: #{id} has quantity {} of {}",
                            node.remaining, node.total
                        ));
                    }
                    if self.index.get(&id) != Some(&cur) {
                        return Err(format!(
                            "{side:?} stops {trigger}: index disagrees on #{id}"
                        ));
                    }
                    if self.reached(side, level) {
                        return Err(format!(
                            "{side:?} stops {trigger}: #{id} should have triggered"
                        ));
                    }
                    count += 1;
                    total += node.remaining;
                    prev = cur;
                    cur = node.next;
                }
                if prev != lvl.tail {
                    return Err(format!(
                        "{side:?} stops {trigger}: tail does not point at last stop"
                    ));
                }
                if count != lvl.order_count || total != lvl.total_qty {
                    return Err(format!(
                        "{side:?} stops {trigger}: aggregates say {}/{} but queue holds {count}/{total}",
                        lvl.order_count, lvl.total_qty
                    ));
                }
                stops += count as usize;
            }
        }
        Ok(stops)
    }
}
