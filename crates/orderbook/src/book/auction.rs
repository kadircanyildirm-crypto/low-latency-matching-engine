//! Trading phases, and the uncross that ends a call phase.
//!
//! In a call phase orders rest without matching, so the book may be crossed. Leaving the
//! phase executes everything that can trade, all at one price, chosen among the candidate
//! prices (the prices of the orders on the book, and the reference price) by these rules,
//! each breaking the ties the one before leaves:
//!
//! 1. the most executable quantity: the smaller of what the bids at or above the price and
//!    the asks at or below it add up to;
//! 2. the least surplus, the quantity left on the larger side;
//! 3. market pressure: if every remaining price leaves its surplus on the buy side, the
//!    highest; if every one leaves it on the sell side, the lowest;
//! 4. the price closest to the reference price;
//! 5. the lowest.
//!
//! No other price executes more: between two neighbouring candidates both sides' sums stay
//! what they are at one of them. Hidden iceberg quantity counts, because the uncross, like
//! continuous matching, takes an iceberg one tranche at a time and cycles through them all.
//!
//! The execution follows price-time priority on both sides: the oldest order at the best
//! bid trades with the oldest order at the best ask, at the auction price, as much as both
//! show, until one side has nothing left at or through that price. That side is filled
//! completely, the other in priority order, and the last order it reaches may be filled in
//! part. At the volume-maximizing price this always leaves the book uncrossed.
//!
//! Self-trade prevention does not apply to the uncross. Cancelling an order that crosses the
//! auction price would leave crossed orders behind at some other price, which a single
//! price cannot clear, so an owner whose buy and sell orders both cross the auction price
//! trades with itself there.

use super::{HalfBook, OrderBook};
use crate::index::IdIndex;
use crate::owners::Owners;
use crate::pool::{NIL, OrderPool};
use crate::types::{Event, EventSink, Phase, Price, Qty, Side};

/// The candidates that tie under rules 1 and 2 so far, offered in ascending price order.
struct Choice {
    volume: Qty,
    surplus: Qty,
    /// Whether some of them leave a surplus of bids, and whether some leave one of asks.
    buy_surplus: bool,
    sell_surplus: bool,
    lowest: u32,
    highest: u32,
}

impl Choice {
    fn new() -> Self {
        Self {
            volume: 0,
            surplus: 0,
            buy_surplus: false,
            sell_surplus: false,
            lowest: 0,
            highest: 0,
        }
    }

    /// Considers the candidate `level`, at which `demand` lots are bid at or above and
    /// `supply` lots offered at or below. Candidates arrive in ascending order.
    fn offer(&mut self, level: u32, demand: Qty, supply: Qty) {
        let volume = demand.min(supply);
        let surplus = demand.abs_diff(supply);
        if volume < self.volume || (volume == self.volume && surplus > self.surplus) {
            return;
        }
        if volume > self.volume || surplus < self.surplus {
            *self = Self {
                volume,
                surplus,
                lowest: level,
                ..Self::new()
            };
        }
        self.buy_surplus |= demand > supply;
        self.sell_surplus |= supply > demand;
        self.highest = level;
    }

    /// The auction price by rules 3 to 5.
    ///
    /// Rule 4 needs no search. The reference price is a candidate itself, and when it lies
    /// between two tied prices it ties with them: the bids at or above a price only shrink
    /// as the price rises and the asks at or below it only grow, so both sums at the
    /// reference lie between their values at the two, and so do volume and surplus. The
    /// closest tied price is therefore the reference clamped to the tied range, and two
    /// tied prices are never equally close to it.
    fn price(&self, reference: Option<u32>) -> u32 {
        match (self.buy_surplus, self.sell_surplus) {
            (true, false) => self.highest,
            (false, true) => self.lowest,
            _ => reference.map_or(self.lowest, |reference| {
                reference.clamp(self.lowest, self.highest)
            }),
        }
    }
}

impl OrderBook {
    /// The price and quantity the book would uncross at if its call phase ended now, or
    /// `None` if nothing would trade. Market data publishes this during a call. Outside a
    /// call phase the book is never crossed, so it is always `None` there.
    pub fn indicative_uncross(&self) -> Option<(Price, Qty)> {
        self.uncross_level()
            .map(|(level, volume)| (self.price_of(level), volume))
    }

    /// Moves the book to `phase`. Leaving a call phase uncrosses the book first; only a
    /// call phase can leave the book crossed, so for any other phase this does nothing.
    ///
    /// Phase changes are rare, and kept out of line so that the code of the uncross does
    /// not weigh on the commands of continuous trading.
    #[cold]
    #[inline(never)]
    pub(super) fn set_phase<S: EventSink>(&mut self, phase: Phase, sink: &mut S) {
        if phase != Phase::Auction {
            self.uncross(sink);
        }
        self.phase = phase;
        sink.on_event(Event::PhaseChanged { phase });
    }

    /// Executes everything that can trade at the auction price, in price-time priority on
    /// both sides. The trades set the reference price and trigger stops like any others.
    fn uncross<S: EventSink>(&mut self, sink: &mut S) {
        let Some((level, _)) = self.uncross_level() else {
            return;
        };
        let price = self.price_of(level);
        self.reference = Some(level);
        self.traded = Some((level, level));
        let Self {
            bids,
            asks,
            pool,
            index,
            owners,
            next_trade_id,
            ..
        } = self;
        while let (Some(bid), Some(ask)) = (bids.best, asks.best) {
            if bid < level || ask > level {
                break;
            }
            let buy = bids.levels[bid as usize].head;
            let sell = asks.levels[ask as usize].head;
            let (fill, buy_leaves) = pool.fill(buy, pool.visible(sell));
            let (_, sell_leaves) = pool.fill(sell, fill);
            let trade_id = *next_trade_id;
            *next_trade_id += 1;
            sink.on_event(Event::Trade {
                trade_id,
                taker: pool.get(buy).id,
                maker: pool.get(sell).id,
                taker_side: Side::Buy,
                price,
                qty: fill,
                taker_leaves: buy_leaves,
                maker_leaves: sell_leaves,
            });
            bids.levels[bid as usize].total_qty -= fill;
            asks.levels[ask as usize].total_qty -= fill;
            bids.settle_head(bid, price, pool, index, owners, sink);
            asks.settle_head(ask, price, pool, index, owners, sink);
        }
    }

    /// The auction price's level and the quantity it executes, if the book is crossed.
    ///
    /// Walks the candidates in ascending order from the best ask to the best bid, the only
    /// prices at which anything executes, keeping running sums of the asks at or below and
    /// the bids at or above the candidate. It costs O(1) per occupied level in that range,
    /// plus the orders of the levels where icebergs rest, and allocates nothing.
    fn uncross_level(&self) -> Option<(u32, Qty)> {
        let (bid, ask) = (self.bids.best?, self.asks.best?);
        if bid < ask {
            return None;
        }
        let mut demand: Qty = 0;
        let mut next = Some(bid);
        while let Some(level) = next.filter(|&level| level >= ask) {
            demand += self.leaves_at(&self.bids, level);
            next = self.bids.after(level);
        }
        let (mut supply, mut choice, mut level) = (0, Choice::new(), ask);
        loop {
            supply += self.leaves_at(&self.asks, level);
            // Every candidate from the best ask to the best bid executes something, since
            // both best levels take part; past the best bid nothing would.
            debug_assert!(
                demand > 0 && supply > 0,
                "candidate {level} executes nothing"
            );
            choice.offer(level, demand, supply);
            demand -= self.leaves_at(&self.bids, level);
            let above = level as usize + 1;
            let next = [
                self.asks.occupied.next_at_or_after(above),
                self.bids.occupied.next_at_or_after(above),
                self.reference
                    .map(|reference| reference as usize)
                    .filter(|&reference| reference >= above),
            ]
            .into_iter()
            .flatten()
            .min();
            match next {
                Some(next) if next <= bid as usize => level = next as u32,
                _ => break,
            }
        }
        Some((choice.price(self.reference), choice.volume))
    }

    /// Open quantity of the orders at `level` of `half`, hidden iceberg quantity included:
    /// what they show, unless icebergs rest there and their queue must be summed.
    fn leaves_at(&self, half: &HalfBook, level: u32) -> Qty {
        let lvl = &half.levels[level as usize];
        if lvl.icebergs == 0 {
            return lvl.total_qty;
        }
        let (mut leaves, mut slot) = (0, lvl.head);
        while slot != NIL {
            let node = self.pool.get(slot);
            leaves += node.remaining;
            slot = node.next;
        }
        leaves
    }
}

impl HalfBook {
    /// After the order at the head of `level` traded in an uncross: takes it off the book if
    /// it is filled, or shows an iceberg's next tranche at the back of the queue and of its
    /// owner's list, as continuous matching does.
    fn settle_head<S: EventSink>(
        &mut self,
        level: u32,
        price: Price,
        pool: &mut OrderPool,
        index: &mut IdIndex,
        owners: &mut Owners,
        sink: &mut S,
    ) {
        let lvl = &mut self.levels[level as usize];
        let head = lvl.head;
        let node = *pool.get(head);
        if node.remaining == 0 {
            lvl.pop_front(pool, index, owners);
            if lvl.head == NIL {
                self.level_emptied(level);
            }
        } else if pool.visible(head) == 0 {
            let visible = pool.replenish(head);
            lvl.total_qty += visible;
            lvl.requeue_head(pool);
            owners.unlink(head, node.owner);
            owners.link(head, node.owner);
            sink.on_event(Event::Replenished {
                id: node.id,
                side: node.side,
                price,
                visible,
            });
        }
    }
}
