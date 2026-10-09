//! Deliberately naive order book used as a test oracle.
//!
//! It shares no code or data structures with the real engine: `BTreeMap` of price ->
//! `VecDeque` of orders, prices instead of level indices, linear searches, allocation
//! everywhere. It is short enough to check by reading, and the property tests require the
//! engine to produce exactly the same events and the same book.

use std::collections::{BTreeMap, HashMap, VecDeque};

use orderbook::{
    BookConfig, BookSnapshot, CancelReason, Command, Event, OrderId, OwnerId, Price, Qty,
    QueuedOrder, RejectReason, SelfTradePolicy, Side, StopOrder, TimeInForce,
};

use super::Snapshot;

#[derive(Clone, Copy, Debug)]
struct Order {
    id: OrderId,
    owner: OwnerId,
    leaves: Qty,
    total: Qty,
    post_only: bool,
    /// Iceberg display quantity.
    display: Option<Qty>,
    /// What the order shows: `leaves` for a plain order, the tranche left for an iceberg.
    visible: Qty,
}

type Ladder = BTreeMap<Price, VecDeque<Order>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Halt {
    Filled,
    Empty,
    Limit,
    SelfTrade,
}

#[derive(Clone)]
pub struct ReferenceBook {
    cfg: BookConfig,
    bids: Ladder,
    asks: Ladder,
    orders: HashMap<OrderId, (Side, Price)>,
    trades: u64,
    /// Last trade price, or the configured reference before the first trade.
    reference: Option<Price>,
    /// Pending stops in arrival order; trigger order is worked out by sorting when needed.
    pending: Vec<StopOrder>,
    /// Lowest and highest price traded at during the current command.
    traded: Option<(Price, Price)>,
}

impl ReferenceBook {
    pub fn new(cfg: BookConfig) -> Self {
        Self {
            cfg,
            bids: Ladder::new(),
            asks: Ladder::new(),
            orders: HashMap::new(),
            trades: 0,
            reference: cfg.reference_price,
            pending: Vec::new(),
            traded: None,
        }
    }

    /// A book in the state `snapshot` records, which the engine's `restore` accepted: each
    /// order joins the back of its level's queue and each stop the pending list, both in
    /// snapshot order.
    pub fn restore(snapshot: &BookSnapshot) -> Self {
        let mut book = Self::new(snapshot.config);
        book.trades = snapshot.trade_count;
        book.reference = snapshot.reference_price;
        for order in &snapshot.orders {
            book.ladder(order.side)
                .entry(order.price)
                .or_default()
                .push_back(Order {
                    id: order.id,
                    owner: order.owner,
                    leaves: order.leaves,
                    total: order.leaves + order.filled,
                    post_only: order.post_only,
                    display: order.display,
                    visible: order.visible,
                });
            book.orders.insert(order.id, (order.side, order.price));
        }
        book.pending = snapshot.stops.clone();
        book
    }

    /// One side's pending stops in trigger order: buy stops lowest trigger first, sell stops
    /// highest first; the stable sort keeps arrival order within a trigger.
    pub fn stops(&self, side: Side) -> Vec<StopOrder> {
        let mut stops: Vec<StopOrder> = self
            .pending
            .iter()
            .filter(|s| s.side == side)
            .copied()
            .collect();
        stops.sort_by_key(|s| match side {
            Side::Buy => i128::from(s.trigger),
            Side::Sell => -i128::from(s.trigger),
        });
        stops
    }

    /// Resting orders and pending stops: each takes one of `max_orders`.
    fn held(&self) -> usize {
        self.orders.len() + self.pending.len()
    }

    fn taken(&self, id: OrderId) -> bool {
        self.orders.contains_key(&id) || self.pending.iter().any(|s| s.id == id)
    }

    pub fn reference_price(&self) -> Option<Price> {
        self.reference
    }

    pub fn trade_count(&self) -> u64 {
        self.trades
    }

    pub fn process(&mut self, command: Command, out: &mut Vec<Event>) {
        self.traded = None;
        if let Err(reason) = self.apply(command, out) {
            out.push(Event::Rejected {
                id: command.id().expect("only commands with an id are rejected"),
                reason,
            });
        }
        self.release_stops(out);
    }

    /// Releases, one at a time, the stops this command's trades reached: the lowest buy
    /// trigger at or below the highest trade price, else the highest sell trigger at or
    /// above the lowest trade price. Released stops trade and can reach more.
    fn release_stops(&mut self, out: &mut Vec<Event>) {
        while let Some((low, high)) = self.traded {
            let next = self
                .stops(Side::Buy)
                .into_iter()
                .find(|s| s.trigger <= high)
                .or_else(|| {
                    self.stops(Side::Sell)
                        .into_iter()
                        .find(|s| s.trigger >= low)
                });
            let Some(stop) = next else {
                return;
            };
            self.pending.retain(|s| s.id != stop.id);
            out.push(Event::Triggered { id: stop.id });
            let StopOrder {
                id,
                owner,
                side,
                qty,
                ..
            } = stop;
            match stop.limit {
                None => self.execute_market(id, owner, side, qty, out),
                Some(limit) => {
                    let refused = if self.outside_protection(side, limit) {
                        Some(CancelReason::PriceProtection)
                    } else if self.outside_band(side, limit) {
                        Some(CancelReason::PriceBand)
                    } else {
                        None
                    };
                    match refused {
                        Some(reason) => out.push(Event::Cancelled { id, qty, reason }),
                        None => {
                            self.execute_limit(id, owner, side, limit, qty, qty, (false, None), out)
                        }
                    }
                }
            }
        }
    }

    fn apply(&mut self, command: Command, out: &mut Vec<Event>) -> Result<(), RejectReason> {
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
                self.check_owner(owner)?;
                self.check_qty(qty)?;
                let may_rest = matches!(tif, TimeInForce::Gtc | TimeInForce::PostOnly);
                if let Some(display) = display {
                    if display == 0 || display >= qty || !may_rest || !self.covers(display, qty) {
                        return Err(RejectReason::InvalidDisplay);
                    }
                }
                self.check_band(price)?;
                if self.taken(id) {
                    return Err(RejectReason::DuplicateOrderId);
                }
                if self.outside_protection(side, price) {
                    return Err(RejectReason::PriceOutsideProtection);
                }
                if self.outside_band(side, price) {
                    return Err(RejectReason::PriceOutsideBand);
                }
                let crosses = self.crosses(side, price);
                if tif == TimeInForce::PostOnly && crosses {
                    return Err(RejectReason::PostOnlyWouldCross);
                }
                if may_rest && self.held() >= self.cfg.max_orders as usize && !crosses {
                    return Err(RejectReason::BookFull);
                }
                out.push(Event::Accepted { id });
                match tif {
                    TimeInForce::Gtc | TimeInForce::PostOnly => {
                        let post_only = tif == TimeInForce::PostOnly;
                        let shape = (post_only, display);
                        self.execute_limit(id, owner, side, price, qty, qty, shape, out);
                    }
                    TimeInForce::Ioc => {
                        let (left, halt) =
                            self.match_incoming(id, owner, side, qty, Some(price), out);
                        if left > 0 {
                            let reason = if halt == Halt::SelfTrade {
                                CancelReason::SelfTrade
                            } else {
                                CancelReason::ImmediateOrCancel
                            };
                            out.push(Event::Cancelled {
                                id,
                                qty: left,
                                reason,
                            });
                        }
                    }
                    TimeInForce::Fok => {
                        // Try it on a copy of the whole book, and keep the outcome only if
                        // every lot traded.
                        let mut trial = self.clone();
                        let mut trial_out = Vec::new();
                        let (left, _) =
                            trial.match_incoming(id, owner, side, qty, Some(price), &mut trial_out);
                        if left == 0 {
                            *self = trial;
                            out.extend(trial_out);
                        } else {
                            out.push(Event::Cancelled {
                                id,
                                qty,
                                reason: CancelReason::FillOrKill,
                            });
                        }
                    }
                }
            }
            Command::Market {
                id,
                owner,
                side,
                qty,
            } => {
                self.check_owner(owner)?;
                self.check_qty(qty)?;
                if self.taken(id) {
                    return Err(RejectReason::DuplicateOrderId);
                }
                out.push(Event::Accepted { id });
                self.execute_market(id, owner, side, qty, out);
            }
            Command::Stop {
                id,
                owner,
                side,
                trigger,
                limit,
                qty,
            } => {
                self.check_owner(owner)?;
                self.check_qty(qty)?;
                self.check_band(trigger)?;
                if let Some(limit) = limit {
                    self.check_band(limit)?;
                }
                if self.taken(id) {
                    return Err(RejectReason::DuplicateOrderId);
                }
                let reached = self.reference.is_some_and(|last| match side {
                    Side::Buy => trigger <= last,
                    Side::Sell => trigger >= last,
                });
                if reached {
                    return Err(RejectReason::StopWouldTrigger);
                }
                if self.held() >= self.cfg.max_orders as usize {
                    return Err(RejectReason::BookFull);
                }
                out.push(Event::Accepted { id });
                self.pending.push(StopOrder {
                    id,
                    owner,
                    side,
                    trigger,
                    limit,
                    qty,
                });
                out.push(Event::StopPlaced {
                    id,
                    side,
                    trigger,
                    limit,
                    qty,
                });
            }
            Command::Cancel { id, owner } => {
                if let Some(i) = self
                    .pending
                    .iter()
                    .position(|s| s.id == id && s.owner == owner)
                {
                    let stop = self.pending.remove(i);
                    out.push(Event::Cancelled {
                        id,
                        qty: stop.qty,
                        reason: CancelReason::Requested,
                    });
                    return Ok(());
                }
                let (side, price) = self.owned(id, owner)?;
                let order = self.take_out(id, side, price);
                out.push(Event::Cancelled {
                    id,
                    qty: order.leaves,
                    reason: CancelReason::Requested,
                });
            }
            Command::Modify {
                id,
                owner,
                price,
                qty,
            } => {
                self.check_qty(qty)?;
                self.check_band(price)?;
                if self.pending.iter().any(|s| s.id == id && s.owner == owner) {
                    return Err(RejectReason::PendingStop);
                }
                let (side, old_price) = self.owned(id, owner)?;
                let order = *self.find(id, side, old_price);
                let filled = order.total - order.leaves;
                if qty <= filled {
                    self.take_out(id, side, old_price);
                    out.push(Event::Modified {
                        id,
                        price,
                        qty,
                        leaves: 0,
                    });
                } else if price == old_price && qty <= order.total {
                    let order = self.find(id, side, old_price);
                    order.leaves = qty - filled;
                    order.total = qty;
                    order.visible = order.visible.min(order.leaves);
                    out.push(Event::Modified {
                        id,
                        price,
                        qty,
                        leaves: qty - filled,
                    });
                } else {
                    if self.outside_protection(side, price) {
                        return Err(RejectReason::PriceOutsideProtection);
                    }
                    if self.outside_band(side, price) {
                        return Err(RejectReason::PriceOutsideBand);
                    }
                    if order.post_only && self.crosses(side, price) {
                        return Err(RejectReason::PostOnlyWouldCross);
                    }
                    if order
                        .display
                        .is_some_and(|display| !self.covers(display, qty))
                    {
                        return Err(RejectReason::InvalidDisplay);
                    }
                    self.take_out(id, side, old_price);
                    out.push(Event::Modified {
                        id,
                        price,
                        qty,
                        leaves: qty - filled,
                    });
                    let leaves = qty - filled;
                    let shape = (order.post_only, order.display);
                    self.execute_limit(id, owner, side, price, leaves, qty, shape, out);
                }
            }
            Command::CancelAll { owner } => {
                // Walk the whole book in priority order and take out the owner's orders.
                let mut count = 0;
                for side in [Side::Buy, Side::Sell] {
                    let prices: Vec<Price> = match side {
                        Side::Buy => self.bids.keys().rev().copied().collect(),
                        Side::Sell => self.asks.keys().copied().collect(),
                    };
                    for price in prices {
                        let ids: Vec<OrderId> = self.ladder_ref(side)[&price]
                            .iter()
                            .filter(|o| o.owner == owner)
                            .map(|o| o.id)
                            .collect();
                        for id in ids {
                            let order = self.take_out(id, side, price);
                            out.push(Event::Cancelled {
                                id,
                                qty: order.leaves,
                                reason: CancelReason::MassCancel,
                            });
                            count += 1;
                        }
                    }
                }
                for side in [Side::Buy, Side::Sell] {
                    for stop in self.stops(side).into_iter().filter(|s| s.owner == owner) {
                        self.pending.retain(|s| s.id != stop.id);
                        out.push(Event::Cancelled {
                            id: stop.id,
                            qty: stop.qty,
                            reason: CancelReason::MassCancel,
                        });
                        count += 1;
                    }
                }
                out.push(Event::MassCancelled { owner, count });
            }
        }
        Ok(())
    }

    /// `max_iceberg_tranches` tranches of `display` cover `qty`.
    fn covers(&self, display: Qty, qty: Qty) -> bool {
        u128::from(display) * u128::from(self.cfg.max_iceberg_tranches) >= u128::from(qty)
    }

    fn check_owner(&self, owner: OwnerId) -> Result<(), RejectReason> {
        if owner >= self.cfg.max_owners {
            return Err(RejectReason::InvalidOwner);
        }
        Ok(())
    }

    fn check_qty(&self, qty: Qty) -> Result<(), RejectReason> {
        if qty == 0 || qty > self.cfg.max_order_qty {
            return Err(RejectReason::InvalidQuantity);
        }
        Ok(())
    }

    fn check_band(&self, price: Price) -> Result<(), RejectReason> {
        if price < self.cfg.min_price || price > self.cfg.max_price {
            return Err(RejectReason::PriceOutOfRange);
        }
        Ok(())
    }

    fn owned(&self, id: OrderId, owner: OwnerId) -> Result<(Side, Price), RejectReason> {
        let &(side, price) = self.orders.get(&id).ok_or(RejectReason::UnknownOrder)?;
        let queue = self.ladder_ref(side).get(&price).unwrap();
        let order = queue.iter().find(|o| o.id == id).unwrap();
        if order.owner != owner {
            return Err(RejectReason::UnknownOrder);
        }
        Ok((side, price))
    }

    fn best(&self, side: Side) -> Option<Price> {
        match side {
            Side::Buy => self.bids.keys().next_back().copied(),
            Side::Sell => self.asks.keys().next().copied(),
        }
    }

    fn crosses(&self, side: Side, price: Price) -> bool {
        match side {
            Side::Buy => self.best(Side::Sell).is_some_and(|ask| price >= ask),
            Side::Sell => self.best(Side::Buy).is_some_and(|bid| price <= bid),
        }
    }

    fn outside_protection(&self, side: Side, price: Price) -> bool {
        let Some(ticks) = self.cfg.price_protection else {
            return false;
        };
        let (price, ticks) = (i128::from(price), i128::from(ticks));
        match side {
            Side::Buy => self
                .best(Side::Sell)
                .is_some_and(|ask| price > i128::from(ask) + ticks),
            Side::Sell => self
                .best(Side::Buy)
                .is_some_and(|bid| price < i128::from(bid) - ticks),
        }
    }

    fn protection_cap(&self, side: Side) -> Option<Price> {
        let ticks = i64::from(self.cfg.price_protection?);
        Some(match side {
            Side::Buy => self.best(Side::Sell)? + ticks,
            Side::Sell => self.best(Side::Buy)? - ticks,
        })
    }

    fn outside_band(&self, side: Side, price: Price) -> bool {
        let (Some(ticks), Some(reference)) = (self.cfg.price_band, self.reference) else {
            return false;
        };
        let (price, reference, ticks) =
            (i128::from(price), i128::from(reference), i128::from(ticks));
        match side {
            Side::Buy => price > reference + ticks,
            Side::Sell => price < reference - ticks,
        }
    }

    fn band_cap(&self, side: Side) -> Option<Price> {
        let ticks = i64::from(self.cfg.price_band?);
        let reference = self.reference?;
        Some(match side {
            Side::Buy => reference + ticks,
            Side::Sell => reference - ticks,
        })
    }

    /// A market order: matches up to the tighter of its caps; a tie names price protection.
    fn execute_market(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        qty: Qty,
        out: &mut Vec<Event>,
    ) {
        let (protection, band) = (self.protection_cap(side), self.band_cap(side));
        let (cap, stop) = match (protection, band) {
            (p, None) => (p, CancelReason::PriceProtection),
            (None, b) => (b, CancelReason::PriceBand),
            (Some(p), Some(b)) => {
                let band_tighter = match side {
                    Side::Buy => b < p,
                    Side::Sell => b > p,
                };
                if band_tighter {
                    (Some(b), CancelReason::PriceBand)
                } else {
                    (Some(p), CancelReason::PriceProtection)
                }
            }
        };
        let (unfilled, halt) = self.match_incoming(id, owner, side, qty, cap, out);
        if unfilled > 0 {
            let reason = match halt {
                Halt::SelfTrade => CancelReason::SelfTrade,
                Halt::Limit => stop,
                Halt::Empty | Halt::Filled => CancelReason::NoLiquidity,
            };
            out.push(Event::Cancelled {
                id,
                qty: unfilled,
                reason,
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_limit(
        &mut self,
        id: OrderId,
        owner: OwnerId,
        side: Side,
        price: Price,
        open: Qty,
        total: Qty,
        (post_only, display): (bool, Option<Qty>),
        out: &mut Vec<Event>,
    ) {
        let (left, halt) = self.match_incoming(id, owner, side, open, Some(price), out);
        if left == 0 {
            return;
        }
        if halt == Halt::SelfTrade {
            out.push(Event::Cancelled {
                id,
                qty: left,
                reason: CancelReason::SelfTrade,
            });
            return;
        }
        let visible = display.map_or(left, |display| display.min(left));
        self.ladder(side)
            .entry(price)
            .or_default()
            .push_back(Order {
                id,
                owner,
                leaves: left,
                total,
                post_only,
                display,
                visible,
            });
        self.orders.insert(id, (side, price));
        out.push(Event::Rested {
            id,
            side,
            price,
            qty: left,
            visible,
        });
    }

    fn match_incoming(
        &mut self,
        taker: OrderId,
        owner: OwnerId,
        side: Side,
        mut qty: Qty,
        limit: Option<Price>,
        out: &mut Vec<Event>,
    ) -> (Qty, Halt) {
        let policy = self.cfg.self_trade;
        while qty > 0 {
            let Some(best) = self.best(side.opposite()) else {
                return (qty, Halt::Empty);
            };
            let crosses = match (side, limit) {
                (_, None) => true,
                (Side::Buy, Some(l)) => best <= l,
                (Side::Sell, Some(l)) => best >= l,
            };
            if !crosses {
                return (qty, Halt::Limit);
            }
            let opposite = match side {
                Side::Buy => &mut self.asks,
                Side::Sell => &mut self.bids,
            };
            let queue = opposite.get_mut(&best).unwrap();
            while qty > 0 && !queue.is_empty() {
                let front = queue.front_mut().unwrap();
                if front.owner == owner {
                    if policy == SelfTradePolicy::CancelIncoming {
                        return (qty, Halt::SelfTrade);
                    }
                    let gone = queue.pop_front().unwrap();
                    self.orders.remove(&gone.id);
                    out.push(Event::Cancelled {
                        id: gone.id,
                        qty: gone.leaves,
                        reason: CancelReason::SelfTrade,
                    });
                    continue;
                }
                let fill = qty.min(front.visible);
                front.leaves -= fill;
                front.visible -= fill;
                qty -= fill;
                self.trades += 1;
                self.reference = Some(best);
                self.traded = Some(match self.traded {
                    None => (best, best),
                    Some((low, high)) => (low.min(best), high.max(best)),
                });
                out.push(Event::Trade {
                    trade_id: self.trades,
                    taker,
                    maker: front.id,
                    taker_side: side,
                    price: best,
                    qty: fill,
                    taker_leaves: qty,
                    maker_leaves: front.leaves,
                });
                if front.leaves == 0 {
                    let done = queue.pop_front().unwrap();
                    self.orders.remove(&done.id);
                } else if front.visible == 0 {
                    // An iceberg's tranche ran out: show the next one at the back.
                    let mut iceberg = queue.pop_front().unwrap();
                    iceberg.visible = iceberg.display.unwrap().min(iceberg.leaves);
                    out.push(Event::Replenished {
                        id: iceberg.id,
                        side: side.opposite(),
                        price: best,
                        visible: iceberg.visible,
                    });
                    queue.push_back(iceberg);
                }
            }
            if queue.is_empty() {
                opposite.remove(&best);
            }
        }
        (0, Halt::Filled)
    }

    fn find(&mut self, id: OrderId, side: Side, price: Price) -> &mut Order {
        self.ladder(side)
            .get_mut(&price)
            .unwrap()
            .iter_mut()
            .find(|o| o.id == id)
            .unwrap()
    }

    /// Removes an order from its queue and the id map.
    fn take_out(&mut self, id: OrderId, side: Side, price: Price) -> Order {
        self.orders.remove(&id);
        let ladder = self.ladder(side);
        let queue = ladder.get_mut(&price).unwrap();
        let pos = queue.iter().position(|o| o.id == id).unwrap();
        let order = queue.remove(pos).unwrap();
        if queue.is_empty() {
            ladder.remove(&price);
        }
        order
    }

    fn ladder(&mut self, side: Side) -> &mut Ladder {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn ladder_ref(&self, side: Side) -> &Ladder {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let level = |(price, queue): (&Price, &VecDeque<Order>)| {
            let orders = queue
                .iter()
                .map(|o| QueuedOrder {
                    id: o.id,
                    owner: o.owner,
                    leaves: o.leaves,
                    filled: o.total - o.leaves,
                    post_only: o.post_only,
                    display: o.display,
                    visible: o.visible,
                })
                .collect();
            (*price, orders)
        };
        [
            self.bids.iter().rev().map(level).collect(),
            self.asks.iter().map(level).collect(),
        ]
    }
}
