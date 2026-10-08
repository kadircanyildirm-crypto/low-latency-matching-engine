//! Deliberately naive order book used as a test oracle.
//!
//! It shares no code or data structures with the real engine: `BTreeMap` of price ->
//! `VecDeque` of orders, prices instead of level indices, linear searches, allocation
//! everywhere. It is short enough to check by reading, and the property tests require the
//! engine to produce exactly the same events and the same book.

use std::collections::{BTreeMap, HashMap, VecDeque};

use orderbook::{
    BookConfig, CancelReason, Command, Event, OrderId, OwnerId, Price, Qty, QueuedOrder,
    RejectReason, SelfTradePolicy, Side,
};

use super::Snapshot;

#[derive(Clone, Copy, Debug)]
struct Order {
    id: OrderId,
    owner: OwnerId,
    leaves: Qty,
    total: Qty,
}

type Ladder = BTreeMap<Price, VecDeque<Order>>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Halt {
    Filled,
    Empty,
    Limit,
    SelfTrade,
}

pub struct ReferenceBook {
    cfg: BookConfig,
    bids: Ladder,
    asks: Ladder,
    orders: HashMap<OrderId, (Side, Price)>,
    trades: u64,
}

impl ReferenceBook {
    pub fn new(cfg: BookConfig) -> Self {
        Self {
            cfg,
            bids: Ladder::new(),
            asks: Ladder::new(),
            orders: HashMap::new(),
            trades: 0,
        }
    }

    pub fn trade_count(&self) -> u64 {
        self.trades
    }

    pub fn process(&mut self, command: Command, out: &mut Vec<Event>) {
        if let Err(reason) = self.apply(command, out) {
            out.push(Event::Rejected {
                id: command.id().expect("only commands with an id are rejected"),
                reason,
            });
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
            } => {
                self.check_owner(owner)?;
                self.check_qty(qty)?;
                self.check_band(price)?;
                if self.orders.contains_key(&id) {
                    return Err(RejectReason::DuplicateOrderId);
                }
                if self.outside_protection(side, price) {
                    return Err(RejectReason::PriceOutsideProtection);
                }
                if self.orders.len() >= self.cfg.max_orders as usize && !self.crosses(side, price) {
                    return Err(RejectReason::BookFull);
                }
                out.push(Event::Accepted { id });
                self.execute_limit(id, owner, side, price, qty, qty, out);
            }
            Command::Market {
                id,
                owner,
                side,
                qty,
            } => {
                self.check_owner(owner)?;
                self.check_qty(qty)?;
                if self.orders.contains_key(&id) {
                    return Err(RejectReason::DuplicateOrderId);
                }
                out.push(Event::Accepted { id });
                let cap = self.protection_cap(side);
                let (unfilled, halt) = self.match_incoming(id, owner, side, qty, cap, out);
                if unfilled > 0 {
                    let reason = match halt {
                        Halt::SelfTrade => CancelReason::SelfTrade,
                        Halt::Limit => CancelReason::PriceProtection,
                        Halt::Empty | Halt::Filled => CancelReason::NoLiquidity,
                    };
                    out.push(Event::Cancelled {
                        id,
                        qty: unfilled,
                        reason,
                    });
                }
            }
            Command::Cancel { id, owner } => {
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
                    self.take_out(id, side, old_price);
                    out.push(Event::Modified {
                        id,
                        price,
                        qty,
                        leaves: qty - filled,
                    });
                    self.execute_limit(id, owner, side, price, qty - filled, qty, out);
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
                out.push(Event::MassCancelled { owner, count });
            }
        }
        Ok(())
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
        let ticks = i128::from(self.cfg.price_protection?);
        match side {
            Side::Buy => {
                let ask = i128::from(self.best(Side::Sell)?);
                Some((ask + ticks).min(i128::from(self.cfg.max_price)) as Price)
            }
            Side::Sell => {
                let bid = i128::from(self.best(Side::Buy)?);
                Some((bid - ticks).max(i128::from(self.cfg.min_price)) as Price)
            }
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
        self.ladder(side)
            .entry(price)
            .or_default()
            .push_back(Order {
                id,
                owner,
                leaves: left,
                total,
            });
        self.orders.insert(id, (side, price));
        out.push(Event::Rested {
            id,
            side,
            price,
            qty: left,
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
                let fill = qty.min(front.leaves);
                front.leaves -= fill;
                qty -= fill;
                self.trades += 1;
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
