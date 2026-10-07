//! Deliberately naive order book used as a test oracle.
//!
//! It shares no code or data structures with the real engine: `BTreeMap` of price ->
//! `VecDeque` of orders, linear searches, allocation everywhere. It is short enough to check
//! by reading, and the property tests require the engine to produce exactly the same events
//! and the same book.

use std::collections::{BTreeMap, HashMap, VecDeque};

use orderbook::{
    BookConfig, CancelReason, Command, Event, OrderId, Price, Qty, RejectReason, Side,
};

use super::Snapshot;

type Ladder = BTreeMap<Price, VecDeque<(OrderId, Qty)>>;

pub struct ReferenceBook {
    cfg: BookConfig,
    bids: Ladder,
    asks: Ladder,
    orders: HashMap<OrderId, (Side, Price)>,
}

impl ReferenceBook {
    pub fn new(cfg: BookConfig) -> Self {
        Self {
            cfg,
            bids: Ladder::new(),
            asks: Ladder::new(),
            orders: HashMap::new(),
        }
    }

    pub fn process(&mut self, command: Command, out: &mut Vec<Event>) {
        if let Err(reason) = self.apply(command, out) {
            out.push(Event::Rejected {
                id: command.id(),
                reason,
            });
        }
    }

    fn apply(&mut self, command: Command, out: &mut Vec<Event>) -> Result<(), RejectReason> {
        match command {
            Command::Limit {
                id,
                side,
                price,
                qty,
            } => {
                self.check_qty_and_price(qty, Some(price))?;
                if self.orders.contains_key(&id) {
                    return Err(RejectReason::DuplicateOrderId);
                }
                if self.orders.len() >= self.cfg.max_orders as usize {
                    return Err(RejectReason::BookFull);
                }
                out.push(Event::Accepted { id });
                self.execute_limit(id, side, price, qty, out);
            }
            Command::Market { id, side, qty } => {
                self.check_qty_and_price(qty, None)?;
                if self.orders.contains_key(&id) {
                    return Err(RejectReason::DuplicateOrderId);
                }
                out.push(Event::Accepted { id });
                let unfilled = self.match_incoming(id, side, qty, None, out);
                if unfilled > 0 {
                    out.push(Event::Cancelled {
                        id,
                        qty: unfilled,
                        reason: CancelReason::NoLiquidity,
                    });
                }
            }
            Command::Cancel { id } => {
                let (side, price) = self.orders.remove(&id).ok_or(RejectReason::UnknownOrder)?;
                let qty = self.take_out(id, side, price);
                out.push(Event::Cancelled {
                    id,
                    qty,
                    reason: CancelReason::Requested,
                });
            }
            Command::Modify { id, price, qty } => {
                self.check_qty_and_price(qty, Some(price))?;
                let &(side, old_price) = self.orders.get(&id).ok_or(RejectReason::UnknownOrder)?;
                let queue = self.ladder(side).get_mut(&old_price).unwrap();
                let entry = queue.iter_mut().find(|(oid, _)| *oid == id).unwrap();
                if price == old_price && qty <= entry.1 {
                    entry.1 = qty;
                    out.push(Event::Modified { id, price, qty });
                } else {
                    self.take_out(id, side, old_price);
                    self.orders.remove(&id);
                    out.push(Event::Modified { id, price, qty });
                    self.execute_limit(id, side, price, qty, out);
                }
            }
        }
        Ok(())
    }

    fn check_qty_and_price(&self, qty: Qty, price: Option<Price>) -> Result<(), RejectReason> {
        if qty == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        if let Some(p) = price {
            if p < self.cfg.min_price || p > self.cfg.max_price {
                return Err(RejectReason::PriceOutOfRange);
            }
        }
        Ok(())
    }

    fn execute_limit(
        &mut self,
        id: OrderId,
        side: Side,
        price: Price,
        qty: Qty,
        out: &mut Vec<Event>,
    ) {
        let remaining = self.match_incoming(id, side, qty, Some(price), out);
        if remaining > 0 {
            self.ladder(side)
                .entry(price)
                .or_default()
                .push_back((id, remaining));
            self.orders.insert(id, (side, price));
            out.push(Event::Rested {
                id,
                side,
                price,
                qty: remaining,
            });
        }
    }

    fn match_incoming(
        &mut self,
        taker: OrderId,
        side: Side,
        mut qty: Qty,
        limit: Option<Price>,
        out: &mut Vec<Event>,
    ) -> Qty {
        while qty > 0 {
            let opposite = match side {
                Side::Buy => &mut self.asks,
                Side::Sell => &mut self.bids,
            };
            let best = match side {
                Side::Buy => opposite.keys().next().copied(),
                Side::Sell => opposite.keys().next_back().copied(),
            };
            let Some(best) = best else { break };
            let crosses = match (side, limit) {
                (_, None) => true,
                (Side::Buy, Some(l)) => best <= l,
                (Side::Sell, Some(l)) => best >= l,
            };
            if !crosses {
                break;
            }
            let queue = opposite.get_mut(&best).unwrap();
            while qty > 0 && !queue.is_empty() {
                let front = queue.front_mut().unwrap();
                let fill = qty.min(front.1);
                front.1 -= fill;
                qty -= fill;
                out.push(Event::Trade {
                    taker,
                    maker: front.0,
                    taker_side: side,
                    price: best,
                    qty: fill,
                });
                if front.1 == 0 {
                    let (maker, _) = queue.pop_front().unwrap();
                    self.orders.remove(&maker);
                }
            }
            if queue.is_empty() {
                opposite.remove(&best);
            }
        }
        qty
    }

    /// Removes an order from its queue and returns its open quantity.
    fn take_out(&mut self, id: OrderId, side: Side, price: Price) -> Qty {
        let ladder = self.ladder(side);
        let queue = ladder.get_mut(&price).unwrap();
        let pos = queue.iter().position(|(oid, _)| *oid == id).unwrap();
        let (_, qty) = queue.remove(pos).unwrap();
        if queue.is_empty() {
            ladder.remove(&price);
        }
        qty
    }

    fn ladder(&mut self, side: Side) -> &mut Ladder {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let level = |(price, queue): (&Price, &VecDeque<(OrderId, Qty)>)| {
            (*price, queue.iter().copied().collect())
        };
        [
            self.bids.iter().rev().map(level).collect(),
            self.asks.iter().map(level).collect(),
        ]
    }
}
