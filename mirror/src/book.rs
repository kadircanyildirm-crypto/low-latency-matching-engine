//! What the mirror keeps of its own orders on the exchange, and what it sends so that they
//! match the venue's book.
//!
//! Each of the mirror's orders stands for one order of the venue's, by the venue's id. Given
//! the venue's book, [`Mirror::follow`] cancels the orders the venue no longer has near its
//! best prices, changes those whose quantity changed (a smaller quantity at the same price
//! keeps the order's place in its queue, on the exchange as on the venue), and places those
//! it does not have yet, in the venue's order, so that they queue as the venue's do. It
//! learns what became of its orders from the exchange's reports, through
//! [`Mirror::on_report`]: an order a visitor or the trade tape filled is placed again if the
//! venue still has it.

use std::collections::HashMap;

use orderbook::{Side, TimeInForce};
use protocol::{Inbound, NewOrder, OrderKind, Report, ReportKind};

use crate::bitstamp::{Book, Resting};

/// One of the mirror's orders.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Own {
    side: Side,
    price: i64,
    /// The exchange's id, once it accepted the order.
    order_id: Option<u64>,
    /// The order's total quantity, and what is left of it, as the exchange last said.
    qty: u64,
    leaves: u64,
    /// What is left once a modify that was sent is applied.
    modifying: Option<u64>,
    /// A cancel was sent.
    cancelling: bool,
}

/// The mirror's orders, by the venue's ids.
#[derive(Debug)]
pub struct Mirror {
    /// Prices of each side followed: the venue's orders at its best `levels` prices.
    levels: usize,
    own: HashMap<u64, Own>,
    /// The venue's id of an order placed and not yet accepted, by its client reference.
    by_ref: HashMap<u64, u64>,
    /// The venue's id of an order, by the exchange's id.
    by_order: HashMap<u64, u64>,
    next_ref: u64,
}

impl Mirror {
    /// A mirror of the venue's best `levels` prices on each side, with no orders.
    pub fn new(levels: usize) -> Mirror {
        Mirror {
            levels,
            own: HashMap::new(),
            by_ref: HashMap::new(),
            by_order: HashMap::new(),
            next_ref: 0,
        }
    }

    /// Forgets every order, as after the exchange cancelled them all.
    pub fn clear(&mut self) {
        self.own.clear();
        self.by_ref.clear();
        self.by_order.clear();
    }

    /// The orders the mirror has on the exchange, or on their way there.
    pub fn len(&self) -> usize {
        self.own.len()
    }

    /// Whether the mirror has no orders.
    pub fn is_empty(&self) -> bool {
        self.own.is_empty()
    }

    /// Whether everything sent has been answered: every order accepted, and no modify or
    /// cancel outstanding.
    pub fn is_settled(&self) -> bool {
        self.own
            .values()
            .all(|own| own.order_id.is_some() && own.modifying.is_none() && !own.cancelling)
    }

    /// Follows a report about one of the mirror's orders.
    pub fn on_report(&mut self, report: &Report) {
        if report.kind == ReportKind::Accepted {
            if let Some(venue) = self.by_ref.remove(&report.client_ref) {
                if let Some(own) = self.own.get_mut(&venue) {
                    own.order_id = Some(report.order_id);
                    self.by_order.insert(report.order_id, venue);
                }
            }
            return;
        }
        let Some(&venue) = self.by_order.get(&report.order_id) else {
            // A new order refused before it was accepted.
            if let ReportKind::Rejected(_) = report.kind {
                self.on_refused(report.client_ref);
            }
            return;
        };
        let own = self.own.get_mut(&venue).expect("an order of the mirror");
        match report.kind {
            ReportKind::Rested { qty, .. } => own.leaves = qty,
            ReportKind::Fill { leaves, .. } => {
                own.leaves = leaves;
                if leaves == 0 {
                    self.forget(venue);
                }
            }
            ReportKind::Modified { price, qty, leaves } => {
                own.price = price;
                own.qty = qty;
                own.leaves = leaves;
                own.modifying = None;
                if leaves == 0 {
                    self.forget(venue);
                }
            }
            ReportKind::Cancelled { .. } => self.forget(venue),
            // A modify or cancel refused: the order stays as it was, and is looked at again.
            ReportKind::Rejected(_) => {
                own.modifying = None;
                own.cancelling = false;
            }
            _ => {}
        }
    }

    /// The exchange refused the new order placed with `client_ref`.
    pub fn on_refused(&mut self, client_ref: u64) {
        if let Some(venue) = self.by_ref.remove(&client_ref) {
            self.own.remove(&venue);
        }
    }

    fn forget(&mut self, venue: u64) {
        if let Some(own) = self.own.remove(&venue) {
            if let Some(id) = own.order_id {
                self.by_order.remove(&id);
            }
        }
    }

    /// What to send for the mirror's orders to match the venue's orders at the best
    /// `levels` prices of each side of `book`: cancels first, so that a side moving across
    /// never meets the mirror's own orders, then changes, then new orders in the venue's
    /// order. An order still on its way is left until it is answered.
    pub fn follow(&mut self, book: &Book) -> Vec<Inbound> {
        let mut wanted: Vec<(Side, Resting)> = Vec::new();
        for (side, orders) in [(Side::Buy, &book.bids), (Side::Sell, &book.asks)] {
            let mut prices = 0;
            let mut last = None;
            for &order in orders {
                if last != Some(order.price) {
                    prices += 1;
                    last = Some(order.price);
                }
                if prices > self.levels {
                    break;
                }
                wanted.push((side, order));
            }
        }
        let wanted_ids: std::collections::HashSet<u64> =
            wanted.iter().map(|(_, order)| order.id).collect();

        let mut cancels: Vec<u64> = Vec::new();
        for (venue, own) in &mut self.own {
            if wanted_ids.contains(venue) || own.cancelling {
                continue;
            }
            if let Some(id) = own.order_id {
                cancels.push(id);
                own.cancelling = true;
            }
        }
        cancels.sort_unstable();
        let mut out: Vec<Inbound> = cancels
            .into_iter()
            .map(|order_id| Inbound::Cancel { order_id })
            .collect();

        let mut new_orders = Vec::new();
        for (side, order) in wanted {
            match self.own.get_mut(&order.id) {
                None => {
                    self.next_ref += 1;
                    self.by_ref.insert(self.next_ref, order.id);
                    self.own.insert(
                        order.id,
                        Own {
                            side,
                            price: order.price,
                            order_id: None,
                            qty: order.lots,
                            leaves: order.lots,
                            modifying: None,
                            cancelling: false,
                        },
                    );
                    new_orders.push(Inbound::NewOrder(NewOrder {
                        client_ref: self.next_ref,
                        side,
                        qty: order.lots,
                        kind: OrderKind::Limit {
                            price: order.price,
                            tif: TimeInForce::Gtc,
                            display: None,
                        },
                    }));
                }
                Some(own) if own.order_id.is_none() || own.cancelling => {}
                Some(own) => {
                    let showing = own.modifying.unwrap_or(own.leaves);
                    if own.price != order.price || showing != order.lots {
                        // A modify sets the total: what has traded stays traded.
                        let filled = own.qty - own.leaves;
                        out.push(Inbound::Modify {
                            order_id: own.order_id.expect("an accepted order"),
                            price: order.price,
                            qty: filled + order.lots,
                        });
                        own.modifying = Some(order.lots);
                    }
                }
            }
        }
        out.extend(new_orders);
        out
    }
}

/// The order that sends a venue's trade again: an immediate-or-cancel order on the taker's
/// side that takes the same quantity at the same price, or better, and nothing more.
pub fn retrade(client_ref: u64, side: Side, price: i64, lots: u64) -> Inbound {
    Inbound::NewOrder(NewOrder {
        client_ref,
        side,
        qty: lots,
        kind: OrderKind::Limit {
            price,
            tif: TimeInForce::Ioc,
            display: None,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orderbook::{CancelReason, RejectReason};

    fn resting(id: u64, price: i64, lots: u64) -> Resting {
        Resting { id, price, lots }
    }

    fn report(order_id: u64, client_ref: u64, kind: ReportKind) -> Report {
        Report {
            seq: 0,
            order_id,
            client_ref,
            kind,
        }
    }

    /// Accepts every new order in `sent`, giving it the exchange id `first`, `first + 1`...
    fn accept(mirror: &mut Mirror, sent: &[Inbound], first: u64) {
        let refs = sent.iter().filter_map(|message| match message {
            Inbound::NewOrder(order) => Some(order.client_ref),
            _ => None,
        });
        for (id, client_ref) in (first..).zip(refs) {
            mirror.on_report(&report(id, client_ref, ReportKind::Accepted));
        }
    }

    fn placed(sent: &[Inbound]) -> Vec<(Side, i64, u64)> {
        sent.iter()
            .filter_map(|message| match *message {
                Inbound::NewOrder(NewOrder {
                    side,
                    qty,
                    kind: OrderKind::Limit { price, .. },
                    ..
                }) => Some((side, price, qty)),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn the_mirror_follows_the_venue() {
        let mut mirror = Mirror::new(2);
        let book = Book {
            bids: vec![
                resting(11, 1_000, 5),
                resting(12, 1_000, 3),
                resting(13, 999, 7),
                // A third price: not followed.
                resting(14, 998, 9),
            ],
            asks: vec![resting(21, 1_002, 4)],
        };
        let sent = mirror.follow(&book);
        // In the venue's order, so they queue as the venue's do.
        assert_eq!(
            placed(&sent),
            [
                (Side::Buy, 1_000, 5),
                (Side::Buy, 1_000, 3),
                (Side::Buy, 999, 7),
                (Side::Sell, 1_002, 4),
            ]
        );
        assert!(!mirror.is_settled());
        // Nothing is sent twice while it is on its way.
        assert!(mirror.follow(&book).is_empty());
        accept(&mut mirror, &sent, 100);
        assert!(mirror.is_settled());
        assert!(mirror.follow(&book).is_empty());

        // On the venue, 12 partly traded, 13 went, 15 came at a new best bid, 21 went.
        let book = Book {
            bids: vec![
                resting(15, 1_001, 2),
                resting(11, 1_000, 5),
                resting(12, 1_000, 1),
            ],
            asks: vec![resting(22, 1_003, 6)],
        };
        let sent = mirror.follow(&book);
        assert_eq!(
            sent[..3],
            [
                Inbound::Cancel { order_id: 102 },
                Inbound::Cancel { order_id: 103 },
                // Smaller at the same price: it keeps its place.
                Inbound::Modify {
                    order_id: 101,
                    price: 1_000,
                    qty: 1
                },
            ]
        );
        assert_eq!(
            placed(&sent[3..]),
            [(Side::Buy, 1_001, 2), (Side::Sell, 1_003, 6)]
        );
        assert_eq!(sent.len(), 5);
        accept(&mut mirror, &sent, 200);
        let cancelled = ReportKind::Cancelled {
            qty: 7,
            reason: CancelReason::Requested,
        };
        mirror.on_report(&report(102, 0, cancelled));
        mirror.on_report(&report(103, 0, cancelled));
        let modified = ReportKind::Modified {
            price: 1_000,
            qty: 1,
            leaves: 1,
        };
        mirror.on_report(&report(101, 0, modified));
        assert!(mirror.is_settled());
        assert_eq!(mirror.len(), 4);
        assert!(mirror.follow(&book).is_empty());

        // A visitor takes 2 of the venue's 5 at 1,000: the mirror puts them back.
        let fill = ReportKind::Fill {
            trade_id: 1,
            side: Side::Buy,
            price: 1_000,
            qty: 2,
            leaves: 3,
        };
        mirror.on_report(&report(100, 0, fill));
        assert_eq!(
            mirror.follow(&book),
            [Inbound::Modify {
                order_id: 100,
                price: 1_000,
                qty: 7
            }]
        );
        // A refused modify is tried again.
        mirror.on_report(&report(
            100,
            0,
            ReportKind::Rejected(RejectReason::UnknownOrder),
        ));
        assert_eq!(mirror.follow(&book).len(), 1);
        // An order filled completely is placed again while the venue still has it.
        let filled = ReportKind::Fill {
            trade_id: 2,
            side: Side::Sell,
            price: 1_003,
            qty: 6,
            leaves: 0,
        };
        mirror.on_report(&report(201, 0, filled));
        let sent = mirror.follow(&book);
        assert_eq!(placed(&sent), [(Side::Sell, 1_003, 6)]);
        // One refused by the exchange is forgotten, and tried again next time.
        let Inbound::NewOrder(order) = sent[sent.len() - 1] else {
            panic!("a new order");
        };
        mirror.on_refused(order.client_ref);
        assert_eq!(placed(&mirror.follow(&book)), [(Side::Sell, 1_003, 6)]);

        mirror.clear();
        assert!(mirror.is_empty());
        assert_eq!(placed(&mirror.follow(&book)).len(), 4);
    }

    #[test]
    fn a_trade_is_sent_again_as_it_happened() {
        assert_eq!(
            retrade(7, Side::Sell, 250_543, 4_590),
            Inbound::NewOrder(NewOrder {
                client_ref: 7,
                side: Side::Sell,
                qty: 4_590,
                kind: OrderKind::Limit {
                    price: 250_543,
                    tif: TimeInForce::Ioc,
                    display: None,
                },
            })
        );
    }
}
