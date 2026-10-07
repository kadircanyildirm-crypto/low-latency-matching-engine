#![allow(dead_code)]

pub mod reference;

use orderbook::{OrderBook, OrderId, Price, Qty, Side};

/// Every order on the book: `[bids, asks]`, each best price first, each level's queue in
/// time priority.
pub type Snapshot = [Vec<(Price, Vec<(OrderId, Qty)>)>; 2];

pub fn snapshot(book: &OrderBook) -> Snapshot {
    let side = |side: Side| {
        book.depth(side)
            .map(|level| (level.price, book.queue(side, level.price).collect()))
            .collect()
    };
    [side(Side::Buy), side(Side::Sell)]
}
