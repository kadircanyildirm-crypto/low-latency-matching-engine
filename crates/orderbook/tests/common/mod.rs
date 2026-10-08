#![allow(dead_code)]

pub mod reference;
pub mod strategies;

use orderbook::{OrderBook, Price, QueuedOrder, Side};

/// Every order on the book: `[bids, asks]`, each best price first, each level's queue in
/// time priority.
pub type Snapshot = [Vec<(Price, Vec<QueuedOrder>)>; 2];

pub fn snapshot(book: &OrderBook) -> Snapshot {
    let side = |side: Side| {
        book.depth(side)
            .map(|level| (level.price, book.queue(side, level.price).collect()))
            .collect()
    };
    [side(Side::Buy), side(Side::Sell)]
}

/// FNV-1a over a canonical byte encoding of events. Unlike `DefaultHasher`, its output is
/// fixed forever and identical on every platform, so it can be pinned in a golden test.
pub struct Fnv(u64);

impl Fnv {
    pub fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    pub fn write_u64(&mut self, value: u64) {
        for byte in value.to_le_bytes() {
            self.0 ^= u64::from(byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    pub fn event(&mut self, event: &orderbook::Event) {
        use orderbook::Event::*;
        let side = |s: Side| s as u64;
        match *event {
            Accepted { id } => [0, id].iter().for_each(|&v| self.write_u64(v)),
            Rejected { id, reason } => [1, id, reason as u64]
                .iter()
                .for_each(|&v| self.write_u64(v)),
            Trade {
                trade_id,
                taker,
                maker,
                taker_side,
                price,
                qty,
                taker_leaves,
                maker_leaves,
            } => [
                2,
                trade_id,
                taker,
                maker,
                side(taker_side),
                price as u64,
                qty,
                taker_leaves,
                maker_leaves,
            ]
            .iter()
            .for_each(|&v| self.write_u64(v)),
            Rested {
                id,
                side: s,
                price,
                qty,
                visible,
            } => {
                [3, id, side(s), price as u64, qty]
                    .iter()
                    .for_each(|&v| self.write_u64(v));
                // Only icebergs show less than they hold; plain orders keep the encoding
                // the golden values were pinned with.
                if visible != qty {
                    self.write_u64(visible);
                }
            }
            Cancelled { id, qty, reason } => [4, id, qty, reason as u64]
                .iter()
                .for_each(|&v| self.write_u64(v)),
            Modified {
                id,
                price,
                qty,
                leaves,
            } => [5, id, price as u64, qty, leaves]
                .iter()
                .for_each(|&v| self.write_u64(v)),
            MassCancelled { owner, count } => [6, u64::from(owner), u64::from(count)]
                .iter()
                .for_each(|&v| self.write_u64(v)),
            Replenished {
                id,
                side: s,
                price,
                visible,
            } => [7, id, side(s), price as u64, visible]
                .iter()
                .for_each(|&v| self.write_u64(v)),
        }
    }

    pub fn finish(&self) -> u64 {
        self.0
    }
}
