//! The last hour of trades as five-second candles, so that a chart opened now shows where
//! the price has been, not only what it does from now on.

use std::collections::VecDeque;

use serde::Serialize;

/// The length of a candle, in seconds.
pub const INTERVAL: u64 = 5;

/// Candles kept: an hour's worth.
const KEPT: usize = 720;

/// One interval's trades: open, high, low and close prices, and the quantity traded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Candle {
    /// The start of the interval, in seconds since the Unix epoch.
    pub t: u64,
    /// The first trade's price.
    pub o: i64,
    /// The highest price.
    pub h: i64,
    /// The lowest price.
    pub l: i64,
    /// The last trade's price.
    pub c: i64,
    /// The quantity traded.
    pub v: u64,
}

/// The candles of the last hour, oldest first. Intervals without trades have none.
#[derive(Clone, Debug, Default)]
pub struct Candles {
    bars: VecDeque<Candle>,
}

impl Candles {
    /// A trade of `qty` at `price`, at `time` seconds since the Unix epoch. A clock that
    /// went back puts the trade in the newest candle.
    pub fn record(&mut self, time: u64, price: i64, qty: u64) {
        let start = time - time % INTERVAL;
        match self.bars.back_mut() {
            Some(bar) if bar.t >= start => {
                bar.h = bar.h.max(price);
                bar.l = bar.l.min(price);
                bar.c = price;
                bar.v = bar.v.saturating_add(qty);
            }
            _ => {
                self.bars.push_back(Candle {
                    t: start,
                    o: price,
                    h: price,
                    l: price,
                    c: price,
                    v: qty,
                });
                if self.bars.len() > KEPT {
                    self.bars.pop_front();
                }
            }
        }
    }

    /// The candles, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = &Candle> {
        self.bars.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trades_fall_into_their_interval() {
        let mut candles = Candles::default();
        candles.record(1_000, 100, 2);
        candles.record(1_004, 103, 1);
        candles.record(1_002, 98, 4);
        candles.record(1_005, 101, 1);
        // A clock that went back.
        candles.record(1_001, 99, 1);
        let bars: Vec<Candle> = candles.iter().copied().collect();
        assert_eq!(
            bars,
            [
                Candle {
                    t: 1_000,
                    o: 100,
                    h: 103,
                    l: 98,
                    c: 98,
                    v: 7
                },
                Candle {
                    t: 1_005,
                    o: 101,
                    h: 101,
                    l: 99,
                    c: 99,
                    v: 2
                },
            ]
        );
        for second in 0..2_000 {
            candles.record(10_000 + second * INTERVAL, 1, 1);
        }
        assert_eq!(candles.iter().count(), KEPT);
        assert_eq!(candles.iter().last().unwrap().t, 10_000 + 1_999 * INTERVAL);
    }
}
