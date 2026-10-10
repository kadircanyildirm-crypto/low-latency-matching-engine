//! Bots that keep a demo market alive: market makers quoting around a fair price that
//! wanders, noise traders that cross the spread now and then, and trend followers that
//! trade with the recent move. They are ordinary clients of the binary protocol, each with
//! an account of its own, and watch the market through its market data like anyone else.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use orderbook::workload::SplitMix64;
use orderbook::{Side, TimeInForce};
use protocol::{Inbound, NewOrder, OrderKind, Outbound};

use crate::accounts::Account;
use crate::client::Client;

/// What a bot does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// Quotes a few levels on each side of a fair price that wanders and is pulled back
    /// towards the starting mid, replacing all its quotes every interval.
    MarketMaker,
    /// Now and then buys or sells a few lots at once, crossing the spread.
    Noise,
    /// Buys when the recent trades rose, sells when they fell.
    Trend,
}

/// One bot.
#[derive(Clone, Copy, Debug)]
pub struct Bot {
    /// What it does.
    pub strategy: Strategy,
    /// The account it trades for.
    pub account: Account,
    /// The price the market starts around, in ticks.
    pub mid: i64,
    /// How often it acts, on average.
    pub interval: Duration,
    /// Seeds its choices.
    pub seed: u64,
}

/// What a bot knows of the market, from its market data.
#[derive(Debug, Default)]
struct Market {
    bids: BTreeMap<i64, u64>,
    asks: BTreeMap<i64, u64>,
    /// Recent trade prices, newest last.
    trades: VecDeque<i64>,
}

impl Market {
    fn apply(&mut self, message: &Outbound) {
        match *message {
            Outbound::BookSnapshot { .. } => {
                self.bids.clear();
                self.asks.clear();
            }
            Outbound::LevelUpdate(level) => {
                let half = match level.side {
                    Side::Buy => &mut self.bids,
                    Side::Sell => &mut self.asks,
                };
                if level.orders == 0 {
                    half.remove(&level.price);
                } else {
                    half.insert(level.price, level.qty);
                }
            }
            Outbound::TradeTick(trade) => {
                self.trades.push_back(trade.price);
                if self.trades.len() > 50 {
                    self.trades.pop_front();
                }
            }
            _ => {}
        }
    }

    fn best_bid(&self) -> Option<i64> {
        self.bids.keys().next_back().copied()
    }

    fn best_ask(&self) -> Option<i64> {
        self.asks.keys().next().copied()
    }
}

/// Runs `bot` against the gateway at `addr` until `stop` is set or the connection fails.
pub fn run(addr: SocketAddr, bot: Bot, stop: &AtomicBool) -> io::Result<()> {
    let (mut client, _) = Client::login(addr, bot.account.id, bot.account.token)?;
    client.send(&Inbound::Subscribe)?;
    let mut rng = SplitMix64::new(bot.seed ^ u64::from(bot.account.id));
    let mut market = Market::default();
    let mut fair = bot.mid;
    let mut next_ref = 1;
    let mut next_action = Instant::now();
    let interval_ns = bot.interval.as_nanos().max(1) as u64;
    while !stop.load(Ordering::Relaxed) {
        while let Some(message) = client.try_receive()? {
            if let Outbound::Logout { .. } = message {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "logged out",
                ));
            }
            market.apply(&message);
        }
        let now = Instant::now();
        if now < next_action {
            thread::sleep(Duration::from_millis(5).min(next_action - now));
            continue;
        }
        // Acts on average once an interval, at random within it.
        next_action = now + Duration::from_nanos(interval_ns / 2 + rng.below(interval_ns));
        let mut order = |side, price: i64, qty, tif| {
            next_ref += 1;
            Inbound::NewOrder(NewOrder {
                client_ref: next_ref,
                side,
                qty,
                kind: OrderKind::Limit {
                    price: price.max(1),
                    tif,
                    display: None,
                },
            })
        };
        match bot.strategy {
            Strategy::MarketMaker => {
                // A random walk pulled back towards the mid, and towards the last trade.
                let step = rng.below(5) as i64 - 2;
                let pull = (bot.mid - fair) / 200;
                let last = market.trades.back().map_or(0, |&p| (p - fair) / 4);
                fair += step + pull + last;
                client.queue(&Inbound::MassCancel);
                for level in 0..10_i64 {
                    // Further from the price, more size, as on a real book.
                    let qty = 5 + rng.below(45) + 8 * level.unsigned_abs();
                    let gap = 1 + level * 2;
                    client.queue(&order(Side::Buy, fair - gap, qty, TimeInForce::Gtc));
                    client.queue(&order(Side::Sell, fair + gap, qty, TimeInForce::Gtc));
                }
            }
            Strategy::Noise => {
                let side = if rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let reference = match side {
                    Side::Buy => market.best_ask(),
                    Side::Sell => market.best_bid(),
                };
                if let Some(price) = reference {
                    let slip = rng.below(3) as i64;
                    let price = match side {
                        Side::Buy => price + slip,
                        Side::Sell => price - slip,
                    };
                    client.queue(&order(side, price, 1 + rng.below(20), TimeInForce::Ioc));
                }
            }
            Strategy::Trend => {
                if market.trades.len() >= 20 {
                    let recent: i64 = market.trades.iter().rev().take(5).sum::<i64>() / 5;
                    let older: i64 = market.trades.iter().rev().take(20).sum::<i64>() / 20;
                    let side = match recent - older {
                        diff if diff > 1 => Some(Side::Buy),
                        diff if diff < -1 => Some(Side::Sell),
                        _ => None,
                    };
                    let reference = match side {
                        Some(Side::Buy) => market.best_ask(),
                        Some(Side::Sell) => market.best_bid(),
                        None => None,
                    };
                    if let (Some(side), Some(price)) = (side, reference) {
                        client.queue(&order(side, price, 1 + rng.below(10), TimeInForce::Ioc));
                    }
                }
            }
        }
        client.flush()?;
    }
    if bot.strategy == Strategy::MarketMaker {
        client.send(&Inbound::MassCancel)?;
    }
    client.send(&Inbound::Logout)
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{LevelUpdate, TradeTick};

    #[test]
    fn the_market_view_follows_its_data() {
        let mut market = Market::default();
        let level = |side, price, qty, orders| {
            Outbound::LevelUpdate(LevelUpdate {
                seq: 1,
                side,
                price,
                qty,
                orders,
            })
        };
        market.apply(&level(Side::Buy, 99, 5, 1));
        market.apply(&level(Side::Buy, 98, 5, 1));
        market.apply(&level(Side::Sell, 101, 5, 1));
        assert_eq!(
            (market.best_bid(), market.best_ask()),
            (Some(99), Some(101))
        );
        market.apply(&level(Side::Buy, 99, 0, 0));
        assert_eq!(market.best_bid(), Some(98));
        for price in 0..60 {
            market.apply(&Outbound::TradeTick(TradeTick {
                seq: 2,
                trade_id: 1,
                side: Side::Buy,
                price,
                qty: 1,
            }));
        }
        assert_eq!(market.trades.len(), 50);
        assert_eq!(market.trades.back(), Some(&59));
        market.apply(&Outbound::BookSnapshot { seq: 3, levels: 0 });
        assert_eq!((market.best_bid(), market.best_ask()), (None, None));
    }
}
