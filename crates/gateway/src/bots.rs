//! Bots that keep a demo market alive: market makers quoting around a fair price that
//! wanders, each in a style of its own; noise traders that cross the spread now and then;
//! trend followers that trade with the recent move; passive traders that leave small orders
//! behind the best prices; an iceberg that shows a little of a large order; stops waiting
//! beyond the market; and a whale that now and then takes several levels at once. They are
//! ordinary clients of the binary protocol, each with an account of its own, and watch the
//! market through its market data like anyone else.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use orderbook::workload::SplitMix64;
use orderbook::{BookConfig, Side, TimeInForce};
use protocol::{Inbound, NewOrder, OrderKind, Outbound, ReportKind};

use crate::accounts::Account;
use crate::client::Client;

/// What a bot does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// Quotes six to ten levels on each side of a fair price that wanders and is pulled
    /// back towards the starting mid, every tick or every other one, and replaces its quotes
    /// only once the fair price has moved two ticks, or now and then.
    MarketMaker,
    /// Now and then buys or sells a few lots at once, crossing the spread.
    Noise,
    /// Buys when the recent trades rose, sells when they fell.
    Trend,
    /// Leaves small limit orders at or a few ticks behind the best price on their side, and
    /// cancels them all every thirty.
    Passive,
    /// Rests one large iceberg order near the best price, showing a small part of it at a
    /// time; places another once it is done or the market has moved away from it.
    Iceberg,
    /// Places stop orders six to twenty-one ticks beyond the market, and cancels those
    /// still waiting every twelve.
    Stops,
    /// Sends a large immediate-or-cancel order that takes several levels at once.
    Whale,
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

/// What a bot remembers between its actions.
#[derive(Debug, Default)]
struct Memory {
    /// The fair price a market maker believes in.
    fair: i64,
    /// The fair price its quotes are around, if it has quotes out.
    quoted: Option<i64>,
    /// Orders placed since the bot last cleared them.
    placed: u32,
    /// The client reference and price of a resting iceberg order.
    iceberg: Option<(u64, i64)>,
}

impl Memory {
    /// Follows the bot's own reports.
    fn apply(&mut self, message: &Outbound) {
        let Outbound::Report(report) = message else {
            return;
        };
        let gone = match report.kind {
            ReportKind::Fill { leaves, .. } => leaves == 0,
            ReportKind::Cancelled { .. } | ReportKind::Rejected(_) => true,
            _ => false,
        };
        if gone && self.iceberg.is_some_and(|(r, _)| r == report.client_ref) {
            self.iceberg = None;
        }
    }
}

/// Runs `bot` against the gateway at `addr` until `stop` is set or the connection fails.
pub fn run(addr: SocketAddr, bot: Bot, stop: &AtomicBool) -> io::Result<()> {
    let (mut client, _) = Client::login(addr, bot.account.id, bot.account.token)?;
    client.send(&Inbound::Subscribe)?;
    let mut rng = rng(&bot);
    let mut market = Market::default();
    let mut memory = Memory {
        fair: bot.mid,
        ..Memory::default()
    };
    // Each market maker has a style of its own: how many levels, how far apart, how big.
    let levels = 6 + rng.below(5) as i64;
    let spacing = 1 + rng.below(2) as i64;
    let base = 4 + rng.below(12);
    let mut next_ref = 1;
    let mut next_action = Instant::now();
    let mut last_sent = Instant::now();
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
            memory.apply(&message);
        }
        let now = Instant::now();
        // The exchange logs out a session that stays silent: a bot that acts seldom says
        // it is still there.
        if now - last_sent >= HEARTBEAT {
            client.send(&Inbound::Heartbeat)?;
            last_sent = now;
        }
        if now < next_action {
            thread::sleep(Duration::from_millis(5).min(next_action - now));
            continue;
        }
        last_sent = now;
        // Acts on average once an interval, at random within it.
        next_action = now + Duration::from_nanos(interval_ns / 2 + rng.below(interval_ns));
        let mut order = |side, price: i64, qty, kind: Kind| {
            next_ref += 1;
            let price = price.max(1);
            let kind = match kind {
                Kind::Limit(tif) => OrderKind::Limit {
                    price,
                    tif,
                    display: None,
                },
                Kind::Iceberg(display) => OrderKind::Limit {
                    price,
                    tif: TimeInForce::Gtc,
                    display: Some(display),
                },
                Kind::Stop => OrderKind::Stop {
                    trigger: price,
                    limit: None,
                },
            };
            let message = Inbound::NewOrder(NewOrder {
                client_ref: next_ref,
                side,
                qty,
                kind,
            });
            (next_ref, message)
        };
        let side = if rng.below(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let gtc = Kind::Limit(TimeInForce::Gtc);
        let ioc = Kind::Limit(TimeInForce::Ioc);
        match bot.strategy {
            Strategy::MarketMaker => {
                // A random walk pulled back towards the mid, and towards the last trade.
                let step = rng.below(5) as i64 - 2;
                let pull = (bot.mid - memory.fair) / 200;
                let last = market.trades.back().map_or(0, |&p| (p - memory.fair) / 4);
                memory.fair += step + pull + last;
                // Quotes move only once the price has, or now and then: until then they
                // keep their place in their queues, among everyone else's orders.
                let moved = memory
                    .quoted
                    .is_none_or(|quoted| (quoted - memory.fair).abs() >= 2);
                if moved || rng.below(6) == 0 {
                    client.queue(&Inbound::MassCancel);
                    for level in 0..levels {
                        // Further from the price, more size, as on a real book.
                        let qty = base + rng.below(2 * base) + base / 2 * level.unsigned_abs();
                        let gap = 1 + level * spacing;
                        let fair = memory.fair;
                        client.queue(&order(Side::Buy, fair - gap, qty, gtc).1);
                        client.queue(&order(Side::Sell, fair + gap, qty, gtc).1);
                    }
                    memory.quoted = Some(memory.fair);
                }
            }
            Strategy::Noise => {
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
                    client.queue(&order(side, price, 1 + rng.below(20), ioc).1);
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
                        client.queue(&order(side, price, 1 + rng.below(10), ioc).1);
                    }
                }
            }
            Strategy::Passive => {
                // A small order at or behind the best price on its side, as people leave
                // them; every so often it clears them all and starts again.
                if memory.placed >= 30 {
                    client.queue(&Inbound::MassCancel);
                    memory.placed = 0;
                }
                let behind = rng.below(6) as i64;
                let price = match side {
                    Side::Buy => market.best_bid().map(|p| p - behind),
                    Side::Sell => market.best_ask().map(|p| p + behind),
                };
                if let Some(price) = price {
                    let qty = if rng.below(8) == 0 {
                        40 + rng.below(80)
                    } else {
                        1 + rng.below(25)
                    };
                    client.queue(&order(side, price, qty, gtc).1);
                    memory.placed += 1;
                }
            }
            Strategy::Iceberg => {
                // One large order near the best price that shows a small part of itself; a
                // new one once it is done, or once the market has moved away from it.
                let touch = match side {
                    Side::Buy => market.best_bid(),
                    Side::Sell => market.best_ask(),
                };
                match (memory.iceberg, touch) {
                    (Some((_, at)), _) => {
                        let far = [market.best_bid(), market.best_ask()]
                            .into_iter()
                            .flatten()
                            .all(|best| (best - at).abs() > 12);
                        if far {
                            client.queue(&Inbound::MassCancel);
                        }
                    }
                    (None, Some(touch)) => {
                        let price = match side {
                            Side::Buy => touch - rng.below(3) as i64,
                            Side::Sell => touch + rng.below(3) as i64,
                        };
                        let (qty, display) = iceberg(&mut rng);
                        let (client_ref, message) = order(side, price, qty, Kind::Iceberg(display));
                        client.queue(&message);
                        memory.iceberg = Some((client_ref, price));
                    }
                    (None, None) => {}
                }
            }
            Strategy::Stops => {
                // A stop beyond the market, which a move may trigger; every so often it
                // clears those still waiting.
                if memory.placed >= 12 {
                    client.queue(&Inbound::MassCancel);
                    memory.placed = 0;
                }
                let away = 6 + rng.below(16) as i64;
                let trigger = match side {
                    Side::Buy => market.best_ask().map(|p| p + away),
                    Side::Sell => market.best_bid().map(|p| p - away),
                };
                if let Some(trigger) = trigger {
                    client.queue(&order(side, trigger, 5 + rng.below(25), Kind::Stop).1);
                    memory.placed += 1;
                }
            }
            Strategy::Whale => {
                // A large order that takes several levels at once.
                let reference = match side {
                    Side::Buy => market.best_ask().map(|p| p + 8),
                    Side::Sell => market.best_bid().map(|p| p - 8),
                };
                if let Some(price) = reference {
                    client.queue(&order(side, price, 120 + rng.below(240), ioc).1);
                }
            }
        }
        client.flush()?;
    }
    if bot.strategy != Strategy::Noise && bot.strategy != Strategy::Trend {
        client.send(&Inbound::MassCancel)?;
    }
    client.send(&Inbound::Logout)
}

/// A bot's random choices: seeded from its seed and account, mixed, so that bots with
/// nearby seeds and accounts do not act alike.
fn rng(bot: &Bot) -> SplitMix64 {
    SplitMix64::new(
        bot.seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(u64::from(bot.account.id)),
    )
}

/// An iceberg's quantity and the part it shows: within the book's default limit on
/// tranches, so the book takes it.
fn iceberg(rng: &mut SplitMix64) -> (u64, u64) {
    let display = 15 + rng.below(25);
    let tranches = 4 + rng.below(u64::from(BookConfig::DEFAULT_MAX_ICEBERG_TRANCHES) - 3);
    (display * tranches, display)
}

/// How long a bot stays silent at most.
const HEARTBEAT: Duration = Duration::from_secs(1);

/// What kind of order a bot sends.
#[derive(Clone, Copy, Debug)]
enum Kind {
    Limit(TimeInForce),
    /// A limit order that shows this much at a time.
    Iceberg(u64),
    /// A stop that becomes a market order once its price trades.
    Stop,
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::{LevelUpdate, TradeTick};

    /// Bots of one kind, numbered one after the other as the bots binary numbers them, do
    /// not make the same choices.
    #[test]
    fn bots_alike_choose_differently() {
        let bot = |n: u32| Bot {
            strategy: Strategy::Noise,
            account: Account {
                id: n,
                token: 0,
                max_open_orders: 1,
                messages_per_second: 1,
                funds: None,
            },
            mid: 100,
            interval: Duration::from_secs(1),
            seed: 1 + u64::from(n),
        };
        let firsts: std::collections::HashSet<u64> =
            (1..50).map(|n| rng(&bot(n)).next_u64()).collect();
        assert_eq!(firsts.len(), 49);
    }

    /// An iceberg is one the book takes: no more tranches than it allows.
    #[test]
    fn icebergs_fit_the_book() {
        let mut rng = SplitMix64::new(7);
        for _ in 0..10_000 {
            let (qty, display) = iceberg(&mut rng);
            assert!(display > 0 && qty > display);
            assert!(qty <= display * u64::from(BookConfig::DEFAULT_MAX_ICEBERG_TRANCHES));
        }
    }

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
