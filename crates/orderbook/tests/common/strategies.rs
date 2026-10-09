//! Proptest strategies: a random book configuration plus a command sequence that fits it.
//!
//! Inputs are biased toward the places bugs live: ids from a small pool (duplicates and
//! unknown ids), few owners (self-trades, and sometimes one outside the owner table), prices near bitset word boundaries and band
//! edges or just outside the band, and quantities at zero, at `max_order_qty`, just above
//! it, and at `u64::MAX`.
//!
//! Two scenarios in three change trading phases: about one command in sixteen moves the
//! book to a random phase, so calls fill up with crossing orders, uncross, and halts and
//! closes refuse orders. Only those scenarios may switch on the volatility interruption,
//! since nothing else would ever end the call it starts.

use orderbook::{
    BookConfig, Command, OrderId, OwnerId, Phase, Price, Qty, SelfTradePolicy, Side, TimeInForce,
};
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;

pub fn config() -> impl Strategy<Value = BookConfig> {
    let band = prop_oneof![
        // Dense: queues build up, most orders cross.
        Just((100, 140)),
        // Sparse: levels spread over several bitset words and summary words.
        Just((0, 9_999)),
        // Negative prices are legal.
        Just((-5_000, 5_000)),
    ];
    let protection = prop_oneof![
        2 => Just(None),
        1 => (0u32..=12).prop_map(Some),
    ];
    let policy = prop_oneof![
        Just(SelfTradePolicy::CancelResting),
        Just(SelfTradePolicy::CancelIncoming),
    ];
    // Owners are drawn from 0..4, so a table of 3 sometimes sees an owner it cannot hold.
    let max_owners = prop_oneof![Just(3u32), Just(BookConfig::DEFAULT_MAX_OWNERS)];
    // Small displays against small and huge quantities: some icebergs fit, some would need
    // too many tranches; zero disallows icebergs altogether.
    let tranches = prop_oneof![
        Just(0u32),
        Just(3),
        Just(BookConfig::DEFAULT_MAX_ICEBERG_TRANCHES)
    ];
    let price_band = prop_oneof![
        2 => Just(None),
        1 => (0u32..=12).prop_map(Some),
    ];
    // Where the band starts before the first trade: nowhere, mid-band, or at an edge.
    let reference = 0u8..4;
    (
        band,
        (1u32..=24, max_owners, tranches),
        any::<bool>(),
        (protection, price_band, reference, any::<bool>()),
        policy,
    )
        .prop_map(
            |(
                (min_price, max_price),
                (max_orders, max_owners, max_iceberg_tranches),
                huge_qty,
                (price_protection, price_band, reference, auction_on_band),
                self_trade,
            )| {
                let reference_price = match reference {
                    0 => None,
                    1 => Some(min_price + (max_price - min_price) / 2),
                    2 => Some(min_price),
                    _ => Some(max_price),
                };
                BookConfig {
                    min_price,
                    max_price,
                    max_orders,
                    max_owners,
                    max_iceberg_tranches,
                    // Either small, so `max_order_qty + 1` shows up often, or as large as the
                    // capacity allows, so level totals approach `u64::MAX`.
                    max_order_qty: if huge_qty {
                        u64::MAX / u64::from(max_orders)
                    } else {
                        50
                    },
                    price_protection,
                    price_band,
                    reference_price,
                    auction_on_band,
                    self_trade,
                }
            },
        )
}

/// A config together with a command sequence for it.
pub fn scenario(max_len: usize) -> impl Strategy<Value = (BookConfig, Vec<Command>)> {
    let sessions = prop_oneof![1 => Just(false), 2 => Just(true)];
    (config(), sessions).prop_flat_map(move |(cfg, sessions)| {
        let cfg = BookConfig {
            auction_on_band: cfg.auction_on_band && sessions,
            ..cfg
        };
        let commands = prop::collection::vec(command(cfg, sessions), 1..max_len);
        (Just(cfg), commands)
    })
}

pub fn command(cfg: BookConfig, sessions: bool) -> BoxedStrategy<Command> {
    let orders = prop_oneof![
        6 => (id(), owner(), side(), price(cfg), qty(cfg), tif(), display())
            .prop_map(|(id, owner, side, price, qty, tif, display)| Command::Limit {
                id, owner, side, price, qty, tif, display,
            }),
        1 => (id(), owner(), side(), qty(cfg))
            .prop_map(|(id, owner, side, qty)| Command::Market { id, owner, side, qty }),
        2 => (id(), owner())
            .prop_map(|(id, owner)| Command::Cancel { id, owner }),
        3 => (id(), owner(), price(cfg), qty(cfg))
            .prop_map(|(id, owner, price, qty)| Command::Modify { id, owner, price, qty }),
        1 => owner().prop_map(|owner| Command::CancelAll { owner }),
        2 => (id(), owner(), side(), price(cfg), prop::option::of(price(cfg)), qty(cfg))
            .prop_map(|(id, owner, side, trigger, limit, qty)| Command::Stop {
                id, owner, side, trigger, limit, qty,
            }),
    ];
    if !sessions {
        return orders.boxed();
    }
    prop_oneof![
        15 => orders,
        1 => phase().prop_map(|phase| Command::SetPhase { phase }),
    ]
    .boxed()
}

/// One call phase and its uncross: a few orders of one to three lots, some of them icebergs,
/// over eleven ticks, and a few stops, entered in a call phase and uncrossed by moving to
/// any other phase. The sums of so few small orders tie often, so every tie-break of the
/// auction rules gets to decide, with a reference price inside, outside or missing.
pub fn auction() -> impl Strategy<Value = (BookConfig, Vec<Command>)> {
    let reference = prop_oneof![
        1 => Just(None),
        4 => (90i64..=110).prop_map(Some),
    ];
    let policy = prop_oneof![
        Just(SelfTradePolicy::CancelResting),
        Just(SelfTradePolicy::CancelIncoming),
    ];
    let order = (id(), owner(), side(), 95i64..=105, 1u64..=3, any::<bool>()).prop_map(
        |(id, owner, side, price, qty, iceberg)| Command::Limit {
            id,
            owner,
            side,
            price,
            qty,
            tif: TimeInForce::Gtc,
            display: (iceberg && qty > 1).then_some(1),
        },
    );
    let stop = (
        id(),
        owner(),
        side(),
        90i64..=110,
        prop::option::of(90i64..=110),
    )
        .prop_map(|(id, owner, side, trigger, limit)| Command::Stop {
            id,
            owner,
            side,
            trigger,
            limit,
            qty: 1,
        });
    let entry = prop_oneof![6 => order, 1 => stop];
    let end = prop_oneof![
        4 => Just(Phase::Continuous),
        1 => Just(Phase::Halted),
        1 => Just(Phase::Closed),
    ];
    (
        reference,
        policy,
        prop::collection::vec(entry, 1..14),
        end,
        prop::collection::vec(entry_after(), 0..4),
    )
        .prop_map(|(reference_price, self_trade, entries, end, after)| {
            let cfg = BookConfig {
                reference_price,
                self_trade,
                ..BookConfig::new(90, 110, 16)
            };
            let mut commands = vec![Command::SetPhase {
                phase: Phase::Auction,
            }];
            commands.extend(entries);
            commands.push(Command::SetPhase { phase: end });
            commands.extend(after);
            (cfg, commands)
        })
}

/// A few market orders after the uncross, which can trade what it left behind.
fn entry_after() -> impl Strategy<Value = Command> {
    (id(), owner(), side(), 1u64..=3).prop_map(|(id, owner, side, qty)| Command::Market {
        id,
        owner,
        side,
        qty,
    })
}

/// Calls as often as continuous trading, so books cross and uncross; now and then a halt
/// or the close.
fn phase() -> impl Strategy<Value = Phase> {
    prop_oneof![
        3 => Just(Phase::Continuous),
        3 => Just(Phase::Auction),
        1 => Just(Phase::Halted),
        1 => Just(Phase::Closed),
    ]
}

fn id() -> impl Strategy<Value = OrderId> {
    0..40u64
}

fn owner() -> impl Strategy<Value = OwnerId> {
    0..4u32
}

/// Mostly plain orders; icebergs with small displays, so tranches run out and replenish
/// often; now and then a display of zero.
fn display() -> impl Strategy<Value = Option<Qty>> {
    prop_oneof![
        8 => Just(None),
        3 => (1..=4u64).prop_map(Some),
        1 => Just(Some(0)),
    ]
}

fn tif() -> impl Strategy<Value = TimeInForce> {
    prop_oneof![
        5 => Just(TimeInForce::Gtc),
        1 => Just(TimeInForce::Ioc),
        1 => Just(TimeInForce::Fok),
        1 => Just(TimeInForce::PostOnly),
    ]
}

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn price(cfg: BookConfig) -> BoxedStrategy<Price> {
    let (min, max) = (cfg.min_price, cfg.max_price);
    let outside = prop::sample::select(vec![min - 1, max + 1, Price::MIN, Price::MAX]);
    if max - min <= 64 {
        return prop_oneof![20 => min..=max, 1 => outside].boxed();
    }
    // Clusters around band edges, the middle, and level indices at bitset word (64) and
    // summary word (4096) boundaries, so the next-best search crosses them.
    let centers: Vec<Price> = [0, 63, 64, 127, 4_095, 4_096, (max - min) / 2, max - min]
        .iter()
        .map(|offset| min + offset)
        .filter(|p| *p <= max)
        .collect();
    let clustered = (prop::sample::select(centers), -3i64..=3)
        .prop_map(move |(center, delta)| (center + delta).clamp(min, max));
    prop_oneof![20 => clustered, 1 => outside].boxed()
}

fn qty(cfg: BookConfig) -> BoxedStrategy<Qty> {
    let max = cfg.max_order_qty;
    let small = 30.min(max);
    prop_oneof![
        1 => Just(0),
        24 => 1..=small,
        // Small lots add up to equal sums often, so auction prices tie on volume and
        // surplus and the later tie-breaks decide.
        6 => 1..=2u64.min(max),
        3 => max.saturating_sub(3).max(1)..=max,
        1 => Just(max.saturating_add(1)),
        1 => Just(u64::MAX),
    ]
    .boxed()
}
