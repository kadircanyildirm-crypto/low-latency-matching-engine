//! Proptest strategies: a random book configuration plus a command sequence that fits it.
//!
//! Inputs are biased toward the places bugs live: ids from a small pool (duplicates and
//! unknown ids), few owners (self-trades, and sometimes one outside the owner table), prices near bitset word boundaries and band
//! edges or just outside the band, and quantities at zero, at `max_order_qty`, just above
//! it, and at `u64::MAX`.

use orderbook::{BookConfig, Command, OrderId, OwnerId, Price, Qty, SelfTradePolicy, Side};
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
    (
        band,
        1u32..=24,
        max_owners,
        any::<bool>(),
        protection,
        policy,
    )
        .prop_map(
            |(
                (min_price, max_price),
                max_orders,
                max_owners,
                huge_qty,
                price_protection,
                self_trade,
            )| {
                BookConfig {
                    min_price,
                    max_price,
                    max_orders,
                    max_owners,
                    // Either small, so `max_order_qty + 1` shows up often, or as large as the
                    // capacity allows, so level totals approach `u64::MAX`.
                    max_order_qty: if huge_qty {
                        u64::MAX / u64::from(max_orders)
                    } else {
                        50
                    },
                    price_protection,
                    self_trade,
                }
            },
        )
}

/// A config together with a command sequence for it.
pub fn scenario(max_len: usize) -> impl Strategy<Value = (BookConfig, Vec<Command>)> {
    config().prop_flat_map(move |cfg| (Just(cfg), prop::collection::vec(command(cfg), 1..max_len)))
}

pub fn command(cfg: BookConfig) -> BoxedStrategy<Command> {
    prop_oneof![
        6 => (id(), owner(), side(), price(cfg), qty(cfg))
            .prop_map(|(id, owner, side, price, qty)| Command::Limit { id, owner, side, price, qty }),
        1 => (id(), owner(), side(), qty(cfg))
            .prop_map(|(id, owner, side, qty)| Command::Market { id, owner, side, qty }),
        2 => (id(), owner())
            .prop_map(|(id, owner)| Command::Cancel { id, owner }),
        3 => (id(), owner(), price(cfg), qty(cfg))
            .prop_map(|(id, owner, price, qty)| Command::Modify { id, owner, price, qty }),
        1 => owner().prop_map(|owner| Command::CancelAll { owner }),
    ]
    .boxed()
}

fn id() -> impl Strategy<Value = OrderId> {
    0..40u64
}

fn owner() -> impl Strategy<Value = OwnerId> {
    0..4u32
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
        3 => max.saturating_sub(3).max(1)..=max,
        1 => Just(max.saturating_add(1)),
        1 => Just(u64::MAX),
    ]
    .boxed()
}
