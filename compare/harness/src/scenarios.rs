//! The order flows every engine replays.
//!
//! They are the latency benchmark's scenarios (`crates/orderbook/benches/latency.rs`),
//! restricted to what every engine in the comparison supports: GTC limit orders, market
//! orders and cancels, plus one scenario with price moves. Stops, icebergs, IOC/FOK/post-only
//! limits and mass cancels are left out, and the book that records a stream has price
//! protection and the price band switched off.

use orderbook::workload::{Mix, SplitMix64, WorkloadConfig};
use orderbook::{BookConfig, SelfTradePolicy};

/// Participants the generator spreads orders over (`WorkloadConfig::default().owners`).
pub const PARTICIPANTS: u32 = 64;

/// The participant, in `0..PARTICIPANTS`, the generator assigned order `id` to in a stream
/// recorded with `seed`, exactly as `Workload` derives it.
///
/// The streams themselves give each side its own owner, so that self-trade prevention can
/// never fire. An engine with self-trade prevention off can use this instead, where the
/// owner matters for something else: OrderBook-rs keeps a list of order ids per user, and
/// lumping every order under one user would make each removal from it scan the whole book.
#[inline]
pub fn participant(id: u64, seed: u64) -> u32 {
    (SplitMix64::new(id ^ seed).next_u64() % u64::from(PARTICIPANTS)) as u32
}

/// One order flow.
#[derive(Clone, Copy, Debug)]
pub struct Scenario {
    /// File stem and report label.
    pub name: &'static str,
    /// One-line description.
    pub about: &'static str,
    /// The generator's settings.
    pub workload: WorkloadConfig,
    /// Commands replayed before measuring, to bring the book to its steady state.
    pub warmup: usize,
}

impl Scenario {
    /// Whether the stream contains moves, which not every engine supports.
    pub fn has_moves(&self) -> bool {
        self.workload.mix.modify > 0
    }
}

/// Limit, market and cancel only: the default mix with its modifies turned into cancels.
const LMC: Mix = Mix {
    passive_limit: 55,
    aggressive_limit: 10,
    market: 5,
    cancel: 30,
    mass_cancel: 0,
    stop: 0,
    session: 0,
    modify: 0,
};

/// Every scenario, in report order.
pub fn all() -> Vec<Scenario> {
    let base = WorkloadConfig::default();
    vec![
        Scenario {
            name: "baseline",
            about: "~5k resting orders on ~220 levels near the touch; fits in cache",
            workload: WorkloadConfig { mix: LMC, ..base },
            warmup: 500_000,
        },
        Scenario {
            name: "sweep",
            about: "40% aggressive and market flow, sizes up to 1000; multi-level fills",
            workload: WorkloadConfig {
                max_qty: 1_000,
                mix: Mix {
                    passive_limit: 40,
                    aggressive_limit: 20,
                    market: 20,
                    cancel: 20,
                    ..LMC
                },
                ..base
            },
            warmup: 500_000,
        },
        Scenario {
            name: "deep",
            about: "~850k-1M resting orders on ~10k levels; far beyond the caches",
            workload: WorkloadConfig {
                max_live: 1_000_000,
                passive_depth: 5_000,
                mix: LMC,
                ..base
            },
            warmup: 4_000_000,
        },
        Scenario {
            name: "modify",
            about: "baseline flow with 5% price moves: cancel/replace keeping the open quantity",
            // The default mix: 25% cancels and 5% modifies, which the exporter turns into
            // price moves.
            workload: base,
            warmup: 500_000,
        },
    ]
}

/// The book that records a stream: as large as the flow needs, price protection and the
/// band off, and two owners, one per side, so self-trade prevention never fires.
pub fn book_config(workload: &WorkloadConfig) -> BookConfig {
    BookConfig {
        min_price: workload.min_price,
        max_price: workload.max_price,
        max_orders: workload.max_live,
        max_owners: 2,
        max_order_qty: 1_000_000,
        max_iceberg_tranches: BookConfig::DEFAULT_MAX_ICEBERG_TRANCHES,
        price_protection: None,
        price_band: None,
        reference_price: None,
        self_trade: SelfTradePolicy::CancelResting,
        auction_on_band: false,
    }
}

#[cfg(test)]
mod tests {
    use orderbook::Command;
    use orderbook::workload::Workload;

    use super::*;

    #[test]
    fn participant_is_the_generators_owner() {
        let cfg = WorkloadConfig::default();
        assert_eq!(cfg.owners, PARTICIPANTS);
        let mut workload = Workload::new(cfg);
        let mut checked = 0;
        for _ in 0..1_000 {
            if let Command::Limit { id, owner, .. } | Command::Market { id, owner, .. } =
                workload.next_command()
            {
                assert_eq!(owner, participant(id, cfg.seed));
                checked += 1;
            }
        }
        assert!(checked > 500);
    }
}
