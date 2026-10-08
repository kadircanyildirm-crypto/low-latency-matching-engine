//! An experiment, not a check: how well each price control keeps a book two-sided under
//! order flow that ignores the book.
//!
//! The synthetic participants price their orders off a mid that random-walks on its own, as
//! a fair value moved by news would, and four owners trading under `CancelIncoming` leave
//! many stale orders behind. That is a harsh test for any price control, and the numbers
//! back two claims in `docs/DESIGN.md`:
//!
//! - Price protection, measured from the opposite best, lets a stale order far from the
//!   market anchor the band: orders priced at the market get rejected and one side of the
//!   book stays empty for long stretches.
//! - A price band measured from the last trade cannot be moved by stale orders, but once
//!   the market drifts more than the band away from the last trade, nothing can trade to
//!   move it, and the book freezes. Exchanges re-anchor with a trading halt and a reopening
//!   auction.
//!
//! Run with `cargo test --release --test market_health -- --ignored --nocapture`.

use orderbook::workload::{EventCounts, Mix, Workload, WorkloadConfig};
use orderbook::{BookConfig, EventSink, OrderBook, SelfTradePolicy};

#[test]
#[ignore = "an experiment that prints a table; run it explicitly"]
fn one_sided_time_under_each_price_control() {
    let cfg = WorkloadConfig {
        owners: 4,
        mix: Mix {
            passive_limit: 50,
            aggressive_limit: 5,
            market: 20,
            cancel: 20,
            mass_cancel: 0,
            stop: 0,
            modify: 5,
        },
        ..WorkloadConfig::default()
    };
    let mut controls: Vec<(String, Option<u32>, Option<u32>)> = vec![("none".into(), None, None)];
    for ticks in [1, 2, 4] {
        controls.push((format!("protection {ticks}"), Some(ticks), None));
    }
    for ticks in [1, 2, 4, 8, 50] {
        controls.push((format!("band {ticks}"), None, Some(ticks)));
    }

    println!("control         one-sided  rejects  protection stops  band stops  trades");
    for (name, price_protection, price_band) in controls {
        let book_cfg = BookConfig {
            price_protection,
            price_band,
            reference_price: Some(cfg.initial_mid),
            self_trade: SelfTradePolicy::CancelIncoming,
            ..cfg.book_config()
        };
        let mut book = OrderBook::new(book_cfg);
        let mut workload = Workload::new(cfg);
        let mut counts = EventCounts::default();
        let mut events = Vec::new();
        let (mut samples, mut one_sided) = (0u32, 0u32);
        for step in 0..600_000 {
            events.clear();
            book.process(workload.next_command(), &mut events);
            for event in &events {
                workload.observe(event);
                if step >= 100_000 {
                    counts.on_event(*event);
                }
            }
            if step >= 100_000 && step % 1_000 == 0 {
                samples += 1;
                if book.best_bid().is_none() || book.best_ask().is_none() {
                    one_sided += 1;
                }
            }
        }
        println!(
            "{name:<14} {:>9}% {:>8} {:>17} {:>11} {:>7}",
            one_sided * 100 / samples,
            counts.rejected,
            counts.protection_cancels,
            counts.band_cancels,
            counts.trades
        );
    }
}
