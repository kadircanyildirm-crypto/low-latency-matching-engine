//! An experiment, not a check: how well each price control keeps a book two-sided under
//! order flow that ignores the book.
//!
//! The synthetic participants price their orders off a mid that random-walks on its own, as
//! a fair value moved by news would, and four owners trading under `CancelIncoming` leave
//! many stale orders behind. That is a harsh test for any price control, and the numbers
//! back three claims in `docs/DESIGN.md`:
//!
//! - Price protection, measured from the opposite best, lets a stale order far from the
//!   market anchor the band: orders priced at the market get rejected and one side of the
//!   book stays empty for long stretches.
//! - A price band measured from the last trade cannot be moved by stale orders, but once
//!   the market drifts more than the band away from the last trade, nothing can trade to
//!   move it, and the book freezes.
//! - Exchanges re-anchor a band with a trading halt and a reopening auction. Here the band
//!   interrupts trading with a call when it stops a market order (`auction_on_band`), and
//!   the experiment, acting as the sequencer, reopens with an uncross a fixed number of
//!   commands later. The participants keep sending what they always send, so a call
//!   refuses their market, immediate-or-cancel and fill-or-kill orders.
//!
//! Run with `cargo test --release --test market_health -- --ignored --nocapture`.

use orderbook::workload::{EventCounts, Mix, Workload, WorkloadConfig};
use orderbook::{BookConfig, Command, EventSink, OrderBook, Phase, SelfTradePolicy};

/// A price control: protection ticks, band ticks, and how many commands a call started by
/// the band lasts before the reopening uncross (`None`: the band does not start calls).
struct Control {
    name: String,
    protection: Option<u32>,
    band: Option<u32>,
    reopen_after: Option<u32>,
}

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
            session: 0,
        },
        ..WorkloadConfig::default()
    };
    let control = |name: String, protection, band, reopen_after| Control {
        name,
        protection,
        band,
        reopen_after,
    };
    let mut controls = vec![control("none".into(), None, None, None)];
    for ticks in [1, 2, 4] {
        controls.push(control(
            format!("protection {ticks}"),
            Some(ticks),
            None,
            None,
        ));
    }
    for ticks in [1, 2, 4, 8, 50] {
        controls.push(control(format!("band {ticks}"), None, Some(ticks), None));
    }
    for reopen in [100, 1_000] {
        for ticks in [1, 2, 4, 8, 50] {
            let name = format!("band {ticks} + call {reopen}");
            controls.push(control(name, None, Some(ticks), Some(reopen)));
        }
    }

    println!(
        "control              one-sided  in call  rejects  protection stops  band stops  calls  trades"
    );
    for control in controls {
        let book_cfg = BookConfig {
            price_protection: control.protection,
            price_band: control.band,
            reference_price: Some(cfg.initial_mid),
            auction_on_band: control.reopen_after.is_some(),
            self_trade: SelfTradePolicy::CancelIncoming,
            ..cfg.book_config()
        };
        let mut book = OrderBook::new(book_cfg);
        let mut workload = Workload::new(cfg);
        let mut counts = EventCounts::default();
        let mut events = Vec::new();
        let (mut samples, mut one_sided, mut in_call) = (0u32, 0u32, 0u32);
        let mut call_length = 0;
        for step in 0..600_000 {
            let mut commands = [Some(workload.next_command()), None];
            // The sequencer ends each call the band started once it has lasted long enough.
            if book.phase() == Phase::Auction {
                call_length += 1;
                if control.reopen_after == Some(call_length) {
                    call_length = 0;
                    commands[1] = Some(Command::SetPhase {
                        phase: Phase::Continuous,
                    });
                }
            }
            for command in commands.into_iter().flatten() {
                events.clear();
                book.process(command, &mut events);
                for event in &events {
                    workload.observe(event);
                    if step >= 100_000 {
                        counts.on_event(*event);
                    }
                }
            }
            if step >= 100_000 && step % 1_000 == 0 {
                samples += 1;
                if book.best_bid().is_none() || book.best_ask().is_none() {
                    one_sided += 1;
                }
                if book.phase() == Phase::Auction {
                    in_call += 1;
                }
            }
        }
        println!(
            "{:<20} {:>9}% {:>7}% {:>8} {:>17} {:>11} {:>6} {:>7}",
            control.name,
            one_sided * 100 / samples,
            in_call * 100 / samples,
            counts.rejected,
            counts.protection_cancels,
            counts.band_cancels,
            counts.calls,
            counts.trades
        );
    }
}
