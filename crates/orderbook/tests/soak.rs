//! Long runs of realistic synthetic flow: differential against the reference book, and
//! determinism checks.

mod common;

use common::reference::ReferenceBook;
use common::{Fnv, snapshot};
use orderbook::workload::{EventCounts, Mix, Workload, WorkloadConfig};
use orderbook::{BookConfig, EventSink, OrderBook, SelfTradePolicy};

fn config() -> WorkloadConfig {
    WorkloadConfig {
        min_price: 0,
        max_price: 2_000,
        initial_mid: 1_000,
        max_live: 2_000,
        ..WorkloadConfig::default()
    }
}

fn soak(cfg: WorkloadConfig, book_cfg: BookConfig, steps: usize) -> EventCounts {
    let mut engine = OrderBook::new(book_cfg);
    let mut reference = ReferenceBook::new(book_cfg);
    let mut workload = Workload::new(cfg);
    let mut counts = EventCounts::default();
    let (mut got, mut want) = (Vec::new(), Vec::new());

    for step in 0..steps {
        let command = workload.next_command();
        got.clear();
        want.clear();
        engine.process(command, &mut got);
        reference.process(command, &mut want);
        assert_eq!(got, want, "events differ at step {step}: {command:?}");
        for event in &got {
            counts.on_event(*event);
            workload.observe(event);
        }
        if step % 5_000 == 0 {
            engine.validate().unwrap();
            assert_eq!(snapshot(&engine), reference.snapshot(), "step {step}");
            // The participants rebuilt the set of resting orders from events alone.
            assert_eq!(workload.live_orders(), engine.order_count(), "step {step}");
        }
    }
    engine.validate().unwrap();
    assert_eq!(snapshot(&engine), reference.snapshot());
    assert_eq!(workload.live_orders(), engine.order_count());
    counts
}

#[test]
fn synthetic_flow_matches_reference() {
    let cfg = config();
    let counts = soak(cfg, cfg.book_config(), 200_000);

    // Participants only cancel and modify orders they know are resting and stay under the
    // order limit, so nothing is rejected...
    assert_eq!(counts.rejected, 0, "{counts:?}");
    // ...and the run exercised every path, self-trade prevention included. Its protection
    // band is too wide to stop anything; `protected_config` covers protection.
    assert!(counts.trades > 10_000, "{counts:?}");
    assert!(counts.rested > 10_000, "{counts:?}");
    assert!(counts.cancelled > 10_000, "{counts:?}");
    assert!(counts.modified > 1_000, "{counts:?}");
    assert!(counts.self_trade_cancels > 100, "{counts:?}");
}

/// Few owners for frequent self-trades under `CancelIncoming`, and many market orders against
/// a one-tick protection band for frequent protection stops and rejections.
fn protected_config() -> (WorkloadConfig, BookConfig) {
    let cfg = WorkloadConfig {
        owners: 4,
        seed: 7,
        mix: Mix {
            passive_limit: 50,
            aggressive_limit: 5,
            market: 20,
            cancel: 20,
            modify: 5,
        },
        ..config()
    };
    let book_cfg = BookConfig {
        self_trade: SelfTradePolicy::CancelIncoming,
        price_protection: Some(1),
        ..cfg.book_config()
    };
    (cfg, book_cfg)
}

#[test]
fn synthetic_flow_matches_reference_under_cancel_incoming() {
    let (cfg, book_cfg) = protected_config();
    let counts = soak(cfg, book_cfg, 100_000);
    assert!(counts.self_trade_cancels > 100, "{counts:?}");
    assert!(counts.protection_cancels > 100, "{counts:?}");
    // Aggressive limits priced more than a tick through the opposite best.
    assert!(counts.rejected > 100, "{counts:?}");
}

/// Fingerprint of every event and the final book after 100k commands of synthetic flow,
/// and the final book's [`OrderBook::digest`].
fn fingerprint(cfg: WorkloadConfig, book_cfg: BookConfig) -> (u64, u64) {
    let mut book = OrderBook::new(book_cfg);
    let mut workload = Workload::new(cfg);
    let mut events = Vec::new();
    let mut hash = Fnv::new();
    for _ in 0..100_000 {
        events.clear();
        book.process(workload.next_command(), &mut events);
        for event in &events {
            workload.observe(event);
            hash.event(event);
        }
    }
    for (price, queue) in snapshot(&book).iter().flatten() {
        hash.write_u64(*price as u64);
        for order in queue {
            [order.id, u64::from(order.owner), order.leaves, order.filled]
                .iter()
                .for_each(|&v| hash.write_u64(v));
        }
    }
    (hash.finish(), book.digest())
}

#[test]
fn same_commands_produce_the_same_events() {
    let cfg = config();
    assert_eq!(
        fingerprint(cfg, cfg.book_config()),
        fingerprint(cfg, cfg.book_config())
    );
}

/// Pinned values: CI runs this on Linux, Windows and macOS, so a match proves the engine and
/// its state digest are deterministic across platforms, which replay on a standby machine
/// depends on. Between them the two runs cover both self-trade policies, protection stops
/// and rejections. They also flag any change in behaviour; if a change is intended, update
/// the constants.
#[test]
fn output_matches_the_golden_fingerprints() {
    let cfg = config();
    let (events, digest) = fingerprint(cfg, cfg.book_config());
    assert_eq!(events, GOLDEN_DEFAULT, "default flow: got {events:#018x}");
    assert_eq!(
        digest, GOLDEN_DEFAULT_DIGEST,
        "default flow digest: got {digest:#018x}"
    );

    let (cfg, book_cfg) = protected_config();
    let (events, digest) = fingerprint(cfg, book_cfg);
    assert_eq!(
        events, GOLDEN_PROTECTED,
        "protected flow: got {events:#018x}"
    );
    assert_eq!(
        digest, GOLDEN_PROTECTED_DIGEST,
        "protected flow digest: got {digest:#018x}"
    );
}

const GOLDEN_DEFAULT: u64 = 0xa83b_9f96_80cf_a652;
const GOLDEN_DEFAULT_DIGEST: u64 = 0x81af_9e3f_bb93_84c6;
const GOLDEN_PROTECTED: u64 = 0x3c49_10ff_8e00_3caa;
const GOLDEN_PROTECTED_DIGEST: u64 = 0xc0c3_b878_27a9_adc9;
