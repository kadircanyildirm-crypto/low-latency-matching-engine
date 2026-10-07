//! Long runs of realistic synthetic flow: differential against the reference book, and a
//! determinism check.

mod common;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use common::reference::ReferenceBook;
use common::snapshot;
use orderbook::workload::{EventCounts, Workload, WorkloadConfig};
use orderbook::{EventSink, OrderBook};

/// A narrow band keeps `validate()` (which walks every level) cheap.
fn config() -> WorkloadConfig {
    WorkloadConfig {
        min_price: 0,
        max_price: 2_000,
        initial_mid: 1_000,
        max_live: 2_000,
        ..WorkloadConfig::default()
    }
}

#[test]
fn synthetic_flow_matches_reference() {
    let cfg = config();
    let mut engine = OrderBook::new(cfg.book_config());
    let mut reference = ReferenceBook::new(cfg.book_config());
    let mut workload = Workload::new(cfg);
    let mut counts = EventCounts::default();
    let (mut got, mut want) = (Vec::new(), Vec::new());

    for step in 0..200_000 {
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
        if step % 10_000 == 0 {
            engine.validate().unwrap();
            assert_eq!(snapshot(&engine), reference.snapshot(), "step {step}");
            // The client rebuilt the set of resting orders from events alone.
            assert_eq!(workload.live_orders(), engine.order_count(), "step {step}");
        }
    }
    engine.validate().unwrap();
    assert_eq!(snapshot(&engine), reference.snapshot());

    // The client only cancels and modifies orders it knows are resting and stays under
    // the order limit, so nothing is ever rejected...
    assert_eq!(counts.rejected, 0, "{counts:?}");
    // ...and the run exercised every path.
    assert!(counts.trades > 10_000, "{counts:?}");
    assert!(counts.rested > 10_000, "{counts:?}");
    assert!(counts.cancelled > 10_000, "{counts:?}");
    assert!(counts.modified > 1_000, "{counts:?}");
}

#[test]
fn same_commands_produce_the_same_events() {
    let fingerprint = || {
        let cfg = config();
        let mut book = OrderBook::new(cfg.book_config());
        let mut workload = Workload::new(cfg);
        let mut events = Vec::new();
        let mut hasher = DefaultHasher::new();
        for _ in 0..100_000 {
            events.clear();
            book.process(workload.next_command(), &mut events);
            events.iter().for_each(|e| workload.observe(e));
            events.hash(&mut hasher);
        }
        (hasher.finish(), common::snapshot(&book))
    };
    assert_eq!(fingerprint(), fingerprint());
}
