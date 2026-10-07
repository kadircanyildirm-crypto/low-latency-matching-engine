//! Property test: on random command sequences the engine must emit exactly the events of
//! the reference book, end up with exactly the same orders in the same queue order, and keep
//! its internal invariants after every single command.
//!
//! Ids come from a small pool and prices from a narrow band (plus some out-of-band values),
//! so duplicates, unknown ids, crossing orders, multi-level sweeps, modifies of live orders
//! and a full book all show up often.

mod common;

use common::reference::ReferenceBook;
use common::snapshot;
use orderbook::{BookConfig, Command, OrderBook, OrderId, Price, Qty, Side};
use proptest::prelude::*;

const CFG: BookConfig = BookConfig {
    min_price: 100,
    max_price: 140,
    max_orders: 24,
};

fn id() -> impl Strategy<Value = OrderId> {
    0..40u64
}

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn price() -> impl Strategy<Value = Price> {
    prop_oneof![
        20 => 100..=140i64,
        1 => prop::sample::select(vec![99, 141, Price::MIN, Price::MAX]),
    ]
}

fn qty() -> impl Strategy<Value = Qty> {
    prop_oneof![
        1 => Just(0u64),
        30 => 1..=30u64,
    ]
}

fn command() -> impl Strategy<Value = Command> {
    prop_oneof![
        6 => (id(), side(), price(), qty()).prop_map(|(id, side, price, qty)| Command::Limit { id, side, price, qty }),
        1 => (id(), side(), qty()).prop_map(|(id, side, qty)| Command::Market { id, side, qty }),
        2 => id().prop_map(|id| Command::Cancel { id }),
        2 => (id(), price(), qty()).prop_map(|(id, price, qty)| Command::Modify { id, price, qty }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1_000))]

    #[test]
    fn engine_matches_reference(commands in prop::collection::vec(command(), 1..300)) {
        let mut engine = OrderBook::new(CFG);
        let mut reference = ReferenceBook::new(CFG);
        let (mut got, mut want) = (Vec::new(), Vec::new());
        for (step, &command) in commands.iter().enumerate() {
            got.clear();
            want.clear();
            engine.process(command, &mut got);
            reference.process(command, &mut want);
            prop_assert_eq!(&got, &want, "events differ at step {}: {:?}", step, command);
            prop_assert_eq!(snapshot(&engine), reference.snapshot(), "books differ at step {}", step);
            if let Err(violation) = engine.validate() {
                prop_assert!(false, "invariant broken at step {}: {}", step, violation);
            }
        }
    }
}
