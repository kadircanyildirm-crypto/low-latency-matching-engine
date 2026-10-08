//! Property test: on random configurations and command sequences, the engine must emit
//! exactly the events of the reference book, end up with exactly the same orders in the
//! same queue order, and keep its internal invariants after every single command.

mod common;

use common::reference::ReferenceBook;
use common::snapshot;
use common::strategies::scenario;
use orderbook::{OrderBook, Side};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1_000))]

    #[test]
    fn engine_matches_reference((cfg, commands) in scenario(300)) {
        let mut engine = OrderBook::new(cfg);
        let mut reference = ReferenceBook::new(cfg);
        let (mut got, mut want) = (Vec::new(), Vec::new());
        for (step, &command) in commands.iter().enumerate() {
            got.clear();
            want.clear();
            engine.process(command, &mut got);
            reference.process(command, &mut want);
            prop_assert_eq!(&got, &want, "events differ at step {}: {:?}", step, command);
            prop_assert_eq!(snapshot(&engine), reference.snapshot(), "books differ at step {}", step);
            prop_assert_eq!(engine.trade_count(), reference.trade_count());
            prop_assert_eq!(engine.reference_price(), reference.reference_price());
            for side in [Side::Buy, Side::Sell] {
                let stops: Vec<_> = engine.stops(side).collect();
                prop_assert_eq!(stops, reference.stops(side), "stops differ at step {}", step);
            }
            if let Err(violation) = engine.validate() {
                prop_assert!(false, "invariant broken at step {}: {}", step, violation);
            }
        }
    }
}
