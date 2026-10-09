//! Property test: on random configurations and command sequences, the engine must emit
//! exactly the events of the reference book, end up with exactly the same orders in the
//! same queue order, and keep its internal invariants after every single command.

mod common;

use common::reference::ReferenceBook;
use common::snapshot;
use common::strategies::{auction, scenario};
use orderbook::{BookConfig, Command, OrderBook, Side};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1_000))]

    #[test]
    fn engine_matches_reference((cfg, commands) in scenario(300)) {
        if let Err(difference) = compare(cfg, &commands) {
            prop_assert!(false, "{}", difference);
        }
    }

    /// Small calls whose sums tie often, so that every auction rule gets to decide.
    #[test]
    fn engine_uncrosses_like_the_reference((cfg, commands) in auction()) {
        if let Err(difference) = compare(cfg, &commands) {
            prop_assert!(false, "{}", difference);
        }
    }
}

fn compare(cfg: BookConfig, commands: &[Command]) -> Result<(), String> {
    let mut engine = OrderBook::new(cfg);
    let mut reference = ReferenceBook::new(cfg);
    let (mut got, mut want) = (Vec::new(), Vec::new());
    for (step, &command) in commands.iter().enumerate() {
        got.clear();
        want.clear();
        engine.process(command, &mut got);
        reference.process(command, &mut want);
        let differs = |what: &str| format!("{what} differ at step {step}: {command:?}");
        if got != want {
            return Err(format!(
                "{}\n  engine:    {got:?}\n  reference: {want:?}",
                differs("events")
            ));
        }
        if snapshot(&engine) != reference.snapshot() {
            return Err(differs("books"));
        }
        if (
            engine.trade_count(),
            engine.reference_price(),
            engine.phase(),
        ) != (
            reference.trade_count(),
            reference.reference_price(),
            reference.phase(),
        ) {
            return Err(differs("trade counts, reference prices or phases"));
        }
        for side in [Side::Buy, Side::Sell] {
            if engine.stops(side).collect::<Vec<_>>() != reference.stops(side) {
                return Err(differs("stops"));
            }
        }
        engine
            .validate()
            .map_err(|violation| format!("invariant broken at step {step}: {violation}"))?;
    }
    Ok(())
}
