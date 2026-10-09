//! Differential fuzzing: on a configuration and a command stream the fuzzer chooses, the
//! engine must emit exactly the reference book's events, hold exactly its orders in the
//! same queue order, its pending stops in the same trigger order, the same trade count and
//! reference price, and keep its internal invariants (`validate()`) after every command.
//!
//! This is `tests/differential.rs` driven by coverage feedback instead of a fixed random
//! distribution, and over any configuration: bands anywhere in the `i64` range short of
//! the reference's headroom, any capacity up to 64 orders, any number of protection and
//! band ticks and iceberg tranches.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use orderbook::OrderBook;
use orderbook_fuzz::reference::ReferenceBook;
use orderbook_fuzz::{CommandInput, ConfigInput, MAX_COMMANDS, Prices, assert_matches};

#[derive(Arbitrary, Debug)]
struct Input {
    config: ConfigInput,
    commands: Vec<CommandInput>,
}

fuzz_target!(|input: Input| {
    let config = input.config.config(Prices::ForReference);
    let mut engine = OrderBook::new(config);
    let mut reference = ReferenceBook::new(config);
    let (mut got, mut want) = (Vec::new(), Vec::new());
    for (step, command) in input.commands.iter().take(MAX_COMMANDS).enumerate() {
        let command = command.command(&config);
        got.clear();
        want.clear();
        engine.process(command, &mut got);
        reference.process(command, &mut want);
        assert_eq!(got, want, "events differ at step {step}: {command:?}");
        assert_matches(&engine, &reference, step, &command);
    }
    assert_eq!(engine.digest(), engine.snapshot().digest());
});
