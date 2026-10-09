//! Snapshot round trip at a cut point the fuzzer chooses: the snapshot of a live book must
//! restore, the restored book must be healthy and in the same state (equal snapshot, equal
//! digest), and from then on it must emit exactly the original's events for every command.
//!
//! The reference book does not take part, so the band may lie anywhere in the `i64` range,
//! `i64::MIN` and `i64::MAX` included; `validate()` runs after every command on both books.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use orderbook::{Command, OrderBook};
use orderbook_fuzz::{CommandInput, ConfigInput, MAX_COMMANDS, Prices};

#[derive(Arbitrary, Debug)]
struct Input {
    config: ConfigInput,
    /// Where to take the snapshot, modulo the number of commands plus one.
    cut: u16,
    commands: Vec<CommandInput>,
}

fn check(book: &OrderBook, what: &str, step: usize) {
    if let Err(violation) = book.validate() {
        panic!("{what} book broken at step {step}: {violation}");
    }
}

fuzz_target!(|input: Input| {
    let config = input.config.config(Prices::Unrestricted);
    let commands: Vec<Command> = input
        .commands
        .iter()
        .take(MAX_COMMANDS)
        .map(|command| command.command(&config))
        .collect();
    let cut = usize::from(input.cut) % (commands.len() + 1);

    let mut original = OrderBook::new(config);
    let mut events = Vec::new();
    for (step, &command) in commands[..cut].iter().enumerate() {
        original.process(command, &mut events);
        check(&original, "original", step);
    }

    let snapshot = original.snapshot();
    assert_eq!(snapshot.digest(), original.digest());
    let mut restored = match OrderBook::restore(&snapshot) {
        Ok(book) => book,
        Err(error) => panic!("a live book's snapshot was refused: {error}"),
    };
    check(&restored, "restored", cut);
    assert_eq!(restored.snapshot(), snapshot);
    assert_eq!(restored.digest(), original.digest());

    let (mut got, mut want) = (Vec::new(), Vec::new());
    for (step, &command) in commands.iter().enumerate().skip(cut) {
        got.clear();
        want.clear();
        restored.process(command, &mut got);
        original.process(command, &mut want);
        assert_eq!(got, want, "events differ at step {step}: {command:?}");
        check(&original, "original", step);
        check(&restored, "restored", step);
    }
    assert_eq!(restored.snapshot(), original.snapshot());
    assert_eq!(restored.digest(), original.digest());
});
