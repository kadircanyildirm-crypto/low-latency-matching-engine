//! Where the engine's time goes: measuring changes nothing the engine hands on, and counts
//! what it measured.

mod common;

use std::path::Path;

use engine::sim::SimStorage;
use engine::{Discard, Engine, EngineConfig, MATCH_SAMPLE, Seq, Timings};
use orderbook::{BookConfig, Command, Event, OrderBook, Side, TimeInForce};

const DIR: &str = "data";

fn open(book: BookConfig) -> Engine<SimStorage> {
    let config = EngineConfig::new(book);
    Engine::open_with(SimStorage::new(), Path::new(DIR), config, &mut Discard)
        .unwrap()
        .0
}

/// The events a book on its own gives for `commands`, numbered from 1.
fn bare(book: BookConfig, commands: &[Command]) -> Vec<(Seq, Event)> {
    let mut book = OrderBook::new(book);
    let mut all = Vec::new();
    for (seq, &command) in (1..).zip(commands) {
        let mut events = Vec::new();
        book.process(command, &mut events);
        all.extend(events.into_iter().map(|event| (seq, event)));
    }
    all
}

/// Commands measured alone hand on the same events, in the same order, as the others; and
/// the engine counts every batch and command, and the one command in 64 it measured.
#[test]
fn measuring_changes_nothing_and_counts_what_it_measured() {
    let (book, commands) = common::flow(7, 5_000);
    let mut engine = open(book);
    let mut events: Vec<(Seq, Event)> = Vec::new();
    for batch in commands.chunks(7) {
        engine.submit_batch(batch, &mut events).unwrap();
    }
    assert_eq!(events, bare(book, &commands));
    let timings = engine.take_timings();
    assert_eq!(timings.batches, 5_000_u64.div_ceil(7));
    assert_eq!(timings.commands, 5_000);
    assert_eq!(timings.matched, 5_000 / MATCH_SAMPLE);
    assert!(timings.apply_ns >= timings.match_ns, "{timings:?}");
    // Taken once.
    assert_eq!(engine.take_timings(), Timings::default());
}

/// A measured command with more events than are reserved for it hands them all on, in
/// order, and is not counted: the time would include handing them on.
#[test]
fn a_command_with_too_many_events_is_not_measured() {
    let book = BookConfig::new(1, 1_000, 1_024);
    // One-lot offers at one price, then, on a measured sequence number, a buy that takes
    // 300 of them: 300 trades.
    let sweep = 6 * MATCH_SAMPLE;
    let mut commands: Vec<Command> = (1..sweep)
        .map(|id| Command::Limit {
            id,
            owner: 0,
            side: Side::Sell,
            price: 100,
            qty: 1,
            tif: TimeInForce::Gtc,
            display: None,
        })
        .collect();
    commands.push(Command::Market {
        id: sweep,
        owner: 1,
        side: Side::Buy,
        qty: 300,
    });
    let mut engine = open(book);
    let mut events: Vec<(Seq, Event)> = Vec::new();
    engine.submit_batch(&commands, &mut events).unwrap();
    let swept = events.iter().filter(|(seq, _)| *seq == sweep).count();
    assert!(swept > 256, "{swept} events");
    assert_eq!(events, bare(book, &commands));
    // The offers at 64 to 320 were measured; the sweep was not.
    assert_eq!(engine.take_timings().matched, sweep / MATCH_SAMPLE - 1);
}
