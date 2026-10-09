//! An engine split into its writer and its matcher, as a pipeline runs them: the writer ahead
//! by any number of commands, snapshots taken by the matcher after the writer syncs, and the
//! segments they free removed by the writer later. A power failure at any point leaves a
//! journal and snapshots that recover a prefix holding every durable command, and the
//! matcher's events are the engine's.

mod common;

use std::path::Path;

use engine::sim::{CrashModel, SimStorage};
use engine::{Discard, Engine, EngineConfig, Error, Seq, SyncPolicy};
use orderbook::Event;
use orderbook::workload::SplitMix64;

const DIR: &str = "data";

#[test]
fn a_split_engine_recovers_like_a_whole_one() {
    let (book, commands) = common::flow(80, 400);
    let digests = common::digests(book, &commands);
    // The events of each command, from an engine that is not split.
    let whole: Vec<(Seq, Event)> = {
        let storage = SimStorage::new();
        let config = EngineConfig::new(book);
        let (mut engine, _) =
            Engine::open_with(storage, Path::new(DIR), config, &mut Discard).unwrap();
        let mut events = Vec::new();
        engine.submit_batch(&commands, &mut events).unwrap();
        events
    };
    for seed in 0..300 {
        let mut rng = SplitMix64::new(seed);
        let config = EngineConfig {
            sync: if seed % 3 == 0 {
                SyncPolicy::Always
            } else {
                SyncPolicy::Os
            },
            // Small segments roll, and sync, often; a large one never does here.
            segment_capacity: if rng.below(2) == 0 {
                1 + rng.below(40) as u32
            } else {
                1_000
            },
            keep_snapshots: 1 + rng.below(3) as usize,
            ..EngineConfig::new(book)
        };
        let storage = SimStorage::new();
        let (engine, _) =
            Engine::open_with(storage.clone(), Path::new(DIR), config, &mut Discard).unwrap();
        let (mut writer, mut matcher) = engine.split();
        let mut events = Vec::new();
        // Segments a snapshot freed, not yet removed.
        let mut trim: Option<Seq> = None;
        let stop = 1 + rng.below(commands.len() as u64) as usize;
        while matcher.last_seq() < stop as u64 {
            match rng.below(10) {
                // The writer runs ahead.
                0..=3 => {
                    let from = writer.last_seq() as usize;
                    let to = (from + 1 + rng.below(20) as usize).min(commands.len());
                    writer.write(&commands[from..to]).unwrap();
                }
                // The matcher catches up with some of it.
                4..=7 => {
                    let behind = writer.last_seq() - matcher.last_seq();
                    for _ in 0..rng.below(behind + 1) {
                        let seq = matcher.last_seq() + 1;
                        matcher
                            .apply(seq, commands[seq as usize - 1], &mut events)
                            .unwrap();
                    }
                }
                // A snapshot of what the matcher has applied, once the writer has synced it.
                8 => {
                    writer.sync().unwrap();
                    if let Some(seq) = matcher.snapshot(writer.durable_seq()).unwrap() {
                        trim = Some(seq);
                    }
                }
                // The writer removes what a snapshot freed, some time later.
                _ => {
                    if let Some(seq) = trim.take() {
                        writer.remove_through(seq).unwrap();
                    }
                }
            }
        }
        assert_eq!(&events[..], &whole[..events.len()], "seed {seed}");
        assert_eq!(
            matcher.book().digest(),
            digests[matcher.last_seq() as usize]
        );
        // The power fails.
        let durable = writer.durable_seq();
        let journaled = writer.last_seq();
        let model = if rng.below(2) == 0 {
            CrashModel::AnyOrder
        } else {
            CrashModel::InOrder
        };
        let crashed = storage.crash(&mut rng, model);
        drop((writer, matcher));
        let (engine, _) = Engine::open_with(crashed, Path::new(DIR), config, &mut Discard)
            .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let kept = engine.last_seq();
        assert!(
            kept >= durable && kept <= journaled,
            "seed {seed}: {kept} of {durable}..={journaled}"
        );
        assert_eq!(
            engine.book().digest(),
            digests[kept as usize],
            "seed {seed}"
        );
    }
}

/// The halves keep the engine's rules: commands in sequence, no snapshot of what was never
/// applied, and a poisoned writer refuses everything.
#[test]
fn the_halves_keep_the_rules() {
    let (book, commands) = common::flow(81, 20);
    let storage = SimStorage::new();
    let config = EngineConfig {
        segment_capacity: 8,
        ..EngineConfig::new(book)
    };
    let (engine, _) =
        Engine::open_with(storage.clone(), Path::new(DIR), config, &mut Discard).unwrap();
    let (mut writer, mut matcher) = engine.split();
    assert_eq!(writer.write(&commands[..10]).unwrap(), 10);
    assert_eq!(writer.write(&[]).unwrap(), 10);
    // Nothing applied yet: the snapshot would be of the state recovery started from.
    assert_eq!(matcher.snapshot(writer.durable_seq()).unwrap(), None);
    for (seq, &command) in (1..).zip(&commands[..10]) {
        matcher.apply(seq, command, &mut Discard).unwrap();
    }
    assert!(!matcher.snapshot_due());
    // A failed automatic snapshot is told once, and puts the next one off.
    matcher.postpone_snapshot(Error::Poisoned);
    assert!(matches!(matcher.take_failure(), Some(Error::Poisoned)));
    assert!(matcher.take_failure().is_none());
    // One snapshot of two kept: nothing to remove yet.
    assert_eq!(matcher.snapshot(writer.durable_seq()).unwrap(), None);
    assert_eq!(matcher.last_snapshot(), 10);
    assert_eq!(matcher.config().segment_capacity, 8);
    assert!(matcher.take_failure().is_none());
    assert!(!writer.is_poisoned());
    storage.set_failing(true);
    assert!(writer.write(&commands[10..]).is_err());
    assert!(writer.is_poisoned());
    storage.set_failing(false);
    assert!(matches!(
        writer.write(&commands[10..]),
        Err(Error::Poisoned)
    ));
    assert!(matches!(writer.sync(), Err(Error::Poisoned)));
    assert!(writer.take_failure().is_none());
    assert!(matches!(writer.close(), Err(Error::Poisoned)));
}

#[test]
#[should_panic(expected = "applied in sequence")]
fn commands_out_of_sequence_are_refused() {
    let (book, commands) = common::flow(82, 2);
    let (engine, _) = Engine::open_with(
        SimStorage::new(),
        Path::new(DIR),
        EngineConfig::new(book),
        &mut Discard,
    )
    .unwrap();
    let (_, mut matcher) = engine.split();
    let _ = matcher.apply(2, commands[1], &mut Discard);
}

#[test]
#[should_panic(expected = "not through the snapshot")]
fn a_snapshot_needs_the_journal_durable_through_it() {
    let (book, commands) = common::flow(83, 5);
    let config = EngineConfig {
        sync: SyncPolicy::Os,
        ..EngineConfig::new(book)
    };
    let (engine, _) =
        Engine::open_with(SimStorage::new(), Path::new(DIR), config, &mut Discard).unwrap();
    let (mut writer, mut matcher) = engine.split();
    writer.write(&commands).unwrap();
    for (seq, &command) in (1..).zip(&commands) {
        matcher.apply(seq, command, &mut Discard).unwrap();
    }
    let _ = matcher.snapshot(writer.durable_seq());
}

/// An engine whose writer failed refuses a snapshot, even with nothing new to take.
#[test]
fn a_poisoned_engine_takes_no_snapshot() {
    let (book, commands) = common::flow(84, 20);
    let storage = SimStorage::new();
    let (mut engine, _) = Engine::open_with(
        storage.clone(),
        Path::new(DIR),
        EngineConfig::new(book),
        &mut Discard,
    )
    .unwrap();
    engine.submit_batch(&commands[..10], &mut Discard).unwrap();
    engine.snapshot().unwrap();
    storage.set_failing(true);
    assert!(engine.submit_batch(&commands[10..], &mut Discard).is_err());
    storage.set_failing(false);
    assert!(matches!(engine.snapshot(), Err(Error::Poisoned)));
}
