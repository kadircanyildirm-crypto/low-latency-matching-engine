//! The process dies at every single change it makes to the disk, in turn: in the middle of
//! appending a batch, rolling to a new segment, writing, checking and renaming a snapshot,
//! deleting old files, and recovering. After each death, the power may fail as well, and
//! the next recovery may itself die partway through.
//!
//! Every time, recovery must succeed, keep every command the dead process had made durable
//! (or, if only the process died, every command it had journaled), invent none, and rebuild
//! exactly the state after the commands it kept; finishing the stream from there must end in
//! the state of a run that never failed.

mod common;

use std::path::Path;

use engine::sim::{CrashModel, SimStorage};
use engine::{Discard, Engine, EngineConfig, Error, Seq, SyncPolicy};
use orderbook::workload::SplitMix64;
use orderbook::{Command, Event};

const DIR: &str = "data";

fn open(storage: &SimStorage, config: EngineConfig) -> Result<Engine<SimStorage>, Error> {
    Engine::open_with(storage.clone(), Path::new(DIR), config, &mut Discard).map(|(e, _)| e)
}

/// Submits `commands` in batches of up to five until the stream ends or the disk refuses;
/// returns the number of commands acknowledged, and the size of the batch that failed.
fn run(engine: &mut Engine<SimStorage>, commands: &[Command], from: usize) -> (usize, usize) {
    let mut events: Vec<(Seq, Event)> = Vec::new();
    let mut next = from;
    while next < commands.len() {
        let n = (1 + next % 5).min(commands.len() - next);
        if engine
            .submit_batch(&commands[next..next + n], &mut events)
            .is_err()
        {
            return (next, n);
        }
        events.clear();
        next += n;
    }
    (next, 0)
}

/// One stream, interrupted after `changes` changes, then again at random points: each
/// time a kill or a power failure, and a recovery that may itself be interrupted.
fn interrupt(config: EngineConfig, commands: &[Command], digests: &[u64], changes: u64, seed: u64) {
    let mut rng = SplitMix64::new(seed);
    let mut storage = SimStorage::new();
    let mut engine = open(&storage, config).unwrap();
    let mut budget = changes;
    let mut next = 0;
    for round in 0..3 {
        storage.die_after(budget);
        let (acked, failing) = run(&mut engine, commands, next);
        if !storage.is_dead() {
            // The stream ended before the budget did.
            assert_eq!(acked, commands.len());
            break;
        }
        let durable = engine.durable_seq() as usize;
        drop(engine);
        let power = rng.below(2) == 0;
        if power {
            let model = if rng.below(2) == 0 {
                CrashModel::InOrder
            } else {
                CrashModel::AnyOrder
            };
            storage = storage.crash(&mut rng, model);
        } else {
            storage.revive();
        }
        // Recovery dies too, now and then, after a random number of changes.
        while rng.below(3) == 0 {
            storage.die_after(rng.below(40));
            let result = open(&storage, config);
            assert!(
                result.is_ok() || storage.is_dead(),
                "recovery failed without dying"
            );
            drop(result);
            storage.revive();
        }
        let label = format!("{config:?}, dead after {changes} changes, round {round}, seed {seed}");
        engine = open(&storage, config).unwrap_or_else(|e| panic!("{label}: {e}"));
        let kept = engine.last_seq() as usize;
        assert!(
            kept <= acked + failing,
            "{label}: recovered commands never submitted"
        );
        if power {
            assert!(
                kept >= durable,
                "{label}: lost durable commands: {kept} of {durable}"
            );
        } else {
            assert!(
                kept >= acked,
                "{label}: a killed process lost commands: {kept} of {acked}"
            );
        }
        assert_eq!(
            engine.book().digest(),
            digests[kept],
            "{label}: state after {kept}"
        );
        next = kept;
        budget = rng.below(30);
    }
    storage.revive();
    let from = engine.last_seq() as usize;
    let (done, _) = run(&mut engine, commands, from);
    assert_eq!(done, commands.len());
    assert_eq!(engine.book().digest(), digests[commands.len()]);
}

/// How many changes a whole run makes, so every one of them can be the last.
fn changes_in_a_run(config: EngineConfig, commands: &[Command]) -> u64 {
    let storage = SimStorage::new();
    let mut engine = open(&storage, config).unwrap();
    let before = storage.changes();
    run(&mut engine, commands, 0);
    storage.changes() - before
}

#[test]
fn death_at_every_change_loses_nothing_durable() {
    let (book, commands) = common::flow(40, 60);
    let digests = common::digests(book, &commands);
    for (sync, keep) in [(SyncPolicy::Always, 2), (SyncPolicy::Os, 1)] {
        let config = EngineConfig {
            sync,
            segment_capacity: 7,
            snapshot_every: Some(11),
            keep_snapshots: keep,
            ..EngineConfig::new(book)
        };
        let total = changes_in_a_run(config, &commands);
        assert!(total > 100, "{total} changes");
        for changes in 0..=total {
            for seed in 0..12 {
                interrupt(config, &commands, &digests, changes, changes * 12 + seed);
            }
        }
    }
}
