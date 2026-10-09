//! Recovery on a simulated disk: clean restarts, snapshots and retention, power failures at
//! random points under every sync policy, damaged files, and failing writes.

mod common;

use std::path::Path;

use engine::storage::{CrashModel, SimStorage};
use engine::{Engine, EngineConfig, Error, SyncPolicy};
use orderbook::workload::SplitMix64;
use orderbook::{BookConfig, Command, Event};

const DIR: &str = "data";

fn open(storage: &SimStorage, config: EngineConfig) -> Result<Engine<SimStorage>, Error> {
    Engine::open_with(storage.clone(), Path::new(DIR), config).map(|(engine, _)| engine)
}

fn submit_all(engine: &mut Engine<SimStorage>, commands: &[Command]) {
    let mut events: Vec<Event> = Vec::new();
    for &command in commands {
        engine.submit(command, &mut events).unwrap();
        events.clear();
    }
}

fn file_names(storage: &SimStorage) -> Vec<String> {
    storage
        .files()
        .into_iter()
        .map(|(path, _)| path.file_name().unwrap().to_str().unwrap().to_owned())
        .collect()
}

#[test]
fn a_reopened_engine_continues_where_it_stopped() {
    let (book, commands) = common::flow(1, 2_000);
    let digests = common::digests(book, &commands);
    let config = EngineConfig {
        segment_capacity: 300,
        ..EngineConfig::new(book)
    };
    let storage = SimStorage::new();
    let mut engine = open(&storage, config).unwrap();
    submit_all(&mut engine, &commands[..1_234]);
    assert_eq!(engine.last_seq(), 1_234);
    assert_eq!(engine.durable_seq(), 1_234);
    drop(engine);

    let (mut engine, report) = Engine::open_with(storage.clone(), Path::new(DIR), config).unwrap();
    assert_eq!(report.snapshot, None);
    assert_eq!(report.journal.replayed, 1_234);
    assert_eq!(report.journal.cleared_records, 0);
    assert_eq!(engine.book().digest(), digests[1_234]);
    submit_all(&mut engine, &commands[1_234..]);
    assert_eq!(engine.book().digest(), digests[2_000]);
    // Seven segments of 300 hold 2,000 records.
    assert_eq!(file_names(&storage).len(), 7);
}

#[test]
fn snapshots_bound_replay_and_retention_removes_what_they_supersede() {
    let (book, commands) = common::flow(2, 3_000);
    let digests = common::digests(book, &commands);
    let config = EngineConfig {
        segment_capacity: 100,
        snapshot_every: Some(250),
        keep_snapshots: 2,
        ..EngineConfig::new(book)
    };
    let storage = SimStorage::new();
    let mut engine = open(&storage, config).unwrap();
    submit_all(&mut engine, &commands);
    // A snapshot is taken before the batch that crosses each multiple of 250.
    assert_eq!(engine.last_snapshot(), 2_750);
    let names = file_names(&storage);
    let snapshots: Vec<_> = names.iter().filter(|n| n.ends_with(".snap")).collect();
    assert_eq!(
        snapshots,
        [
            "snapshot-00000000000000002500.snap",
            "snapshot-00000000000000002750.snap"
        ]
    );
    // The oldest snapshot kept needs the journal from 2,501 on: segments 2,501.. to 2,901...
    let segments: Vec<_> = names.iter().filter(|n| n.ends_with(".log")).collect();
    assert_eq!(
        segments.first().unwrap().as_str(),
        "journal-00000000000000002501.log"
    );
    assert_eq!(
        segments.last().unwrap().as_str(),
        "journal-00000000000000002901.log"
    );
    drop(engine);

    let (engine, report) = Engine::open_with(storage.clone(), Path::new(DIR), config).unwrap();
    assert_eq!(report.snapshot, Some(2_750));
    assert_eq!(report.journal.replayed, 250);
    assert_eq!(engine.book().digest(), digests[3_000]);
}

#[test]
fn a_damaged_snapshot_falls_back_to_the_one_before() {
    let (book, commands) = common::flow(3, 1_000);
    let digests = common::digests(book, &commands);
    let config = EngineConfig {
        segment_capacity: 64,
        snapshot_every: Some(300),
        ..EngineConfig::new(book)
    };
    let storage = SimStorage::new();
    let mut engine = open(&storage, config).unwrap();
    submit_all(&mut engine, &commands);
    drop(engine);
    storage.flip_bit(Path::new("data/snapshot-00000000000000000900.snap"), 12_345);

    let (engine, report) = Engine::open_with(storage.clone(), Path::new(DIR), config).unwrap();
    assert_eq!(report.snapshot, Some(600));
    assert_eq!(report.damaged_snapshots.len(), 1);
    assert_eq!(report.damaged_snapshots[0].0, 900);
    assert_eq!(report.journal.replayed, 400);
    assert_eq!(engine.book().digest(), digests[1_000]);
    assert!(file_names(&storage).contains(&"snapshot-00000000000000000900.damaged".to_owned()));
}

#[test]
fn files_of_another_configuration_are_refused() {
    let (book, commands) = common::flow(4, 100);
    let storage = SimStorage::new();
    let mut engine = open(&storage, EngineConfig::new(book)).unwrap();
    submit_all(&mut engine, &commands);
    drop(engine);
    let other = BookConfig {
        max_orders: book.max_orders + 1,
        ..book
    };
    assert!(matches!(
        open(&storage, EngineConfig::new(other)),
        Err(Error::ConfigMismatch { .. })
    ));
}

#[test]
fn a_failed_write_poisons_the_engine_and_applies_nothing() {
    let (book, commands) = common::flow(5, 100);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    let mut engine = open(&storage, EngineConfig::new(book)).unwrap();
    submit_all(&mut engine, &commands[..50]);
    storage.set_failing(true);
    let mut events = Vec::new();
    assert!(matches!(
        engine.submit(commands[50], &mut events),
        Err(Error::Io(_))
    ));
    assert!(events.is_empty());
    assert_eq!(engine.book().digest(), digests[50]);
    storage.set_failing(false);
    assert!(matches!(
        engine.submit(commands[50], &mut events),
        Err(Error::Poisoned)
    ));
    assert!(matches!(engine.sync(), Err(Error::Poisoned)));
    assert!(matches!(engine.snapshot(), Err(Error::Poisoned)));
    drop(engine);
    // The record may or may not have reached the disk; here the write failed outright.
    let engine = open(&storage, EngineConfig::new(book)).unwrap();
    assert_eq!(engine.last_seq(), 50);
    assert_eq!(engine.book().digest(), digests[50]);
}

/// One run of the crash property: random settings, power failures at random points, and
/// recovery after each.
fn crash_and_recover(seed: u64) -> CrashStats {
    let mut rng = SplitMix64::new(seed);
    let len = 200 + rng.below(400) as usize;
    let (book, commands) = common::flow(seed, len);
    let digests = common::digests(book, &commands);
    let config = EngineConfig {
        sync: if rng.below(2) == 0 {
            SyncPolicy::Always
        } else {
            SyncPolicy::Os
        },
        segment_capacity: 1 + rng.below(80) as u32,
        snapshot_every: match rng.below(3) {
            0 => None,
            _ => Some(1 + rng.below(150)),
        },
        keep_snapshots: 1 + rng.below(3) as usize,
        ..EngineConfig::new(book)
    };
    let model = if rng.below(2) == 0 {
        CrashModel::InOrder
    } else {
        CrashModel::AnyOrder
    };
    let mut stats = CrashStats::default();
    let mut storage = SimStorage::new();
    let mut events = Vec::new();
    let mut next = 0;
    for _crash in 0..4 {
        let mut engine = open(&storage, config).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let recovered = engine.last_seq() as usize;
        assert_eq!(
            engine.book().digest(),
            digests[recovered],
            "seed {seed}: the state after recovering {recovered} commands"
        );
        assert!(
            recovered <= next,
            "seed {seed}: recovered commands never submitted"
        );
        stats.lost += (next - recovered) as u64;
        next = recovered;
        // Run on to a random point, in batches of random size, then fail.
        let stop = next + rng.below((len - next) as u64 + 1) as usize;
        while next < stop {
            let n = (1 + rng.below(8) as usize).min(stop - next);
            engine
                .submit_batch(&commands[next..next + n], &mut events)
                .unwrap();
            events.clear();
            next += n;
            if rng.below(20) == 0 {
                engine.sync().unwrap();
            }
        }
        let durable = engine.durable_seq() as usize;
        if config.sync == SyncPolicy::Always {
            assert_eq!(durable, next, "seed {seed}");
        }
        // Either the process dies, and its writes stay with the OS, unsynced; or the power
        // fails, and only what was synced is certain to survive.
        let killed = rng.below(2) == 0;
        drop(engine);
        if killed {
            stats.kills += 1;
        } else {
            storage = storage.crash(&mut rng, model);
            stats.crashes += 1;
        }
        let engine = open(&storage, config).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        let kept = engine.last_seq() as usize;
        if killed {
            assert_eq!(kept, next, "seed {seed}: a killed process lost commands");
        } else {
            assert!(
                kept >= durable,
                "seed {seed}: lost durable commands: recovered {kept} of {durable}"
            );
        }
    }
    // Finish the stream: the final state is the uncrashed one.
    let mut engine = open(&storage, config).unwrap();
    let recovered = engine.last_seq() as usize;
    assert_eq!(engine.book().digest(), digests[recovered], "seed {seed}");
    for &command in &commands[recovered..] {
        engine.submit(command, &mut events).unwrap();
        events.clear();
    }
    assert_eq!(engine.book().digest(), digests[len], "seed {seed}");
    stats
}

#[derive(Debug, Default)]
struct CrashStats {
    crashes: u64,
    kills: u64,
    lost: u64,
}

/// Hundreds of power failures and killed processes, several in a row: after each, recovery
/// succeeds, keeps every command that was durable (and, after a kill, every command at
/// all), invents none, and rebuilds exactly the state after the commands it kept; and once
/// the lost commands are resubmitted, the state is that of a run that never crashed.
#[test]
fn power_failures_at_random_points_lose_nothing_durable() {
    let mut total = CrashStats::default();
    for seed in 0..400 {
        let stats = crash_and_recover(seed);
        total.crashes += stats.crashes;
        total.kills += stats.kills;
        total.lost += stats.lost;
    }
    // The unsynced tails do get lost, so the crashes test something.
    assert!(total.lost > 1_000, "{total:?}");
}

/// A bit flipped anywhere in any file, after a clean shutdown: recovery either refuses, or
/// rebuilds exactly the state after the commands it kept, which are all of them but at most
/// the last. It never comes up in a state no prefix of the commands leads to.
#[test]
fn damage_anywhere_is_refused_or_harmless() {
    let (mut refused, mut recovered, mut lost_last) = (0, 0, 0);
    for seed in 0..300 {
        let mut rng = SplitMix64::new(seed);
        let (book, commands) = common::flow(seed, 300);
        let digests = common::digests(book, &commands);
        let config = EngineConfig {
            segment_capacity: 1 + rng.below(100) as u32,
            snapshot_every: Some(1 + rng.below(200)),
            ..EngineConfig::new(book)
        };
        let storage = SimStorage::new();
        let mut engine = open(&storage, config).unwrap();
        submit_all(&mut engine, &commands);
        drop(engine);
        let files = storage.files();
        let (path, size) = &files[rng.below(files.len() as u64) as usize];
        storage.flip_bit(path, rng.below(size * 8));
        match open(&storage, config) {
            Ok(engine) => {
                recovered += 1;
                let n = engine.last_seq() as usize;
                assert_eq!(engine.book().digest(), digests[n], "seed {seed}");
                // Everything was synced, one command at a time. Only the last record has no
                // later record to vouch for it, so only it can go missing without an error.
                assert!(n + 1 >= commands.len(), "seed {seed}: kept {n}");
                lost_last += usize::from(n < commands.len());
            }
            Err(Error::Corrupt { .. } | Error::MissingJournal { .. }) => refused += 1,
            Err(error) => panic!("seed {seed}: {error}"),
        }
    }
    assert!(
        refused > 10 && recovered > 10,
        "{refused} refused, {recovered} recovered, {lost_last} lost the last command"
    );
}
