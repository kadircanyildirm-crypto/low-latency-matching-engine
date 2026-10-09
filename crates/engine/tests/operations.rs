//! What the engine does around the journal: events with their sequence numbers, also when
//! replayed; replay verified against snapshots; snapshots that fail without stopping
//! trading; recovery after a failed sync; settings that change between runs; rules
//! versions; and the small contracts of the API.

mod common;

use std::path::Path;

use engine::sim::{CrashModel, SimStorage};
use engine::storage::{Storage, StorageFile};
use engine::{Discard, Engine, EngineConfig, Error, RecoveryReport, Seq, SyncPolicy};
use orderbook::workload::SplitMix64;
use orderbook::{BookConfig, Command, Event, OrderBook};

const DIR: &str = "data";

fn open(
    storage: &SimStorage,
    config: EngineConfig,
) -> Result<(Engine<SimStorage>, RecoveryReport), Error> {
    Engine::open_with(storage.clone(), Path::new(DIR), config, &mut Discard)
}

fn small(book: BookConfig) -> EngineConfig {
    EngineConfig {
        segment_capacity: 10,
        ..EngineConfig::new(book)
    }
}

/// Every event carries the sequence number of its command, in submission order, and the
/// events of replayed commands come again under the same numbers: a consumer that tracks
/// the last number it handled sees each event exactly once.
#[test]
fn events_carry_their_sequence_numbers_also_on_replay() {
    let (book, commands) = common::flow(50, 80);
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    let mut live: Vec<(Seq, Event)> = Vec::new();
    for chunk in commands[..50].chunks(7) {
        engine.submit_batch(chunk, &mut live).unwrap();
    }
    // The same events the book alone emits, tagged command by command.
    let mut alone = OrderBook::new(book);
    let mut expected = Vec::new();
    for (seq, &command) in (1..).zip(&commands[..50]) {
        let mut events = Vec::new();
        alone.process(command, &mut events);
        expected.extend(events.into_iter().map(|event| (seq, event)));
    }
    assert_eq!(live, expected);

    // The process stops; a consumer had handled up to 30. Replay delivers 1 to 50 again,
    // and what it has not seen is exactly what it missed.
    drop(engine);
    let mut replayed: Vec<(Seq, Event)> = Vec::new();
    let (mut engine, _) =
        Engine::open_with(storage.clone(), Path::new(DIR), small(book), &mut replayed).unwrap();
    assert_eq!(replayed, expected);
    let mut more: Vec<(Seq, Event)> = Vec::new();
    engine.submit_batch(&commands[50..], &mut more).unwrap();
    assert!(more.iter().all(|&(seq, _)| seq > 50));
    assert_eq!(more.first().map(|&(seq, _)| seq), Some(51));
}

/// Opening replays from the snapshot before the newest, or from the start, and checks that
/// it reaches the newest snapshot's state.
#[test]
fn opening_verifies_replay_against_the_newest_snapshot() {
    let (book, commands) = common::flow(51, 100);
    let config = EngineConfig {
        snapshot_every: Some(30),
        ..small(book)
    };
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, config).unwrap();
    let mut events = Vec::new();
    for &command in &commands {
        engine.submit(command, &mut events).unwrap();
    }
    drop(engine);
    // Snapshots at 30, 60 and 90; two are kept.
    let (_, report) = open(&storage, config).unwrap();
    assert_eq!(report.snapshot, Some(90));
    assert_eq!(report.verified, Some(90));
    let (_, report) = open(
        &storage,
        EngineConfig {
            verify_replay: false,
            ..config
        },
    )
    .unwrap();
    assert_eq!(report.verified, None);

    // A snapshot that does not match its journal: the newest one replaced by a valid
    // snapshot of another state, as if the files came from two different runs.
    let other = SimStorage::new();
    let (_, other_commands) = common::flow(52, 100);
    let (mut engine, _) = open(&other, config).unwrap();
    for &command in &other_commands {
        engine.submit(command, &mut events).unwrap();
    }
    drop(engine);
    let mut bytes = vec![0; 0];
    let path = Path::new(DIR).join("snapshot-00000000000000000090.snap");
    {
        let mut from = other.clone();
        let mut file = from.open(&path).unwrap();
        bytes.resize(file.size().unwrap() as usize, 0);
        file.read_at(0, &mut bytes).unwrap();
    }
    let mut to = storage.clone();
    let mut file = to.create(&path).unwrap();
    file.write_at(0, &bytes).unwrap();
    // Refused before anything is repaired or delivered.
    let files = storage.files();
    let mut delivered: Vec<(Seq, Event)> = Vec::new();
    assert!(matches!(
        Engine::open_with(storage.clone(), Path::new(DIR), config, &mut delivered),
        Err(Error::Divergence { seq: 90 })
    ));
    assert!(delivered.is_empty());
    assert_eq!(storage.files(), files);
}

/// A snapshot that cannot be written does not stop trading: the batch goes ahead, the
/// failure is reported, and the next attempt waits another interval.
#[test]
fn a_failing_snapshot_does_not_stop_trading() {
    let (book, commands) = common::flow(53, 60);
    let digests = common::digests(book, &commands);
    let config = EngineConfig {
        snapshot_every: Some(10),
        ..small(book)
    };
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, config).unwrap();
    let mut events = Vec::new();
    storage.set_failing_names(Some(".tmp"));
    for &command in &commands[..25] {
        engine.submit(command, &mut events).unwrap();
    }
    assert!(engine.take_failure().is_some());
    assert!(engine.take_failure().is_none());
    assert_eq!(engine.last_snapshot(), 0);
    storage.set_failing_names(None);
    for &command in &commands[25..] {
        engine.submit(command, &mut events).unwrap();
    }
    // The failure at 10 put the next attempt off to 20, which failed too; 30 worked.
    assert!(engine.last_snapshot() >= 30);
    assert_eq!(engine.book().digest(), digests[60]);
    drop(engine);
    let (engine, _) = open(&storage, config).unwrap();
    assert_eq!(engine.book().digest(), digests[60]);
}

/// A segment that cannot be created poisons the engine; reopening recovers everything
/// journaled before.
#[test]
fn a_failing_roll_poisons_the_engine() {
    let (book, commands) = common::flow(54, 15);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    let mut events = Vec::new();
    engine.submit_batch(&commands[..10], &mut events).unwrap();
    // The next segment is prepared while the current one fills; it cannot be finished.
    storage.set_failing_names(Some("00000000000000000011"));
    assert!(engine.submit(commands[10], &mut events).is_err());
    assert!(matches!(
        engine.submit(commands[10], &mut events),
        Err(Error::Poisoned)
    ));
    drop(engine);
    storage.set_failing_names(None);
    let (engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.last_seq(), 10);
    assert_eq!(engine.book().digest(), digests[10]);
}

/// After a failed sync, Linux keeps the unsynced pages readable but never writes them. A
/// process that reopens the journal without a reboot sees them; recovery must write them
/// again before it calls them durable, or a power failure would take them back.
#[test]
fn records_left_by_a_failed_sync_are_written_again() {
    let (book, commands) = common::flow(55, 30);
    let digests = common::digests(book, &commands);
    for seed in 0..20 {
        let storage = SimStorage::new();
        let (mut engine, _) = open(&storage, small(book)).unwrap();
        let mut events = Vec::new();
        engine.submit_batch(&commands[..5], &mut events).unwrap();
        storage.set_failing_syncs(true);
        assert!(engine.submit_batch(&commands[5..8], &mut events).is_err());
        drop(engine);
        storage.set_failing_syncs(false);
        // The same boot: the records written before the failed sync are in the cache.
        let (engine, report) = open(&storage, small(book)).unwrap();
        assert_eq!(engine.last_seq(), 8);
        let claimed = engine.durable_seq() as usize;
        drop(engine);
        let crashed = storage.crash(&mut SplitMix64::new(seed), CrashModel::AnyOrder);
        let (engine, _) = open(&crashed, small(book)).unwrap();
        let kept = engine.last_seq() as usize;
        assert!(kept >= claimed, "seed {seed}: {kept} of {claimed}");
        assert_eq!(engine.book().digest(), digests[kept]);
        assert_eq!(report.journal.rewritten_records, 3);
    }
}

/// Segment size and the number of snapshots kept can change between runs.
#[test]
fn settings_can_change_between_runs() {
    let (book, commands) = common::flow(56, 120);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    let mut events = Vec::new();
    for (round, (capacity, keep)) in [(10, 3), (7, 1), (13, 2), (4, 3)].into_iter().enumerate() {
        let config = EngineConfig {
            segment_capacity: capacity,
            keep_snapshots: keep,
            snapshot_every: Some(9),
            ..EngineConfig::new(book)
        };
        let (mut engine, _) = open(&storage, config).unwrap();
        assert_eq!(engine.book().digest(), digests[round * 30]);
        engine
            .submit_batch(&commands[round * 30..round * 30 + 30], &mut events)
            .unwrap();
    }
    let (engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.book().digest(), digests[120]);
}

/// A journal written under other matching rules is not replayed; a snapshot is state and
/// loads whatever rules it was taken under.
#[test]
fn a_journal_of_other_rules_is_not_replayed() {
    let (book, commands) = common::flow(57, 15);
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    let mut events = Vec::new();
    engine.submit_batch(&commands, &mut events).unwrap();
    drop(engine);
    // Rewrite the second segment's rules version, with a valid checksum.
    let path = Path::new(DIR).join("journal-00000000000000000011.log");
    let mut fs = storage.clone();
    let mut file = fs.open(&path).unwrap();
    let mut header = [0; 64];
    file.read_at(0, &mut header).unwrap();
    header[36..40].copy_from_slice(&(orderbook::RULES_VERSION + 1).to_le_bytes());
    let crc = crc32fast::hash(&header[..60]);
    header[60..].copy_from_slice(&crc.to_le_bytes());
    file.write_at(0, &header).unwrap();
    assert!(matches!(
        open(&storage, small(book)),
        Err(Error::RulesMismatch { found, .. }) if found == orderbook::RULES_VERSION + 1
    ));
}

/// Damage to files that recovery does not need, older snapshots and segments before the
/// newest snapshot, does not stop it.
#[test]
fn damage_to_files_recovery_does_not_need_is_harmless() {
    let (book, commands) = common::flow(58, 100);
    let digests = common::digests(book, &commands);
    let config = EngineConfig {
        snapshot_every: Some(40),
        ..small(book)
    };
    // Snapshots at 40 and 80; segments 41 to 100 are kept, 81 onwards are needed.
    for (file, bit) in [
        ("snapshot-00000000000000000040.snap", 5_000),
        ("journal-00000000000000000041.log", 3),
        ("journal-00000000000000000061.log", 64 * 8 + 100),
    ] {
        let storage = SimStorage::new();
        let (mut engine, _) = open(&storage, config).unwrap();
        let mut events = Vec::new();
        for &command in &commands {
            engine.submit(command, &mut events).unwrap();
        }
        drop(engine);
        storage.flip_bit(&Path::new(DIR).join(file), bit);
        let (engine, report) = open(&storage, config).unwrap_or_else(|e| panic!("{file}: {e}"));
        assert_eq!(report.snapshot, Some(80), "{file}");
        assert_eq!(report.verified, None, "{file}: nothing to verify against");
        assert_eq!(engine.book().digest(), digests[100], "{file}");
    }
}

/// A read error is the disk's, not the file's: recovery stops and sets nothing aside.
#[test]
fn read_errors_set_nothing_aside() {
    let (book, commands) = common::flow(59, 30);
    let config = EngineConfig {
        snapshot_every: Some(10),
        ..small(book)
    };
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, config).unwrap();
    let mut events = Vec::new();
    for &command in &commands {
        engine.submit(command, &mut events).unwrap();
    }
    drop(engine);
    let files = storage.files();
    storage.set_failing_reads(true);
    assert!(matches!(open(&storage, config), Err(Error::Io(_))));
    storage.set_failing_reads(false);
    assert_eq!(storage.files(), files);
}

#[test]
fn small_contracts() {
    let (book, commands) = common::flow(60, 5);
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    let mut events: Vec<(Seq, Event)> = Vec::new();
    engine.submit_batch(&commands, &mut events).unwrap();
    // An empty batch journals nothing and returns the last sequence number.
    assert_eq!(engine.submit_batch(&[], &mut events).unwrap(), 5);
    assert_eq!(engine.last_seq(), 5);
    // Closing syncs, under either policy.
    engine.close().unwrap();
    let os = EngineConfig {
        sync: SyncPolicy::Os,
        ..small(book)
    };
    let (mut engine, _) = open(&storage, os).unwrap();
    engine.submit(commands[0], &mut events).unwrap();
    assert_eq!(engine.durable_seq(), 5);
    engine.close().unwrap();
    let crashed = storage.crash(&mut SplitMix64::new(1), CrashModel::InOrder);
    let (engine, _) = open(&crashed, os).unwrap();
    assert_eq!(engine.last_seq(), 6);
}

#[test]
#[should_panic(expected = "snapshots need at least one command between them")]
fn snapshots_every_zero_commands_panics() {
    let (book, _) = common::flow(61, 0);
    let _ = open(
        &SimStorage::new(),
        EngineConfig {
            snapshot_every: Some(0),
            ..small(book)
        },
    );
}

/// A panic while a command is applied, in the book or in the output, leaves the engine
/// poisoned.
#[test]
fn a_panic_while_applying_a_command_poisons_the_engine() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    struct Explode;
    impl engine::Output for Explode {
        fn on_event(&mut self, _: Seq, _: Event) {
            panic!("a consumer failed");
        }
    }
    let (book, commands) = common::flow(62, 5);
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = engine.submit(commands[0], &mut Explode);
    }));
    assert!(result.is_err());
    let mut events: Vec<(Seq, Event)> = Vec::new();
    assert!(matches!(
        engine.submit(commands[1], &mut events),
        Err(Error::Poisoned)
    ));
    // The command was journaled before the panic: recovery applies it.
    drop(engine);
    let (engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.last_seq(), 1);
    let _: Command = commands[0];
}

/// Rewrites the rules version in the header of the segment starting at `first`, keeping
/// the checksum valid, as if an earlier version of the rules had written it.
fn set_rules(storage: &SimStorage, first: u64, rules: u32) {
    let path = Path::new(DIR).join(format!("journal-{first:020}.log"));
    let mut fs = storage.clone();
    let mut file = fs.open(&path).unwrap();
    let mut header = [0; 64];
    file.read_at(0, &mut header).unwrap();
    header[36..40].copy_from_slice(&rules.to_le_bytes());
    let crc = crc32fast::hash(&header[..60]);
    header[60..].copy_from_slice(&crc.to_le_bytes());
    file.write_at(0, &header).unwrap();
}

/// The upgrade path: the old version takes a snapshot as it stops, and the new one, under
/// other rules, starts from it, cuts the old journal there and goes on in a new segment.
#[test]
fn a_new_rules_version_starts_from_the_old_versions_snapshot() {
    let (book, commands) = common::flow(63, 40);
    let digests = common::digests(book, &commands);
    for at in [25, 30] {
        let storage = SimStorage::new();
        let (mut engine, _) = open(&storage, small(book)).unwrap();
        let mut events: Vec<(Seq, Event)> = Vec::new();
        engine.submit_batch(&commands[..at], &mut events).unwrap();
        engine.snapshot().unwrap();
        engine.close().unwrap();
        for first in [1, 11, 21] {
            set_rules(&storage, first, orderbook::RULES_VERSION + 1);
        }
        let (mut engine, report) =
            open(&storage, small(book)).unwrap_or_else(|e| panic!("{at}: {e}"));
        assert_eq!(engine.last_seq(), at as u64);
        assert_eq!(engine.book().digest(), digests[at]);
        assert!(
            report.unverified.is_some(),
            "{at}: other rules cannot be replayed"
        );
        engine.submit_batch(&commands[at..], &mut events).unwrap();
        drop(engine);
        let (engine, _) = open(&storage, small(book)).unwrap();
        assert_eq!(engine.book().digest(), digests[40], "{at}");
    }
}

/// Commands written under other rules after the snapshot cannot be replayed.
#[test]
fn commands_under_other_rules_after_the_snapshot_are_refused() {
    let (book, commands) = common::flow(64, 30);
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    let mut events: Vec<(Seq, Event)> = Vec::new();
    engine.submit_batch(&commands[..15], &mut events).unwrap();
    engine.snapshot().unwrap();
    engine.submit_batch(&commands[15..], &mut events).unwrap();
    drop(engine);
    set_rules(&storage, 11, orderbook::RULES_VERSION + 1);
    assert!(matches!(
        open(&storage, small(book)),
        Err(Error::RulesMismatch { .. })
    ));
}

/// A consumer that tracks its position gets exactly the events after it, even when it is
/// behind the newest snapshot; one that has seen commands a power failure took back is told.
#[test]
fn consumers_resume_where_they_stood() {
    struct Consumer {
        handled: Seq,
        events: Vec<(Seq, Event)>,
    }
    impl engine::Output for Consumer {
        fn on_event(&mut self, seq: Seq, event: Event) {
            self.events.push((seq, event));
        }
        fn resume_after(&self) -> Option<Seq> {
            Some(self.handled)
        }
    }
    let (book, commands) = common::flow(65, 100);
    let config = EngineConfig {
        snapshot_every: Some(20),
        keep_snapshots: 3,
        ..small(book)
    };
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, config).unwrap();
    let mut all: Vec<(Seq, Event)> = Vec::new();
    for &command in &commands {
        engine.submit(command, &mut all).unwrap();
    }
    drop(engine);
    // Snapshots at 40, 60 and 80 are kept; the consumer stands at 70.
    let mut consumer = Consumer {
        handled: 70,
        events: Vec::new(),
    };
    let (engine, report) =
        Engine::open_with(storage.clone(), Path::new(DIR), config, &mut consumer).unwrap();
    assert_eq!(report.snapshot, Some(60));
    assert_eq!(engine.last_seq(), 100);
    let expected: Vec<_> = all.iter().filter(|(seq, _)| *seq > 70).cloned().collect();
    assert_eq!(consumer.events, expected);
    drop(engine);

    // Under the OS policy, a power failure takes back what was not synced; a consumer that
    // had handled it is told, rather than skipping the new commands of the same numbers.
    let os = EngineConfig {
        sync: SyncPolicy::Os,
        ..small(book)
    };
    let mut told = 0;
    for seed in 0..20 {
        let storage = SimStorage::new();
        let (mut engine, _) = open(&storage, os).unwrap();
        let mut seen: Vec<(Seq, Event)> = Vec::new();
        engine.submit_batch(&commands[..20], &mut seen).unwrap();
        engine.sync().unwrap();
        engine.submit_batch(&commands[20..25], &mut seen).unwrap();
        drop(engine);
        let crashed = storage.crash(&mut SplitMix64::new(seed), CrashModel::InOrder);
        let mut consumer = Consumer {
            handled: 25,
            events: Vec::new(),
        };
        match Engine::open_with(crashed, Path::new(DIR), os, &mut consumer) {
            Ok((engine, _)) => assert_eq!(engine.last_seq(), 25, "everything survived"),
            Err(Error::ConsumerAhead { consumer, journal }) => {
                assert_eq!(consumer, 25);
                assert!((20..25).contains(&journal));
                told += 1;
            }
            Err(error) => panic!("{error}"),
        }
    }
    assert!(told > 0);
}

/// A prepared segment that cannot be written does not stop trading: the records are
/// journaled, the failure is reported, and the segment is prepared again at the roll.
#[test]
fn a_failing_preparation_does_not_stop_trading() {
    let (book, commands) = common::flow(66, 25);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    let mut events: Vec<(Seq, Event)> = Vec::new();
    storage.set_failing_names(Some(".next"));
    engine.submit_batch(&commands[..5], &mut events).unwrap();
    assert!(engine.take_failure().is_some());
    engine.submit_batch(&commands[5..9], &mut events).unwrap();
    storage.set_failing_names(None);
    engine.submit_batch(&commands[9..], &mut events).unwrap();
    assert_eq!(engine.book().digest(), digests[25]);
    drop(engine);
    let (engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.book().digest(), digests[25]);
}

/// After a clean shutdown, nothing has to be written again; and if the journal then ends
/// before what the shutdown recorded as durable, that is damage, not a crash.
#[test]
fn a_clean_shutdown_is_recorded() {
    let (book, commands) = common::flow(67, 30);
    let os = EngineConfig {
        sync: SyncPolicy::Os,
        ..small(book)
    };
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, os).unwrap();
    let mut events: Vec<(Seq, Event)> = Vec::new();
    engine.submit_batch(&commands[..25], &mut events).unwrap();
    engine.close().unwrap();
    let (engine, report) = open(&storage, os).unwrap();
    assert_eq!(engine.last_seq(), 25);
    assert_eq!(report.journal.rewritten_records, 0);
    engine.close().unwrap();
    // The last record, which no later record vouches for, is damaged.
    storage.flip_bit(
        &Path::new(DIR).join("journal-00000000000000000021.log"),
        (64 + 4 * 64) * 8 + 100,
    );
    assert!(matches!(open(&storage, os), Err(Error::Corrupt { .. })));
}

/// A segment missing among the ones only older snapshots need costs only the verification.
#[test]
fn a_missing_older_segment_only_skips_verification() {
    let (book, commands) = common::flow(68, 100);
    let digests = common::digests(book, &commands);
    let config = EngineConfig {
        snapshot_every: Some(30),
        ..small(book)
    };
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, config).unwrap();
    let mut events: Vec<(Seq, Event)> = Vec::new();
    for &command in &commands {
        engine.submit(command, &mut events).unwrap();
    }
    drop(engine);
    // Snapshots at 60 and 90; segments 61 to 100 are kept. Recovery from 90 needs only 91.
    storage
        .clone()
        .remove(&Path::new(DIR).join("journal-00000000000000000071.log"))
        .unwrap();
    let (engine, report) = open(&storage, config).unwrap();
    assert_eq!(report.snapshot, Some(90));
    assert_eq!(report.verified, None);
    assert!(report.unverified.is_some());
    assert_eq!(engine.book().digest(), digests[100]);
}
