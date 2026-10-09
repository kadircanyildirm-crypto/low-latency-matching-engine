//! One test per way the files on disk can disagree with what the engine expects: leftover
//! temporary files, snapshots that lie about themselves, segments that are missing, short or
//! damaged in particular places, and a journal that ends before its snapshot.

mod common;

use std::path::{Path, PathBuf};

use engine::sim::SimStorage;
use engine::storage::{FsStorage, Storage, StorageFile};
use engine::{Discard, Engine, EngineConfig, Error, RECORD_SIZE, RecoveryReport, Seq};
use orderbook::{BookConfig, Command, Event};

const DIR: &str = "data";

fn open(
    storage: &SimStorage,
    config: EngineConfig,
) -> Result<(Engine<SimStorage>, RecoveryReport), Error> {
    Engine::open_with(storage.clone(), Path::new(DIR), config, &mut Discard)
}

fn run(storage: &SimStorage, config: EngineConfig, commands: &[Command]) {
    let (mut engine, _) = open(storage, config).unwrap();
    let mut events: Vec<(Seq, Event)> = Vec::new();
    for &command in commands {
        engine.submit(command, &mut events).unwrap();
        events.clear();
    }
}

fn segment(first_seq: u64) -> PathBuf {
    Path::new(DIR).join(format!("journal-{first_seq:020}.log"))
}

fn snapshot(seq: u64) -> PathBuf {
    Path::new(DIR).join(format!("snapshot-{seq:020}.snap"))
}

/// Bit `bit` of the record in `slot` of a segment.
fn record_bit(slot: u64, bit: u64) -> u64 {
    (64 + slot * RECORD_SIZE as u64) * 8 + bit
}

fn small(book: BookConfig) -> EngineConfig {
    EngineConfig {
        segment_capacity: 10,
        ..EngineConfig::new(book)
    }
}

#[test]
fn leftover_temporary_files_are_deleted() {
    let (book, commands) = common::flow(20, 35);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    run(&storage, small(book), &commands[..30]);
    // The next segment is being prepared under a temporary name.
    let prepared = Path::new(DIR).join("journal-00000000000000000031.next");
    assert!(storage.files().iter().any(|(path, _)| *path == prepared));
    storage.flip_bit(&prepared, 3);
    let mut fs = storage.clone();
    let partial = Path::new(DIR).join("snapshot-00000000000000000030.tmp");
    fs.create(&partial)
        .unwrap()
        .write_at(0, b"half a snapshot")
        .unwrap();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.last_seq(), 30);
    assert!(
        storage
            .files()
            .iter()
            .all(|(path, _)| *path != partial && *path != prepared)
    );
    // The segment the prepared file was for starts afresh.
    let mut events = Vec::new();
    engine.submit_batch(&commands[30..], &mut events).unwrap();
    assert!(storage.files().iter().any(|(path, _)| *path == segment(31)));
    drop(engine);
    let (engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.book().digest(), digests[35]);
}

#[test]
fn a_snapshot_is_taken_once_per_state() {
    let (book, commands) = common::flow(21, 30);
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.config().segment_capacity, 10);
    let mut events = Vec::new();
    engine.submit_batch(&commands, &mut events).unwrap();
    engine.snapshot().unwrap();
    let files = storage.files();
    engine.snapshot().unwrap();
    assert_eq!(storage.files(), files);
    assert_eq!(engine.last_snapshot(), 30);
}

/// A snapshot whose header, length, digest or configuration does not match its file name or
/// its contents is set aside, or, for another configuration, refused.
#[test]
fn snapshots_that_contradict_themselves_are_not_used() {
    let (book, commands) = common::flow(22, 40);
    let digests = common::digests(book, &commands);
    let config = EngineConfig {
        snapshot_every: Some(20),
        ..small(book)
    };
    let fresh = || {
        let storage = SimStorage::new();
        run(&storage, config, &commands);
        // A snapshot at 20; the one at 40 would come before a 41st command.
        storage
    };
    let check = |storage: &SimStorage, reason: &str| {
        let (engine, report) = open(storage, config).unwrap();
        assert_eq!(report.snapshot, None, "{reason}");
        assert_eq!(report.damaged_snapshots.len(), 1, "{reason}");
        assert!(report.damaged_snapshots[0].1.contains(reason), "{report:?}");
        assert_eq!(engine.book().digest(), digests[40]);
    };

    // Another snapshot's file under this name.
    let storage = fresh();
    let mut fs = storage.clone();
    fs.rename(&snapshot(20), &snapshot(19)).unwrap();
    check(&storage, "sequence number");

    // Longer than its header says.
    let storage = fresh();
    let mut fs = storage.clone();
    let mut file = fs.open(&snapshot(20)).unwrap();
    let size = file.size().unwrap();
    file.set_len(size + 1).unwrap();
    check(&storage, "length");

    // A header that checks out, with the wrong digest.
    let storage = fresh();
    let mut fs = storage.clone();
    let mut file = fs.open(&snapshot(20)).unwrap();
    let mut header = [0; 64];
    file.read_at(0, &mut header).unwrap();
    header[24] ^= 1;
    let crc = crc32fast::hash(&header[..60]);
    header[60..].copy_from_slice(&crc.to_le_bytes());
    file.write_at(0, &header).unwrap();
    check(&storage, "digest");

    // Shorter than a header.
    let storage = fresh();
    let mut fs = storage.clone();
    fs.open(&snapshot(20)).unwrap().set_len(10).unwrap();
    check(&storage, "header");

    // Another configuration: refused outright, since the engine is opened with the wrong one.
    let storage = fresh();
    let other = BookConfig {
        max_owners: book.max_owners + 1,
        ..book
    };
    assert!(matches!(
        open(
            &storage,
            EngineConfig {
                book: other,
                ..config
            }
        ),
        Err(Error::ConfigMismatch { .. })
    ));
}

#[test]
fn a_missing_segment_in_the_middle_is_refused() {
    let (book, commands) = common::flow(23, 35);
    let storage = SimStorage::new();
    run(&storage, small(book), &commands);
    storage.clone().remove(&segment(11)).unwrap();
    assert!(matches!(
        open(&storage, small(book)),
        Err(Error::Corrupt { .. })
    ));
    // Without the first segment, replay cannot start.
    let storage = SimStorage::new();
    run(&storage, small(book), &commands);
    storage.clone().remove(&segment(1)).unwrap();
    assert!(matches!(
        open(&storage, small(book)),
        Err(Error::MissingJournal { from: 1 })
    ));
}

/// The journal can only end before its snapshot through damage to synced data, since it is
/// synced before every snapshot. Recovery refuses, and deletes nothing.
#[test]
fn a_journal_that_ends_before_its_snapshot_is_refused() {
    let (book, commands) = common::flow(24, 35);
    let storage = SimStorage::new();
    let config = EngineConfig {
        snapshot_every: Some(30),
        ..small(book)
    };
    run(&storage, config, &commands);
    // Segments 1, 11, 21 and 31 hold commands 1 to 35; lose the two that reach past 20.
    let mut fs = storage.clone();
    fs.remove(&segment(31)).unwrap();
    fs.remove(&segment(21)).unwrap();
    let files = storage.files();
    assert!(matches!(open(&storage, config), Err(Error::Corrupt { .. })));
    assert_eq!(storage.files(), files);
}

/// Damage to the last record of a full segment, when the next segment holds nothing valid,
/// cannot be told from a crash: the record is cut, and the empty segment goes.
#[test]
fn damage_before_an_empty_segment_cuts_the_log() {
    let (book, commands) = common::flow(25, 11);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    run(&storage, small(book), &commands);
    storage.flip_bit(&segment(1), record_bit(9, 100));
    storage.flip_bit(&segment(11), record_bit(0, 100));
    let (engine, report) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.last_seq(), 9);
    assert_eq!(engine.book().digest(), digests[9]);
    assert_eq!(report.journal.removed_segments, 1);
    assert_eq!(report.journal.cleared_records, 1);
    assert!(storage.files().iter().all(|(path, _)| *path != segment(11)));
}

/// A damaged header in the newest segment is a crash during its creation when no record
/// stands behind it, and damage when one does.
#[test]
fn a_damaged_newest_header_is_removed_only_if_nothing_follows_it() {
    let (book, commands) = common::flow(26, 15);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    run(&storage, small(book), &commands);
    storage.flip_bit(&segment(11), 3);
    assert!(matches!(
        open(&storage, small(book)),
        Err(Error::Corrupt { .. })
    ));

    let storage = SimStorage::new();
    run(&storage, small(book), &commands[..11]);
    storage.flip_bit(&segment(11), 3);
    storage.flip_bit(&segment(11), record_bit(0, 7));
    let (engine, report) = open(&storage, small(book)).unwrap();
    assert_eq!(report.journal.removed_segments, 1);
    assert_eq!(engine.last_seq(), 10);
    assert_eq!(engine.book().digest(), digests[10]);
}

/// A segment file that lost its preallocated tail, or ends inside a record, reads as empty
/// from there.
#[test]
fn a_short_segment_reads_as_empty_past_its_end() {
    let (book, commands) = common::flow(27, 15);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    run(&storage, small(book), &commands);
    // Records 11 to 15 are slots 0 to 4; cut the file inside slot 6.
    let len = 64 + 6 * RECORD_SIZE as u64 + 10;
    storage
        .clone()
        .open(&segment(11))
        .unwrap()
        .set_len(len)
        .unwrap();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.last_seq(), 15);
    assert_eq!(engine.book().digest(), digests[15]);
    // Appending grows the file again.
    let (_, more) = common::flow(27, 20);
    let mut events = Vec::new();
    engine.submit_batch(&more[15..], &mut events).unwrap();
    drop(engine);
    let (engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.last_seq(), 20);

    // Cut right after the last record: replay ends exactly at the end of the file.
    let storage = SimStorage::new();
    run(&storage, small(book), &commands);
    let len = 64 + 5 * RECORD_SIZE as u64;
    storage
        .clone()
        .open(&segment(11))
        .unwrap()
        .set_len(len)
        .unwrap();
    let (engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.last_seq(), 15);

    // A damaged header in front of a file that ends inside its first record.
    let storage = SimStorage::new();
    run(&storage, small(book), &commands[..11]);
    let mut file = storage.clone().open(&segment(11)).unwrap();
    file.set_len(64 + 10).unwrap();
    storage.flip_bit(&segment(11), 3);
    let (engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.last_seq(), 10);
}

/// Segments larger than recovery reads at once.
#[test]
fn large_segments_replay_across_reads() {
    let (book, commands) = common::flow(28, 17_000);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    let config = EngineConfig {
        segment_capacity: 20_000,
        ..EngineConfig::new(book)
    };
    {
        let (mut engine, _) = open(&storage, config).unwrap();
        let mut events = Vec::new();
        for chunk in commands.chunks(1_000) {
            engine.submit_batch(chunk, &mut events).unwrap();
            events.clear();
        }
    }
    let (engine, report) = open(&storage, config).unwrap();
    assert_eq!(report.journal.replayed, 17_000);
    assert_eq!(engine.book().digest(), digests[17_000]);
}

#[test]
#[should_panic(expected = "max_price must be >= min_price")]
fn an_invalid_book_configuration_panics() {
    let book = BookConfig {
        max_price: 0,
        ..BookConfig::new(1, 100, 10)
    };
    let _ = open(&SimStorage::new(), EngineConfig::new(book));
}

#[test]
fn errors_explain_themselves() {
    let file = PathBuf::from("data/x");
    for error in [
        Error::Io(std::io::Error::other("disk")),
        Error::Corrupt {
            file: file.clone(),
            detail: "bad".into(),
        },
        Error::ConfigMismatch { file },
        Error::MissingJournal { from: 7 },
        Error::Poisoned,
        Error::Locked {
            dir: PathBuf::from("data"),
        },
    ] {
        assert!(!error.to_string().is_empty());
        let io = matches!(error, Error::Io(_));
        assert_eq!(std::error::Error::source(&error).is_some(), io);
    }
}

/// The real file system's storage lists only files, and syncs directories.
#[test]
fn the_file_system_storage_lists_files_only() {
    let dir = std::env::temp_dir().join(format!("engine-fs-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut fs = FsStorage;
    fs.create_dir_all(&dir.join("nested")).unwrap();
    let mut file = fs.create(&dir.join("a")).unwrap();
    file.write_at(0, b"abc").unwrap();
    file.write_at(5, b"z").unwrap();
    file.sync().unwrap();
    let mut read = [0; 6];
    file.read_at(0, &mut read).unwrap();
    assert_eq!(&read, b"abc\0\0z");
    assert!(file.read_at(4, &mut read).is_err());
    assert_eq!(file.size().unwrap(), 6);
    // A write or read that ends where the next write starts lets that write skip the seek;
    // anything else must seek.
    file.write_at(1, b"xyz").unwrap();
    file.write_at(3, b"W").unwrap();
    file.read_at(1, &mut read[..3]).unwrap();
    assert_eq!(&read[..3], b"xyW");
    file.write_at(3, b"V").unwrap();
    file.read_at(0, &mut read).unwrap();
    assert_eq!(&read, b"axyV\0z");
    file.set_len(100).unwrap();
    assert_eq!(file.size().unwrap(), 100);
    drop(file);
    fs.rename(&dir.join("a"), &dir.join("b")).unwrap();
    fs.sync_dir(&dir).unwrap();
    assert_eq!(fs.list(&dir).unwrap(), ["b"]);
    fs.remove(&dir.join("b")).unwrap();
    assert!(fs.open(&dir.join("b")).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

/// A segment file under another segment's name is damage, not a crash.
#[test]
fn a_segment_under_the_wrong_name_is_refused() {
    let (book, commands) = common::flow(29, 25);
    let storage = SimStorage::new();
    run(&storage, small(book), &commands);
    let mut fs = storage.clone();
    fs.rename(&segment(11), &segment(12)).unwrap();
    assert!(matches!(
        open(&storage, small(book)),
        Err(Error::Corrupt { .. })
    ));
}

/// Segments are preallocated to their full size, header included.
#[test]
fn segments_are_preallocated() {
    let (book, commands) = common::flow(30, 25);
    let storage = SimStorage::new();
    run(&storage, small(book), &commands);
    for (path, size) in storage.files() {
        assert_eq!(size, 64 + 10 * RECORD_SIZE as u64, "{}", path.display());
    }
}

/// A snapshot that falls exactly at the end of a segment: the next segment starts right
/// after it, and nothing of the journal is lost or dropped.
#[test]
fn a_snapshot_at_a_segment_boundary_keeps_the_journal() {
    let (book, commands) = common::flow(31, 20);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    let (mut engine, _) = open(&storage, small(book)).unwrap();
    let mut events = Vec::new();
    engine.submit_batch(&commands, &mut events).unwrap();
    engine.snapshot().unwrap();
    drop(engine);
    let (mut engine, report) = open(&storage, small(book)).unwrap();
    assert_eq!(report.snapshot, Some(20));
    assert_eq!(report.journal.removed_segments, 0);
    assert_eq!(engine.book().digest(), digests[20]);
    // The journal continues in a new segment, and the old ones are still there.
    let (_, more) = common::flow(31, 25);
    engine.submit_batch(&more[20..], &mut events).unwrap();
    drop(engine);
    let names: Vec<_> = storage.files().into_iter().map(|(path, _)| path).collect();
    for first in [1, 11, 21] {
        assert!(names.contains(&segment(first)), "{names:?}");
    }
    let (engine, _) = open(&storage, small(book)).unwrap();
    assert_eq!(engine.last_seq(), 25);
}

/// A valid record in the wrong slot, as a misdirected write would leave it, ends replay
/// instead of being applied under another sequence number.
#[test]
fn a_record_in_the_wrong_slot_ends_replay() {
    let (book, commands) = common::flow(32, 10);
    let digests = common::digests(book, &commands);
    let storage = SimStorage::new();
    let config = EngineConfig {
        segment_capacity: 20,
        ..EngineConfig::new(book)
    };
    run(&storage, config, &commands);
    let mut file = storage.clone().open(&segment(1)).unwrap();
    let mut record = [0; RECORD_SIZE];
    file.read_at(64 + 2 * RECORD_SIZE as u64, &mut record)
        .unwrap();
    file.write_at(64 + 9 * RECORD_SIZE as u64, &record).unwrap();
    let (engine, report) = open(&storage, config).unwrap();
    assert_eq!(engine.last_seq(), 9);
    assert_eq!(engine.book().digest(), digests[9]);
    assert_eq!(report.journal.cleared_records, 1);
}

/// Snapshot headers whose checksum holds: a wrong magic number is damage, and recovery
/// falls back; another format version, or reserved bytes in use, is a file from another
/// version, and recovery stops without touching it; the rules version is only a record.
#[test]
fn snapshot_headers_are_checked_field_by_field() {
    let (book, commands) = common::flow(33, 20);
    let digests = common::digests(book, &commands);
    let config = EngineConfig {
        snapshot_every: Some(10),
        ..small(book)
    };
    let with = |byte: usize| {
        let storage = SimStorage::new();
        run(&storage, config, &commands);
        let mut file = storage.clone().open(&snapshot(10)).unwrap();
        let mut header = [0; 64];
        file.read_at(0, &mut header).unwrap();
        header[byte] ^= 1;
        let crc = crc32fast::hash(&header[..60]);
        header[60..].copy_from_slice(&crc.to_le_bytes());
        file.write_at(0, &header).unwrap();
        storage
    };
    let storage = with(0);
    let (engine, report) = open(&storage, config).unwrap();
    assert_eq!(report.damaged_snapshots.len(), 1);
    assert!(report.damaged_snapshots[0].1.contains("invalid header"));
    assert_eq!(engine.book().digest(), digests[20]);
    for byte in [8, 50] {
        let storage = with(byte);
        let files = storage.files();
        assert!(
            matches!(open(&storage, config), Err(Error::Unsupported { .. })),
            "byte {byte}"
        );
        assert_eq!(storage.files(), files);
    }
    let storage = with(12);
    let (engine, report) = open(&storage, config).unwrap();
    assert_eq!(report.snapshot, Some(10));
    assert_eq!(engine.book().digest(), digests[20]);
}

/// A second engine on the same directory is refused while the first is open, on the
/// simulated disk and on the real file system, and admitted once it is closed.
#[test]
fn one_engine_per_directory() {
    let (book, _) = common::flow(34, 0);
    let storage = SimStorage::new();
    let (first, _) = open(&storage, small(book)).unwrap();
    assert!(matches!(
        open(&storage, small(book)),
        Err(Error::Locked { .. })
    ));
    drop(first);
    open(&storage, small(book)).unwrap();

    let dir = std::env::temp_dir().join(format!("engine-lock-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let (first, _) = Engine::open(&dir, small(book), &mut Discard).unwrap();
    assert!(matches!(
        Engine::open(&dir, small(book), &mut Discard),
        Err(Error::Locked { .. })
    ));
    drop(first);
    drop(Engine::open(&dir, small(book), &mut Discard).unwrap());
    let _ = std::fs::remove_dir_all(&dir);
}
