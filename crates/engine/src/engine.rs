//! The sequencer: numbers each command, journals it, and only then applies it.

use std::path::{Path, PathBuf};

use orderbook::{BookConfig, BookSnapshot, Command, Event, EventSink, OrderBook, Phase};

use crate::journal::{Journal, JournalReport};
use crate::snapshots;
use crate::storage::{FsStorage, Storage};
use crate::{Error, Seq};

/// When the journal is synced to stable storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Before [`Engine::submit`] or [`Engine::submit_batch`] returns: a crash loses
    /// nothing that was acknowledged. A batch shares one sync, which is group commit.
    Always,
    /// Only on [`Engine::sync`] and before a snapshot; otherwise the OS writes the journal
    /// back when it chooses. A crashed process loses nothing, since its writes are already
    /// with the OS, but a power failure loses what the OS had not yet written back.
    Os,
}

/// How an [`Engine`] stores its journal and snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineConfig {
    /// The book's configuration. The journal and every snapshot record it, and the engine
    /// refuses files written for another one.
    pub book: BookConfig,
    /// When the journal is synced.
    pub sync: SyncPolicy,
    /// Records per journal segment file; each record takes 64 bytes.
    pub segment_capacity: u32,
    /// Take a snapshot whenever this many commands have been journaled since the last one.
    /// `None` takes snapshots only on [`Engine::snapshot`].
    pub snapshot_every: Option<u64>,
    /// Snapshots to keep, at least one. Older ones are deleted, and so are the journal
    /// segments that only the deleted ones needed.
    pub keep_snapshots: usize,
}

impl EngineConfig {
    /// [`SyncPolicy::Always`], segments of 2²⁰ records (64 MiB), snapshots only on
    /// request, and the last two kept.
    pub fn new(book: BookConfig) -> Self {
        EngineConfig {
            book,
            sync: SyncPolicy::Always,
            segment_capacity: 1 << 20,
            snapshot_every: None,
            keep_snapshots: 2,
        }
    }
}

/// What opening an engine found on disk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// The snapshot recovery started from, if any.
    pub snapshot: Option<Seq>,
    /// Newer snapshots that failed to load, with the reason. They are renamed to
    /// `snapshot-<seq>.damaged` and no longer used.
    pub damaged_snapshots: Vec<(Seq, String)>,
    /// What replaying the journal found and did.
    pub journal: JournalReport,
}

/// The order book behind a sequencer and a write-ahead journal.
pub struct Engine<S: Storage = FsStorage> {
    book: OrderBook,
    journal: Journal<S>,
    dir: PathBuf,
    config: EngineConfig,
    /// The sequence number of the newest snapshot, or of the state recovery started from.
    last_snapshot: Seq,
    poisoned: bool,
    /// Scratch space for encoding snapshots.
    snapshot_buf: Vec<u8>,
}

/// Discards the events of replayed commands: they were delivered before the crash.
struct Discard;

impl EventSink for Discard {
    #[inline]
    fn on_event(&mut self, _: Event) {}
}

impl Engine<FsStorage> {
    /// Opens the engine whose files are in `dir`, creating it if `dir` holds none, and
    /// recovers its state: the newest intact snapshot, then every journaled command after
    /// it.
    ///
    /// # Panics
    ///
    /// If `config.book` is a configuration [`OrderBook::new`] refuses, or
    /// `config.segment_capacity` or `config.keep_snapshots` is zero.
    pub fn open(
        dir: impl AsRef<Path>,
        config: EngineConfig,
    ) -> Result<(Engine<FsStorage>, RecoveryReport), Error> {
        Engine::open_with(FsStorage, dir.as_ref(), config)
    }
}

impl<S: Storage> Engine<S> {
    /// [`Engine::open`] on another storage, such as the crash-testing
    /// [`SimStorage`](crate::storage::SimStorage).
    ///
    /// # Panics
    ///
    /// As [`Engine::open`].
    pub fn open_with(
        mut storage: S,
        dir: &Path,
        config: EngineConfig,
    ) -> Result<(Engine<S>, RecoveryReport), Error> {
        if let Err(error) = config.book.check() {
            panic!("{error}");
        }
        assert!(config.keep_snapshots > 0, "keep at least one snapshot");
        storage.create_dir_all(dir)?;
        snapshots::remove_partial(&mut storage, dir)?;

        let mut report = RecoveryReport::default();
        let mut start = None;
        for seq in snapshots::list(&mut storage, dir)?.into_iter().rev() {
            match snapshots::read(&mut storage, dir, seq, &config.book) {
                Ok(book) => {
                    start = Some((seq, book));
                    break;
                }
                Err(Error::Corrupt { detail, .. }) => {
                    let path = snapshots::path(dir, seq);
                    storage.rename(&path, &path.with_extension("damaged"))?;
                    storage.sync_dir(dir)?;
                    report.damaged_snapshots.push((seq, detail));
                }
                Err(error) => return Err(error),
            }
        }
        let (after, mut book) = start.unwrap_or_else(|| (0, OrderBook::new(config.book)));
        report.snapshot = (after > 0).then_some(after);

        let (journal, journal_report) = Journal::open(
            storage,
            dir,
            fingerprint(&config.book),
            config.segment_capacity,
            after,
            |_, command| {
                book.process(command, &mut Discard);
                Ok(())
            },
        )?;
        report.journal = journal_report;
        Ok((
            Engine {
                book,
                journal,
                dir: dir.to_owned(),
                config,
                last_snapshot: after,
                poisoned: false,
                snapshot_buf: Vec::new(),
            },
            report,
        ))
    }

    /// Journals `command` under the next sequence number, syncs as the policy says, then
    /// applies it to the book, which reports to `sink`. Returns the sequence number.
    ///
    /// An error means the command was not applied. If a write or sync failed, the engine
    /// is poisoned: the record may or may not have reached the disk, so whether recovery
    /// will apply it is unknown, and the engine refuses further commands until reopened.
    pub fn submit<E: EventSink>(&mut self, command: Command, sink: &mut E) -> Result<Seq, Error> {
        self.submit_batch(std::slice::from_ref(&command), sink)
    }

    /// [`submit`](Self::submit) for several commands: they are journaled together, with a
    /// single sync under [`SyncPolicy::Always`] (group commit), and then applied in order.
    /// Returns the sequence number of the last one.
    pub fn submit_batch<E: EventSink>(
        &mut self,
        commands: &[Command],
        sink: &mut E,
    ) -> Result<Seq, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if let Some(every) = self.config.snapshot_every {
            if self.journal.last_seq() - self.last_snapshot >= every {
                self.snapshot()?;
            }
        }
        let mut journaled = self.journal.append(commands);
        if self.config.sync == SyncPolicy::Always {
            journaled = journaled.and_then(|seq| self.journal.sync().map(|()| seq));
        }
        let last = journaled.inspect_err(|_| self.poisoned = true)?;
        for &command in commands {
            self.book.process(command, sink);
        }
        Ok(last)
    }

    /// Makes every journaled command durable.
    pub fn sync(&mut self) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.journal.sync().inspect_err(|_| self.poisoned = true)
    }

    /// Writes a snapshot of the book now, after syncing the journal, then deletes the
    /// snapshots and journal segments that are no longer needed. Does nothing if the newest
    /// snapshot is already of the current state.
    pub fn snapshot(&mut self) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let seq = self.journal.last_seq();
        if seq == self.last_snapshot {
            return Ok(());
        }
        // The journal must reach the snapshot before the snapshot exists.
        self.sync()?;
        let storage = self.journal.storage();
        snapshots::write(storage, &self.dir, seq, &self.book, &mut self.snapshot_buf)?;
        self.last_snapshot = seq;
        let seqs = snapshots::list(storage, &self.dir)?;
        let keep_from = seqs.len().saturating_sub(self.config.keep_snapshots);
        for &old in &seqs[..keep_from] {
            snapshots::remove(storage, &self.dir, old)?;
        }
        if keep_from > 0 {
            storage.sync_dir(&self.dir)?;
        }
        self.journal.remove_through(seqs[keep_from])?;
        Ok(())
    }

    /// The book.
    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    /// The sequence number of the last command submitted, or recovered.
    pub fn last_seq(&self) -> Seq {
        self.journal.last_seq()
    }

    /// The highest sequence number known to be on stable storage.
    pub fn durable_seq(&self) -> Seq {
        self.journal.durable()
    }

    /// The sequence number of the newest snapshot, or of the state recovery started from.
    pub fn last_snapshot(&self) -> Seq {
        self.last_snapshot
    }

    /// The configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }
}

/// A digest of the configuration alone, which every journal segment records.
fn fingerprint(config: &BookConfig) -> u64 {
    BookSnapshot {
        config: *config,
        trade_count: 0,
        reference_price: None,
        phase: Phase::Continuous,
        orders: Vec::new(),
        stops: Vec::new(),
    }
    .digest()
}
