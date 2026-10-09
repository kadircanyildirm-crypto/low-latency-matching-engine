//! The sequencer: numbers each command, journals it, and only then applies it.

use std::path::{Path, PathBuf};

use orderbook::{BookConfig, BookSnapshot, Command, Event, EventSink, OrderBook, Phase};

use crate::journal::{self, Expect, Journal, JournalReport};
use crate::snapshots;
use crate::storage::{FsStorage, Storage};
use crate::{Error, Seq};

/// When the journal is synced to stable storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Before [`Engine::submit`] or [`Engine::submit_batch`] returns: a crash loses
    /// nothing that was acknowledged. A batch shares one sync, which is group commit,
    /// unless it fills a segment: the full segment is synced before the next is created.
    Always,
    /// Only when a segment fills up, on [`Engine::sync`] and [`Engine::close`], and before
    /// a snapshot; otherwise the OS writes the journal back when it chooses. A crashed
    /// process loses nothing, since its writes are already with the OS, but a power failure
    /// loses what the OS had not yet written back.
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
    /// Records per new journal segment file; each record takes 64 bytes. Segments already
    /// on disk keep their own size.
    pub segment_capacity: u32,
    /// Take a snapshot before the first batch after this many commands have been journaled
    /// since the last one. `None` takes snapshots only on [`Engine::snapshot`].
    pub snapshot_every: Option<u64>,
    /// Snapshots to keep, at least one. Older ones are deleted. Once there are this many,
    /// so are the journal segments that hold nothing after the oldest kept one; until then
    /// the whole journal stays. With more than one, a damaged newest snapshot can fall back
    /// to an older one.
    pub keep_snapshots: usize,
    /// On opening, replay the journal from the snapshot before the newest one (or from the
    /// start, if the journal still begins there) and check that it reaches the newest
    /// snapshot's state. This proves on every start that replay is deterministic and that
    /// the files belong together, at the cost of replaying one snapshot interval.
    pub verify_replay: bool,
}

impl EngineConfig {
    /// [`SyncPolicy::Always`], segments of 2¹⁶ records (4 MiB), snapshots only on
    /// request, the last two kept, and replay verified on opening.
    pub fn new(book: BookConfig) -> Self {
        EngineConfig {
            book,
            sync: SyncPolicy::Always,
            segment_capacity: 1 << 16,
            snapshot_every: None,
            keep_snapshots: 2,
            verify_replay: true,
        }
    }
}

/// What opening an engine found on disk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// The snapshot recovery started from, if any.
    pub snapshot: Option<Seq>,
    /// Newer snapshots that failed to load, with the reason. They are renamed to
    /// `snapshot-<seq>.damaged`, once the rest of recovery has succeeded, and no longer
    /// used.
    pub damaged_snapshots: Vec<(Seq, String)>,
    /// The snapshot whose state replaying the journal from the one before it (or from the
    /// start) reproduced, if [`EngineConfig::verify_replay`] found the means to check.
    pub verified: Option<Seq>,
    /// Why the snapshot recovery started from could not be verified, if it could not.
    pub unverified: Option<String>,
    /// What replaying the journal found and did.
    pub journal: JournalReport,
}

/// Receives the book's events, each with the sequence number of the command it belongs to.
///
/// A command's events arrive while it is applied, after it was journaled. If the process
/// stops in between, they are lost with it; recovery replays the command and delivers them
/// again with the same sequence number.
///
/// A consumer that remembers the last sequence number it has fully handled says so through
/// [`resume_after`](Output::resume_after). Recovery then starts from a snapshot no later
/// than that, delivers the events of exactly the commands after it, and refuses with
/// [`Error::ConsumerAhead`] if the consumer has seen events of commands the journal no
/// longer holds, which a power failure under [`SyncPolicy::Os`] can cause. The journal and
/// snapshots must reach back to where the slowest consumer stands.
pub trait Output {
    /// Called once per event, in order.
    fn on_event(&mut self, seq: Seq, event: Event);

    /// The sequence number of the last command whose events this consumer has fully
    /// handled, or `None` if it does not keep track: then recovery starts from the newest
    /// snapshot and delivers the events of the commands after it.
    fn resume_after(&self) -> Option<Seq> {
        None
    }
}

impl Output for Vec<(Seq, Event)> {
    #[inline]
    fn on_event(&mut self, seq: Seq, event: Event) {
        self.push((seq, event));
    }
}

/// An [`Output`] that drops every event.
#[derive(Clone, Copy, Debug, Default)]
pub struct Discard;

impl Output for Discard {
    #[inline]
    fn on_event(&mut self, _: Seq, _: Event) {}
}

impl EventSink for Discard {
    #[inline]
    fn on_event(&mut self, _: Event) {}
}

/// Tags the book's events with the sequence number of the command being applied.
struct Tagged<'a, O: Output> {
    seq: Seq,
    out: &'a mut O,
}

impl<O: Output> EventSink for Tagged<'_, O> {
    #[inline]
    fn on_event(&mut self, event: Event) {
        self.out.on_event(self.seq, event);
    }
}

/// The order book behind a sequencer and a write-ahead journal.
pub struct Engine<S: Storage = FsStorage> {
    /// Held while the engine is open, so no second engine writes the same journal.
    _lock: S::Lock,
    book: OrderBook,
    journal: Journal<S>,
    dir: PathBuf,
    config: EngineConfig,
    /// The sequence number of the newest snapshot, or of the state recovery started from.
    last_snapshot: Seq,
    /// When the next automatic snapshot is due.
    next_snapshot: Option<Seq>,
    /// Why the last automatic snapshot failed, until someone asks.
    snapshot_failure: Option<Error>,
    poisoned: bool,
    /// Scratch space for encoding snapshots.
    snapshot_buf: Vec<u8>,
}

impl Engine<FsStorage> {
    /// Opens the engine whose files are in `dir`, creating it if `dir` holds none, and
    /// recovers its state: the newest intact snapshot, then every journaled command after
    /// it, whose events go to `out` with their sequence numbers.
    ///
    /// # Panics
    ///
    /// If `config.book` is a configuration [`OrderBook::new`] refuses, or
    /// `config.segment_capacity`, `config.keep_snapshots` or `config.snapshot_every` is
    /// zero.
    pub fn open<O: Output>(
        dir: impl AsRef<Path>,
        config: EngineConfig,
        out: &mut O,
    ) -> Result<(Engine<FsStorage>, RecoveryReport), Error> {
        Engine::open_with(FsStorage, dir.as_ref(), config, out)
    }
}

impl<S: Storage> Engine<S> {
    /// [`Engine::open`] on another storage, such as the crash-testing
    /// [`SimStorage`](crate::sim::SimStorage).
    ///
    /// # Panics
    ///
    /// As [`Engine::open`].
    pub fn open_with<O: Output>(
        mut storage: S,
        dir: &Path,
        config: EngineConfig,
        out: &mut O,
    ) -> Result<(Engine<S>, RecoveryReport), Error> {
        if let Err(error) = config.book.check() {
            panic!("{error}");
        }
        assert!(config.keep_snapshots > 0, "keep at least one snapshot");
        assert!(
            config.snapshot_every != Some(0),
            "snapshots need at least one command between them"
        );
        storage.create_dir_all(dir)?;
        let lock = storage.lock(dir)?.ok_or_else(|| Error::Locked {
            dir: dir.to_owned(),
        })?;
        snapshots::remove_partial(&mut storage, dir)?;
        let expect = Expect {
            fingerprint: fingerprint(&config.book),
            rules: orderbook::RULES_VERSION,
            capacity: config.segment_capacity,
        };
        let resume = out.resume_after();

        // The newest snapshot that loads, and is no later than what the consumer has
        // handled. Damaged ones are only set aside once everything else has worked; anything
        // but damage, a read error included, stops recovery.
        let mut report = RecoveryReport::default();
        let snapshot_seqs = snapshots::list(&mut storage, dir)?;
        let mut start = None;
        for &seq in snapshot_seqs
            .iter()
            .rev()
            .filter(|&&seq| resume.is_none_or(|resume| seq <= resume))
        {
            match snapshots::read(&mut storage, dir, seq, &config.book) {
                Ok(book) => {
                    start = Some((seq, book));
                    break;
                }
                Err(Error::Corrupt { detail, .. }) => report.damaged_snapshots.push((seq, detail)),
                Err(error) => return Err(error),
            }
        }
        let (after, mut book) = start.unwrap_or_else(|| (0, OrderBook::new(config.book)));
        report.snapshot = (after > 0).then_some(after);

        // Check that the journal replays to the snapshot, before anything is repaired or
        // delivered.
        if config.verify_replay && after > 0 {
            let older = snapshot_seqs.iter().rev().copied().find(|&seq| seq < after);
            let base = match older {
                Some(seq) => match snapshots::read(&mut storage, dir, seq, &config.book) {
                    Ok(book) => Ok((seq, book)),
                    Err(Error::Corrupt { detail, .. }) => {
                        report.damaged_snapshots.push((seq, detail.clone()));
                        Err(format!("the snapshot at {seq} is damaged: {detail}"))
                    }
                    Err(error) => return Err(error),
                },
                None => Ok((0, OrderBook::new(config.book))),
            };
            match base {
                Ok((from, mut replica)) => {
                    let replayed =
                        journal::read_range(&mut storage, dir, expect, from + 1, after, |_, c| {
                            replica.process(c, &mut Discard);
                        })?;
                    match replayed {
                        Ok(()) if replica.digest() == book.digest() => {
                            report.verified = Some(after);
                        }
                        Ok(()) => return Err(Error::Divergence { seq: after }),
                        Err(reason) => report.unverified = Some(reason),
                    }
                }
                Err(reason) => report.unverified = Some(reason),
            }
        }

        let (journal, journal_report) =
            Journal::open(storage, dir, expect, after, |seq, command| {
                if resume.is_none_or(|resume| seq > resume) {
                    book.process(
                        command,
                        &mut Tagged {
                            seq,
                            out: &mut *out,
                        },
                    );
                } else {
                    book.process(command, &mut Discard);
                }
                Ok(())
            })?;
        report.journal = journal_report;
        let mut journal = journal;
        if let Some(resume) = resume.filter(|&resume| resume > journal.last_seq()) {
            return Err(Error::ConsumerAhead {
                consumer: resume,
                journal: journal.last_seq(),
            });
        }

        // Not synced: if a rename is lost, the next recovery finds the damage again.
        let storage = journal.storage();
        for (seq, _) in &report.damaged_snapshots {
            let path = snapshots::path(dir, *seq);
            storage.rename(&path, &path.with_extension("damaged"))?;
        }

        Ok((
            Engine {
                _lock: lock,
                book,
                journal,
                dir: dir.to_owned(),
                config,
                last_snapshot: after,
                next_snapshot: config.snapshot_every.map(|every| after + every),
                snapshot_failure: None,
                poisoned: false,
                snapshot_buf: Vec::new(),
            },
            report,
        ))
    }

    /// Journals `command` under the next sequence number, syncs as the policy says, then
    /// applies it to the book, whose events go to `out` tagged with that number. Returns the
    /// sequence number.
    ///
    /// An error means the command was not applied. If a write or sync failed, the engine
    /// is poisoned: the record may or may not have reached the disk, so whether recovery
    /// will apply it is unknown, and the engine refuses further commands until reopened. A
    /// panic while the book applies a command poisons it too.
    pub fn submit<O: Output>(&mut self, command: Command, out: &mut O) -> Result<Seq, Error> {
        self.submit_batch(std::slice::from_ref(&command), out)
    }

    /// [`submit`](Self::submit) for several commands: they are journaled together, with a
    /// single sync under [`SyncPolicy::Always`] (group commit), and then applied in order.
    /// Returns the sequence number of the last one; an empty batch journals nothing and
    /// returns that of the last command before it.
    ///
    /// A due automatic snapshot is taken first. If it fails, the batch goes ahead anyway,
    /// the next attempt waits another `snapshot_every` commands, and
    /// [`take_failure`](Self::take_failure) says why.
    pub fn submit_batch<O: Output>(
        &mut self,
        commands: &[Command],
        out: &mut O,
    ) -> Result<Seq, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self
            .next_snapshot
            .is_some_and(|due| self.journal.last_seq() >= due)
        {
            if let Err(error) = self.snapshot() {
                if self.poisoned {
                    return Err(error);
                }
                self.snapshot_failure = Some(error);
                self.next_snapshot = self
                    .config
                    .snapshot_every
                    .map(|every| self.journal.last_seq() + every);
            }
        }
        let first = self.journal.last_seq() + 1;
        let mut journaled = self.journal.append(commands);
        if self.config.sync == SyncPolicy::Always {
            journaled = journaled.and_then(|seq| self.journal.sync().map(|()| seq));
        }
        let last = journaled.inspect_err(|_| self.poisoned = true)?;
        // Poisoned until every command is applied: a panic in the book leaves it so.
        self.poisoned = true;
        for (seq, &command) in (first..).zip(commands) {
            self.book.process(command, &mut Tagged { seq, out });
        }
        self.poisoned = false;
        Ok(last)
    }

    /// Makes every journaled command durable.
    pub fn sync(&mut self) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.journal.sync().inspect_err(|_| self.poisoned = true)
    }

    /// Syncs the journal, records that everything in it is durable, and closes the engine.
    /// The record spares the next recovery from writing the journal's tail again. Dropping
    /// an engine closes it too, but without the sync under [`SyncPolicy::Os`], without the
    /// record, and without a chance to report an error.
    pub fn close(mut self) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.journal.close()
    }

    /// Writes a snapshot of the book now, after syncing the journal, reads it back to check
    /// that it restores to exactly this book, then deletes the snapshots and journal
    /// segments that are no longer needed. Does nothing if the newest snapshot is already of
    /// the current state.
    ///
    /// A snapshot that fails to write or to check is not used, and nothing is deleted; the
    /// engine stays usable. Only a failed journal sync poisons it.
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
        // Nothing older may go before this one is known to load.
        let check = snapshots::read(storage, &self.dir, seq, &self.config.book);
        if let Err(error) = check.and_then(|copy| {
            if copy.digest() == self.book.digest() {
                Ok(())
            } else {
                Err(Error::Corrupt {
                    file: snapshots::path(&self.dir, seq),
                    detail: "it restores to another state".into(),
                })
            }
        }) {
            let path = snapshots::path(&self.dir, seq);
            let _ = storage.rename(&path, &path.with_extension("damaged"));
            return Err(error);
        }
        self.last_snapshot = seq;
        self.next_snapshot = self.config.snapshot_every.map(|every| seq + every);
        let seqs = snapshots::list(storage, &self.dir)?;
        let keep_from = seqs.len().saturating_sub(self.config.keep_snapshots);
        for &old in &seqs[..keep_from] {
            snapshots::remove(storage, &self.dir, old)?;
        }
        // Damaged snapshots are kept for inspection only while they are among the newest.
        snapshots::remove_damaged_before(storage, &self.dir, seqs[keep_from])?;
        storage.sync_dir(&self.dir)?;
        // Until there are as many snapshots as are kept, the whole journal stays, so a
        // damaged newest snapshot can fall back to an older one or to the start.
        if seqs.len() - keep_from == self.config.keep_snapshots {
            self.journal.remove_through(seqs[keep_from])?;
        }
        Ok(())
    }

    /// The last failure that did not stop the engine, if there was one since the last call:
    /// an automatic snapshot that could not be taken, or a next journal segment that could
    /// not be prepared (it is prepared again when it is needed).
    pub fn take_failure(&mut self) -> Option<Error> {
        self.snapshot_failure
            .take()
            .or_else(|| self.journal.take_prepare_failure())
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
