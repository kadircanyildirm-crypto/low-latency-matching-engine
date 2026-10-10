//! The sequencer: numbers each command, journals it, and only then applies it.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use orderbook::{BookConfig, BookSnapshot, Command, Event, EventSink, OrderBook, Phase};

use crate::journal::{self, Expect, Journal, JournalReport};
use crate::snapshots;
use crate::storage::{FsStorage, Storage};
use crate::{Error, Seq};

/// One command in this many has the book's time on it measured alone: see [`Timings`].
pub const MATCH_SAMPLE: Seq = 64;

/// Events a sampled command may have and still be measured: past them, its events go out
/// as they come, and it is not counted. The space is reserved once, so measuring allocates
/// nothing.
const SAMPLE_EVENTS: usize = 256;

/// Where the engine's time went, since it was last asked: [`Engine::take_timings`],
/// [`Writer::take_timings`] and [`Matcher::take_timings`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Timings {
    /// Batches journaled.
    pub batches: u64,
    /// Nanoseconds spent appending them to the journal.
    pub write_ns: u64,
    /// Nanoseconds spent syncing the journal.
    pub sync_ns: u64,
    /// Commands applied.
    pub commands: u64,
    /// Nanoseconds spent applying them, with the output's handling of their events.
    pub apply_ns: u64,
    /// Commands whose matching was measured alone: one in [`MATCH_SAMPLE`] by sequence
    /// number. The book's events are collected while it works and handed on after, so the
    /// time is the book's own.
    pub matched: u64,
    /// Nanoseconds the book spent on them.
    pub match_ns: u64,
}

impl Timings {
    /// Adds `other` to these.
    pub fn add(&mut self, other: Timings) {
        self.batches += other.batches;
        self.write_ns += other.write_ns;
        self.sync_ns += other.sync_ns;
        self.commands += other.commands;
        self.apply_ns += other.apply_ns;
        self.matched += other.matched;
        self.match_ns += other.match_ns;
    }
}

/// A duration in nanoseconds, as far as a `u64` goes.
fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

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

    /// Called with each command before its events, for consumers that keep state about
    /// commands, such as who placed an order, and must rebuild it on recovery. Does nothing
    /// unless implemented.
    fn on_command(&mut self, seq: Seq, command: Command) {
        let _ = (seq, command);
    }

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

/// Collects the events of a command being measured, to hand them on after. Once the
/// reserved space is full, it hands on what it has and the rest as they come.
struct Sampled<'a, O: Output> {
    seq: Seq,
    events: &'a mut Vec<Event>,
    out: &'a mut O,
    spilled: bool,
}

impl<O: Output> EventSink for Sampled<'_, O> {
    #[inline]
    fn on_event(&mut self, event: Event) {
        if !self.spilled && self.events.len() < self.events.capacity() {
            self.events.push(event);
            return;
        }
        if !self.spilled {
            self.spilled = true;
            for event in self.events.drain(..) {
                self.out.on_event(self.seq, event);
            }
        }
        self.out.on_event(self.seq, event);
    }
}

/// The order book behind a sequencer and a write-ahead journal.
///
/// An engine is a [`Writer`], which journals commands, and a [`Matcher`], which applies
/// them to the book and takes snapshots, working in step on one thread.
/// [`split`](Engine::split) separates them, for a pipeline that runs each on a thread of its
/// own.
pub struct Engine<S: Storage = FsStorage> {
    writer: Writer<S>,
    matcher: Matcher<S>,
}

/// The journal half of an [`Engine`]: it numbers commands and writes them, and syncs them as
/// the policy says.
pub struct Writer<S: Storage = FsStorage> {
    /// Held while the writer exists, so no second engine writes the same journal.
    _lock: S::Lock,
    journal: Journal<S>,
    sync: SyncPolicy,
    poisoned: bool,
    timings: Timings,
}

/// The book half of an [`Engine`]: it applies journaled commands to the book and takes
/// snapshots of it.
pub struct Matcher<S: Storage = FsStorage> {
    book: OrderBook,
    storage: S,
    dir: PathBuf,
    config: EngineConfig,
    /// The sequence number of the last command applied.
    last_seq: Seq,
    /// The sequence number of the newest snapshot, or of the state recovery started from.
    last_snapshot: Seq,
    /// When the next automatic snapshot is due.
    next_snapshot: Option<Seq>,
    /// Why the last automatic snapshot failed, until someone asks.
    snapshot_failure: Option<Error>,
    poisoned: bool,
    /// Scratch space for encoding snapshots.
    snapshot_buf: Vec<u8>,
    timings: Timings,
    /// The events of a command being measured.
    sample: Vec<Event>,
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
                    out.on_command(seq, command);
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

        let last_seq = journal.last_seq();
        let writer = Writer {
            _lock: lock,
            journal,
            sync: config.sync,
            poisoned: false,
            timings: Timings::default(),
        };
        let matcher = Matcher {
            book,
            storage: writer.journal.storage_clone(),
            dir: dir.to_owned(),
            config,
            last_seq,
            last_snapshot: after,
            next_snapshot: config.snapshot_every.map(|every| after + every),
            snapshot_failure: None,
            poisoned: false,
            snapshot_buf: Vec::new(),
            timings: Timings::default(),
            sample: Vec::with_capacity(SAMPLE_EVENTS),
        };
        Ok((Engine { writer, matcher }, report))
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
        if self.writer.poisoned || self.matcher.poisoned {
            return Err(Error::Poisoned);
        }
        if self.matcher.snapshot_due() {
            if let Err(error) = self.snapshot() {
                if self.writer.poisoned {
                    return Err(error);
                }
                self.matcher.postpone_snapshot(error);
            }
        }
        let first = self.writer.last_seq() + 1;
        let last = self.writer.write(commands)?;
        self.matcher
            .apply_all((first..).zip(commands.iter().copied()), out)?;
        Ok(last)
    }

    /// Makes every journaled command durable.
    pub fn sync(&mut self) -> Result<(), Error> {
        self.writer.sync()
    }

    /// Syncs the journal, records that everything in it is durable, and closes the engine.
    /// The record spares the next recovery from writing the journal's tail again. Dropping
    /// an engine closes it too, but without the sync under [`SyncPolicy::Os`], without the
    /// record, and without a chance to report an error.
    pub fn close(self) -> Result<(), Error> {
        if self.matcher.poisoned {
            return Err(Error::Poisoned);
        }
        self.writer.close()
    }

    /// Writes a snapshot of the book now, after syncing the journal, reads it back to check
    /// that it restores to exactly this book, then deletes the snapshots and journal
    /// segments that are no longer needed. Does nothing if the newest snapshot is already of
    /// the current state.
    ///
    /// A snapshot that fails to write or to check is not used, and nothing is deleted; the
    /// engine stays usable. Only a failed journal sync poisons it.
    pub fn snapshot(&mut self) -> Result<(), Error> {
        if self.writer.poisoned || self.matcher.poisoned {
            return Err(Error::Poisoned);
        }
        if self.matcher.last_seq == self.matcher.last_snapshot {
            return Ok(());
        }
        // The journal must reach the snapshot before the snapshot exists.
        self.writer.sync()?;
        if let Some(seq) = self.matcher.snapshot(self.writer.durable_seq())? {
            self.writer.remove_through(seq)?;
        }
        Ok(())
    }

    /// The last failure that did not stop the engine, if there was one since the last call:
    /// an automatic snapshot that could not be taken, or a next journal segment that could
    /// not be prepared (it is prepared again when it is needed).
    pub fn take_failure(&mut self) -> Option<Error> {
        self.matcher
            .snapshot_failure
            .take()
            .or_else(|| self.writer.take_failure())
    }

    /// The book.
    pub fn book(&self) -> &OrderBook {
        &self.matcher.book
    }

    /// Where the engine's time went since the last call.
    pub fn take_timings(&mut self) -> Timings {
        let mut timings = self.writer.take_timings();
        timings.add(self.matcher.take_timings());
        timings
    }

    /// The sequence number of the last command submitted, or recovered.
    pub fn last_seq(&self) -> Seq {
        self.writer.last_seq()
    }

    /// The highest sequence number known to be on stable storage.
    pub fn durable_seq(&self) -> Seq {
        self.writer.durable_seq()
    }

    /// The sequence number of the newest snapshot, or of the state recovery started from.
    pub fn last_snapshot(&self) -> Seq {
        self.matcher.last_snapshot
    }

    /// The configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.matcher.config
    }

    /// Separates the engine into its writer and its matcher, for a pipeline that journals on
    /// one thread and matches on another. The caller then keeps the promises the engine
    /// keeps itself: the matcher applies each command only once the writer has journaled it
    /// (and synced it, under [`SyncPolicy::Always`]), in sequence order; before the
    /// matcher takes a snapshot, the writer syncs at least as far; and segments are removed
    /// only through the sequence number a snapshot returns.
    pub fn split(self) -> (Writer<S>, Matcher<S>) {
        (self.writer, self.matcher)
    }
}

impl<S: Storage> Writer<S> {
    /// Journals `commands` under the next sequence numbers, and syncs them under
    /// [`SyncPolicy::Always`]. Returns the sequence number of the last one; an empty batch
    /// journals nothing and returns that of the last command before it.
    ///
    /// An error poisons the writer: the records may or may not have reached the disk, so
    /// whether recovery will apply them is unknown, and it refuses everything until the
    /// journal is reopened.
    pub fn write(&mut self, commands: &[Command]) -> Result<Seq, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let start = Instant::now();
        let mut journaled = self.journal.append(commands);
        let appended = Instant::now();
        self.timings.batches += 1;
        self.timings.write_ns += nanos(appended - start);
        if self.sync == SyncPolicy::Always {
            journaled = journaled.and_then(|seq| self.journal.sync().map(|()| seq));
            self.timings.sync_ns += nanos(appended.elapsed());
        }
        journaled.inspect_err(|_| self.poisoned = true)
    }

    /// Makes every journaled command durable.
    pub fn sync(&mut self) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let start = Instant::now();
        let synced = self.journal.sync().inspect_err(|_| self.poisoned = true);
        self.timings.sync_ns += nanos(start.elapsed());
        synced
    }

    /// Where the writer's time went since the last call: the batches it journaled, and
    /// how long appending and syncing took.
    pub fn take_timings(&mut self) -> Timings {
        std::mem::take(&mut self.timings)
    }

    /// Deletes the journal segments that hold nothing after `seq`: what
    /// [`Matcher::snapshot`] returned.
    pub fn remove_through(&mut self, seq: Seq) -> Result<(), Error> {
        self.journal.remove_through(seq).map(|_| ())
    }

    /// Syncs the journal, records that everything in it is durable, and closes it, as
    /// [`Engine::close`] does.
    pub fn close(mut self) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.journal.close()
    }

    /// Why the next journal segment could not be prepared, if that failed since the last
    /// call. It is prepared again when it is needed.
    pub fn take_failure(&mut self) -> Option<Error> {
        self.journal.take_prepare_failure()
    }

    /// The sequence number of the last command journaled.
    pub fn last_seq(&self) -> Seq {
        self.journal.last_seq()
    }

    /// The highest sequence number known to be on stable storage.
    pub fn durable_seq(&self) -> Seq {
        self.journal.durable()
    }

    /// Whether a failed write or sync has poisoned the writer.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
}

impl<S: Storage> Matcher<S> {
    /// Applies the command journaled under `seq`, which must follow the last one applied;
    /// its events go to `out` tagged with `seq`. A panic in the book poisons the matcher.
    ///
    /// # Panics
    ///
    /// If `seq` does not follow the last command applied.
    pub fn apply<O: Output>(
        &mut self,
        seq: Seq,
        command: Command,
        out: &mut O,
    ) -> Result<(), Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        assert_eq!(seq, self.last_seq + 1, "commands are applied in sequence");
        // Poisoned until the command is applied: a panic in the book leaves it so.
        self.poisoned = true;
        out.on_command(seq, command);
        if seq % MATCH_SAMPLE == 0 {
            self.sample.clear();
            let mut sampled = Sampled {
                seq,
                events: &mut self.sample,
                out: &mut *out,
                spilled: false,
            };
            let start = Instant::now();
            self.book.process(command, &mut sampled);
            let took = start.elapsed();
            if !sampled.spilled {
                self.timings.matched += 1;
                self.timings.match_ns += nanos(took);
            }
            for event in self.sample.drain(..) {
                out.on_event(seq, event);
            }
        } else {
            self.book.process(command, &mut Tagged { seq, out });
        }
        self.poisoned = false;
        self.last_seq = seq;
        Ok(())
    }

    /// Applies `commands`, each as [`apply`](Self::apply) does, and counts how long they
    /// took together in the [`Timings`]. It stops at the first error.
    pub fn apply_all<O: Output>(
        &mut self,
        commands: impl IntoIterator<Item = (Seq, Command)>,
        out: &mut O,
    ) -> Result<(), Error> {
        let start = Instant::now();
        let mut result = Ok(());
        for (seq, command) in commands {
            if let Err(error) = self.apply(seq, command, out) {
                result = Err(error);
                break;
            }
            self.timings.commands += 1;
        }
        self.timings.apply_ns += nanos(start.elapsed());
        result
    }

    /// Where the matcher's time went since the last call: the commands it applied and how
    /// long they took, and the book's own time on those it measured alone.
    pub fn take_timings(&mut self) -> Timings {
        std::mem::take(&mut self.timings)
    }

    /// Whether an automatic snapshot is due: `snapshot_every` commands have been applied
    /// since the last one, or since the last failed attempt.
    pub fn snapshot_due(&self) -> bool {
        self.next_snapshot.is_some_and(|due| self.last_seq >= due)
    }

    /// Records why an automatic snapshot failed, for [`take_failure`](Self::take_failure),
    /// and puts the next attempt off by another `snapshot_every` commands.
    pub fn postpone_snapshot(&mut self, error: Error) {
        self.snapshot_failure = Some(error);
        self.next_snapshot = self
            .config
            .snapshot_every
            .map(|every| self.last_seq + every);
    }

    /// Writes a snapshot of the book now, reads it back to check that it restores to
    /// exactly this book, and deletes the snapshots no longer needed. `durable` is the
    /// writer's [`durable_seq`](Writer::durable_seq): the journal must reach the snapshot
    /// before the snapshot exists, so that the journal and an older snapshot can rebuild
    /// it too. Returns the sequence number through which the writer may delete journal
    /// segments, once there are as many snapshots as are kept. Does nothing if the newest
    /// snapshot is already of the current state.
    ///
    /// A snapshot that fails to write or to check is not used, and nothing is deleted.
    ///
    /// # Panics
    ///
    /// If `durable` is before [`last_seq`](Self::last_seq).
    pub fn snapshot(&mut self, durable: Seq) -> Result<Option<Seq>, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let seq = self.last_seq;
        if seq == self.last_snapshot {
            return Ok(None);
        }
        assert!(
            durable >= seq,
            "the journal is durable through {durable}, not through the snapshot at {seq}"
        );
        let storage = &mut self.storage;
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
        Ok((seqs.len() - keep_from == self.config.keep_snapshots).then_some(seqs[keep_from]))
    }

    /// Why the last automatic snapshot failed, if it did since the last call.
    pub fn take_failure(&mut self) -> Option<Error> {
        self.snapshot_failure.take()
    }

    /// The book.
    pub fn book(&self) -> &OrderBook {
        &self.book
    }

    /// The sequence number of the last command applied.
    pub fn last_seq(&self) -> Seq {
        self.last_seq
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
