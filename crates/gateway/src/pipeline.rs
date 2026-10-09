//! The engine on threads of its own: a [`Core`] that journals on one thread and matches on
//! another, connected to the server's thread by ring buffers.
//!
//! ```text
//!   network thread           writer thread             matcher thread
//!   sessions, risk,  ──A──▶  journal: write, sync, ──B──▶  book: apply,  ──┐
//!   routing          ◀──────────────────────C──────────── snapshots      ◀─┘
//! ```
//!
//! - **A** carries commands, numbered by the exchange in the order the writer journals
//!   them.
//! - **B** carries each command with its sequence number, once it is journaled, and
//!   synced under `SyncPolicy::Always`: the book never sees a command the journal could
//!   lose, and since reports come from the book's events, no client hears of one either.
//! - **C** carries the book's events back to the network thread, which routes them.
//!
//! The writer takes whatever waits in A as one batch, so the slower the disk, the larger
//! each sync's batch: group commit happens by itself. Segment rolls and syncs stay on the
//! writer's thread; the matcher never waits for the disk, except for a snapshot under
//! `SyncPolicy::Os`, which needs the journal synced through it first. It asks the writer
//! for that sync, and the writer, which owns the journal, also removes the segments the
//! snapshot freed.
//!
//! Every ring is bounded. When A is full the network thread stops reading its sockets, so
//! clients wait; the writer waits for room in B, and the matcher for room in C. The network
//! thread never blocks on a ring and always drains C, so the waits cannot form a cycle.
//!
//! A failure on either thread ends it, and the rings close behind it: the matcher applies
//! what the writer journaled before it stopped, the network thread delivers what the
//! matcher applied, and then reports the failure, which stops the server.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};

use engine::storage::Storage;
use engine::{Engine, Error, Matcher, Output, Seq, Writer};
use orderbook::{Command, Event};
use ring::{Backoff, Consumer, Producer, Wait};

use crate::exchange::{Exchange, Mailbox};
use crate::server::Core;

/// How a pipeline runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PipelineConfig {
    /// Items each ring holds.
    pub capacity: usize,
    /// How the writer and the matcher wait for work.
    pub wait: Wait,
    /// The logical core to pin the writer's thread to, if any.
    pub writer_core: Option<usize>,
    /// The logical core to pin the matcher's thread to, if any.
    pub matcher_core: Option<usize>,
}

impl Default for PipelineConfig {
    /// Rings of 65,536 items, backing off while idle, no pinning.
    fn default() -> Self {
        PipelineConfig {
            capacity: 1 << 16,
            wait: Wait::Backoff,
            writer_core: None,
            matcher_core: None,
        }
    }
}

/// Commands the writer takes from A at most per batch.
const MAX_BATCH: usize = 4_096;

/// What the threads tell each other.
#[derive(Debug, Default)]
struct Shared {
    /// The writer's durable sequence number.
    durable: AtomicU64,
    /// The matcher's last applied sequence number, published after its events.
    applied: AtomicU64,
    /// The sequence number the matcher needs synced, for a snapshot.
    sync_to: AtomicU64,
    /// The sequence number through which the writer may remove segments; zero for none.
    trim: AtomicU64,
    /// Set by the writer when it fails, so a matcher waiting for a sync stops waiting.
    failed: AtomicBool,
}

/// The writer's and the matcher's threads, behind a server.
pub struct Pipeline<S: Storage> {
    commands: Producer<Command>,
    events: Consumer<(Seq, Event)>,
    shared: Arc<Shared>,
    /// Commands taken from the exchange, from `pending_at` on not yet in A.
    pending: Vec<Command>,
    pending_at: usize,
    /// The sequence number of the last command put into A.
    handed: Seq,
    writer: Option<JoinHandle<Result<Writer<S>, Error>>>,
    matcher: Option<JoinHandle<Result<Matcher<S>, Error>>>,
    /// Whether a thread failed, and the failure was reported.
    failed: bool,
}

impl<S: Storage + 'static> Pipeline<S>
where
    Writer<S>: Send,
    Matcher<S>: Send,
{
    /// Splits `engine` and starts its writer and matcher on threads of their own.
    pub fn start(engine: Engine<S>, config: PipelineConfig) -> io::Result<Pipeline<S>> {
        let last_seq = engine.last_seq();
        let (writer, matcher) = engine.split();
        let shared = Arc::new(Shared::default());
        shared
            .durable
            .store(writer.durable_seq(), Ordering::Release);
        shared.applied.store(last_seq, Ordering::Release);
        let (commands, a) = ring::channel(config.capacity);
        let (b_in, b) = ring::channel(config.capacity);
        let (c_in, events) = ring::channel(config.capacity);
        let writer = {
            let shared = shared.clone();
            thread::Builder::new()
                .name("writer".into())
                .spawn(move || {
                    pin(config.writer_core);
                    write(writer, a, b_in, &shared, config.wait)
                })?
        };
        let matcher = {
            let shared = shared.clone();
            thread::Builder::new()
                .name("matcher".into())
                .spawn(move || {
                    pin(config.matcher_core);
                    apply(matcher, b, c_in, &shared, config.wait)
                })?
        };
        Ok(Pipeline {
            commands,
            events,
            shared,
            pending: Vec::new(),
            pending_at: 0,
            handed: last_seq,
            writer: Some(writer),
            matcher: Some(matcher),
            failed: false,
        })
    }

    /// Stops the threads once they have journaled and applied every command handed over,
    /// discarding the events still to come, and gives the halves back, to close the
    /// journal. Commands taken from the exchange but not yet handed over are dropped. If a
    /// thread failed, says why.
    pub fn stop(mut self) -> Result<(Writer<S>, Matcher<S>), Error> {
        if self.failed {
            return Err(Error::Poisoned);
        }
        // Closing A ends the writer once it has taken everything; it then closes B, which
        // ends the matcher once it has applied everything.
        let (closed, _) = ring::channel(1);
        drop(std::mem::replace(&mut self.commands, closed));
        let matcher = self.matcher.take().expect("a matcher thread");
        let mut backoff = Backoff::new(Wait::Backoff);
        while !matcher.is_finished() {
            // The matcher may be waiting for room for its events.
            if self.events.drain(usize::MAX).count() == 0 {
                backoff.snooze();
            }
        }
        let writer = self.writer.take().expect("a writer thread");
        let writer = join(writer)?;
        let matcher = join(matcher)?;
        Ok((writer, matcher))
    }

    /// The sequence number of the last command the matcher has applied.
    pub fn applied(&self) -> Seq {
        self.shared.applied.load(Ordering::Acquire)
    }

    /// The highest sequence number on stable storage.
    pub fn durable(&self) -> Seq {
        self.shared.durable.load(Ordering::Acquire)
    }

    /// Why a thread stopped: the writer's failure, which stops the matcher too, or else a
    /// panic in the book, which poisons the matcher.
    fn failure(&mut self) -> Error {
        if let Some(Err(error)) = self.writer.take().map(join) {
            return error;
        }
        drop(self.matcher.take().map(join));
        Error::Poisoned
    }
}

impl<S: Storage + 'static> Core for Pipeline<S>
where
    Writer<S>: Send,
    Matcher<S>: Send,
{
    fn turn(&mut self, exchange: &mut Exchange, mail: &mut impl Mailbox) -> Result<(), Error> {
        if self.failed {
            return Err(Error::Poisoned);
        }
        if self.pending_at == self.pending.len() {
            self.pending.clear();
            self.pending_at = 0;
            exchange.take_batch(&mut self.pending);
        }
        let mut rest = self.pending[self.pending_at..].iter().copied();
        let handed = self.commands.push_from(&mut rest);
        self.pending_at += handed;
        self.handed += handed as u64;
        for (seq, event) in self.events.drain(usize::MAX) {
            exchange.deliver(seq, event, mail);
        }
        // The matcher closes C only when it stops, and it stops early only on a failure.
        if self.events.is_closed() {
            self.failed = true;
            return Err(self.failure());
        }
        Ok(())
    }

    fn busy(&mut self) -> bool {
        // The matcher publishes how far it has applied after the events of those commands,
        // so the ring is checked after that: checked before, it could look empty just
        // before the last events arrived, and they would wait for the next round.
        let applied = self.applied();
        self.pending_at < self.pending.len() || applied < self.handed || !self.events.is_empty()
    }

    fn backed_up(&self) -> bool {
        self.pending_at < self.pending.len()
    }
}

/// The result of a stage's thread, a panic counting as a poisoned stage.
fn join<T>(thread: JoinHandle<Result<T, Error>>) -> Result<T, Error> {
    thread.join().unwrap_or(Err(Error::Poisoned))
}

fn pin(core: Option<usize>) {
    let Some(id) = core else { return };
    if let Some(core) = core_affinity::get_core_ids()
        .unwrap_or_default()
        .into_iter()
        .find(|core| core.id == id)
    {
        core_affinity::set_for_current(core);
    }
}

/// The writer's loop: takes batches from A, journals them, passes them on into B, and
/// serves the matcher's requests.
fn write<S: Storage>(
    mut writer: Writer<S>,
    mut input: Consumer<Command>,
    mut output: Producer<(Seq, Command)>,
    shared: &Shared,
    wait: Wait,
) -> Result<Writer<S>, Error> {
    let result = write_all(&mut writer, &mut input, &mut output, shared, wait);
    if result.is_err() {
        shared.failed.store(true, Ordering::Release);
    }
    result.map(|()| writer)
}

fn write_all<S: Storage>(
    writer: &mut Writer<S>,
    input: &mut Consumer<Command>,
    output: &mut Producer<(Seq, Command)>,
    shared: &Shared,
    wait: Wait,
) -> Result<(), Error> {
    let mut batch = Vec::with_capacity(MAX_BATCH);
    let mut backoff = Backoff::new(wait);
    loop {
        serve(writer, shared)?;
        batch.extend(input.drain(MAX_BATCH));
        if batch.is_empty() {
            if input.is_closed() {
                return Ok(());
            }
            backoff.snooze();
            continue;
        }
        backoff.reset();
        let first = writer.last_seq() + 1;
        writer.write(&batch)?;
        shared
            .durable
            .store(writer.durable_seq(), Ordering::Release);
        let mut items = (first..).zip(batch.drain(..)).inspect(|&(seq, command)| {
            // The exchange numbered the commands; the journal must agree.
            if let Command::Limit { id, .. }
            | Command::Market { id, .. }
            | Command::Stop { id, .. } = command
            {
                assert_eq!(id, seq, "an order's id is its command's sequence number");
            }
        });
        let mut left = items.size_hint().0;
        while left > 0 {
            let pushed = output.push_from(&mut items);
            left -= pushed;
            if pushed == 0 {
                if output.is_closed() {
                    return Err(Error::Poisoned);
                }
                // The matcher may be waiting for a sync while this waits for room.
                serve(writer, shared)?;
                backoff.snooze();
            }
        }
        backoff.reset();
    }
}

/// Removes the segments a snapshot freed, and syncs if the matcher waits for it.
fn serve<S: Storage>(writer: &mut Writer<S>, shared: &Shared) -> Result<(), Error> {
    let trim = shared.trim.swap(0, Ordering::AcqRel);
    if trim > 0 {
        writer.remove_through(trim)?;
    }
    if shared.sync_to.load(Ordering::Acquire) > writer.durable_seq() {
        writer.sync()?;
        shared
            .durable
            .store(writer.durable_seq(), Ordering::Release);
    }
    Ok(())
}

/// Puts the matcher's events into C, waiting for room.
struct Events<'a> {
    ring: &'a mut Producer<(Seq, Event)>,
    wait: Wait,
}

impl Output for Events<'_> {
    fn on_event(&mut self, seq: Seq, event: Event) {
        // If the network thread is gone, nobody reads them.
        let _ = self.ring.push((seq, event), self.wait);
    }
}

/// The matcher's loop: applies what comes through B, sends the events into C, and takes
/// the snapshots that fall due.
fn apply<S: Storage>(
    mut matcher: Matcher<S>,
    mut input: Consumer<(Seq, Command)>,
    mut output: Producer<(Seq, Event)>,
    shared: &Shared,
    wait: Wait,
) -> Result<Matcher<S>, Error> {
    let mut events = Events {
        ring: &mut output,
        wait,
    };
    while input.wait(wait) {
        for (seq, command) in input.drain(MAX_BATCH) {
            matcher.apply(seq, command, &mut events)?;
        }
        shared.applied.store(matcher.last_seq(), Ordering::Release);
        if matcher.snapshot_due() {
            snapshot(&mut matcher, shared, wait);
        }
    }
    Ok(matcher)
}

/// Takes a snapshot once the writer has synced through it. A snapshot that cannot be
/// taken is put off, as the engine puts off its own.
fn snapshot<S: Storage>(matcher: &mut Matcher<S>, shared: &Shared, wait: Wait) {
    let seq = matcher.last_seq();
    shared.sync_to.fetch_max(seq, Ordering::AcqRel);
    let mut backoff = Backoff::new(wait);
    while shared.durable.load(Ordering::Acquire) < seq {
        if shared.failed.load(Ordering::Acquire) {
            return matcher.postpone_snapshot(Error::Poisoned);
        }
        backoff.snooze();
    }
    match matcher.snapshot(shared.durable.load(Ordering::Acquire)) {
        Ok(Some(trim)) => shared.trim.store(trim, Ordering::Release),
        Ok(None) => {}
        Err(error) => matcher.postpone_snapshot(error),
    }
}
