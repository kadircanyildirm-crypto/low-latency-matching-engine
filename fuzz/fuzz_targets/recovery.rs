//! Crash recovery under any sequence of commands, syncs, snapshots, power failures, killed
//! processes, processes that die in the middle of a change, and flipped bits that the
//! fuzzer chooses, on the simulated disk.
//!
//! The engine runs the fuzzer's commands. Alongside, the target keeps the commands the
//! engine has acknowledged, in sequence order, and the book's digest after each prefix of
//! them. After every failure:
//!
//! - recovery succeeds, unless a bit was flipped since the last clean recovery, in which case
//!   it may refuse;
//! - it keeps no command that was never submitted;
//! - without a flipped bit, it keeps every command that was durable at a power failure, and
//!   every command acknowledged before a kill. A flipped bit in a record no later record
//!   vouches for cannot be told from a torn write, and costs the records from it on;
//! - the recovered book is exactly the book after the commands it kept, and healthy.
//!
//! Commands that recovery lost are forgotten, as a client would resubmit them, and the run
//! goes on.

#![no_main]

use std::path::Path;

use arbitrary::Arbitrary;
use engine::sim::{CrashModel, SimStorage};
use engine::{Discard, Engine, EngineConfig, Error, Seq, SyncPolicy};
use libfuzzer_sys::fuzz_target;
use orderbook::workload::SplitMix64;
use orderbook::{BookConfig, Command, Event, OrderBook};
use orderbook_fuzz::{CommandInput, ConfigInput, Prices};

#[derive(Arbitrary, Debug)]
struct Input {
    config: ConfigInput,
    always: bool,
    segment: SegmentInput,
    /// Commands between snapshots: never, or 1 to 64.
    snapshot_every: Option<u8>,
    /// Snapshots kept: 1 to 3.
    keep: u8,
    verify: bool,
    steps: Vec<Step>,
}

#[derive(Arbitrary, Debug)]
enum Step {
    Submit(Vec<CommandInput>),
    Sync,
    Snapshot,
    /// Power fails: unsynced writes survive as the seeded model chooses.
    PowerFailure(Failure),
    /// The process dies; everything it wrote stays with the OS.
    Kill,
    /// The process will die after this many more changes to the disk, in whatever it is
    /// doing then; the power fails with it, or not.
    DieAfter {
        changes: u8,
        power: Option<Failure>,
    },
    /// A bit of some file flips.
    FlipBit {
        file: u8,
        bit: u64,
    },
}

#[derive(Arbitrary, Clone, Copy, Debug)]
struct Failure {
    in_order: bool,
    seed: u64,
    /// Recovery itself dies after this many changes, and the power fails again or not.
    recovery_dies: Option<(u8, bool)>,
}

/// Records per segment.
#[derive(Arbitrary, Clone, Copy, Debug)]
enum SegmentInput {
    /// 1 to 32: many rolls.
    Small(u8),
    /// 1,024 to 2,816: the next segment is prepared in two or three pieces.
    Large(u8),
}

/// Most steps and commands per input, so runs stay fast.
const MAX_STEPS: usize = 64;
const MAX_BATCH: usize = 16;

/// The commands the engine acknowledged, and the reference book after each prefix.
struct History {
    book: BookConfig,
    commands: Vec<Command>,
    digests: Vec<u64>,
    reference: OrderBook,
}

impl History {
    fn new(book: BookConfig) -> History {
        let reference = OrderBook::new(book);
        History {
            book,
            commands: Vec::new(),
            digests: vec![reference.digest()],
            reference,
        }
    }

    fn push(&mut self, command: Command) {
        let mut ignored = Vec::new();
        self.reference.process(command, &mut ignored);
        self.commands.push(command);
        self.digests.push(self.reference.digest());
    }

    /// Forgets everything after the first `kept` commands.
    fn truncate(&mut self, kept: usize) {
        self.commands.truncate(kept);
        self.digests.truncate(kept + 1);
        self.reference = OrderBook::new(self.book);
        let mut ignored = Vec::new();
        for &command in &self.commands {
            self.reference.process(command, &mut ignored);
        }
    }
}

fuzz_target!(|input: Input| {
    let book = input.config.config(Prices::Unrestricted);
    let config = EngineConfig {
        sync: if input.always {
            SyncPolicy::Always
        } else {
            SyncPolicy::Os
        },
        segment_capacity: match input.segment {
            SegmentInput::Small(n) => 1 + u32::from(n % 32),
            SegmentInput::Large(n) => 1_024 + 256 * u32::from(n % 8),
        },
        snapshot_every: input.snapshot_every.map(|n| 1 + u64::from(n % 64)),
        keep_snapshots: 1 + usize::from(input.keep % 3),
        verify_replay: input.verify,
        ..EngineConfig::new(book)
    };
    let dir = Path::new("data");
    let open = |storage: &SimStorage| {
        Engine::open_with(storage.clone(), dir, config, &mut Discard).map(|(engine, _)| engine)
    };
    let mut storage = SimStorage::new();
    let mut engine = open(&storage).expect("a new engine");
    let mut history = History::new(book);
    // The batch whose submission failed when the process died: some or all of it may be
    // in the journal.
    let mut in_flight: Vec<Command> = Vec::new();
    // What happens to the power when the process dies on its own.
    let mut death: Option<Failure> = None;
    let mut damaged = false;
    let mut events: Vec<(Seq, Event)> = Vec::new();

    for step in input.steps.iter().take(MAX_STEPS) {
        let failure = match step {
            Step::Submit(batch) => {
                let batch: Vec<Command> = batch
                    .iter()
                    .take(MAX_BATCH)
                    .map(|command| command.command(&book))
                    .collect();
                events.clear();
                match engine.submit_batch(&batch, &mut events) {
                    Ok(seq) => {
                        assert_eq!(seq as usize, history.commands.len() + batch.len());
                        batch.into_iter().for_each(|command| history.push(command));
                        None
                    }
                    Err(_) if storage.is_dead() => {
                        in_flight = batch;
                        Some(death)
                    }
                    Err(error) => panic!("submit: {error}"),
                }
            }
            Step::Sync => match engine.sync() {
                Ok(()) => None,
                Err(_) if storage.is_dead() => Some(death),
                Err(error) => panic!("sync: {error}"),
            },
            Step::Snapshot => match engine.snapshot() {
                Ok(()) => None,
                Err(_) if storage.is_dead() => Some(death),
                Err(error) => panic!("snapshot: {error}"),
            },
            Step::FlipBit { file, bit } => {
                let files = storage.files();
                if let Some((path, size)) = files.get(usize::from(*file) % files.len().max(1)) {
                    if *size > 0 {
                        storage.flip_bit(path, bit % (size * 8));
                        damaged = true;
                    }
                }
                None
            }
            Step::DieAfter { changes, power } => {
                storage.die_after(u64::from(*changes));
                death = *power;
                None
            }
            Step::PowerFailure(failure) => Some(Some(*failure)),
            Step::Kill => Some(None),
        };
        let Some(power) = failure else {
            continue;
        };

        // The process is gone, by its own death or by the step's.
        let durable = engine.durable_seq() as usize;
        let acked = history.commands.len();
        drop(engine);
        let mut power_failed = false;
        match power {
            Some(Failure {
                in_order,
                seed,
                recovery_dies,
            }) => {
                let model = if in_order {
                    CrashModel::InOrder
                } else {
                    CrashModel::AnyOrder
                };
                let mut rng = SplitMix64::new(seed);
                storage = storage.crash(&mut rng, model);
                power_failed = true;
                if let Some((changes, again)) = recovery_dies {
                    storage.die_after(u64::from(changes));
                    let result = open(&storage);
                    assert!(
                        result.is_ok() || storage.is_dead() || damaged,
                        "recovery failed without dying"
                    );
                    drop(result);
                    if again {
                        storage = storage.crash(&mut rng, model);
                    } else {
                        storage.revive();
                    }
                }
            }
            None => storage.revive(),
        }
        engine = match open(&storage) {
            Ok(engine) => engine,
            Err(Error::Corrupt { .. } | Error::MissingJournal { .. }) if damaged => return,
            Err(error) => panic!("recovery failed: {error}"),
        };
        let kept = engine.last_seq() as usize;
        assert!(
            kept <= acked + in_flight.len(),
            "recovered commands never submitted"
        );
        if !damaged {
            if !power_failed {
                assert!(
                    kept >= acked,
                    "a killed process lost commands: {kept} of {acked}"
                );
            } else {
                assert!(
                    kept >= durable,
                    "lost durable commands: {kept} of {durable}"
                );
            }
        }
        // Commands of the failed batch that made it into the journal count as submitted.
        for &command in in_flight.iter().take(kept.saturating_sub(acked)) {
            history.push(command);
        }
        in_flight.clear();
        if kept < history.commands.len() {
            history.truncate(kept);
        }
        assert_eq!(
            engine.book().digest(),
            history.digests[kept],
            "the state after recovering {kept} commands"
        );
        if let Err(violation) = engine.book().validate() {
            panic!("recovered book is broken: {violation}");
        }
        damaged = false;
        death = None;
    }
});
