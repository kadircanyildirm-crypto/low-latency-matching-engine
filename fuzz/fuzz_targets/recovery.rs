//! Crash recovery under any sequence of commands, syncs, snapshots, power failures, killed
//! processes and flipped bits that the fuzzer chooses, on the simulated disk.
//!
//! The engine runs the fuzzer's commands. Alongside, the target keeps the commands the
//! engine has acknowledged, in sequence order, and the book's digest after each prefix of
//! them. After every crash or kill:
//!
//! - recovery succeeds, unless a bit was flipped since the last clean recovery, in which case
//!   it may refuse;
//! - it keeps no command that was never submitted;
//! - without a flipped bit, it keeps every command that was durable at a power failure, and
//!   every command at all after a kill. A flipped bit in a record no later record vouches
//!   for cannot be told from a torn write, and costs the records from it on;
//! - the recovered book is exactly the book after the commands it kept, and healthy.
//!
//! Commands that recovery lost are forgotten, as a client would resubmit them, and the run
//! goes on.

#![no_main]

use std::path::Path;

use arbitrary::Arbitrary;
use engine::storage::{CrashModel, SimStorage};
use engine::{Engine, EngineConfig, Error, SyncPolicy};
use libfuzzer_sys::fuzz_target;
use orderbook::workload::SplitMix64;
use orderbook::{Command, Event, OrderBook};
use orderbook_fuzz::{CommandInput, ConfigInput, Prices};

#[derive(Arbitrary, Debug)]
struct Input {
    config: ConfigInput,
    always: bool,
    /// Records per segment: 1 to 32.
    segment: u8,
    /// Commands between snapshots: never, or 1 to 64.
    snapshot_every: Option<u8>,
    /// Snapshots kept: 1 to 3.
    keep: u8,
    steps: Vec<Step>,
}

#[derive(Arbitrary, Debug)]
enum Step {
    Submit(Vec<CommandInput>),
    Sync,
    Snapshot,
    /// Power fails: unsynced writes survive as the seeded model chooses.
    PowerFailure {
        in_order: bool,
        seed: u64,
    },
    /// The process dies; everything it wrote stays with the OS.
    Kill,
    /// A bit of some file flips.
    FlipBit {
        file: u8,
        bit: u64,
    },
}

/// Most steps and commands per input, so runs stay fast.
const MAX_STEPS: usize = 64;
const MAX_BATCH: usize = 16;

fuzz_target!(|input: Input| {
    let book = input.config.config(Prices::Unrestricted);
    let config = EngineConfig {
        sync: if input.always {
            SyncPolicy::Always
        } else {
            SyncPolicy::Os
        },
        segment_capacity: 1 + u32::from(input.segment % 32),
        snapshot_every: input.snapshot_every.map(|n| 1 + u64::from(n % 64)),
        keep_snapshots: 1 + usize::from(input.keep % 3),
        ..EngineConfig::new(book)
    };
    let dir = Path::new("data");
    let mut storage = SimStorage::new();
    let (mut engine, _) = Engine::open_with(storage.clone(), dir, config).expect("a new engine");

    // The acknowledged commands, and the reference book's digest after each prefix.
    let mut history: Vec<Command> = Vec::new();
    let mut reference = OrderBook::new(book);
    let mut digests = vec![reference.digest()];
    let mut damaged = false;
    let mut events: Vec<Event> = Vec::new();

    for step in input.steps.iter().take(MAX_STEPS) {
        match step {
            Step::Submit(batch) => {
                let batch: Vec<Command> = batch
                    .iter()
                    .take(MAX_BATCH)
                    .map(|command| command.command(&book))
                    .collect();
                if batch.is_empty() {
                    continue;
                }
                events.clear();
                match engine.submit_batch(&batch, &mut events) {
                    Ok(seq) => assert_eq!(seq as usize, history.len() + batch.len()),
                    // Damage found while taking a snapshot: nothing was journaled.
                    Err(Error::Corrupt { .. }) if damaged => return,
                    Err(error) => panic!("submit: {error}"),
                }
                for command in batch {
                    let mut ignored = Vec::new();
                    reference.process(command, &mut ignored);
                    history.push(command);
                    digests.push(reference.digest());
                }
            }
            Step::Sync => engine.sync().expect("sync"),
            Step::Snapshot => match engine.snapshot() {
                Ok(()) => {}
                Err(Error::Corrupt { .. }) if damaged => return,
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
            }
            Step::PowerFailure { .. } | Step::Kill => {
                let durable = engine.durable_seq() as usize;
                drop(engine);
                let killed = matches!(step, Step::Kill);
                if let Step::PowerFailure { in_order, seed } = step {
                    let model = if *in_order {
                        CrashModel::InOrder
                    } else {
                        CrashModel::AnyOrder
                    };
                    storage = storage.crash(&mut SplitMix64::new(*seed), model);
                }
                engine = match Engine::open_with(storage.clone(), dir, config) {
                    Ok((engine, _)) => engine,
                    Err(Error::Corrupt { .. } | Error::MissingJournal { .. }) if damaged => {
                        return;
                    }
                    Err(error) => panic!("recovery failed: {error}"),
                };
                let kept = engine.last_seq() as usize;
                assert!(kept <= history.len(), "recovered commands never submitted");
                if !damaged {
                    if killed {
                        assert_eq!(kept, history.len(), "a killed process lost commands");
                    } else {
                        assert!(
                            kept >= durable,
                            "lost durable commands: {kept} of {durable}"
                        );
                    }
                }
                assert_eq!(
                    engine.book().digest(),
                    digests[kept],
                    "the state after recovering {kept} commands"
                );
                if let Err(violation) = engine.book().validate() {
                    panic!("recovered book is broken: {violation}");
                }
                // Forget what was lost and rebuild the reference from what was kept.
                history.truncate(kept);
                digests.truncate(kept + 1);
                reference = OrderBook::new(book);
                for &command in &history {
                    let mut ignored = Vec::new();
                    reference.process(command, &mut ignored);
                }
                damaged = false;
            }
        }
    }
});
