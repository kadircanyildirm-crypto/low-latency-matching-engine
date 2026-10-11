//! The exchange across restarts.
//!
//! The book is rebuilt by the engine, from a snapshot and the journal after it. The exchange
//! keeps what the book does not know, paper money above all, and rebuilds it the same way:
//! from a [`Checkpoint`] of its own, saved now and then in the engine's directory, and from
//! the commands and events after it, which the engine replays to it on opening
//! ([`engine::Output::resume_after`]). It then checks that the orders it knows are exactly
//! those on the recovered book.
//!
//! Before the first checkpoint, or if none reads, the exchange is rebuilt from the whole
//! journal, from the first command: never from the book alone, which would give every paper
//! account its starting money back. If the journal no longer reaches back that far, opening
//! fails rather than guess.
//!
//! A checkpoint is written to a temporary file, synced, renamed into place and the
//! directory synced, so a crash leaves the old one or the new one, never half of one; the
//! one before it is kept too. Its first line is a CRC-32 of the rest, so that damage which
//! leaves the JSON readable, a digit of someone's cash, is found: such a checkpoint is
//! passed over like one that does not parse. Checkpoints from before the checksum, plain
//! JSON, still read. The journal must still reach back to the checkpoint: it is
//! saved far more often than the engine takes snapshots, whose retention decides how far
//! back the journal goes.

use std::fmt;
use std::path::Path;

use engine::storage::{Storage, StorageFile};
use engine::{Engine, EngineConfig, Output, Seq};
use orderbook::{Command, Event, OrderBook};

use crate::accounts::Account;
use crate::exchange::{Checkpoint, Exchange, SetupError, Timing};

const PREFIX: &str = "gateway-";
/// What a checkpoint file starts with, before the checksum of the JSON after its first line.
const CHECKSUM: &[u8] = b"crc32 ";
const SUFFIX: &str = ".json";
const TEMPORARY: &str = ".json.tmp";

fn name(seq: Seq) -> String {
    format!("{PREFIX}{seq:020}{SUFFIX}")
}

fn seq_of(name: &str) -> Option<Seq> {
    let digits = name.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

/// Why the exchange could not be opened.
#[derive(Debug)]
pub enum RecoveryError {
    /// The engine could not be opened.
    Engine(engine::Error),
    /// The accounts do not fit the book, or the book holds orders no gateway placed.
    Setup(SetupError),
    /// The checkpoint and the book disagree.
    Inconsistent(String),
    /// Reading the checkpoints failed.
    Io(std::io::Error),
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecoveryError::Engine(error) => write!(f, "engine: {error}"),
            RecoveryError::Setup(error) => write!(f, "{error}"),
            RecoveryError::Inconsistent(detail) => {
                write!(f, "the checkpoint and the book disagree: {detail}")
            }
            RecoveryError::Io(error) => write!(f, "reading the checkpoints: {error}"),
        }
    }
}

impl std::error::Error for RecoveryError {}

/// Saves `checkpoint` in `dir`, durably, and deletes the checkpoints older than the one
/// before it.
pub fn save<S: Storage>(
    storage: &mut S,
    dir: &Path,
    checkpoint: &Checkpoint,
) -> std::io::Result<()> {
    let json = serde_json::to_vec(checkpoint).map_err(std::io::Error::other)?;
    let mut bytes = CHECKSUM.to_vec();
    bytes.extend_from_slice(format!("{:08x}\n", crc32fast::hash(&json)).as_bytes());
    bytes.extend_from_slice(&json);
    let path = dir.join(name(checkpoint.seq));
    let temporary = path.with_extension("json.tmp");
    let mut file = storage.create(&temporary)?;
    file.write_at(0, &bytes)?;
    file.sync()?;
    storage.rename(&temporary, &path)?;
    storage.sync_dir(dir)?;
    let mut seqs: Vec<Seq> = storage
        .list(dir)?
        .iter()
        .filter_map(|n| seq_of(n))
        .collect();
    seqs.sort_unstable();
    let keep_from = seqs.len().saturating_sub(2);
    for &old in &seqs[..keep_from] {
        storage.remove(&dir.join(name(old)))?;
    }
    Ok(())
}

/// The newest checkpoint in `dir` that reads, if there is one.
pub fn latest<S: Storage>(storage: &mut S, dir: &Path) -> std::io::Result<Option<Checkpoint>> {
    let mut seqs: Vec<Seq> = match storage.list(dir) {
        Ok(names) => names.iter().filter_map(|n| seq_of(n)).collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    seqs.sort_unstable();
    for &seq in seqs.iter().rev() {
        let mut file = storage.open(&dir.join(name(seq)))?;
        let mut bytes = vec![0; usize::try_from(file.size()?).unwrap_or(usize::MAX)];
        file.read_at(0, &mut bytes)?;
        if let Some(checkpoint) = read(&bytes) {
            if checkpoint.seq == seq {
                return Ok(Some(checkpoint));
            }
        }
    }
    Ok(None)
}

/// The checkpoint in a file's bytes, if its checksum, when it has one, matches, and its JSON
/// reads.
fn read(bytes: &[u8]) -> Option<Checkpoint> {
    let json = match bytes.strip_prefix(CHECKSUM) {
        Some(rest) => {
            let (hex, json) = rest.split_at_checked(9)?;
            let crc = std::str::from_utf8(hex.strip_suffix(b"\n")?).ok()?;
            let crc = u32::from_str_radix(crc, 16).ok()?;
            (crc32fast::hash(json) == crc).then_some(json)?
        }
        // From before checkpoints had a checksum.
        None => bytes,
    };
    serde_json::from_slice(json).ok()
}

/// The commands and events after a checkpoint, as the engine replays them.
struct Replay {
    after: Seq,
    records: Vec<(Seq, Result<Command, Event>)>,
}

impl Output for Replay {
    fn on_event(&mut self, seq: Seq, event: Event) {
        self.records.push((seq, Err(event)));
    }

    fn on_command(&mut self, seq: Seq, command: Command) {
        self.records.push((seq, Ok(command)));
    }

    fn resume_after(&self) -> Option<Seq> {
        Some(self.after)
    }
}

/// What opening found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Recovered {
    /// The checkpoint the exchange started from, if there was one.
    pub checkpoint: Option<Seq>,
    /// Commands replayed after it.
    pub replayed: u64,
}

/// Opens the engine in `dir` and the exchange in front of it, for `accounts`: from the
/// newest checkpoint and the journal after it, or from the whole journal if there is none.
pub fn open<S: Storage>(
    mut storage: S,
    dir: &Path,
    config: EngineConfig,
    accounts: &[Account],
    timing: Timing,
) -> Result<(Engine<S>, Exchange, Recovered), RecoveryError> {
    for name in storage.list(dir).unwrap_or_default() {
        if name.starts_with(PREFIX) && name.ends_with(TEMPORARY) {
            storage.remove(&dir.join(name)).map_err(RecoveryError::Io)?;
        }
    }
    let checkpoint = latest(&mut storage, dir).map_err(RecoveryError::Io)?;
    let mut replay = Replay {
        after: checkpoint.as_ref().map_or(0, |checkpoint| checkpoint.seq),
        records: Vec::new(),
    };
    let (engine, _) =
        Engine::open_with(storage, dir, config, &mut replay).map_err(RecoveryError::Engine)?;
    let mut exchange = match &checkpoint {
        Some(checkpoint) => Exchange::restore(checkpoint, &config.book, accounts, timing),
        // The exchange before the first command, which the whole journal follows.
        None => Exchange::new(&OrderBook::new(config.book), 0, accounts, timing),
    }
    .map_err(RecoveryError::Setup)?;
    let mut replayed = 0;
    for (seq, record) in replay.records {
        match record {
            Ok(command) => {
                exchange.replay_command(seq, command);
                replayed += 1;
            }
            Err(event) => exchange.replay_event(seq, event),
        }
    }
    if exchange.last_seq() != engine.last_seq() {
        return Err(RecoveryError::Inconsistent(format!(
            "the exchange reached {}, the journal {}",
            exchange.last_seq(),
            engine.last_seq()
        )));
    }
    exchange
        .finish(engine.book())
        .map_err(RecoveryError::Inconsistent)?;
    let recovered = Recovered {
        checkpoint: checkpoint.map(|checkpoint| checkpoint.seq),
        replayed,
    };
    Ok((engine, exchange, recovered))
}
