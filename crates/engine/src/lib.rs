//! The order book behind a sequencer and a write-ahead journal.
//!
//! [`Engine`] gives every command the next sequence number, appends it to the journal and
//! only then applies it to the book, so the journal always holds at least every command
//! the book has seen. The book is deterministic, so replaying the journal rebuilds its
//! exact state. Snapshots bound how much has to be replayed: recovery loads the newest
//! intact snapshot and replays only the commands after it.
//!
//! How much a crash can lose depends on the [`SyncPolicy`]: nothing that was acknowledged
//! with [`SyncPolicy::Always`], and whatever the OS had not yet written back with
//! [`SyncPolicy::Os`]. Recovery never invents or reorders commands, and it refuses to
//! continue, rather than quietly dropping commands, when synced data is damaged.
//!
//! See DESIGN.md for the formats and the reasoning behind them.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codec;
mod engine;
mod journal;
pub mod sim;
pub mod snapshots;
pub mod storage;

use std::fmt;
use std::io;
use std::path::PathBuf;

pub use engine::{Discard, Engine, EngineConfig, Output, RecoveryReport, SyncPolicy};
pub use journal::{JournalReport, RECORD_SIZE};

/// A command's position in the total order the sequencer assigns: 1, 2, 3, ...
pub type Seq = u64;

/// Why the engine could not open or could not accept a command.
#[derive(Debug)]
pub enum Error {
    /// A file operation failed.
    Io(io::Error),
    /// A file is damaged in a way recovery must not paper over.
    Corrupt {
        /// The damaged file.
        file: PathBuf,
        /// What is wrong with it.
        detail: String,
    },
    /// A file was written for a book with another configuration.
    ConfigMismatch {
        /// The file.
        file: PathBuf,
    },
    /// Commands from `from` on are needed but no journal segment holds them.
    MissingJournal {
        /// The first missing sequence number.
        from: Seq,
    },
    /// An earlier write or sync failed, so the journal may not hold what the engine
    /// believes it does. The engine accepts nothing more; reopening it recovers from what
    /// is actually on disk.
    Poisoned,
    /// Another engine has the directory open. Two writers would corrupt the journal.
    Locked {
        /// The directory.
        dir: PathBuf,
    },
    /// A file was written in a format this version does not read, such as by a newer
    /// version. Nothing is changed or deleted.
    Unsupported {
        /// The file.
        file: PathBuf,
        /// What is unsupported.
        detail: String,
    },
    /// Journal segments that replay needs were written under another version of the
    /// matching rules ([`orderbook::RULES_VERSION`]); replaying them under these rules could
    /// reach another state. Take a snapshot with the old version first.
    RulesMismatch {
        /// The segment.
        file: PathBuf,
        /// The rules version it was written under.
        found: u32,
    },
    /// Replaying the journal from an older snapshot did not reach the state of the newer
    /// one: the matching is not deterministic, or the files do not belong together.
    Divergence {
        /// The newer snapshot's sequence number.
        seq: Seq,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(error) => write!(f, "I/O error: {error}"),
            Error::Corrupt { file, detail } => write!(f, "{} is damaged: {detail}", file.display()),
            Error::ConfigMismatch { file } => {
                write!(
                    f,
                    "{} belongs to a book with another configuration",
                    file.display()
                )
            }
            Error::MissingJournal { from } => {
                write!(f, "the journal from command {from} is missing")
            }
            Error::Poisoned => f.write_str("an earlier write failed; reopen the engine"),
            Error::Locked { dir } => {
                write!(f, "another engine has {} open", dir.display())
            }
            Error::Unsupported { file, detail } => {
                write!(
                    f,
                    "{} is in an unsupported format: {detail}",
                    file.display()
                )
            }
            Error::RulesMismatch { file, found } => write!(
                f,
                "{} was written under matching rules {found}, not {}",
                file.display(),
                orderbook::RULES_VERSION
            ),
            Error::Divergence { seq } => write!(
                f,
                "replaying the journal does not reach the state of the snapshot at {seq}"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
}
