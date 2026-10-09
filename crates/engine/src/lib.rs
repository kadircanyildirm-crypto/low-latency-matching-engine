//! The order book behind a sequencer and a write-ahead journal.
//!
//! So far: the binary encodings that the journal and snapshot files use, and the storage
//! layer they are written through, with a simulated disk for crash tests.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codec;
pub mod storage;
