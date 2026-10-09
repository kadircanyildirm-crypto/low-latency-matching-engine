//! The order book behind a sequencer and a write-ahead journal.
//!
//! This first part holds the binary encodings that the journal and snapshot files use.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod codec;
