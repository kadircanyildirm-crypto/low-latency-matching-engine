//! The exchange's TCP gateway: clients log in, enter orders in the binary protocol of the
//! [`protocol`] crate, and receive reports of what happens to them.
//!
//! - [`accounts`]: who may log in, and their limits.
//! - [`exchange`]: the logic, without sockets: sessions, pre-trade risk, order ids, and the
//!   routing of the book's events to the sessions they concern.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod accounts;
pub mod exchange;

pub use accounts::{Account, AccountsError};
pub use exchange::{Exchange, Mailbox, SessionId, SetupError, Timing};
