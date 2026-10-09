//! The exchange's TCP gateway: clients log in, enter orders in the binary protocol of the
//! [`protocol`] crate, and receive reports of what happens to them.
//!
//! - [`accounts`]: who may log in, and their limits.
//! - [`bots`]: market makers, noise traders and trend followers for a demo market.
//! - [`client`]: a blocking client, for tests and tools.
//! - [`exchange`]: the logic, without sockets: sessions, pre-trade risk, order ids, and the
//!   routing of the book's events to the sessions they concern.
//! - [`server`]: an event loop that connects sockets to an exchange, with a [`Core`] that
//!   runs the engine: on the same thread, or on threads of its own in a [`pipeline`].
//! - [`load`]: a load generator.
//! - [`recovery`]: the exchange across restarts, from checkpoints and the journal.
//! - [`wallet`]: paper money, held by open orders and settled by trades.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod accounts;
pub mod bots;
pub mod client;
pub mod exchange;
pub mod load;
pub mod pipeline;
pub mod recovery;
pub mod server;
pub mod wallet;
pub mod web;

pub use accounts::{Account, AccountsError, Funds};
pub use exchange::{Checkpoint, Exchange, Mailbox, SessionId, SetupError, Timing};
pub use pipeline::{Pipeline, PipelineConfig};
pub use server::{Core, Server, ServerConfig, ServerError};
