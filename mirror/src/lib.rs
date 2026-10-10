//! Mirrors a real venue's market into the exchange: every order resting near the top of
//! the venue's book is placed on the exchange's book at the same price, with the same
//! quantity, in the same order, and every trade on the venue is sent again as an
//! immediate-or-cancel order that takes the same quantity at the same price. Visitors then
//! trade against the real market's orders, with paper money.
//!
//! - [`decimal`]: prices and quantities, exactly, from the venue's decimal strings.
//! - [`bitstamp`]: what Bitstamp's WebSocket sends.
//! - [`book`]: what the mirror keeps of its own orders, and what to send to follow the venue.
//! - [`feed`]: the connection to the venue.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod bitstamp;
pub mod book;
pub mod decimal;
pub mod feed;
