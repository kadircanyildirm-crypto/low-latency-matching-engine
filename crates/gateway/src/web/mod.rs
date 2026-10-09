//! The web gateway: the exchange's own page over HTTP, and sessions over WebSocket that
//! speak JSON, for browsers. The server runs it on a second listener; a browser's session is
//! a session like any other, with the same login, risk limits, reports and market data.
//!
//! Visitors get accounts of their own: a `register` message creates one from a range of ids
//! kept for guests, with a random token, and saves it before answering, so it survives a
//! restart.

pub mod http;
pub mod json;
pub mod ws;

use std::fs::OpenOptions;
use std::io::{self, Write};
use std::ops::Range;
use std::path::PathBuf;

use crate::accounts::Account;
use crate::exchange::Exchange;

/// The page and its files, built into the binary.
pub fn page(path: &str) -> Option<(&'static str, &'static [u8])> {
    Some(match path {
        "/" | "/index.html" => (
            "text/html; charset=utf-8",
            include_bytes!("../../web/index.html"),
        ),
        "/app.js" => (
            "text/javascript; charset=utf-8",
            include_bytes!("../../web/app.js"),
        ),
        "/style.css" => (
            "text/css; charset=utf-8",
            include_bytes!("../../web/style.css"),
        ),
        _ => return None,
    })
}

/// Where guests' accounts come from.
#[derive(Clone, Debug)]
pub struct Guests {
    /// The file accounts are appended to, in the accounts file's format; `None` keeps them
    /// in memory only.
    pub file: Option<PathBuf>,
    /// The ids guests get, the lowest free one first.
    pub ids: Range<u32>,
    /// A guest's open-order limit.
    pub max_open_orders: u32,
    /// A guest's order-entry messages per second.
    pub messages_per_second: u32,
}

impl Guests {
    /// Creates an account for a visitor, adds it to `exchange`, and saves it. Returns
    /// `None` if every id is taken.
    pub fn create(&mut self, exchange: &mut Exchange) -> io::Result<Option<Account>> {
        let Some(id) = self.ids.clone().find(|&id| !exchange.has_account(id)) else {
            return Ok(None);
        };
        let mut token = [0; 8];
        getrandom::fill(&mut token).map_err(|e| io::Error::other(e.to_string()))?;
        let account = Account {
            id,
            token: u64::from_le_bytes(token),
            max_open_orders: self.max_open_orders,
            messages_per_second: self.messages_per_second,
        };
        // Saved before it is used: an account the visitor was told of exists after a crash.
        if let Some(path) = &self.file {
            let mut file = OpenOptions::new().create(true).append(true).open(path)?;
            writeln!(
                file,
                "{} {} {} {}",
                account.id, account.token, account.max_open_orders, account.messages_per_second
            )?;
            file.sync_data()?;
        }
        exchange
            .add_account(account)
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(Some(account))
    }
}
