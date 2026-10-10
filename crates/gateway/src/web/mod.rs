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

use crate::accounts::{self, Account, Funds};
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
        "/favicon.svg" => ("image/svg+xml", include_bytes!("../../web/favicon.svg")),
        "/og.png" => ("image/png", include_bytes!("../../web/og.png")),
        "/style.css" => (
            "text/css; charset=utf-8",
            include_bytes!("../../web/style.css"),
        ),
        _ => return None,
    })
}

/// The page with its link preview pointing at `url`, where the page is published, such as
/// `https://demo.example.com`: the sites that show previews want the image's full address.
pub fn index_at(url: &str) -> Vec<u8> {
    let url = url.trim_end_matches('/');
    let (_, page) = page("/").expect("the page");
    String::from_utf8_lossy(page)
        .replace(
            r#"content="/og.png""#,
            &format!(r#"content="{url}/og.png""#),
        )
        .replace(
            r#"property="og:url" content="/""#,
            &format!(r#"property="og:url" content="{url}/""#),
        )
        .into_bytes()
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
    /// What a guest starts with: guests trade paper money if this is set.
    pub funds: Option<Funds>,
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
            funds: self.funds,
        };
        // Saved before it is used: an account the visitor was told of exists after a crash.
        if let Some(path) = &self.file {
            let mut file = OpenOptions::new().create(true).append(true).open(path)?;
            writeln!(file, "{}", accounts::format(&account))?;
            file.sync_data()?;
        }
        exchange
            .add_account(account)
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(Some(account))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The link preview points at the image where the page is published, and only that
    /// changes.
    #[test]
    fn the_preview_points_where_the_page_is() {
        let page = String::from_utf8(index_at("https://demo.example.com/")).unwrap();
        assert!(page.contains(r#"content="https://demo.example.com/og.png""#));
        assert!(page.contains(r#"property="og:url" content="https://demo.example.com/""#));
        let original = std::str::from_utf8(page_bytes("/")).unwrap();
        assert_eq!(
            page.len(),
            original.len() + 2 * "https://demo.example.com".len()
        );
    }

    fn page_bytes(path: &str) -> &'static [u8] {
        page(path).unwrap().1
    }
}
