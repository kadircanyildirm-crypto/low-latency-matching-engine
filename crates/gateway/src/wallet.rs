//! Paper money: what a paper-trading account owns, and what its open orders hold of it.
//!
//! An order may only be placed if the account has what it would need to fill completely:
//! a buy order holds its open quantity times its limit price in cash, a sell order holds its
//! open quantity in lots. Holds follow the order's open quantity as it trades, and go when
//! it does. A trade settles at its own price, which for a buy order is never above the
//! limit, so what a buy order held for the quantity that traded covers what it paid, and
//! the rest goes back.

use orderbook::{Price, Qty, Side};
use protocol::Balance;
use serde::{Deserialize, Serialize};

use crate::accounts::Funds;

/// What a paper-trading account owns and holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Wallet {
    /// Cash, in price ticks times lots.
    pub cash: i64,
    /// Lots.
    pub position: i64,
    /// Cash held for open buy orders.
    pub cash_held: i64,
    /// Lots held for open sell orders.
    pub position_held: i64,
}

/// What `qty` lots at `price` cost, if it fits.
fn cost(price: Price, qty: Qty) -> Option<i64> {
    i64::try_from(i128::from(price).checked_mul(i128::from(qty))?).ok()
}

impl Wallet {
    /// A wallet with what `funds` gives, holding nothing.
    pub fn new(funds: Funds) -> Wallet {
        Wallet {
            cash: funds.cash,
            position: funds.position,
            ..Wallet::default()
        }
    }

    /// What an order of `side` with `leaves` open at `price` holds: cash for a buy, lots
    /// for a sell. `None` if it does not fit in an `i64`.
    pub fn hold_of(side: Side, price: Price, leaves: Qty) -> Option<i64> {
        match side {
            Side::Buy => cost(price, leaves),
            Side::Sell => i64::try_from(leaves).ok(),
        }
    }

    /// Whether the wallet can hold `amount` more on `side`.
    pub fn covers(&self, side: Side, amount: i64) -> bool {
        match side {
            Side::Buy => self.cash - self.cash_held >= amount,
            Side::Sell => self.position - self.position_held >= amount,
        }
    }

    /// Holds `delta` more on `side`; a negative `delta` releases.
    pub fn hold(&mut self, side: Side, delta: i64) {
        match side {
            Side::Buy => self.cash_held += delta,
            Side::Sell => self.position_held += delta,
        }
    }

    /// Settles a trade of `qty` lots at `price` by an order of `side`.
    pub fn settle(&mut self, side: Side, price: Price, qty: Qty) {
        let amount = cost(price, qty).unwrap_or(i64::MAX);
        let lots = i64::try_from(qty).unwrap_or(i64::MAX);
        match side {
            Side::Buy => {
                self.cash = self.cash.saturating_sub(amount);
                self.position = self.position.saturating_add(lots);
            }
            Side::Sell => {
                self.cash = self.cash.saturating_add(amount);
                self.position = self.position.saturating_sub(lots);
            }
        }
    }

    /// The wallet, as a client is told it.
    pub fn balance(&self) -> Balance {
        Balance {
            cash: self.cash,
            position: self.position,
            cash_held: self.cash_held,
            position_held: self.position_held,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holds_and_trades_add_up() {
        let mut wallet = Wallet::new(Funds {
            cash: 1_000,
            position: 10,
        });
        assert_eq!(Wallet::hold_of(Side::Buy, 100, 7), Some(700));
        assert_eq!(Wallet::hold_of(Side::Sell, 100, 7), Some(7));
        assert_eq!(Wallet::hold_of(Side::Buy, i64::MAX, 2), None);
        assert!(wallet.covers(Side::Buy, 1_000));
        assert!(!wallet.covers(Side::Buy, 1_001));
        wallet.hold(Side::Buy, 700);
        assert!(!wallet.covers(Side::Buy, 301));
        // Three of the seven trade at 90, below the limit of 100.
        wallet.settle(Side::Buy, 90, 3);
        wallet.hold(Side::Buy, -300);
        assert_eq!(
            wallet.balance(),
            Balance {
                cash: 730,
                position: 13,
                cash_held: 400,
                position_held: 0
            }
        );
        wallet.hold(Side::Sell, 13);
        assert!(!wallet.covers(Side::Sell, 1));
        wallet.settle(Side::Sell, 120, 13);
        wallet.hold(Side::Sell, -13);
        assert_eq!((wallet.cash, wallet.position), (2_290, 0));
    }
}
