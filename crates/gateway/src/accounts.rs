//! Who may trade, and how much.

use std::fmt;

/// An account: its id, which is also its owner id in the book, its secret, and its limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Account {
    /// The account's id, and the book's owner id for its orders.
    pub id: u32,
    /// The secret a client logs in with.
    pub token: u64,
    /// Orders and stops the account may have open at once.
    pub max_open_orders: u32,
    /// Order-entry messages (new orders, cancels, modifies, mass cancels) a session may send
    /// per second, with bursts of up to the same number.
    pub messages_per_second: u32,
}

/// Why an accounts file cannot be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountsError {
    /// The line, counting from one.
    pub line: usize,
    /// What is wrong with it.
    pub detail: String,
}

impl fmt::Display for AccountsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.detail)
    }
}

impl std::error::Error for AccountsError {}

/// Reads accounts, one per line: id, token, open-order limit and message rate, separated by
/// whitespace. Blank lines and lines starting with `#` are skipped. Ids must be unique and
/// below `max_owners`, the book's limit.
pub fn parse(text: &str, max_owners: u32) -> Result<Vec<Account>, AccountsError> {
    let mut accounts: Vec<Account> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line_no = index + 1;
        let fail = |detail: &str| AccountsError {
            line: line_no,
            detail: detail.to_owned(),
        };
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        let [id, token, max_open_orders, messages_per_second] = fields[..] else {
            return Err(fail(
                "expected: id token max-open-orders messages-per-second",
            ));
        };
        let number = |field: &str, name: &str| {
            field
                .parse::<u64>()
                .map_err(|_| fail(&format!("{name} is not a number")))
        };
        let account = Account {
            id: u32::try_from(number(id, "id")?).map_err(|_| fail("id is too large"))?,
            token: number(token, "token")?,
            max_open_orders: u32::try_from(number(max_open_orders, "max-open-orders")?)
                .map_err(|_| fail("max-open-orders is too large"))?,
            messages_per_second: u32::try_from(number(messages_per_second, "messages-per-second")?)
                .map_err(|_| fail("messages-per-second is too large"))?,
        };
        if account.id >= max_owners {
            return Err(fail(&format!(
                "id must be below {max_owners}, the book's owners"
            )));
        }
        if accounts.iter().any(|a| a.id == account.id) {
            return Err(fail("the id appears twice"));
        }
        accounts.push(account);
    }
    Ok(accounts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounts_are_read_and_checked() {
        let text = "# id token open rate\n\n1 42 100 1000\n  2 7 5 10  \n";
        let accounts = parse(text, 8).unwrap();
        assert_eq!(
            accounts,
            [
                Account {
                    id: 1,
                    token: 42,
                    max_open_orders: 100,
                    messages_per_second: 1_000
                },
                Account {
                    id: 2,
                    token: 7,
                    max_open_orders: 5,
                    messages_per_second: 10
                }
            ]
        );
        for (text, line) in [
            ("1 2 3", 1),
            ("x 2 3 4", 1),
            ("1 2 3 4\n1 5 6 7", 2),
            ("9 2 3 4", 1),
            ("1 2 99999999999 4", 1),
            ("4294967296 1 1 1", 1),
            ("1 2 3 99999999999", 1),
        ] {
            let error = parse(text, 8).unwrap_err();
            assert_eq!(error.line, line, "{text}");
            assert!(!error.to_string().is_empty());
        }
    }
}
