//! Decimal strings such as `"2505.24"` as integers of a fixed number of decimals, exactly:
//! no floating point between the venue's numbers and the exchange's ticks and lots.

/// `text` in units of `10^-decimals`: `"2505.24"` with 2 decimals is 250524. `None` if it
/// is not a plain non-negative decimal, has more decimals than that, or does not fit.
pub fn parse(text: &str, decimals: u32) -> Option<u64> {
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if whole.is_empty() || !digits(whole) || !digits(fraction) {
        return None;
    }
    // Trailing zeros beyond the decimals change nothing.
    let fraction = fraction.trim_end_matches('0');
    if fraction.len() > decimals as usize {
        return None;
    }
    let scale = 10u64.checked_pow(decimals)?;
    let whole: u64 = whole.parse().ok()?;
    let mut part: u64 = if fraction.is_empty() {
        0
    } else {
        fraction.parse().ok()?
    };
    part = part.checked_mul(10u64.checked_pow(decimals - fraction.len() as u32)?)?;
    whole.checked_mul(scale)?.checked_add(part)
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn decimals_are_read_exactly() {
        assert_eq!(parse("2505.24", 2), Some(250_524));
        assert_eq!(parse("2505.2", 2), Some(250_520));
        assert_eq!(parse("2505", 2), Some(250_500));
        assert_eq!(parse("0.004590", 6), Some(4_590));
        assert_eq!(parse("12.972498", 6), Some(12_972_498));
        assert_eq!(parse("1.500000000", 2), Some(150));
        assert_eq!(parse("0", 6), Some(0));
        // More decimals than there are, signs, exponents, junk, and overflow.
        for bad in [
            "2505.241",
            "-1",
            "+1",
            "1e3",
            "",
            ".5",
            "1.2.3",
            "1,5",
            " 1",
            "99999999999999999999",
            "184467440737095516.16",
        ] {
            assert_eq!(parse(bad, 2), None, "{bad}");
        }
        assert_eq!(parse("1", 20), None);
    }
}
