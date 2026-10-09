//! The binary encodings: every command and snapshot round-trips, and decoding accepts
//! nothing but what encoding produces.

mod common;

use engine::codec::{
    COMMAND_SIZE, DecodeError, decode_command, decode_snapshot, encode_command, encode_snapshot,
};
use orderbook::{BookConfig, Command, OrderBook, Phase, Side, TimeInForce};
use proptest::prelude::*;

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn tif() -> impl Strategy<Value = TimeInForce> {
    prop_oneof![
        Just(TimeInForce::Gtc),
        Just(TimeInForce::Ioc),
        Just(TimeInForce::Fok),
        Just(TimeInForce::PostOnly),
    ]
}

fn phase() -> impl Strategy<Value = Phase> {
    prop_oneof![
        Just(Phase::Continuous),
        Just(Phase::Auction),
        Just(Phase::Halted),
        Just(Phase::Closed),
    ]
}

/// Every command, with every field anywhere in its type's range.
fn command() -> impl Strategy<Value = Command> {
    prop_oneof![
        (
            any::<u64>(),
            any::<u32>(),
            side(),
            any::<i64>(),
            any::<u64>(),
            tif(),
            any::<Option<u64>>()
        )
            .prop_map(
                |(id, owner, side, price, qty, tif, display)| Command::Limit {
                    id,
                    owner,
                    side,
                    price,
                    qty,
                    tif,
                    display
                }
            ),
        (any::<u64>(), any::<u32>(), side(), any::<u64>()).prop_map(|(id, owner, side, qty)| {
            Command::Market {
                id,
                owner,
                side,
                qty,
            }
        }),
        (any::<u64>(), any::<u32>()).prop_map(|(id, owner)| Command::Cancel { id, owner }),
        (any::<u64>(), any::<u32>(), any::<i64>(), any::<u64>()).prop_map(
            |(id, owner, price, qty)| Command::Modify {
                id,
                owner,
                price,
                qty
            }
        ),
        (
            any::<u64>(),
            any::<u32>(),
            side(),
            any::<i64>(),
            any::<Option<i64>>(),
            any::<u64>()
        )
            .prop_map(|(id, owner, side, trigger, limit, qty)| Command::Stop {
                id,
                owner,
                side,
                trigger,
                limit,
                qty
            }),
        any::<u32>().prop_map(|owner| Command::CancelAll { owner }),
        phase().prop_map(|phase| Command::SetPhase { phase }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn commands_round_trip(command in command()) {
        let mut bytes = [0; COMMAND_SIZE];
        encode_command(&command, &mut bytes);
        prop_assert_eq!(decode_command(&bytes), Ok(command));
    }

    /// Changing any byte of an encoding either fails to decode or decodes to a command that
    /// encodes to the changed bytes: no two byte strings decode to the same command.
    #[test]
    fn decoding_accepts_only_canonical_bytes(
        command in command(),
        at in 0..COMMAND_SIZE,
        value in any::<u8>(),
    ) {
        let mut bytes = [0; COMMAND_SIZE];
        encode_command(&command, &mut bytes);
        bytes[at] = value;
        if let Ok(decoded) = decode_command(&bytes) {
            let mut again = [0; COMMAND_SIZE];
            encode_command(&decoded, &mut again);
            prop_assert_eq!(again, bytes);
        }
    }

    /// Arbitrary bytes: decoding never panics, and whatever decodes re-encodes to them.
    #[test]
    fn arbitrary_bytes_decode_canonically_or_not_at_all(bytes in any::<[u8; COMMAND_SIZE]>()) {
        if let Ok(command) = decode_command(&bytes) {
            let mut again = [0; COMMAND_SIZE];
            encode_command(&command, &mut again);
            prop_assert_eq!(again, bytes);
        }
    }

    /// Snapshots of real books, taken at random points of a flow that uses every command
    /// kind, round-trip; and no truncation of one decodes.
    #[test]
    fn snapshots_round_trip(seed in any::<u64>(), len in 0..1_500usize) {
        let (config, commands) = common::flow(seed, len);
        let mut book = OrderBook::new(config);
        let mut events = Vec::new();
        for &command in &commands {
            book.process(command, &mut events);
        }
        let snapshot = book.snapshot();
        let mut bytes = Vec::new();
        encode_snapshot(&snapshot, &mut bytes);
        prop_assert_eq!(decode_snapshot(&bytes), Ok(snapshot));
        for cut in [0, 1, bytes.len() / 2, bytes.len() - 1] {
            prop_assert!(decode_snapshot(&bytes[..cut]).is_err(), "cut at {}", cut);
        }
        let mut longer = bytes.clone();
        longer.push(0);
        prop_assert_eq!(decode_snapshot(&longer), Err(DecodeError::TrailingBytes));
    }
}

/// Each way a command encoding can be invalid.
#[test]
fn invalid_command_bytes_are_named() {
    let mut limit = [0; COMMAND_SIZE];
    encode_command(
        &Command::Limit {
            id: 1,
            owner: 2,
            side: Side::Sell,
            price: 3,
            qty: 4,
            tif: TimeInForce::Gtc,
            display: None,
        },
        &mut limit,
    );
    let with = |at: usize, value: u8| {
        let mut bytes = limit;
        bytes[at] = value;
        decode_command(&bytes)
    };
    assert_eq!(with(0, 0), Err(DecodeError::UnknownKind(0)));
    assert_eq!(with(0, 8), Err(DecodeError::UnknownKind(8)));
    assert_eq!(with(1, 2), Err(DecodeError::InvalidField("side")));
    assert_eq!(with(2, 4), Err(DecodeError::InvalidField("time in force")));
    assert_eq!(with(3, 2), Err(DecodeError::InvalidField("option flag")));
    // No display, but a display value.
    assert_eq!(with(32, 1), Err(DecodeError::NonCanonical));
    let mut phase = [0; COMMAND_SIZE];
    encode_command(
        &Command::SetPhase {
            phase: Phase::Closed,
        },
        &mut phase,
    );
    phase[2] = 4;
    assert_eq!(
        decode_command(&phase),
        Err(DecodeError::InvalidField("phase"))
    );
    for error in [
        DecodeError::UnknownKind(9),
        DecodeError::InvalidField("side"),
        DecodeError::NonCanonical,
        DecodeError::Truncated,
        DecodeError::TrailingBytes,
        DecodeError::InvalidConfig(orderbook::ConfigError::EmptyBand),
    ] {
        assert!(!error.to_string().is_empty());
    }
}

/// A snapshot whose configuration would not build a book is refused, not a panic.
#[test]
fn snapshots_with_invalid_configurations_are_refused() {
    let book = OrderBook::new(BookConfig::new(1, 100, 4));
    let mut bytes = Vec::new();
    encode_snapshot(&book.snapshot(), &mut bytes);
    // max_price (bytes 8..16) below min_price.
    bytes[8..16].copy_from_slice(&0i64.to_le_bytes());
    assert_eq!(
        decode_snapshot(&bytes),
        Err(DecodeError::InvalidConfig(
            orderbook::ConfigError::EmptyBand
        ))
    );
}
