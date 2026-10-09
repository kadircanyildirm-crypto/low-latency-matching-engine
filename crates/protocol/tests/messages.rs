//! Every message round-trips; decoding accepts nothing but what encoding produces, however
//! the bytes arrive.

use orderbook::{CancelReason, Phase, RejectReason, Side, TimeInForce};
use proptest::prelude::*;
use protocol::{
    HEADER_SIZE, Inbound, LevelUpdate, LoginError, LogoutReason, MAX_MESSAGE_SIZE, NewOrder,
    OrderKind, Outbound, ProtocolError, RejectCode, Report, ReportKind, TradeTick, decode_inbound,
    decode_outbound, encode_inbound, encode_outbound,
};

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

fn inbound() -> impl Strategy<Value = Inbound> {
    let kind = prop_oneof![
        (any::<i64>(), tif(), any::<Option<u64>>()).prop_map(|(price, tif, display)| {
            OrderKind::Limit {
                price,
                tif,
                display,
            }
        }),
        Just(OrderKind::Market),
        (any::<i64>(), any::<Option<i64>>())
            .prop_map(|(trigger, limit)| OrderKind::Stop { trigger, limit }),
    ];
    prop_oneof![
        (any::<u16>(), any::<u32>(), any::<u64>()).prop_map(|(version, account, token)| {
            Inbound::Login {
                version,
                account,
                token,
            }
        }),
        Just(Inbound::Logout),
        Just(Inbound::Heartbeat),
        (any::<u64>(), side(), any::<u64>(), kind).prop_map(|(client_ref, side, qty, kind)| {
            Inbound::NewOrder(NewOrder {
                client_ref,
                side,
                qty,
                kind,
            })
        }),
        any::<u64>().prop_map(|order_id| Inbound::Cancel { order_id }),
        (any::<u64>(), any::<i64>(), any::<u64>()).prop_map(|(order_id, price, qty)| {
            Inbound::Modify {
                order_id,
                price,
                qty,
            }
        }),
        Just(Inbound::MassCancel),
        Just(Inbound::Subscribe),
    ]
}

fn report_kind() -> impl Strategy<Value = ReportKind> {
    let reject = prop_oneof![
        Just(RejectReason::InvalidQuantity),
        Just(RejectReason::UnknownOrder),
        Just(RejectReason::MarketClosed),
        Just(RejectReason::PendingStop),
    ];
    let cancel = prop_oneof![
        Just(CancelReason::Requested),
        Just(CancelReason::SelfTrade),
        Just(CancelReason::TradingPhase),
    ];
    let phase = prop_oneof![
        Just(Phase::Continuous),
        Just(Phase::Auction),
        Just(Phase::Halted),
        Just(Phase::Closed),
    ];
    prop_oneof![
        Just(ReportKind::Accepted),
        reject.prop_map(ReportKind::Rejected),
        (
            any::<u64>(),
            side(),
            any::<i64>(),
            any::<u64>(),
            any::<u64>()
        )
            .prop_map(|(trade_id, side, price, qty, leaves)| ReportKind::Fill {
                trade_id,
                side,
                price,
                qty,
                leaves
            }),
        (side(), any::<i64>(), any::<u64>(), any::<u64>()).prop_map(
            |(side, price, qty, visible)| ReportKind::Rested {
                side,
                price,
                qty,
                visible
            }
        ),
        (side(), any::<i64>(), any::<u64>()).prop_map(|(side, price, visible)| {
            ReportKind::Replenished {
                side,
                price,
                visible,
            }
        }),
        (any::<u64>(), cancel).prop_map(|(qty, reason)| ReportKind::Cancelled { qty, reason }),
        (any::<i64>(), any::<u64>(), any::<u64>())
            .prop_map(|(price, qty, leaves)| ReportKind::Modified { price, qty, leaves }),
        (side(), any::<i64>(), any::<Option<i64>>(), any::<u64>()).prop_map(
            |(side, trigger, limit, qty)| ReportKind::StopPlaced {
                side,
                trigger,
                limit,
                qty
            }
        ),
        Just(ReportKind::Triggered),
        any::<u32>().prop_map(|count| ReportKind::MassCancelled { count }),
        phase.prop_map(ReportKind::PhaseChanged),
    ]
}

fn outbound() -> impl Strategy<Value = Outbound> {
    prop_oneof![
        (any::<u32>(), any::<u64>())
            .prop_map(|(account, last_seq)| Outbound::LoginAccepted { account, last_seq }),
        prop_oneof![
            Just(LoginError::UnsupportedVersion),
            Just(LoginError::BadCredentials),
            Just(LoginError::AlreadyLoggedIn),
            Just(LoginError::AlreadyInSession),
        ]
        .prop_map(|reason| Outbound::LoginRejected { reason }),
        Just(Outbound::Heartbeat),
        prop_oneof![
            Just(LogoutReason::Requested),
            Just(LogoutReason::Idle),
            Just(LogoutReason::ProtocolError),
            Just(LogoutReason::SlowConsumer),
            Just(LogoutReason::Shutdown),
        ]
        .prop_map(|reason| Outbound::Logout { reason }),
        (
            prop_oneof![
                Just(RejectCode::TooManyOrders),
                Just(RejectCode::Throttled),
                Just(RejectCode::Unavailable),
            ],
            any::<u64>()
        )
            .prop_map(|(reason, client_ref)| Outbound::Reject { reason, client_ref }),
        (any::<u64>(), any::<u64>(), any::<u64>(), report_kind()).prop_map(
            |(seq, order_id, client_ref, kind)| Outbound::Report(Report {
                seq,
                order_id,
                client_ref,
                kind
            })
        ),
        (any::<u64>(), any::<u32>())
            .prop_map(|(seq, levels)| Outbound::BookSnapshot { seq, levels }),
        (
            any::<u64>(),
            side(),
            any::<i64>(),
            any::<u64>(),
            any::<u32>()
        )
            .prop_map(|(seq, side, price, qty, orders)| Outbound::LevelUpdate(
                LevelUpdate {
                    seq,
                    side,
                    price,
                    qty,
                    orders
                }
            )),
        (
            any::<u64>(),
            any::<u64>(),
            side(),
            any::<i64>(),
            any::<u64>()
        )
            .prop_map(
                |(seq, trade_id, side, price, qty)| Outbound::TradeTick(TradeTick {
                    seq,
                    trade_id,
                    side,
                    price,
                    qty
                })
            ),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn inbound_messages_round_trip(message in inbound()) {
        let mut bytes = Vec::new();
        encode_inbound(&message, &mut bytes);
        prop_assert!(bytes.len() <= MAX_MESSAGE_SIZE);
        prop_assert_eq!(decode_inbound(&bytes), Ok(Some((message, bytes.len()))));
        // Every proper prefix is incomplete, or already wrong in the header.
        for cut in 0..bytes.len() {
            prop_assert_eq!(decode_inbound(&bytes[..cut]), Ok(None), "cut at {}", cut);
        }
    }

    #[test]
    fn outbound_messages_round_trip(message in outbound()) {
        let mut bytes = Vec::new();
        encode_outbound(&message, &mut bytes);
        prop_assert!(bytes.len() <= MAX_MESSAGE_SIZE);
        prop_assert_eq!(decode_outbound(&bytes), Ok(Some((message, bytes.len()))));
        for cut in 0..bytes.len() {
            prop_assert_eq!(decode_outbound(&bytes[..cut]), Ok(None), "cut at {}", cut);
        }
    }

    /// Changing any byte either fails to decode, or decodes to a message that encodes to
    /// the changed bytes.
    #[test]
    fn decoding_accepts_only_canonical_bytes(
        message in inbound(),
        reply in outbound(),
        at in 0..MAX_MESSAGE_SIZE,
        value in any::<u8>(),
    ) {
        let mut bytes = Vec::new();
        encode_inbound(&message, &mut bytes);
        let at = at % bytes.len();
        bytes[at] = value;
        if let Ok(Some((decoded, len))) = decode_inbound(&bytes) {
            let mut again = Vec::new();
            encode_inbound(&decoded, &mut again);
            prop_assert_eq!(&again[..], &bytes[..len]);
        }
        let mut bytes = Vec::new();
        encode_outbound(&reply, &mut bytes);
        let at = at % bytes.len();
        bytes[at] = value;
        if let Ok(Some((decoded, len))) = decode_outbound(&bytes) {
            let mut again = Vec::new();
            encode_outbound(&decoded, &mut again);
            prop_assert_eq!(&again[..], &bytes[..len]);
        }
    }

    /// A stream of messages decodes the same whatever pieces it arrives in.
    #[test]
    fn streams_decode_in_any_pieces(
        messages in prop::collection::vec(inbound(), 1..20),
        pieces in prop::collection::vec(1..30usize, 1..40),
    ) {
        let mut stream = Vec::new();
        for message in &messages {
            encode_inbound(message, &mut stream);
        }
        let mut received = Vec::new();
        let mut buffer = Vec::new();
        let mut at = 0;
        for piece in pieces.iter().cycle() {
            if at >= stream.len() {
                break;
            }
            let end = (at + piece).min(stream.len());
            buffer.extend_from_slice(&stream[at..end]);
            at = end;
            while let Some((message, len)) = decode_inbound(&buffer).unwrap() {
                received.push(message);
                buffer.drain(..len);
            }
        }
        prop_assert_eq!(received, messages);
        prop_assert!(buffer.is_empty());
    }

    /// Arbitrary bytes: decoding never panics, and what decodes re-encodes to them.
    #[test]
    fn arbitrary_bytes_decode_canonically_or_not_at_all(
        bytes in prop::collection::vec(any::<u8>(), 0..100),
    ) {
        if let Ok(Some((message, len))) = decode_inbound(&bytes) {
            let mut again = Vec::new();
            encode_inbound(&message, &mut again);
            prop_assert_eq!(&again[..], &bytes[..len]);
        }
        if let Ok(Some((message, len))) = decode_outbound(&bytes) {
            let mut again = Vec::new();
            encode_outbound(&message, &mut again);
            prop_assert_eq!(&again[..], &bytes[..len]);
        }
    }
}

#[test]
fn bad_headers_are_named() {
    // Unknown type, in either direction.
    assert_eq!(
        decode_inbound(&[4, 0, 99, 0]),
        Err(ProtocolError::UnknownType(99))
    );
    assert_eq!(
        decode_outbound(&[4, 0, 1, 0]),
        Err(ProtocolError::UnknownType(1))
    );
    // A heartbeat that claims to be longer.
    assert_eq!(
        decode_inbound(&[5, 0, 3, 0, 0]),
        Err(ProtocolError::BadLength { kind: 3, len: 5 })
    );
    // Header padding.
    assert_eq!(
        decode_inbound(&[4, 0, 3, 1]),
        Err(ProtocolError::NonCanonical)
    );
    // An invalid side in a new order.
    let mut bytes = Vec::new();
    encode_inbound(
        &Inbound::NewOrder(NewOrder {
            client_ref: 1,
            side: Side::Buy,
            qty: 1,
            kind: OrderKind::Market,
        }),
        &mut bytes,
    );
    bytes[HEADER_SIZE + 8] = 2;
    assert_eq!(
        decode_inbound(&bytes),
        Err(ProtocolError::InvalidField("side"))
    );
    for error in [
        ProtocolError::UnknownType(9),
        ProtocolError::BadLength { kind: 1, len: 2 },
        ProtocolError::InvalidField("side"),
        ProtocolError::NonCanonical,
    ] {
        assert!(!error.to_string().is_empty());
    }
}
