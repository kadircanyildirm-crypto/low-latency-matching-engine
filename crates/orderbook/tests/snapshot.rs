//! Snapshots and the state digest: a book restored from a snapshot taken at any point must
//! behave exactly like the original from then on, the digest must identify the state, and
//! snapshots the engine could never have produced must be refused.

mod common;

use common::reference::ReferenceBook;
use common::strategies::scenario;
use orderbook::{
    BookConfig, BookSnapshot, OrderBook, Phase, SelfTradePolicy, Side, SnapshotError,
    SnapshotOrder, StopOrder,
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Any state that a snapshot left out would make the restored book drift from the
    /// original sooner or later: a different trade id, queue position or rejection.
    ///
    /// The reference book loaded from the same snapshot must continue like the original
    /// too. The `restore` fuzz target compares restored books against it, so this checks
    /// the loading itself.
    #[test]
    fn a_restored_book_continues_exactly_like_the_original(
        (cfg, commands, cut) in scenario(200).prop_flat_map(|(cfg, commands)| {
            let len = commands.len();
            (Just(cfg), Just(commands), 0..=len)
        })
    ) {
        let mut original = OrderBook::new(cfg);
        let mut events = Vec::new();
        for &command in &commands[..cut] {
            original.process(command, &mut events);
        }

        let snapshot = original.snapshot();
        prop_assert_eq!(snapshot.digest(), original.digest());
        let mut restored = OrderBook::restore(&snapshot).expect("a live book's snapshot restores");
        if let Err(violation) = restored.validate() {
            prop_assert!(false, "restored book is broken: {}", violation);
        }
        prop_assert_eq!(&restored.snapshot(), &snapshot);
        prop_assert_eq!(restored.digest(), original.digest());
        let mut reference = ReferenceBook::restore(&snapshot);
        prop_assert_eq!(reference.snapshot(), common::snapshot(&original));

        let (mut got, mut want, mut naive) = (Vec::new(), Vec::new(), Vec::new());
        for (step, &command) in commands[cut..].iter().enumerate() {
            got.clear();
            want.clear();
            naive.clear();
            restored.process(command, &mut got);
            original.process(command, &mut want);
            reference.process(command, &mut naive);
            prop_assert_eq!(&got, &want, "{} commands after the restore: {:?}", step, command);
            prop_assert_eq!(&naive, &want, "reference, {} commands after the restore", step);
        }
        prop_assert_eq!(restored.snapshot(), original.snapshot());
        prop_assert_eq!(reference.snapshot(), common::snapshot(&original));
        for side in [Side::Buy, Side::Sell] {
            let stops: Vec<_> = original.stops(side).collect();
            prop_assert_eq!(reference.stops(side), stops);
        }
        prop_assert_eq!(reference.trade_count(), original.trade_count());
        prop_assert_eq!(reference.reference_price(), original.reference_price());
        prop_assert_eq!(reference.phase(), original.phase());
    }
}

fn order(id: u64, side: Side, price: i64, leaves: u64, filled: u64) -> SnapshotOrder {
    SnapshotOrder {
        id,
        owner: id as u32,
        side,
        price,
        leaves,
        filled,
        post_only: false,
        display: None,
        visible: leaves,
    }
}

/// Two bids at 50 (in time priority) and an ask at 60, with a band around a last trade
/// at 55, while trading is halted.
fn sample() -> BookSnapshot {
    BookSnapshot {
        config: BookConfig {
            price_protection: Some(3),
            price_band: Some(20),
            reference_price: Some(52),
            ..BookConfig::new(1, 100, 8)
        },
        trade_count: 4,
        reference_price: Some(55),
        phase: Phase::Halted,
        orders: vec![
            order(1, Side::Buy, 50, 5, 2),
            SnapshotOrder {
                post_only: true,
                ..order(2, Side::Buy, 50, 7, 0)
            },
            SnapshotOrder {
                display: Some(4),
                visible: 2,
                ..order(3, Side::Sell, 60, 10, 9)
            },
        ],
        stops: vec![
            stop(4, Side::Buy, 58, None),
            stop(5, Side::Buy, 58, Some(59)),
            stop(6, Side::Sell, 52, None),
        ],
    }
}

fn stop(id: u64, side: Side, trigger: i64, limit: Option<i64>) -> StopOrder {
    StopOrder {
        id,
        owner: id as u32,
        side,
        trigger,
        limit,
        qty: 3,
    }
}

#[test]
fn a_snapshot_round_trips_through_restore() {
    let snapshot = sample();
    let book = OrderBook::restore(&snapshot).unwrap();
    book.validate().unwrap();
    assert_eq!(book.snapshot(), snapshot);
    assert_eq!(book.digest(), snapshot.digest());
    assert_eq!(book.trade_count(), 4);
    assert_eq!(book.reference_price(), Some(55));
    assert_eq!(book.phase(), Phase::Halted);
    let buy_stops: Vec<u64> = book.stops(Side::Buy).map(|s| s.id).collect();
    assert_eq!(buy_stops, [4, 5]);
    assert_eq!(book.stop(5), Some(stop(5, Side::Buy, 58, Some(59))));
    assert_eq!(book.order(5), None);
    assert_eq!(book.order_count(), 6);
    let queue: Vec<_> = book.queue(Side::Buy, 50).map(|o| o.id).collect();
    assert_eq!(queue, [1, 2]);
    let info = book.order(3).unwrap();
    assert_eq!((info.leaves, info.filled), (10, 9));
    assert_eq!((info.display, info.visible), (Some(4), 2));
    assert_eq!(book.best_ask().map(|level| level.qty), Some(2));
    assert!(book.order(2).unwrap().post_only && !info.post_only);
}

#[test]
fn only_the_order_within_a_level_matters() {
    // Levels may come in any order; the snapshot taken afterwards is canonical again.
    let mut shuffled = sample();
    shuffled.orders.rotate_left(2);
    let book = OrderBook::restore(&shuffled).unwrap();
    assert_eq!(book.snapshot(), sample());
}

#[test]
fn the_digest_covers_every_field() {
    let base = sample();
    let mut variants: Vec<(&str, BookSnapshot)> = Vec::new();
    let mut vary = |what, change: fn(&mut BookSnapshot)| {
        let mut snapshot = sample();
        change(&mut snapshot);
        variants.push((what, snapshot));
    };
    vary("min_price", |s| s.config.min_price -= 1);
    vary("max_price", |s| s.config.max_price += 1);
    vary("max_orders", |s| s.config.max_orders += 1);
    vary("max_owners", |s| s.config.max_owners += 1);
    vary("max_order_qty", |s| s.config.max_order_qty -= 1);
    vary("protection off", |s| s.config.price_protection = None);
    vary("protection width", |s| s.config.price_protection = Some(4));
    vary("self-trade policy", |s| {
        s.config.self_trade = SelfTradePolicy::CancelIncoming;
    });
    vary("trade count", |s| s.trade_count += 1);
    vary("reference price", |s| s.reference_price = Some(56));
    vary("no reference price", |s| s.reference_price = None);
    vary("band", |s| s.config.price_band = Some(21));
    vary("band off", |s| s.config.price_band = None);
    vary("initial reference", |s| s.config.reference_price = Some(53));
    vary("volatility interruption", |s| {
        s.config.auction_on_band = true
    });
    vary("continuous trading", |s| s.phase = Phase::Continuous);
    vary("call phase", |s| s.phase = Phase::Auction);
    vary("closed", |s| s.phase = Phase::Closed);
    vary("order id", |s| s.orders[0].id = 9);
    vary("owner", |s| s.orders[0].owner = 9);
    vary("side", |s| s.orders[2].side = Side::Buy);
    vary("price", |s| s.orders[2].price += 1);
    vary("leaves", |s| s.orders[0].leaves += 1);
    vary("filled", |s| s.orders[0].filled += 1);
    vary("post-only", |s| s.orders[0].post_only = true);
    vary("stop trigger", |s| s.stops[0].trigger = 57);
    vary("stop limit", |s| s.stops[1].limit = Some(60));
    vary("stop-market to stop-limit", |s| s.stops[0].limit = Some(58));
    vary("stop owner", |s| s.stops[0].owner = 9);
    vary("stop quantity", |s| s.stops[0].qty = 4);
    vary("stop side", |s| s.stops[2].side = Side::Buy);
    vary("stop order", |s| s.stops.swap(0, 1));
    vary("missing stop", |s| {
        s.stops.pop();
    });
    vary("iceberg display", |s| s.orders[2].display = Some(5));
    vary("iceberg visible", |s| s.orders[2].visible = 3);
    vary("plain to iceberg", |s| {
        s.orders[0].display = Some(5);
    });
    vary("queue order", |s| s.orders.swap(0, 1));
    vary("missing order", |s| {
        s.orders.pop();
    });
    for (what, snapshot) in variants {
        assert_ne!(snapshot.digest(), base.digest(), "digest ignores {what}");
    }
}

#[test]
fn the_digest_of_an_empty_book_is_pinned() {
    // Pinned so that any change to the encoding is deliberate: once Phase 2 persists
    // digests, changing it breaks every stored one. CI checks it on every platform.
    let book = OrderBook::new(BookConfig::new(-5, 5, 3));
    assert_eq!(
        book.digest(),
        0xa789_a9d5_272f_3cd1,
        "got {:#018x}",
        book.digest()
    );
}

#[test]
fn snapshots_the_engine_could_never_produce_are_refused() {
    let cfg = BookConfig::new(1, 100, 2);
    let max = cfg.max_order_qty;
    let restore = |orders: Vec<SnapshotOrder>, trade_count| {
        OrderBook::restore(&BookSnapshot {
            config: cfg,
            trade_count,
            reference_price: None,
            phase: Phase::Continuous,
            orders,
            stops: Vec::new(),
        })
        .map(|book| book.order_count())
    };
    let cases = [
        (
            vec![
                order(1, Side::Buy, 10, 1, 0),
                order(2, Side::Buy, 10, 1, 0),
                order(3, Side::Buy, 10, 1, 0),
            ],
            SnapshotError::TooManyOrders,
        ),
        (
            vec![order(1, Side::Buy, 10, 1, 0), order(1, Side::Buy, 11, 1, 0)],
            SnapshotError::DuplicateOrderId(1),
        ),
        (
            vec![order(1, Side::Buy, 0, 1, 0)],
            SnapshotError::InvalidOrder(1),
        ),
        (
            vec![SnapshotOrder {
                owner: cfg.max_owners,
                ..order(1, Side::Buy, 10, 1, 0)
            }],
            SnapshotError::InvalidOrder(1),
        ),
        // A plain order shows all it has.
        (
            vec![SnapshotOrder {
                visible: 1,
                ..order(1, Side::Buy, 10, 2, 0)
            }],
            SnapshotError::InvalidOrder(1),
        ),
        // An iceberg shows between one lot and its display, and never more than it has.
        (
            vec![SnapshotOrder {
                display: Some(3),
                visible: 0,
                ..order(1, Side::Buy, 10, 5, 0)
            }],
            SnapshotError::InvalidOrder(1),
        ),
        (
            vec![SnapshotOrder {
                display: Some(3),
                visible: 4,
                ..order(1, Side::Buy, 10, 5, 0)
            }],
            SnapshotError::InvalidOrder(1),
        ),
        (
            vec![SnapshotOrder {
                display: Some(3),
                visible: 3,
                ..order(1, Side::Buy, 10, 2, 0)
            }],
            SnapshotError::InvalidOrder(1),
        ),
        (
            vec![order(1, Side::Buy, 101, 1, 0)],
            SnapshotError::InvalidOrder(1),
        ),
        (
            vec![order(1, Side::Buy, 10, 0, 3)],
            SnapshotError::InvalidOrder(1),
        ),
        (
            vec![order(1, Side::Buy, 10, max, 1)],
            SnapshotError::InvalidOrder(1),
        ),
        (
            vec![order(1, Side::Buy, 10, u64::MAX, 1)],
            SnapshotError::InvalidOrder(1),
        ),
        (
            vec![
                order(1, Side::Buy, 50, 1, 0),
                order(2, Side::Sell, 50, 1, 0),
            ],
            SnapshotError::Crossed,
        ),
        (
            vec![
                order(1, Side::Buy, 51, 1, 0),
                order(2, Side::Sell, 50, 1, 0),
            ],
            SnapshotError::Crossed,
        ),
    ];
    for (orders, error) in cases {
        assert_eq!(restore(orders.clone(), 0), Err(error), "{orders:?}");
    }
    for trade_count in [BookSnapshot::MAX_TRADE_COUNT + 1, u64::MAX] {
        assert_eq!(
            restore(Vec::new(), trade_count),
            Err(SnapshotError::TradeCountExhausted)
        );
    }
    for price in [0, 101] {
        let snapshot = BookSnapshot {
            config: cfg,
            trade_count: 0,
            reference_price: Some(price),
            phase: Phase::Continuous,
            orders: Vec::new(),
            stops: Vec::new(),
        };
        assert_eq!(
            OrderBook::restore(&snapshot).err(),
            Some(SnapshotError::InvalidReferencePrice)
        );
    }

    // The limits themselves are fine.
    let edge = vec![
        SnapshotOrder {
            owner: cfg.max_owners - 1,
            ..order(1, Side::Buy, 49, max - 1, 1)
        },
        order(2, Side::Sell, 50, 1, 0),
    ];
    assert_eq!(restore(edge, BookSnapshot::MAX_TRADE_COUNT), Ok(2));
}

#[test]
fn stops_the_engine_could_never_hold_are_refused() {
    // The last trade was at 50: buy stops must trigger above it, sell stops below.
    let refused = |stop: StopOrder| {
        let snapshot = BookSnapshot {
            stops: vec![stop],
            ..sample()
        };
        OrderBook::restore(&snapshot).err()
    };
    let invalid = Some(SnapshotError::InvalidOrder(9));
    assert_eq!(refused(stop(9, Side::Buy, 55, None)), invalid);
    assert_eq!(refused(stop(9, Side::Sell, 55, None)), invalid);
    assert_eq!(refused(stop(9, Side::Buy, 101, None)), invalid);
    assert_eq!(refused(stop(9, Side::Buy, 58, Some(0))), invalid);
    assert_eq!(
        refused(StopOrder {
            qty: 0,
            ..stop(9, Side::Buy, 58, None)
        }),
        invalid
    );
    assert_eq!(
        refused(StopOrder {
            owner: 5_000,
            ..stop(9, Side::Buy, 58, None)
        }),
        invalid
    );
    assert_eq!(
        refused(stop(1, Side::Buy, 58, None)),
        Some(SnapshotError::DuplicateOrderId(1))
    );
    // Orders and stops share the capacity.
    let crowded = BookSnapshot {
        config: BookConfig {
            max_orders: 5,
            ..sample().config
        },
        ..sample()
    };
    assert_eq!(
        OrderBook::restore(&crowded).err(),
        Some(SnapshotError::TooManyOrders)
    );
}

#[test]
fn a_crossed_book_restores_only_in_a_call_phase() {
    let crossed = |phase| BookSnapshot {
        config: BookConfig::new(1, 100, 4),
        trade_count: 0,
        reference_price: None,
        phase,
        orders: vec![
            order(1, Side::Buy, 51, 2, 0),
            order(2, Side::Sell, 50, 3, 0),
        ],
        stops: Vec::new(),
    };
    for phase in [Phase::Continuous, Phase::Halted, Phase::Closed] {
        assert_eq!(
            OrderBook::restore(&crossed(phase)).err(),
            Some(SnapshotError::Crossed),
            "{phase:?}"
        );
    }
    let mut book = OrderBook::restore(&crossed(Phase::Auction)).unwrap();
    book.validate().unwrap();
    assert_eq!(book.snapshot(), crossed(Phase::Auction));
    // It uncrosses like the book it was taken from: 2 lots at 50, since both candidate
    // prices leave a lot of surplus on the sell side, which pushes the price down.
    let mut events = Vec::new();
    book.process(
        orderbook::Command::SetPhase {
            phase: Phase::Continuous,
        },
        &mut events,
    );
    assert_eq!(book.trade_count(), 1);
    assert_eq!(book.reference_price(), Some(50));
    book.validate().unwrap();
}

/// The largest trade count a snapshot may carry still leaves room for every trade.
#[test]
fn a_book_restored_at_the_trade_count_limit_keeps_trading() {
    let mut book = OrderBook::restore(&BookSnapshot {
        config: BookConfig::new(1, 100, 4),
        trade_count: BookSnapshot::MAX_TRADE_COUNT,
        reference_price: None,
        phase: Phase::Continuous,
        orders: vec![order(1, Side::Sell, 50, 3, 0)],
        stops: Vec::new(),
    })
    .unwrap();
    let mut events = Vec::new();
    book.process(
        orderbook::Command::Market {
            id: 2,
            owner: 2,
            side: Side::Buy,
            qty: 1,
        },
        &mut events,
    );
    assert!(events.iter().any(|event| matches!(
        event,
        orderbook::Event::Trade { trade_id, .. } if *trade_id == 1 << 63
    )));
    assert_eq!(book.trade_count(), 1 << 63);
}

#[test]
fn errors_explain_themselves() {
    let messages = [
        (SnapshotError::TooManyOrders, "more orders than max_orders"),
        (
            SnapshotError::DuplicateOrderId(7),
            "order id 7 appears twice",
        ),
        (
            SnapshotError::InvalidOrder(7),
            "order 7 cannot rest in this book",
        ),
        (
            SnapshotError::Crossed,
            "the best bid is at or above the best ask outside a call phase",
        ),
        (SnapshotError::TradeCountExhausted, "too few trade ids left"),
        (
            SnapshotError::InvalidReferencePrice,
            "the reference price is outside the band",
        ),
    ];
    for (error, message) in messages {
        assert_eq!(error.to_string(), message);
    }
}
