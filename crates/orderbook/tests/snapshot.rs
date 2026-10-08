//! Snapshots and the state digest: a book restored from a snapshot taken at any point must
//! behave exactly like the original from then on, the digest must identify the state, and
//! snapshots the engine could never have produced must be refused.

mod common;

use common::strategies::scenario;
use orderbook::{
    BookConfig, BookSnapshot, OrderBook, SelfTradePolicy, Side, SnapshotError, SnapshotOrder,
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Any state that a snapshot left out would make the restored book drift from the
    /// original sooner or later: a different trade id, queue position or rejection.
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

        let (mut got, mut want) = (Vec::new(), Vec::new());
        for (step, &command) in commands[cut..].iter().enumerate() {
            got.clear();
            want.clear();
            restored.process(command, &mut got);
            original.process(command, &mut want);
            prop_assert_eq!(&got, &want, "{} commands after the restore: {:?}", step, command);
        }
        prop_assert_eq!(restored.snapshot(), original.snapshot());
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
    }
}

/// Two bids at 50 (in time priority) and an ask at 60.
fn sample() -> BookSnapshot {
    BookSnapshot {
        config: BookConfig {
            price_protection: Some(3),
            ..BookConfig::new(1, 100, 8)
        },
        trade_count: 4,
        orders: vec![
            order(1, Side::Buy, 50, 5, 2),
            order(2, Side::Buy, 50, 7, 0),
            order(3, Side::Sell, 60, 1, 9),
        ],
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
    let queue: Vec<_> = book.queue(Side::Buy, 50).map(|o| o.id).collect();
    assert_eq!(queue, [1, 2]);
    let info = book.order(3).unwrap();
    assert_eq!((info.leaves, info.filled), (1, 9));
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
    vary("max_order_qty", |s| s.config.max_order_qty -= 1);
    vary("protection off", |s| s.config.price_protection = None);
    vary("protection width", |s| s.config.price_protection = Some(4));
    vary("self-trade policy", |s| {
        s.config.self_trade = SelfTradePolicy::CancelIncoming;
    });
    vary("trade count", |s| s.trade_count += 1);
    vary("order id", |s| s.orders[0].id = 9);
    vary("owner", |s| s.orders[0].owner = 9);
    vary("side", |s| s.orders[2].side = Side::Buy);
    vary("price", |s| s.orders[2].price += 1);
    vary("leaves", |s| s.orders[0].leaves += 1);
    vary("filled", |s| s.orders[0].filled += 1);
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
    // Fixed forever, like the encoding it hashes; CI checks it on every platform.
    let book = OrderBook::new(BookConfig::new(-5, 5, 3));
    assert_eq!(
        book.digest(),
        0xb40c_8595_4855_4e47,
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
            orders,
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
    assert_eq!(
        restore(Vec::new(), u64::MAX),
        Err(SnapshotError::TradeCountExhausted)
    );

    // The limits themselves are fine.
    let edge = vec![
        order(1, Side::Buy, 49, max - 1, 1),
        order(2, Side::Sell, 50, 1, 0),
    ];
    assert_eq!(restore(edge, u64::MAX - 1), Ok(2));
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
            "the best bid is at or above the best ask",
        ),
        (SnapshotError::TradeCountExhausted, "no trade ids left"),
    ];
    for (error, message) in messages {
        assert_eq!(error.to_string(), message);
    }
}
