//! Hand-written scenarios, one rule of the matching semantics per test, asserting the exact
//! event sequence.
//!
//! Unless a test is about owners, every order gets its own owner (`owner == id`), so
//! self-trade prevention stays out of the way.

use orderbook::CancelReason::{
    FillOrKill, ImmediateOrCancel, MassCancel, NoLiquidity, PriceBand, PriceProtection, Requested,
    SelfTrade, TradingPhase,
};
use orderbook::Event::*;
use orderbook::RejectReason::*;
use orderbook::Side::{Buy, Sell};
use orderbook::{
    BookConfig, Command, Event, EventSink, LevelInfo, OrderBook, OrderId, OwnerId, Phase, Price,
    Qty, SelfTradePolicy, Side, TimeInForce,
};

const CFG: BookConfig = BookConfig::new(1, 10_000, 1_024);

fn book() -> OrderBook {
    OrderBook::new(CFG)
}

fn run(book: &mut OrderBook, command: Command) -> Vec<Event> {
    let mut events = Vec::new();
    book.process(command, &mut events);
    book.validate().unwrap();
    events
}

fn limit(id: OrderId, side: Side, price: Price, qty: Qty) -> Command {
    limit_by(id as OwnerId, id, side, price, qty)
}

fn limit_by(owner: OwnerId, id: OrderId, side: Side, price: Price, qty: Qty) -> Command {
    limit_tif(owner, id, side, price, qty, TimeInForce::Gtc)
}

fn limit_tif(
    owner: OwnerId,
    id: OrderId,
    side: Side,
    price: Price,
    qty: Qty,
    tif: TimeInForce,
) -> Command {
    Command::Limit {
        id,
        owner,
        side,
        price,
        qty,
        tif,
        display: None,
    }
}

/// A GTC iceberg showing `display` of `qty`.
fn iceberg(
    owner: OwnerId,
    id: OrderId,
    side: Side,
    price: Price,
    qty: Qty,
    display: Qty,
) -> Command {
    Command::Limit {
        id,
        owner,
        side,
        price,
        qty,
        tif: TimeInForce::Gtc,
        display: Some(display),
    }
}

fn replenished(id: OrderId, side: Side, price: Price, visible: Qty) -> Event {
    Replenished {
        id,
        side,
        price,
        visible,
    }
}

fn market(id: OrderId, side: Side, qty: Qty) -> Command {
    market_by(id as OwnerId, id, side, qty)
}

fn market_by(owner: OwnerId, id: OrderId, side: Side, qty: Qty) -> Command {
    Command::Market {
        id,
        owner,
        side,
        qty,
    }
}

fn cancel(id: OrderId) -> Command {
    cancel_by(id as OwnerId, id)
}

fn cancel_by(owner: OwnerId, id: OrderId) -> Command {
    Command::Cancel { id, owner }
}

fn modify(id: OrderId, price: Price, qty: Qty) -> Command {
    Command::Modify {
        id,
        owner: id as OwnerId,
        price,
        qty,
    }
}

fn rested(id: OrderId, side: Side, price: Price, qty: Qty) -> Event {
    Rested {
        id,
        side,
        price,
        qty,
        visible: qty,
    }
}

fn cancelled(id: OrderId, qty: Qty, reason: orderbook::CancelReason) -> Event {
    Cancelled { id, qty, reason }
}

fn modified(id: OrderId, price: Price, qty: Qty, leaves: Qty) -> Event {
    Modified {
        id,
        price,
        qty,
        leaves,
    }
}

fn rejected(id: OrderId, reason: orderbook::RejectReason) -> Event {
    Rejected { id, reason }
}

/// The trades in an event list as `(maker, price, qty)`.
fn fills(events: &[Event]) -> Vec<(OrderId, Price, Qty)> {
    events
        .iter()
        .filter_map(|e| match *e {
            Trade {
                maker, price, qty, ..
            } => Some((maker, price, qty)),
            _ => None,
        })
        .collect()
}

fn level(price: Price, qty: Qty, orders: u32) -> Option<LevelInfo> {
    Some(LevelInfo { price, qty, orders })
}

/// `(id, leaves)` of the orders at a level, in time priority.
fn queue(book: &OrderBook, side: Side, price: Price) -> Vec<(OrderId, Qty)> {
    book.queue(side, price).map(|o| (o.id, o.leaves)).collect()
}

// ---------------------------------------------------------------------------------------
// Price-time priority

#[test]
fn non_crossing_orders_rest_and_set_the_touch() {
    let mut b = book();
    assert_eq!(
        run(&mut b, limit(1, Buy, 99, 10)),
        [Accepted { id: 1 }, rested(1, Buy, 99, 10)]
    );
    run(&mut b, limit(2, Buy, 98, 5));
    run(&mut b, limit(3, Sell, 101, 7));
    run(&mut b, limit(4, Sell, 102, 1));
    assert_eq!(b.best_bid(), level(99, 10, 1));
    assert_eq!(b.best_ask(), level(101, 7, 1));
    assert_eq!(b.order_count(), 4);
}

#[test]
fn trades_report_ids_and_leaves_for_both_sides() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 100, 5));
    run(&mut b, limit(2, Sell, 101, 5));
    assert_eq!(
        run(&mut b, limit(3, Buy, 101, 7)),
        [
            Accepted { id: 3 },
            Trade {
                trade_id: 1,
                taker: 3,
                maker: 1,
                taker_side: Buy,
                price: 100,
                qty: 5,
                taker_leaves: 2,
                maker_leaves: 0,
            },
            Trade {
                trade_id: 2,
                taker: 3,
                maker: 2,
                taker_side: Buy,
                price: 101,
                qty: 2,
                taker_leaves: 0,
                maker_leaves: 3,
            },
        ]
    );
    assert_eq!(b.trade_count(), 2);
    // Trade ids continue across commands.
    let events = run(&mut b, market(4, Buy, 1));
    assert!(matches!(events[1], Trade { trade_id: 3, .. }));
}

#[test]
fn better_price_trades_first() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 102, 5));
    run(&mut b, limit(2, Sell, 101, 5));
    assert_eq!(fills(&run(&mut b, limit(3, Buy, 102, 5))), [(2, 101, 5)]);
    assert_eq!(b.best_ask(), level(102, 5, 1));
}

#[test]
fn same_price_fills_in_arrival_order() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 3));
    run(&mut b, limit(2, Buy, 100, 3));
    run(&mut b, limit(3, Buy, 100, 3));
    assert_eq!(
        fills(&run(&mut b, limit(4, Sell, 100, 7))),
        [(1, 100, 3), (2, 100, 3), (3, 100, 1)]
    );
    assert_eq!(queue(&b, Buy, 100), [(3, 2)]);
}

#[test]
fn partially_filled_maker_keeps_its_place() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 100, 10));
    run(&mut b, limit(2, Sell, 100, 10));
    run(&mut b, limit(3, Buy, 100, 4));
    assert_eq!(queue(&b, Sell, 100), [(1, 6), (2, 10)]);
    assert_eq!(b.best_ask(), level(100, 16, 2));
    let info = b.order(1).unwrap();
    assert_eq!((info.leaves, info.filled), (6, 4));
}

#[test]
fn sweep_trades_at_each_makers_price_and_rests_the_remainder() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 101, 2));
    run(&mut b, limit(2, Sell, 102, 2));
    run(&mut b, limit(3, Sell, 103, 2));
    run(&mut b, limit(4, Sell, 105, 2));
    let events = run(&mut b, limit(5, Buy, 103, 10));
    assert_eq!(fills(&events), [(1, 101, 2), (2, 102, 2), (3, 103, 2)]);
    assert_eq!(events.last(), Some(&rested(5, Buy, 103, 4)));
    assert_eq!(b.best_bid(), level(103, 4, 1));
    assert_eq!(b.best_ask(), level(105, 2, 1));
}

#[test]
fn limit_never_trades_through_its_price() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 5));
    assert_eq!(
        run(&mut b, limit(2, Sell, 101, 5)),
        [Accepted { id: 2 }, rested(2, Sell, 101, 5)]
    );
    assert_eq!(b.order_count(), 2);
}

#[test]
fn market_order_takes_liquidity_and_cancels_the_rest() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 3));
    run(&mut b, limit(2, Buy, 95, 3));
    let events = run(&mut b, market(3, Sell, 10));
    assert_eq!(fills(&events), [(1, 100, 3), (2, 95, 3)]);
    assert_eq!(events.last(), Some(&cancelled(3, 4, NoLiquidity)));
    assert!(b.order_count() == 0 && b.best_bid().is_none());
}

#[test]
fn market_order_on_an_empty_side_is_cancelled() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 3));
    assert_eq!(
        run(&mut b, market(2, Buy, 5)),
        [Accepted { id: 2 }, cancelled(2, 5, NoLiquidity)]
    );
    assert_eq!(b.best_bid(), level(100, 3, 1));
}

// ---------------------------------------------------------------------------------------
// Cancel

#[test]
fn cancel_from_the_middle_of_a_queue_keeps_the_others_in_order() {
    let mut b = book();
    for id in 1..=4 {
        run(&mut b, limit(id, Sell, 100, id));
    }
    assert_eq!(run(&mut b, cancel(2)), [cancelled(2, 2, Requested)]);
    assert_eq!(queue(&b, Sell, 100), [(1, 1), (3, 3), (4, 4)]);
    run(&mut b, cancel(4));
    run(&mut b, cancel(1));
    assert_eq!(queue(&b, Sell, 100), [(3, 3)]);
    run(&mut b, cancel(3));
    assert!(b.best_ask().is_none());
}

#[test]
fn cancelling_the_best_level_moves_the_touch() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 1));
    run(&mut b, limit(2, Buy, 90, 1));
    run(&mut b, cancel(1));
    assert_eq!(b.best_bid(), level(90, 1, 1));
}

#[test]
fn cancel_of_an_unknown_order_is_rejected() {
    let mut b = book();
    assert_eq!(run(&mut b, cancel(9)), [rejected(9, UnknownOrder)]);
    run(&mut b, limit(1, Buy, 100, 1));
    run(&mut b, cancel(1));
    assert_eq!(run(&mut b, cancel(1)), [rejected(1, UnknownOrder)]);
}

#[test]
fn only_the_owner_can_cancel_or_modify() {
    let mut b = book();
    run(&mut b, limit_by(7, 1, Buy, 100, 5));
    let intruder_cancel = Command::Cancel { id: 1, owner: 8 };
    let intruder_modify = Command::Modify {
        id: 1,
        owner: 8,
        price: 100,
        qty: 1,
    };
    // Reported exactly like a missing order, so other owners' ids do not leak.
    assert_eq!(run(&mut b, intruder_cancel), [rejected(1, UnknownOrder)]);
    assert_eq!(run(&mut b, intruder_modify), [rejected(1, UnknownOrder)]);
    assert_eq!(queue(&b, Buy, 100), [(1, 5)]);
    assert_eq!(
        run(&mut b, Command::Cancel { id: 1, owner: 7 }),
        [cancelled(1, 5, Requested)]
    );
}

// ---------------------------------------------------------------------------------------
// Modify (FIX semantics: qty is the new total, including what has been filled)

#[test]
fn reducing_the_total_at_the_same_price_keeps_priority() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 10));
    run(&mut b, limit(2, Buy, 100, 10));
    assert_eq!(run(&mut b, modify(1, 100, 4)), [modified(1, 100, 4, 4)]);
    assert_eq!(queue(&b, Buy, 100), [(1, 4), (2, 10)]);
    assert_eq!(b.best_bid(), level(100, 14, 2));
}

#[test]
fn increasing_the_total_loses_priority() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 10));
    run(&mut b, limit(2, Buy, 100, 10));
    assert_eq!(
        run(&mut b, modify(1, 100, 11)),
        [modified(1, 100, 11, 11), rested(1, Buy, 100, 11)]
    );
    assert_eq!(queue(&b, Buy, 100), [(2, 10), (1, 11)]);
}

#[test]
fn modify_racing_a_fill_cannot_overfill() {
    // The owner sent "10 -> 8" while 5 lots were filling. With FIX semantics the order ends
    // up with 8 in total (5 filled + 3 open), never 5 + 8 = 13.
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 10));
    run(&mut b, market(2, Sell, 5));
    assert_eq!(run(&mut b, modify(1, 100, 8)), [modified(1, 100, 8, 3)]);
    let info = b.order(1).unwrap();
    assert_eq!((info.leaves, info.filled), (3, 5));
}

#[test]
fn modify_to_at_most_the_filled_quantity_ends_the_order() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 10));
    run(&mut b, market(2, Sell, 6));
    assert_eq!(run(&mut b, modify(1, 100, 6)), [modified(1, 100, 6, 0)]);
    assert!(b.order(1).is_none() && b.best_bid().is_none());
}

#[test]
fn moving_the_price_loses_priority_and_can_trade() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 105, 3));
    run(&mut b, limit(2, Buy, 100, 5));
    run(&mut b, limit(3, Buy, 101, 5));
    let events = run(&mut b, modify(2, 105, 5));
    assert_eq!(events[0], modified(2, 105, 5, 5));
    assert_eq!(fills(&events), [(1, 105, 3)]);
    assert_eq!(events.last(), Some(&rested(2, Buy, 105, 2)));
    assert_eq!(b.best_bid(), level(105, 2, 1));
    assert!(b.best_ask().is_none());
    let info = b.order(2).unwrap();
    assert_eq!((info.leaves, info.filled), (2, 3));
}

// ---------------------------------------------------------------------------------------
// Validation and limits

#[test]
fn invalid_commands_are_rejected_without_side_effects() {
    let mut b = OrderBook::new(BookConfig {
        max_order_qty: 1_000,
        ..CFG
    });
    run(&mut b, limit(1, Buy, 100, 5));
    let cases = [
        (limit(2, Buy, 100, 0), InvalidQuantity),
        (limit(2, Buy, 100, 1_001), InvalidQuantity),
        (limit(2, Buy, 100, u64::MAX), InvalidQuantity),
        (limit(2, Buy, 0, 1), PriceOutOfRange),
        (limit(2, Sell, 10_001, 1), PriceOutOfRange),
        (limit(2, Sell, Price::MIN, 1), PriceOutOfRange),
        (limit(2, Sell, Price::MAX, 1), PriceOutOfRange),
        (limit_by(2, 1, Sell, 100, 1), DuplicateOrderId),
        (market(2, Sell, 0), InvalidQuantity),
        (market(2, Sell, 1_001), InvalidQuantity),
        (market_by(2, 1, Sell, 1), DuplicateOrderId),
        (modify(1, 100, 0), InvalidQuantity),
        (modify(1, 100, 1_001), InvalidQuantity),
        (modify(1, 10_001, 1), PriceOutOfRange),
        (modify(7, 100, 1), UnknownOrder),
        // Owner ids must be below max_owners; that check comes first.
        (limit_by(1_024, 2, Buy, 0, 0), InvalidOwner),
        (market_by(1_024, 2, Sell, 0), InvalidOwner),
        // An owner outside the table owns nothing.
        (
            Command::Cancel {
                id: 1,
                owner: 1_024,
            },
            UnknownOrder,
        ),
    ];
    for (command, reason) in cases {
        assert_eq!(
            run(&mut b, command),
            [rejected(command.id().unwrap(), reason)],
            "{command:?}"
        );
    }
    assert_eq!(queue(&b, Buy, 100), [(1, 5)]);
    assert_eq!(b.order_count(), 1);
    assert_eq!(b.trade_count(), 0);
    assert_eq!(
        run(&mut b, Command::CancelAll { owner: u32::MAX }),
        [MassCancelled {
            owner: u32::MAX,
            count: 0
        }]
    );
}

#[test]
fn level_totals_cannot_overflow() {
    // The largest quantities the config allows, all at one level: the level total is
    // exactly u64::MAX - 1 and nothing wraps.
    let max_qty = u64::MAX / 2;
    let mut b = OrderBook::new(BookConfig::new(1, 100, 2));
    assert_eq!(b.config().max_order_qty, max_qty);
    run(&mut b, limit(1, Buy, 50, max_qty));
    run(&mut b, limit(2, Buy, 50, max_qty));
    assert_eq!(b.best_bid(), level(50, 2 * max_qty, 2));
    let events = run(&mut b, market(3, Sell, max_qty));
    assert_eq!(fills(&events), [(1, 50, max_qty)]);
}

#[test]
#[should_panic(expected = "max_orders * max_order_qty must fit in a u64")]
fn configs_that_could_overflow_are_refused() {
    OrderBook::new(BookConfig {
        max_order_qty: u64::MAX / 2 + 1,
        ..BookConfig::new(1, 100, 2)
    });
}

/// `check` names each problem `OrderBook::new` would panic on, and accepts the limits.
#[test]
fn config_check_names_each_problem() {
    use orderbook::ConfigError::*;
    let base = BookConfig::new(1, 100, 2);
    let cases = [
        (
            BookConfig {
                max_price: 0,
                ..base
            },
            EmptyBand,
        ),
        (
            BookConfig {
                min_price: 0,
                max_price: i64::from(u32::MAX) - 1,
                ..base
            },
            BandTooWide,
        ),
        (
            BookConfig {
                max_orders: 0,
                ..base
            },
            MaxOrdersOutOfRange,
        ),
        (
            BookConfig {
                max_orders: u32::MAX,
                max_order_qty: 1,
                ..base
            },
            MaxOrdersOutOfRange,
        ),
        (
            BookConfig {
                max_order_qty: 0,
                ..base
            },
            ZeroMaxOrderQty,
        ),
        (
            BookConfig {
                max_owners: 0,
                ..base
            },
            ZeroMaxOwners,
        ),
        (
            BookConfig {
                max_order_qty: u64::MAX / 2 + 1,
                ..base
            },
            CapacityOverflow,
        ),
        (
            BookConfig {
                reference_price: Some(0),
                ..base
            },
            ReferenceOutsideBand,
        ),
        (
            BookConfig {
                reference_price: Some(101),
                ..base
            },
            ReferenceOutsideBand,
        ),
    ];
    for (config, error) in cases {
        assert_eq!(config.check(), Err(error), "{config:?}");
        assert!(!error.to_string().is_empty());
    }
    let limits = [
        BookConfig {
            max_price: 1,
            ..base
        },
        BookConfig {
            min_price: 0,
            max_price: i64::from(u32::MAX) - 2,
            max_orders: 1,
            ..base
        },
        BookConfig {
            max_orders: u32::MAX - 1,
            max_order_qty: 1,
            max_owners: 1,
            ..base
        },
        BookConfig {
            max_order_qty: u64::MAX / 2,
            reference_price: Some(1),
            ..base
        },
        BookConfig {
            reference_price: Some(100),
            ..base
        },
    ];
    for config in limits {
        assert_eq!(config.check(), Ok(()), "{config:?}");
    }
}

#[test]
fn full_book_refuses_orders_that_could_only_rest() {
    let mut b = OrderBook::new(BookConfig::new(1, 10_000, 2));
    run(&mut b, limit(1, Buy, 100, 5));
    run(&mut b, limit(2, Sell, 110, 5));
    assert_eq!(run(&mut b, limit(3, Buy, 99, 1)), [rejected(3, BookFull)]);
    // Market orders and cancels still work.
    assert_eq!(fills(&run(&mut b, market(4, Buy, 2))), [(2, 110, 2)]);
    run(&mut b, cancel(1));
    assert_eq!(run(&mut b, limit(3, Buy, 99, 1))[0], Accepted { id: 3 });
}

#[test]
fn full_book_still_accepts_crossing_orders() {
    let mut b = OrderBook::new(BookConfig::new(1, 10_000, 2));
    run(&mut b, limit(1, Buy, 100, 5));
    run(&mut b, limit(2, Sell, 110, 5));
    // Fully marketable: trades and leaves the book full.
    let events = run(&mut b, limit(3, Sell, 100, 2));
    assert_eq!(fills(&events), [(1, 100, 2)]);
    // Partially marketable: fills order 1, whose slot the remainder then takes.
    let events = run(&mut b, limit(4, Sell, 100, 5));
    assert_eq!(fills(&events), [(1, 100, 3)]);
    assert_eq!(events.last(), Some(&rested(4, Sell, 100, 2)));
    assert_eq!(b.order_count(), 2);
}

#[test]
fn ids_can_be_reused_once_the_order_is_gone() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 100, 1));
    run(&mut b, limit(2, Buy, 100, 1));
    assert_eq!(run(&mut b, limit(1, Buy, 90, 1))[0], Accepted { id: 1 });
    run(&mut b, cancel(1));
    assert_eq!(run(&mut b, limit(1, Sell, 120, 1))[0], Accepted { id: 1 });
}

// ---------------------------------------------------------------------------------------
// Self-trade prevention

#[test]
fn cancel_resting_removes_own_orders_and_keeps_matching() {
    let mut b = book();
    run(&mut b, limit_by(1, 1, Sell, 100, 2));
    run(&mut b, limit_by(7, 2, Sell, 100, 3));
    run(&mut b, limit_by(2, 3, Sell, 100, 4));
    let events = run(&mut b, limit_by(7, 4, Buy, 100, 9));
    assert_eq!(
        events,
        [
            Accepted { id: 4 },
            Trade {
                trade_id: 1,
                taker: 4,
                maker: 1,
                taker_side: Buy,
                price: 100,
                qty: 2,
                taker_leaves: 7,
                maker_leaves: 0
            },
            cancelled(2, 3, SelfTrade),
            Trade {
                trade_id: 2,
                taker: 4,
                maker: 3,
                taker_side: Buy,
                price: 100,
                qty: 4,
                taker_leaves: 3,
                maker_leaves: 0
            },
            rested(4, Buy, 100, 3),
        ]
    );
}

#[test]
fn cancel_incoming_stops_at_the_first_own_order() {
    let mut b = OrderBook::new(BookConfig {
        self_trade: SelfTradePolicy::CancelIncoming,
        ..CFG
    });
    run(&mut b, limit_by(1, 1, Sell, 100, 2));
    run(&mut b, limit_by(7, 2, Sell, 100, 3));
    run(&mut b, limit_by(2, 3, Sell, 100, 4));
    let events = run(&mut b, limit_by(7, 4, Buy, 100, 9));
    assert_eq!(fills(&events), [(1, 100, 2)]);
    assert_eq!(events.last(), Some(&cancelled(4, 7, SelfTrade)));
    // The owner's resting order and everything behind it are untouched.
    assert_eq!(queue(&b, Sell, 100), [(2, 3), (3, 4)]);
    assert!(b.best_bid().is_none());

    let events = run(&mut b, market_by(7, 5, Buy, 1));
    assert_eq!(events, [Accepted { id: 5 }, cancelled(5, 1, SelfTrade)]);
}

// ---------------------------------------------------------------------------------------
// Time in force

use TimeInForce::{Fok, Ioc, PostOnly};

/// Asks: 100 ×2 (#1), 101 ×3 (#2), 103 ×5 (#3).
fn ask_ladder() -> OrderBook {
    let mut b = book();
    run(&mut b, limit(1, Sell, 100, 2));
    run(&mut b, limit(2, Sell, 101, 3));
    run(&mut b, limit(3, Sell, 103, 5));
    b
}

#[test]
fn ioc_trades_what_it_can_and_cancels_the_rest() {
    let mut b = ask_ladder();
    let events = run(&mut b, limit_tif(9, 9, Buy, 101, 10, Ioc));
    assert_eq!(fills(&events), [(1, 100, 2), (2, 101, 3)]);
    assert_eq!(events.last(), Some(&cancelled(9, 5, ImmediateOrCancel)));
    assert!(b.best_bid().is_none());
    assert_eq!(b.best_ask(), level(103, 5, 1));
}

#[test]
fn ioc_that_cannot_trade_is_cancelled_whole() {
    let mut b = ask_ladder();
    assert_eq!(
        run(&mut b, limit_tif(9, 9, Buy, 99, 4, Ioc)),
        [Accepted { id: 9 }, cancelled(9, 4, ImmediateOrCancel)]
    );
    assert_eq!(b.order_count(), 3);
}

#[test]
fn fok_fills_completely_across_levels() {
    let mut b = ask_ladder();
    let events = run(&mut b, limit_tif(9, 9, Buy, 103, 7, Fok));
    assert_eq!(fills(&events), [(1, 100, 2), (2, 101, 3), (3, 103, 2)]);
    assert!(!events.iter().any(|e| matches!(e, Cancelled { id: 9, .. })));
    assert_eq!(queue(&b, Sell, 103), [(3, 3)]);
}

#[test]
fn fok_that_cannot_fill_is_killed_without_trading() {
    let mut b = ask_ladder();
    // 5 lots up to 101, but 6 wanted: nothing trades.
    assert_eq!(
        run(&mut b, limit_tif(9, 9, Buy, 101, 6, Fok)),
        [Accepted { id: 9 }, cancelled(9, 6, FillOrKill)]
    );
    assert_eq!(b.order_count(), 3);
    assert_eq!(b.trade_count(), 0);
}

#[test]
fn fok_counts_only_liquidity_self_trade_prevention_lets_it_reach() {
    // Under CancelResting the owner's own ask would be cancelled, not traded: 2 + 5 lots
    // reachable, not 2 + 3 + 5.
    let mut b = book();
    run(&mut b, limit_by(1, 1, Sell, 100, 2));
    run(&mut b, limit_by(7, 2, Sell, 101, 3));
    run(&mut b, limit_by(3, 3, Sell, 103, 5));
    assert_eq!(
        run(&mut b, limit_tif(7, 9, Buy, 103, 8, Fok)),
        [Accepted { id: 9 }, cancelled(9, 8, FillOrKill)]
    );
    let events = run(&mut b, limit_tif(7, 10, Buy, 103, 7, Fok));
    assert_eq!(fills(&events), [(1, 100, 2), (3, 103, 5)]);
    assert!(events.contains(&cancelled(2, 3, SelfTrade)));

    // Under CancelIncoming matching would stop at the owner's ask, so only what lies in
    // front of it counts, however much sits behind.
    let mut b = OrderBook::new(BookConfig {
        self_trade: SelfTradePolicy::CancelIncoming,
        ..CFG
    });
    run(&mut b, limit_by(1, 1, Sell, 100, 2));
    run(&mut b, limit_by(7, 2, Sell, 101, 3));
    run(&mut b, limit_by(3, 3, Sell, 103, 50));
    assert_eq!(
        run(&mut b, limit_tif(7, 9, Buy, 103, 3, Fok)),
        [Accepted { id: 9 }, cancelled(9, 3, FillOrKill)]
    );
    assert_eq!(
        fills(&run(&mut b, limit_tif(7, 10, Buy, 103, 2, Fok))),
        [(1, 100, 2)]
    );
}

#[test]
fn orders_that_never_rest_are_not_refused_by_a_full_book() {
    let mut b = OrderBook::new(BookConfig::new(1, 10_000, 1));
    run(&mut b, limit(1, Sell, 100, 1));
    assert_eq!(run(&mut b, limit(2, Buy, 90, 1)), [rejected(2, BookFull)]);
    assert_eq!(
        run(&mut b, limit_tif(2, 2, Buy, 90, 1, Ioc)),
        [Accepted { id: 2 }, cancelled(2, 1, ImmediateOrCancel)]
    );
    assert_eq!(
        run(&mut b, limit_tif(3, 3, Buy, 90, 1, Fok)),
        [Accepted { id: 3 }, cancelled(3, 1, FillOrKill)]
    );
    assert_eq!(
        run(&mut b, limit_tif(4, 4, Buy, 90, 1, PostOnly)),
        [rejected(4, BookFull)]
    );
}

#[test]
fn post_only_rests_and_never_takes_liquidity() {
    let mut b = ask_ladder();
    assert_eq!(
        run(&mut b, limit_tif(9, 9, Buy, 99, 4, PostOnly)),
        [Accepted { id: 9 }, rested(9, Buy, 99, 4)]
    );
    assert!(b.order(9).unwrap().post_only);
    // At or through the best ask it would trade, so it is refused and nothing happens.
    assert_eq!(
        run(&mut b, limit_tif(10, 10, Buy, 100, 1, PostOnly)),
        [rejected(10, PostOnlyWouldCross)]
    );
    assert_eq!(b.order_count(), 4);
}

#[test]
fn post_only_restriction_survives_modifies() {
    let mut b = ask_ladder();
    run(&mut b, limit_tif(9, 9, Buy, 98, 4, PostOnly));
    // Moving it so that it would cross is refused like a new post-only order.
    assert_eq!(
        run(&mut b, modify(9, 100, 4)),
        [rejected(9, PostOnlyWouldCross)]
    );
    assert_eq!(queue(&b, Buy, 98), [(9, 4)]);
    // A move that does not cross is fine, and the order stays post-only.
    assert_eq!(
        run(&mut b, modify(9, 99, 6)),
        [modified(9, 99, 6, 6), rested(9, Buy, 99, 6)]
    );
    assert!(b.order(9).unwrap().post_only);
    // A plain GTC order may be modified into the market.
    run(&mut b, limit(10, Buy, 97, 1));
    assert_eq!(fills(&run(&mut b, modify(10, 100, 1))), [(1, 100, 1)]);
}

// ---------------------------------------------------------------------------------------
// Icebergs

#[test]
fn an_iceberg_shows_only_its_display() {
    let mut b = book();
    assert_eq!(
        run(&mut b, iceberg(7, 1, Sell, 100, 10, 3)),
        [
            Accepted { id: 1 },
            Rested {
                id: 1,
                side: Sell,
                price: 100,
                qty: 10,
                visible: 3
            }
        ]
    );
    // Market data sees 3; the owner sees all 10.
    assert_eq!(b.best_ask(), level(100, 3, 1));
    let info = b.order(1).unwrap();
    assert_eq!((info.leaves, info.visible, info.display), (10, 3, Some(3)));
}

#[test]
fn a_used_up_tranche_is_replenished_at_the_back_of_the_queue() {
    let mut b = book();
    run(&mut b, iceberg(7, 1, Sell, 100, 10, 3));
    run(&mut b, limit(2, Sell, 100, 5));
    // The taker clears the iceberg's tranche, which then queues behind #2.
    let events = run(&mut b, limit(3, Buy, 100, 4));
    assert_eq!(fills(&events), [(1, 100, 3), (2, 100, 1)]);
    assert_eq!(events[2], replenished(1, Sell, 100, 3));
    assert_eq!(queue(&b, Sell, 100), [(2, 4), (1, 7)]);
    assert_eq!(b.best_ask(), level(100, 7, 2));
}

#[test]
fn one_taker_can_take_several_tranches() {
    let mut b = book();
    run(&mut b, iceberg(7, 1, Sell, 100, 10, 3));
    let events = run(&mut b, limit(2, Buy, 100, 8));
    assert_eq!(fills(&events), [(1, 100, 3), (1, 100, 3), (1, 100, 2)]);
    assert_eq!(events[2], replenished(1, Sell, 100, 3));
    assert_eq!(events[4], replenished(1, Sell, 100, 3));
    // The last tranche is partly filled: 1 of it still shows, 2 lots remain in total.
    let info = b.order(1).unwrap();
    assert_eq!((info.leaves, info.visible), (2, 1));
    assert_eq!(b.best_ask(), level(100, 1, 1));
    // The final tranche shows only what is left.
    let events = run(&mut b, limit(3, Buy, 100, 1));
    assert_eq!(events[2], replenished(1, Sell, 100, 1));
}

#[test]
fn an_incoming_iceberg_takes_with_its_whole_quantity() {
    let mut b = ask_ladder();
    let events = run(&mut b, iceberg(9, 9, Buy, 101, 10, 2));
    assert_eq!(fills(&events), [(1, 100, 2), (2, 101, 3)]);
    assert_eq!(
        events.last(),
        Some(&Rested {
            id: 9,
            side: Buy,
            price: 101,
            qty: 5,
            visible: 2
        })
    );
}

#[test]
fn fill_or_kill_counts_hidden_quantity_it_can_reach() {
    let mut b = book();
    run(&mut b, iceberg(7, 1, Sell, 100, 10, 3));
    assert_eq!(
        fills(&run(&mut b, limit_tif(9, 9, Buy, 100, 9, Fok))),
        [(1, 100, 3), (1, 100, 3), (1, 100, 3)]
    );
    // Under CancelIncoming a new tranche lands behind the owner's own order, which stops
    // the match: only the first tranche is in reach.
    let mut b = OrderBook::new(BookConfig {
        self_trade: SelfTradePolicy::CancelIncoming,
        ..CFG
    });
    run(&mut b, iceberg(7, 1, Sell, 100, 10, 3));
    run(&mut b, limit_by(9, 2, Sell, 100, 5));
    assert_eq!(
        run(&mut b, limit_tif(9, 9, Buy, 100, 4, Fok)),
        [Accepted { id: 9 }, cancelled(9, 4, FillOrKill)]
    );
    assert_eq!(
        fills(&run(&mut b, limit_tif(9, 10, Buy, 100, 3, Fok))),
        [(1, 100, 3)]
    );
}

#[test]
fn modifying_an_iceberg_cuts_the_hidden_part_first() {
    let mut b = book();
    run(&mut b, iceberg(7, 1, Sell, 100, 10, 3));
    run(&mut b, limit(2, Sell, 100, 1));
    // Down to 5: the 3 on display stay, priority is kept.
    assert_eq!(
        run(
            &mut b,
            Command::Modify {
                id: 1,
                owner: 7,
                price: 100,
                qty: 5
            }
        ),
        [modified(1, 100, 5, 5)]
    );
    assert_eq!(queue(&b, Sell, 100), [(1, 5), (2, 1)]);
    assert_eq!(b.order(1).unwrap().visible, 3);
    // Down to 2: below the display, so it shows 2.
    run(
        &mut b,
        Command::Modify {
            id: 1,
            owner: 7,
            price: 100,
            qty: 2,
        },
    );
    assert_eq!(b.order(1).unwrap().visible, 2);
    // Up to 12 loses priority and re-enters, still an iceberg showing 3.
    assert_eq!(
        run(
            &mut b,
            Command::Modify {
                id: 1,
                owner: 7,
                price: 100,
                qty: 12
            }
        ),
        [
            modified(1, 100, 12, 12),
            Rested {
                id: 1,
                side: Sell,
                price: 100,
                qty: 12,
                visible: 3
            }
        ]
    );
    assert_eq!(queue(&b, Sell, 100), [(2, 1), (1, 12)]);
}

#[test]
fn cancels_remove_the_hidden_part_too() {
    let mut b = book();
    run(&mut b, iceberg(7, 1, Sell, 100, 10, 3));
    assert_eq!(run(&mut b, cancel_by(7, 1)), [cancelled(1, 10, Requested)]);
    run(&mut b, iceberg(7, 1, Sell, 100, 10, 3));
    // Self-trade prevention removes the whole iceberg, not just its tranche.
    let events = run(&mut b, limit_by(7, 2, Buy, 100, 1));
    assert_eq!(events[1], cancelled(1, 10, SelfTrade));
    assert!(b.best_ask().is_none());
}

#[test]
fn invalid_displays_are_rejected() {
    let mut b = book();
    let cases = [
        iceberg(7, 1, Sell, 100, 10, 0),
        iceberg(7, 1, Sell, 100, 10, 10),
        iceberg(7, 1, Sell, 100, 10, 11),
        Command::Limit {
            id: 1,
            owner: 7,
            side: Sell,
            price: 100,
            qty: 10,
            tif: Ioc,
            display: Some(3),
        },
        Command::Limit {
            id: 1,
            owner: 7,
            side: Sell,
            price: 100,
            qty: 10,
            tif: Fok,
            display: Some(3),
        },
    ];
    for command in cases {
        assert_eq!(
            run(&mut b, command),
            [rejected(1, InvalidDisplay)],
            "{command:?}"
        );
    }
    // A post-only iceberg is fine.
    let post_only_iceberg = Command::Limit {
        id: 1,
        owner: 7,
        side: Sell,
        price: 100,
        qty: 10,
        tif: PostOnly,
        display: Some(3),
    };
    assert_eq!(run(&mut b, post_only_iceberg)[0], Accepted { id: 1 });
}

// ---------------------------------------------------------------------------------------
// Stops

fn stop(
    owner: OwnerId,
    id: OrderId,
    side: Side,
    trigger: Price,
    limit: Option<Price>,
    qty: Qty,
) -> Command {
    Command::Stop {
        id,
        owner,
        side,
        trigger,
        limit,
        qty,
    }
}

/// Asks 101 ×1 (#1), 102 ×1 (#2), 103 ×1 (#3), 104 ×5 (#4); bid 95 ×5 (#5); a trade at 100
/// sets the last price.
fn stop_book() -> OrderBook {
    let mut b = book();
    run(&mut b, limit(90, Sell, 100, 1));
    run(&mut b, limit(91, Buy, 100, 1));
    for (id, price, qty) in [(1, 101, 1), (2, 102, 1), (3, 103, 1), (4, 104, 5)] {
        run(&mut b, limit(id, Sell, price, qty));
    }
    run(&mut b, limit(5, Buy, 95, 5));
    assert_eq!(b.reference_price(), Some(100));
    b
}

#[test]
fn a_stop_waits_off_the_book() {
    let mut b = stop_book();
    assert_eq!(
        run(&mut b, stop(7, 10, Buy, 102, None, 2)),
        [
            Accepted { id: 10 },
            StopPlaced {
                id: 10,
                side: Buy,
                trigger: 102,
                limit: None,
                qty: 2
            }
        ]
    );
    // Invisible to market data, but it holds a slot and its owner can see it.
    assert_eq!(b.best_bid(), level(95, 5, 1));
    assert_eq!(b.order(10), None);
    assert_eq!(b.stop(10).map(|s| s.trigger), Some(102));
    assert_eq!(b.order_count(), 6);
}

#[test]
fn a_stop_triggers_when_a_trade_reaches_it() {
    let mut b = stop_book();
    run(&mut b, stop(7, 10, Buy, 102, None, 2));
    // A trade at 101 does not reach 102.
    assert!(!run(&mut b, limit(20, Buy, 101, 1)).contains(&Triggered { id: 10 }));
    // A trade at 102 does: the stop becomes a market buy and takes 103 and 104.
    let events = run(&mut b, limit(21, Buy, 102, 1));
    let at = events
        .iter()
        .position(|e| *e == Triggered { id: 10 })
        .unwrap();
    assert_eq!(fills(&events[at..]), [(3, 103, 1), (4, 104, 1)]);
    assert!(b.stop(10).is_none());
}

#[test]
fn any_price_the_command_traded_at_counts_not_just_the_last() {
    let mut b = stop_book();
    // With the last trade at 100, a sell stop at 101 is already reached and is refused;
    // one at 99 waits.
    assert_eq!(
        run(&mut b, stop(7, 10, Sell, 101, None, 1)),
        [rejected(10, StopWouldTrigger)]
    );
    run(&mut b, stop(7, 10, Sell, 99, None, 1));
    // A buy that trades at 99 first and then at 101 ends with the last price above the
    // trigger, but it did trade at 99, so the sell stop fires.
    run(&mut b, limit(30, Sell, 99, 1));
    let events = run(&mut b, limit(31, Buy, 101, 2));
    let at = events
        .iter()
        .position(|e| *e == Triggered { id: 10 })
        .unwrap();
    assert_eq!(fills(&events[..at]), [(30, 99, 1), (1, 101, 1)]);
    // The stop then sells into the bid at 95.
    assert_eq!(fills(&events[at..]), [(5, 95, 1)]);
}

#[test]
fn a_stop_limit_rests_what_it_cannot_fill() {
    let mut b = stop_book();
    run(&mut b, stop(7, 10, Buy, 102, Some(103), 5));
    let events = run(&mut b, limit(20, Buy, 102, 2));
    let at = events
        .iter()
        .position(|e| *e == Triggered { id: 10 })
        .unwrap();
    assert_eq!(fills(&events[at..]), [(3, 103, 1)]);
    assert_eq!(events.last(), Some(&rested(10, Buy, 103, 4)));
    let info = b.order(10).unwrap();
    assert_eq!((info.leaves, info.filled), (4, 1));
}

#[test]
fn released_stops_can_trigger_more_stops() {
    let mut b = stop_book();
    run(&mut b, stop(7, 10, Buy, 102, None, 1));
    run(&mut b, stop(8, 11, Buy, 103, None, 1));
    // The trade at 102 releases #10, which buys 103 and so releases #11, which buys 104.
    let events = run(&mut b, limit(20, Buy, 102, 2));
    let triggered: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e, Triggered { .. }))
        .collect();
    assert_eq!(triggered, [&Triggered { id: 10 }, &Triggered { id: 11 }]);
    assert_eq!(fills(&events).last(), Some(&(4, 104, 1)));
}

#[test]
fn stops_release_in_trigger_order_then_time_buy_stops_first() {
    let mut b = stop_book();
    run(&mut b, stop(7, 10, Buy, 102, None, 1));
    run(&mut b, stop(8, 11, Buy, 101, None, 1));
    run(&mut b, stop(9, 12, Buy, 101, None, 1));
    run(&mut b, stop(6, 13, Sell, 99, None, 1));
    // One command trades both at 102 and down at 95: every stop above is reached.
    run(&mut b, limit(40, Sell, 98, 1));
    let events = run(&mut b, limit(41, Buy, 102, 4));
    let order: Vec<OrderId> = events
        .iter()
        .filter_map(|e| match e {
            Triggered { id } => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(order, [11, 12, 10, 13]);
}

#[test]
fn a_triggered_stop_limit_outside_protection_is_cancelled() {
    let mut b = OrderBook::new(BookConfig {
        price_protection: Some(1),
        ..CFG
    });
    run(&mut b, limit(90, Sell, 100, 1));
    run(&mut b, limit(91, Buy, 100, 1));
    run(&mut b, limit(1, Sell, 101, 1));
    run(&mut b, limit(2, Sell, 102, 1));
    run(&mut b, stop(7, 10, Buy, 101, Some(110), 1));
    let events = run(&mut b, limit(20, Buy, 101, 1));
    // When it triggers, the best ask is 102 and its limit of 110 is 8 ticks through it.
    assert_eq!(
        &events[events.len() - 2..],
        [Triggered { id: 10 }, cancelled(10, 1, PriceProtection)]
    );
}

#[test]
fn pending_stops_can_be_cancelled_but_not_modified() {
    let mut b = stop_book();
    run(&mut b, stop(7, 10, Buy, 102, Some(103), 2));
    assert_eq!(
        run(
            &mut b,
            Command::Modify {
                id: 10,
                owner: 7,
                price: 103,
                qty: 1
            }
        ),
        [rejected(10, PendingStop)]
    );
    assert_eq!(
        run(&mut b, Command::Cancel { id: 10, owner: 8 }),
        [rejected(10, UnknownOrder)]
    );
    assert_eq!(run(&mut b, cancel_by(7, 10)), [cancelled(10, 2, Requested)]);
    // Ids are shared with resting orders: a pending stop's id is taken.
    run(&mut b, stop(7, 11, Buy, 102, None, 1));
    assert_eq!(
        run(&mut b, limit(11, Buy, 90, 1)),
        [rejected(11, DuplicateOrderId)]
    );
}

#[test]
fn mass_cancel_takes_pending_stops_after_resting_orders() {
    let mut b = stop_book();
    run(&mut b, stop(7, 10, Sell, 90, None, 1));
    run(&mut b, stop(7, 11, Buy, 103, None, 1));
    run(&mut b, stop(7, 12, Buy, 102, None, 1));
    run(&mut b, limit_by(7, 13, Buy, 94, 1));
    assert_eq!(
        run(&mut b, Command::CancelAll { owner: 7 }),
        [
            cancelled(13, 1, MassCancel),
            cancelled(12, 1, MassCancel),
            cancelled(11, 1, MassCancel),
            cancelled(10, 1, MassCancel),
            MassCancelled { owner: 7, count: 4 },
        ]
    );
}

#[test]
fn stops_hold_capacity() {
    let mut b = OrderBook::new(BookConfig::new(1, 10_000, 2));
    run(&mut b, stop(7, 1, Buy, 200, None, 1));
    run(&mut b, stop(7, 2, Sell, 50, None, 1));
    assert_eq!(
        run(&mut b, stop(7, 3, Sell, 40, None, 1)),
        [rejected(3, BookFull)]
    );
    assert_eq!(run(&mut b, limit(4, Buy, 90, 1)), [rejected(4, BookFull)]);
}

#[test]
fn without_a_last_price_any_trigger_waits() {
    let mut b = book();
    assert_eq!(
        run(&mut b, stop(7, 1, Buy, 1, None, 1))[0],
        Accepted { id: 1 }
    );
    assert_eq!(
        run(&mut b, stop(7, 2, Sell, 10_000, None, 1))[0],
        Accepted { id: 2 }
    );
}

// ---------------------------------------------------------------------------------------
// Mass cancel

#[test]
fn mass_cancel_removes_only_the_owners_orders_in_book_order() {
    let mut b = book();
    // Owner 7 places its orders in an order unrelated to book order, interleaved with
    // others; a cancel/replace also moves one of its bids to the back of its level.
    run(&mut b, limit_by(7, 1, Sell, 106, 1));
    run(&mut b, limit_by(7, 2, Buy, 99, 2));
    run(&mut b, limit_by(8, 3, Buy, 100, 3));
    run(&mut b, limit_by(7, 4, Buy, 100, 4));
    run(&mut b, limit_by(7, 5, Sell, 105, 5));
    run(&mut b, limit_by(7, 6, Buy, 100, 6));
    run(&mut b, limit_by(9, 7, Sell, 105, 7));
    run(
        &mut b,
        Command::Modify {
            id: 4,
            owner: 7,
            price: 100,
            qty: 8,
        },
    );
    assert_eq!(queue(&b, Buy, 100), [(3, 3), (6, 6), (4, 8)]);

    let events = run(&mut b, Command::CancelAll { owner: 7 });
    assert_eq!(
        events,
        [
            cancelled(6, 6, MassCancel),
            cancelled(4, 8, MassCancel),
            cancelled(2, 2, MassCancel),
            cancelled(5, 5, MassCancel),
            cancelled(1, 1, MassCancel),
            MassCancelled { owner: 7, count: 5 },
        ]
    );
    assert_eq!(queue(&b, Buy, 100), [(3, 3)]);
    assert_eq!(queue(&b, Sell, 105), [(7, 7)]);
    assert_eq!(b.order_count(), 2);
}

#[test]
fn mass_cancel_of_an_owner_without_orders_reports_zero() {
    let mut b = book();
    run(&mut b, limit_by(8, 1, Buy, 100, 1));
    assert_eq!(
        run(&mut b, Command::CancelAll { owner: 7 }),
        [MassCancelled { owner: 7, count: 0 }]
    );
    // An owner whose orders all traded away has none left either.
    run(&mut b, limit_by(7, 2, Sell, 100, 1));
    assert_eq!(
        run(&mut b, Command::CancelAll { owner: 7 }),
        [MassCancelled { owner: 7, count: 0 }]
    );
}

#[test]
fn mass_cancel_frees_capacity_and_the_owner_can_return() {
    let mut b = OrderBook::new(BookConfig::new(1, 10_000, 2));
    run(&mut b, limit_by(7, 1, Buy, 100, 1));
    run(&mut b, limit_by(7, 2, Buy, 101, 1));
    assert_eq!(
        run(&mut b, limit_by(8, 3, Buy, 99, 1)),
        [rejected(3, BookFull)]
    );
    run(&mut b, Command::CancelAll { owner: 7 });
    assert_eq!(
        run(&mut b, limit_by(8, 3, Buy, 99, 1))[0],
        Accepted { id: 3 }
    );
    assert_eq!(
        run(&mut b, limit_by(7, 1, Buy, 98, 1))[0],
        Accepted { id: 1 }
    );
    assert_eq!(
        run(&mut b, Command::CancelAll { owner: 7 }),
        [
            cancelled(1, 1, MassCancel),
            MassCancelled { owner: 7, count: 1 }
        ]
    );
}

// ---------------------------------------------------------------------------------------
// Price protection

fn protected_book() -> OrderBook {
    let mut b = OrderBook::new(BookConfig {
        price_protection: Some(5),
        ..CFG
    });
    run(&mut b, limit(1, Sell, 100, 1));
    run(&mut b, limit(2, Sell, 105, 1));
    run(&mut b, limit(3, Sell, 106, 1));
    b
}

#[test]
fn market_orders_stop_at_the_protection_limit() {
    let mut b = protected_book();
    let events = run(&mut b, market(4, Buy, 10));
    assert_eq!(fills(&events), [(1, 100, 1), (2, 105, 1)]);
    assert_eq!(events.last(), Some(&cancelled(4, 8, PriceProtection)));
    assert_eq!(b.best_ask(), level(106, 1, 1));
}

#[test]
fn limit_orders_priced_through_the_protection_are_rejected() {
    let mut b = protected_book();
    assert_eq!(
        run(&mut b, limit(4, Buy, 106, 10)),
        [rejected(4, PriceOutsideProtection)]
    );
    assert_eq!(b.order_count(), 3);
    // Exactly at the limit is fine.
    assert_eq!(fills(&run(&mut b, limit(5, Buy, 105, 1))), [(1, 100, 1)]);
}

#[test]
fn modifies_into_the_protection_band_are_rejected() {
    let mut b = protected_book();
    run(&mut b, limit(4, Buy, 90, 1));
    assert_eq!(
        run(&mut b, modify(4, 106, 1)),
        [rejected(4, PriceOutsideProtection)]
    );
    assert_eq!(queue(&b, Buy, 90), [(4, 1)]);
}

/// A 5-tick band around a reference of 100, and asks from 100 to 110.
fn banded_book() -> OrderBook {
    let mut b = OrderBook::new(BookConfig {
        price_band: Some(5),
        reference_price: Some(100),
        ..CFG
    });
    for (id, price) in (1..).zip(100..=110) {
        run(&mut b, limit(id, Sell, price, 1));
    }
    b
}

#[test]
fn limit_orders_priced_through_the_band_are_rejected() {
    let mut b = banded_book();
    assert_eq!(b.reference_price(), Some(100));
    assert_eq!(
        run(&mut b, limit(20, Buy, 106, 1)),
        [rejected(20, PriceOutsideBand)]
    );
    assert_eq!(
        run(&mut b, limit(21, Sell, 94, 1)),
        [rejected(21, PriceOutsideBand)]
    );
    // At the edge is fine, and so is anything on the passive side.
    assert_eq!(fills(&run(&mut b, limit(22, Buy, 105, 1))), [(1, 100, 1)]);
    assert_eq!(run(&mut b, limit(23, Buy, 50, 1))[0], Accepted { id: 23 });
    assert_eq!(run(&mut b, limit(24, Sell, 200, 1))[0], Accepted { id: 24 });
}

#[test]
fn the_band_follows_the_last_trade() {
    let mut b = banded_book();
    run(&mut b, limit(20, Buy, 104, 5));
    assert_eq!(b.reference_price(), Some(104));
    // 109 was 9 ticks through the old reference; it is 5 through the new one.
    assert_eq!(fills(&run(&mut b, limit(21, Buy, 109, 1))), [(6, 105, 1)]);
    // Modifies are held to the band too.
    run(&mut b, limit(22, Buy, 90, 1));
    assert_eq!(
        run(&mut b, modify(22, 111, 1)),
        [rejected(22, PriceOutsideBand)]
    );
}

#[test]
fn market_orders_stop_at_the_band() {
    let mut b = banded_book();
    let events = run(&mut b, market(20, Buy, 10));
    assert_eq!(
        fills(&events),
        [
            (1, 100, 1),
            (2, 101, 1),
            (3, 102, 1),
            (4, 103, 1),
            (5, 104, 1),
            (6, 105, 1)
        ]
    );
    assert_eq!(events.last(), Some(&cancelled(20, 4, PriceBand)));
}

#[test]
fn a_market_order_names_the_tighter_of_its_caps() {
    let book_with = |protection: u32, band: u32| {
        let mut b = OrderBook::new(BookConfig {
            price_protection: Some(protection),
            price_band: Some(band),
            reference_price: Some(100),
            ..CFG
        });
        for (id, price) in (1..).zip(100..=110) {
            run(&mut b, limit(id, Sell, price, 1));
        }
        b
    };
    let stop = |mut b: OrderBook| *run(&mut b, market(20, Buy, 20)).last().unwrap();
    assert_eq!(stop(book_with(2, 4)), cancelled(20, 17, PriceProtection));
    assert_eq!(stop(book_with(4, 2)), cancelled(20, 17, PriceBand));
    // A tie goes to price protection.
    assert_eq!(stop(book_with(3, 3)), cancelled(20, 16, PriceProtection));
}

#[test]
fn without_a_reference_the_band_waits_for_the_first_trade() {
    let mut b = OrderBook::new(BookConfig {
        price_band: Some(5),
        ..CFG
    });
    assert_eq!(b.reference_price(), None);
    run(&mut b, limit(1, Sell, 500, 1));
    assert_eq!(fills(&run(&mut b, limit(2, Buy, 900, 1))), [(1, 500, 1)]);
    assert_eq!(b.reference_price(), Some(500));
    assert_eq!(
        run(&mut b, limit(3, Buy, 506, 1)),
        [rejected(3, PriceOutsideBand)]
    );
}

#[test]
#[should_panic(expected = "reference_price outside the price band")]
fn a_reference_price_outside_the_band_is_refused() {
    OrderBook::new(BookConfig {
        reference_price: Some(10_001),
        ..CFG
    });
}

#[test]
fn protection_needs_an_opposite_price_to_measure_from() {
    let mut b = OrderBook::new(BookConfig {
        price_protection: Some(5),
        ..CFG
    });
    assert_eq!(
        run(&mut b, limit(1, Buy, 9_000, 1)),
        [Accepted { id: 1 }, rested(1, Buy, 9_000, 1)]
    );
}

// ---------------------------------------------------------------------------------------
// Trading phases and auctions

fn set_phase(phase: Phase) -> Command {
    Command::SetPhase { phase }
}

/// A book built from `cfg`, in a call phase.
fn call_with(cfg: BookConfig) -> OrderBook {
    let mut b = OrderBook::new(cfg);
    assert_eq!(
        run(&mut b, set_phase(Phase::Auction)),
        [PhaseChanged {
            phase: Phase::Auction
        }]
    );
    b
}

fn call() -> OrderBook {
    call_with(CFG)
}

/// An uncross trade: the buy order is reported as the taker.
fn crossing(
    trade_id: u64,
    buy: OrderId,
    sell: OrderId,
    price: Price,
    qty: Qty,
    buy_leaves: Qty,
    sell_leaves: Qty,
) -> Event {
    Trade {
        trade_id,
        taker: buy,
        maker: sell,
        taker_side: Buy,
        price,
        qty,
        taker_leaves: buy_leaves,
        maker_leaves: sell_leaves,
    }
}

fn opens() -> Event {
    PhaseChanged {
        phase: Phase::Continuous,
    }
}

#[test]
fn a_call_collects_orders_without_matching() {
    let mut b = call();
    run(&mut b, limit(1, Sell, 100, 5));
    assert_eq!(
        run(&mut b, limit(2, Buy, 105, 3)),
        [Accepted { id: 2 }, rested(2, Buy, 105, 3)]
    );
    // The book is crossed, and nothing has traded.
    assert_eq!(b.best_bid(), level(105, 3, 1));
    assert_eq!(b.best_ask(), level(100, 5, 1));
    assert_eq!(b.trade_count(), 0);
    // What the uncross would do, as market data publishes it during the call.
    assert_eq!(b.indicative_uncross(), Some((100, 3)));
}

#[test]
fn orders_that_must_trade_at_once_are_refused_in_a_call() {
    let mut b = call();
    run(&mut b, limit(1, Sell, 100, 5));
    assert_eq!(run(&mut b, market(2, Buy, 1)), [rejected(2, AuctionCall)]);
    for tif in [TimeInForce::Ioc, TimeInForce::Fok] {
        assert_eq!(
            run(&mut b, limit_tif(3, 3, Buy, 100, 1, tif)),
            [rejected(3, AuctionCall)]
        );
    }
    // Post-only keeps its promise not to cross; it rests if it does not.
    let post_only = |price| limit_tif(4, 4, Buy, price, 1, TimeInForce::PostOnly);
    assert_eq!(
        run(&mut b, post_only(100)),
        [rejected(4, PostOnlyWouldCross)]
    );
    assert_eq!(run(&mut b, post_only(99))[1], rested(4, Buy, 99, 1));
    // Stops wait for a trade as always.
    assert_eq!(
        run(&mut b, stop(5, 5, Buy, 110, None, 1))[0],
        Accepted { id: 5 }
    );
}

#[test]
fn price_controls_do_not_apply_in_a_call() {
    // Protection and the band guard against what an order trades on arrival; in a call
    // nothing does, and the uncross is what finds the new price.
    let cfg = BookConfig {
        price_protection: Some(1),
        price_band: Some(1),
        reference_price: Some(100),
        ..CFG
    };
    let mut b = call_with(cfg);
    run(&mut b, limit(1, Sell, 100, 5));
    assert_eq!(
        run(&mut b, limit(2, Buy, 150, 1))[1],
        rested(2, Buy, 150, 1)
    );
    assert_eq!(
        run(&mut b, limit(3, Sell, 50, 1))[1],
        rested(3, Sell, 50, 1)
    );
    assert_eq!(
        run(&mut b, modify(2, 160, 2)),
        [modified(2, 160, 2, 2), rested(2, Buy, 160, 2)]
    );
}

#[test]
fn a_full_book_refuses_even_crossing_orders_in_a_call() {
    // A crossing order frees a slot by trading, but nothing trades in a call.
    let mut b = call_with(BookConfig::new(1, 10_000, 2));
    run(&mut b, limit(1, Sell, 100, 1));
    run(&mut b, limit(2, Buy, 99, 1));
    assert_eq!(run(&mut b, limit(3, Buy, 101, 1)), [rejected(3, BookFull)]);
}

#[test]
fn modifies_in_a_call_rest_again_without_trading() {
    let mut b = call();
    run(&mut b, limit(1, Sell, 100, 2));
    run(&mut b, limit(2, Buy, 98, 1));
    run(&mut b, limit(3, Buy, 98, 2));
    assert_eq!(
        run(&mut b, modify(2, 101, 1)),
        [modified(2, 101, 1, 1), rested(2, Buy, 101, 1)]
    );
    // Shrinking in place keeps priority, as in continuous trading.
    run(&mut b, limit(4, Buy, 98, 1));
    assert_eq!(run(&mut b, modify(3, 98, 1)), [modified(3, 98, 1, 1)]);
    assert_eq!(queue(&b, Buy, 98), [(3, 1), (4, 1)]);
}

#[test]
fn leaving_a_call_uncrosses_at_one_price_in_priority_order() {
    let mut b = call();
    for (id, side, price, qty) in [
        (1, Buy, 102, 3),
        (2, Buy, 101, 4),
        (3, Buy, 99, 5),
        (4, Sell, 98, 2),
        (5, Sell, 100, 4),
        (6, Sell, 101, 3),
    ] {
        run(&mut b, limit(id, side, price, qty));
    }
    // At 101, 7 lots bid at or above meet 9 offered at or below: more than at any other
    // price. Every bid that crosses fills; the last ask reached, #6, fills in part.
    assert_eq!(b.indicative_uncross(), Some((101, 7)));
    assert_eq!(
        run(&mut b, set_phase(Phase::Continuous)),
        [
            crossing(1, 1, 4, 101, 2, 1, 0),
            crossing(2, 1, 5, 101, 1, 0, 3),
            crossing(3, 2, 5, 101, 3, 1, 0),
            crossing(4, 2, 6, 101, 1, 0, 2),
            opens(),
        ]
    );
    assert_eq!(b.best_bid(), level(99, 5, 1));
    assert_eq!(b.best_ask(), level(101, 2, 1));
    assert_eq!(b.reference_price(), Some(101));
    assert_eq!(b.indicative_uncross(), None);
}

/// The book after a call with `orders`, uncrossed into continuous trading: the events.
fn uncross(cfg: BookConfig, orders: &[(OrderId, Side, Price, Qty)]) -> Vec<Event> {
    let mut b = call_with(cfg);
    for &(id, side, price, qty) in orders {
        run(&mut b, limit(id, side, price, qty));
    }
    run(&mut b, set_phase(Phase::Continuous))
}

#[test]
fn the_auction_price_leaves_the_least_surplus_among_equal_volumes() {
    // 4 lots trade at 100 and at 102 alike; 100 leaves 2 bid lots over, 102 only one
    // offered lot.
    let orders = [
        (1, Buy, 102, 4),
        (2, Buy, 100, 2),
        (3, Sell, 100, 4),
        (4, Sell, 102, 1),
    ];
    assert_eq!(
        uncross(CFG, &orders),
        [crossing(1, 1, 3, 102, 4, 0, 0), opens()]
    );
}

#[test]
fn market_pressure_moves_the_auction_price_toward_the_surplus() {
    // 3 lots trade at 100 and at 102 alike, leaving the same surplus on the same side.
    let buyers_left = [(1, Buy, 102, 5), (2, Sell, 100, 3)];
    assert_eq!(
        uncross(CFG, &buyers_left),
        [crossing(1, 1, 2, 102, 3, 2, 0), opens()]
    );
    let sellers_left = [(1, Buy, 102, 3), (2, Sell, 100, 5)];
    assert_eq!(
        uncross(CFG, &sellers_left),
        [crossing(1, 1, 2, 100, 3, 0, 2), opens()]
    );
}

#[test]
fn without_pressure_the_price_closest_to_the_reference_wins() {
    // 3 lots trade at any price from 100 to 102, leaving nothing over.
    let orders = [(1, Buy, 102, 3), (2, Sell, 100, 3)];
    let price_with = |reference| {
        let cfg = BookConfig {
            reference_price: reference,
            ..CFG
        };
        match uncross(cfg, &orders)[0] {
            Trade { price, .. } => price,
            other => panic!("expected a trade, got {other:?}"),
        }
    };
    // The reference itself, when it is among the best prices...
    assert_eq!(price_with(Some(101)), 101);
    // ...otherwise the best price closest to it...
    assert_eq!(price_with(Some(90)), 100);
    assert_eq!(price_with(Some(110)), 102);
    // ...and without a reference, the lowest.
    assert_eq!(price_with(None), 100);
}

#[test]
fn the_uncross_takes_icebergs_one_tranche_at_a_time() {
    let mut b = call();
    run(&mut b, iceberg(1, 1, Buy, 101, 4, 2));
    run(&mut b, limit(2, Sell, 100, 3));
    run(&mut b, limit(3, Sell, 101, 1));
    // Hidden quantity counts: 4 lots trade at 101.
    assert_eq!(b.indicative_uncross(), Some((101, 4)));
    assert_eq!(
        run(&mut b, set_phase(Phase::Continuous)),
        [
            crossing(1, 1, 2, 101, 2, 2, 1),
            replenished(1, Buy, 101, 2),
            crossing(2, 1, 2, 101, 1, 1, 0),
            crossing(3, 1, 3, 101, 1, 0, 0),
            opens(),
        ]
    );
    // When one trade uses up both tranches, the buy order replenishes first.
    let mut b = call();
    run(&mut b, iceberg(1, 1, Buy, 100, 4, 2));
    run(&mut b, iceberg(2, 2, Sell, 100, 4, 2));
    assert_eq!(
        run(&mut b, set_phase(Phase::Continuous)),
        [
            crossing(1, 1, 2, 100, 2, 2, 2),
            replenished(1, Buy, 100, 2),
            replenished(2, Sell, 100, 2),
            crossing(2, 1, 2, 100, 2, 0, 0),
            opens(),
        ]
    );
}

#[test]
fn the_uncross_does_not_prevent_self_trades() {
    // Removing either order would leave the other crossed at a price the uncross does not
    // use, so an owner trades with itself at the auction price, under either policy.
    for policy in [
        SelfTradePolicy::CancelResting,
        SelfTradePolicy::CancelIncoming,
    ] {
        let cfg = BookConfig {
            self_trade: policy,
            ..CFG
        };
        let mut b = call_with(cfg);
        run(&mut b, limit_by(7, 1, Buy, 101, 2));
        run(&mut b, limit_by(7, 2, Sell, 100, 2));
        assert_eq!(
            run(&mut b, set_phase(Phase::Continuous)),
            [crossing(1, 1, 2, 100, 2, 0, 0), opens()]
        );
    }
}

#[test]
fn a_book_that_does_not_cross_leaves_the_call_without_trading() {
    let mut b = call();
    run(&mut b, limit(1, Buy, 99, 1));
    run(&mut b, limit(2, Sell, 100, 1));
    assert_eq!(b.indicative_uncross(), None);
    assert_eq!(run(&mut b, set_phase(Phase::Continuous)), [opens()]);
    assert_eq!(b.order_count(), 2);
}

#[test]
fn setting_the_current_phase_again_changes_nothing() {
    let mut b = call();
    run(&mut b, limit(1, Buy, 101, 1));
    run(&mut b, limit(2, Sell, 100, 1));
    assert_eq!(
        run(&mut b, set_phase(Phase::Auction)),
        [PhaseChanged {
            phase: Phase::Auction
        }]
    );
    assert_eq!(b.trade_count(), 0);
    assert_eq!(b.phase(), Phase::Auction);
}

#[test]
fn every_way_out_of_a_call_uncrosses() {
    for phase in [Phase::Halted, Phase::Closed] {
        let mut b = call();
        run(&mut b, limit(1, Buy, 101, 1));
        run(&mut b, limit(2, Sell, 100, 1));
        assert_eq!(
            run(&mut b, set_phase(phase)),
            [crossing(1, 1, 2, 100, 1, 0, 0), PhaseChanged { phase }]
        );
    }
}

#[test]
fn the_auction_price_re_anchors_the_band() {
    let cfg = BookConfig {
        price_band: Some(2),
        reference_price: Some(100),
        ..CFG
    };
    let mut b = call_with(cfg);
    run(&mut b, limit(1, Buy, 110, 1));
    run(&mut b, limit(2, Sell, 110, 1));
    run(&mut b, set_phase(Phase::Continuous));
    assert_eq!(b.reference_price(), Some(110));
    assert_eq!(run(&mut b, limit(3, Buy, 112, 1))[0], Accepted { id: 3 });
    assert_eq!(
        run(&mut b, limit(4, Buy, 113, 1)),
        [rejected(4, PriceOutsideBand)]
    );
}

#[test]
fn a_halt_accepts_only_cancels_and_keeps_pending_stops() {
    let mut b = stop_book();
    run(&mut b, stop(7, 10, Buy, 102, None, 1));
    run(&mut b, limit_by(7, 11, Buy, 90, 1));
    assert_eq!(
        run(&mut b, set_phase(Phase::Halted)),
        [PhaseChanged {
            phase: Phase::Halted
        }]
    );
    assert_eq!(
        run(&mut b, limit(20, Buy, 101, 1)),
        [rejected(20, TradingHalted)]
    );
    assert_eq!(
        run(&mut b, market(21, Buy, 1)),
        [rejected(21, TradingHalted)]
    );
    assert_eq!(
        run(&mut b, stop(7, 22, Buy, 110, None, 1)),
        [rejected(22, TradingHalted)]
    );
    assert_eq!(
        run(
            &mut b,
            Command::Modify {
                id: 11,
                owner: 7,
                price: 90,
                qty: 1
            }
        ),
        [rejected(11, TradingHalted)]
    );
    assert_eq!(run(&mut b, cancel(5)), [cancelled(5, 5, Requested)]);
    // Nothing trades, so the stop just waits, and trading resumes where it stopped.
    assert!(b.stop(10).is_some());
    run(&mut b, set_phase(Phase::Continuous));
    let events = run(&mut b, limit(23, Buy, 102, 2));
    assert!(events.contains(&Triggered { id: 10 }));
    assert_eq!(
        run(&mut b, Command::CancelAll { owner: 7 }),
        [
            cancelled(11, 1, MassCancel),
            MassCancelled { owner: 7, count: 1 }
        ]
    );
}

#[test]
fn the_close_accepts_only_cancels() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 99, 1));
    run(&mut b, set_phase(Phase::Closed));
    assert_eq!(
        run(&mut b, limit(2, Buy, 98, 1)),
        [rejected(2, MarketClosed)]
    );
    assert_eq!(run(&mut b, market(3, Sell, 1)), [rejected(3, MarketClosed)]);
    assert_eq!(run(&mut b, modify(1, 98, 1)), [rejected(1, MarketClosed)]);
    assert_eq!(
        run(&mut b, stop(4, 4, Sell, 90, None, 1)),
        [rejected(4, MarketClosed)]
    );
    assert_eq!(run(&mut b, cancel(1)), [cancelled(1, 1, Requested)]);
}

#[test]
fn stops_the_uncross_reaches_trigger_once_the_new_phase_is_in_force() {
    // The last trade was at 100; the uncross trades at 102 and reaches the stop, which then
    // buys in continuous trading.
    let mut b = stop_book();
    run(&mut b, stop(7, 10, Buy, 102, None, 2));
    run(&mut b, set_phase(Phase::Auction));
    run(&mut b, limit(20, Buy, 102, 2));
    let bought = |trade_id, maker, price, leaves, maker_leaves| Trade {
        trade_id,
        taker: 10,
        maker,
        taker_side: Buy,
        price,
        qty: 1,
        taker_leaves: leaves,
        maker_leaves,
    };
    assert_eq!(
        run(&mut b, set_phase(Phase::Continuous)),
        [
            crossing(2, 20, 1, 102, 1, 1, 0),
            crossing(3, 20, 2, 102, 1, 0, 0),
            opens(),
            Triggered { id: 10 },
            bought(4, 3, 103, 1, 0),
            bought(5, 4, 104, 0, 4),
        ]
    );
}

#[test]
fn stops_triggered_where_their_orders_cannot_work_are_cancelled() {
    for phase in [Phase::Halted, Phase::Closed] {
        let mut b = stop_book();
        run(&mut b, stop(7, 10, Buy, 101, None, 2));
        run(&mut b, stop(7, 11, Buy, 101, Some(103), 2));
        run(&mut b, set_phase(Phase::Auction));
        run(&mut b, limit(20, Buy, 101, 1));
        assert_eq!(
            run(&mut b, set_phase(phase)),
            [
                crossing(2, 20, 1, 101, 1, 0, 0),
                PhaseChanged { phase },
                Triggered { id: 10 },
                cancelled(10, 2, TradingPhase),
                Triggered { id: 11 },
                cancelled(11, 2, TradingPhase),
            ]
        );
    }
}

/// A 2-tick band around 100 that interrupts trading when it stops a market order; asks at
/// 101 and 103.
fn interrupting_book() -> OrderBook {
    let mut b = OrderBook::new(BookConfig {
        price_band: Some(2),
        reference_price: Some(100),
        auction_on_band: true,
        ..CFG
    });
    run(&mut b, limit(1, Sell, 101, 1));
    run(&mut b, limit(2, Sell, 103, 1));
    b
}

#[test]
fn a_market_order_the_band_stops_starts_a_call() {
    let mut b = interrupting_book();
    assert_eq!(
        run(&mut b, market(3, Buy, 5)),
        [
            Accepted { id: 3 },
            Trade {
                trade_id: 1,
                taker: 3,
                maker: 1,
                taker_side: Buy,
                price: 101,
                qty: 1,
                taker_leaves: 4,
                maker_leaves: 0
            },
            cancelled(3, 4, PriceBand),
            PhaseChanged {
                phase: Phase::Auction
            },
        ]
    );
    // The reopening auction may trade beyond the old band and re-anchors it.
    run(&mut b, limit(4, Buy, 103, 1));
    run(&mut b, set_phase(Phase::Continuous));
    assert_eq!(b.reference_price(), Some(103));
}

#[test]
fn only_the_band_starts_a_call() {
    // The protection is tighter here, so it stops the order, and trading goes on.
    let mut b = OrderBook::new(BookConfig {
        price_protection: Some(0),
        price_band: Some(2),
        reference_price: Some(100),
        auction_on_band: true,
        ..CFG
    });
    run(&mut b, limit(1, Sell, 101, 1));
    run(&mut b, limit(2, Sell, 103, 1));
    let events = run(&mut b, market(3, Buy, 5));
    assert_eq!(events.last(), Some(&cancelled(3, 4, PriceProtection)));
    assert_eq!(b.phase(), Phase::Continuous);
}

#[test]
fn stops_released_into_a_call_rest_or_are_cancelled() {
    // The market order's trade at 101 reaches both stops, but the band then interrupts
    // trading: the stop-limit rests in the call, crossing the ask at 103, and the
    // stop-market cannot work.
    let mut b = interrupting_book();
    run(&mut b, stop(7, 10, Buy, 101, Some(104), 1));
    run(&mut b, stop(7, 11, Buy, 101, None, 1));
    let events = run(&mut b, market(3, Buy, 5));
    assert_eq!(
        &events[4..],
        [
            Triggered { id: 10 },
            rested(10, Buy, 104, 1),
            Triggered { id: 11 },
            cancelled(11, 1, TradingPhase),
        ]
    );
    assert_eq!(b.best_bid(), level(104, 1, 1));
    assert_eq!(b.best_ask(), level(103, 1, 1));
}

// ---------------------------------------------------------------------------------------
// API

#[test]
fn commands_report_their_id_and_owner() {
    let commands = [
        limit_by(7, 1, Buy, 100, 1),
        market_by(7, 1, Buy, 1),
        Command::Cancel { id: 1, owner: 7 },
        Command::Modify {
            id: 1,
            owner: 7,
            price: 100,
            qty: 1,
        },
    ];
    for command in commands {
        assert_eq!(
            (command.id(), command.owner()),
            (Some(1), Some(7)),
            "{command:?}"
        );
    }
    let mass_cancel = Command::CancelAll { owner: 7 };
    assert_eq!((mass_cancel.id(), mass_cancel.owner()), (None, Some(7)));
    let phase_change = Command::SetPhase {
        phase: Phase::Halted,
    };
    assert_eq!((phase_change.id(), phase_change.owner()), (None, None));
}

#[test]
fn events_can_go_to_a_trait_object() {
    let mut b = book();
    let mut events = Vec::new();
    let mut sink: &mut dyn EventSink = &mut events;
    b.process(limit(1, Buy, 100, 1), &mut sink);
    assert_eq!(events, [Accepted { id: 1 }, rested(1, Buy, 100, 1)]);
}

#[test]
fn side_opposite() {
    assert_eq!(Buy.opposite(), Sell);
    assert_eq!(Sell.opposite(), Buy);
}

// ---------------------------------------------------------------------------------------
// Ladder edges

#[test]
fn levels_across_bitset_word_boundaries_and_band_edges() {
    // Level index = price - 1, so these hit indices 0, 63, 64, 127, 4095, 4096 and 9999:
    // both ends of the band and both kinds of bitset boundary.
    let prices = [1, 64, 65, 128, 4096, 4097, 10_000];
    let mut b = book();
    for (id, &price) in (1..).zip(&prices) {
        run(&mut b, limit(id, Buy, price, 1));
    }
    let depth: Vec<Price> = b.depth(Buy).map(|l| l.price).collect();
    assert_eq!(depth, prices.iter().rev().copied().collect::<Vec<_>>());

    let trade_prices: Vec<Price> = fills(&run(&mut b, market(100, Sell, 7)))
        .iter()
        .map(|&(_, price, _)| price)
        .collect();
    assert_eq!(trade_prices, depth);
    assert!(b.best_bid().is_none());

    for (id, &price) in (1..).zip(&prices) {
        run(&mut b, limit(id, Sell, price, 1));
    }
    let depth: Vec<Price> = b.depth(Sell).map(|l| l.price).collect();
    assert_eq!(depth, prices);
}

#[test]
fn negative_prices_work() {
    let mut b = OrderBook::new(BookConfig::new(-100, 100, 16));
    run(&mut b, limit(1, Sell, -37, 4));
    let events = run(&mut b, limit(2, Buy, -30, 4));
    assert_eq!(fills(&events), [(1, -37, 4)]);
}

/// An iceberg whose tranche runs out in an uncross shows its next one at its own price,
/// which need not be the auction price.
#[test]
fn an_iceberg_replenished_in_an_uncross_shows_at_its_own_price() {
    let mut b = call();
    run(&mut b, iceberg(1, 1, Buy, 105, 10, 2));
    // Most volume executes at 100: 20 against 30 bid, against 10 at 105.
    run(&mut b, limit(3, Buy, 100, 20));
    run(&mut b, limit(2, Sell, 100, 20));
    let events = run(&mut b, set_phase(Phase::Continuous));
    let price = events
        .iter()
        .find_map(|e| match e {
            Trade { price, .. } => Some(*price),
            _ => None,
        })
        .expect("the uncross trades");
    let replenished_at: Vec<Price> = events
        .iter()
        .filter_map(|e| match e {
            Replenished { price, .. } => Some(*price),
            _ => None,
        })
        .collect();
    assert!(!replenished_at.is_empty(), "{events:?}");
    assert_eq!(price, 100);
    assert!(
        replenished_at.iter().all(|&p| p == 105),
        "auction at {price}, replenished at {replenished_at:?}"
    );
}
