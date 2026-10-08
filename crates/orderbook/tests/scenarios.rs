//! Hand-written scenarios, one rule of the matching semantics per test, asserting the exact
//! event sequence.
//!
//! Unless a test is about owners, every order gets its own owner (`owner == id`), so
//! self-trade prevention stays out of the way.

use orderbook::CancelReason::{MassCancel, NoLiquidity, PriceProtection, Requested, SelfTrade};
use orderbook::Event::*;
use orderbook::RejectReason::*;
use orderbook::Side::{Buy, Sell};
use orderbook::{
    BookConfig, Command, Event, EventSink, LevelInfo, OrderBook, OrderId, OwnerId, Price, Qty,
    SelfTradePolicy, Side,
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
    Command::Limit {
        id,
        owner,
        side,
        price,
        qty,
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
    Command::Cancel {
        id,
        owner: id as OwnerId,
    }
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
        assert_eq!((command.id(), command.owner()), (Some(1), 7), "{command:?}");
    }
    let mass_cancel = Command::CancelAll { owner: 7 };
    assert_eq!((mass_cancel.id(), mass_cancel.owner()), (None, 7));
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
