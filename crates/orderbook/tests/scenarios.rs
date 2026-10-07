//! Hand-written scenarios, one rule of the matching semantics per test, asserting the exact
//! event sequence.

use orderbook::CancelReason::{NoLiquidity, Requested};
use orderbook::Event::*;
use orderbook::RejectReason::*;
use orderbook::Side::{Buy, Sell};
use orderbook::{BookConfig, Command, Event, LevelInfo, OrderBook, OrderId, Price, Qty, Side};

const CFG: BookConfig = BookConfig {
    min_price: 1,
    max_price: 10_000,
    max_orders: 1_024,
};

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
    Command::Limit {
        id,
        side,
        price,
        qty,
    }
}

fn market(id: OrderId, side: Side, qty: Qty) -> Command {
    Command::Market { id, side, qty }
}

fn cancel(id: OrderId) -> Command {
    Command::Cancel { id }
}

fn modify(id: OrderId, price: Price, qty: Qty) -> Command {
    Command::Modify { id, price, qty }
}

fn trade(taker: OrderId, maker: OrderId, taker_side: Side, price: Price, qty: Qty) -> Event {
    Trade {
        taker,
        maker,
        taker_side,
        price,
        qty,
    }
}

fn level(price: Price, qty: Qty, orders: u32) -> Option<LevelInfo> {
    Some(LevelInfo { price, qty, orders })
}

fn queue(book: &OrderBook, side: Side, price: Price) -> Vec<(OrderId, Qty)> {
    book.queue(side, price).collect()
}

#[test]
fn non_crossing_orders_rest_and_set_the_touch() {
    let mut b = book();
    assert_eq!(
        run(&mut b, limit(1, Buy, 99, 10)),
        [
            Accepted { id: 1 },
            Rested {
                id: 1,
                side: Buy,
                price: 99,
                qty: 10
            }
        ]
    );
    run(&mut b, limit(2, Buy, 98, 5));
    run(&mut b, limit(3, Sell, 101, 7));
    run(&mut b, limit(4, Sell, 102, 1));
    assert_eq!(b.best_bid(), level(99, 10, 1));
    assert_eq!(b.best_ask(), level(101, 7, 1));
    assert_eq!(b.order_count(), 4);
}

#[test]
fn better_price_trades_first() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 102, 5));
    run(&mut b, limit(2, Sell, 101, 5));
    assert_eq!(
        run(&mut b, limit(3, Buy, 102, 5)),
        [Accepted { id: 3 }, trade(3, 2, Buy, 101, 5)]
    );
    assert_eq!(b.best_ask(), level(102, 5, 1));
}

#[test]
fn same_price_fills_in_arrival_order() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 3));
    run(&mut b, limit(2, Buy, 100, 3));
    run(&mut b, limit(3, Buy, 100, 3));
    assert_eq!(
        run(&mut b, limit(4, Sell, 100, 7)),
        [
            Accepted { id: 4 },
            trade(4, 1, Sell, 100, 3),
            trade(4, 2, Sell, 100, 3),
            trade(4, 3, Sell, 100, 1),
        ]
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
}

#[test]
fn sweep_trades_at_each_makers_price_and_rests_the_remainder() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 101, 2));
    run(&mut b, limit(2, Sell, 102, 2));
    run(&mut b, limit(3, Sell, 103, 2));
    run(&mut b, limit(4, Sell, 105, 2));
    assert_eq!(
        run(&mut b, limit(5, Buy, 103, 10)),
        [
            Accepted { id: 5 },
            trade(5, 1, Buy, 101, 2),
            trade(5, 2, Buy, 102, 2),
            trade(5, 3, Buy, 103, 2),
            Rested {
                id: 5,
                side: Buy,
                price: 103,
                qty: 4
            },
        ]
    );
    assert_eq!(b.best_bid(), level(103, 4, 1));
    assert_eq!(b.best_ask(), level(105, 2, 1));
}

#[test]
fn limit_never_trades_through_its_price() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 5));
    assert_eq!(
        run(&mut b, limit(2, Sell, 101, 5)),
        [
            Accepted { id: 2 },
            Rested {
                id: 2,
                side: Sell,
                price: 101,
                qty: 5
            }
        ]
    );
    assert_eq!(b.order_count(), 2);
}

#[test]
fn market_order_takes_liquidity_and_cancels_the_rest() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 3));
    run(&mut b, limit(2, Buy, 95, 3));
    assert_eq!(
        run(&mut b, market(3, Sell, 10)),
        [
            Accepted { id: 3 },
            trade(3, 1, Sell, 100, 3),
            trade(3, 2, Sell, 95, 3),
            Cancelled {
                id: 3,
                qty: 4,
                reason: NoLiquidity
            },
        ]
    );
    assert!(b.order_count() == 0 && b.best_bid().is_none());
}

#[test]
fn market_order_on_an_empty_side_is_cancelled() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 3));
    assert_eq!(
        run(&mut b, market(2, Buy, 5)),
        [
            Accepted { id: 2 },
            Cancelled {
                id: 2,
                qty: 5,
                reason: NoLiquidity
            }
        ]
    );
    assert_eq!(b.best_bid(), level(100, 3, 1));
}

#[test]
fn cancel_from_the_middle_of_a_queue_keeps_the_others_in_order() {
    let mut b = book();
    for id in 1..=4 {
        run(&mut b, limit(id, Sell, 100, id));
    }
    assert_eq!(
        run(&mut b, cancel(2)),
        [Cancelled {
            id: 2,
            qty: 2,
            reason: Requested
        }]
    );
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
    assert_eq!(
        run(&mut b, cancel(9)),
        [Rejected {
            id: 9,
            reason: UnknownOrder
        }]
    );
    run(&mut b, limit(1, Buy, 100, 1));
    run(&mut b, cancel(1));
    assert_eq!(
        run(&mut b, cancel(1)),
        [Rejected {
            id: 1,
            reason: UnknownOrder
        }]
    );
}

#[test]
fn reducing_size_at_the_same_price_keeps_priority() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 10));
    run(&mut b, limit(2, Buy, 100, 10));
    assert_eq!(
        run(&mut b, modify(1, 100, 4)),
        [Modified {
            id: 1,
            price: 100,
            qty: 4
        }]
    );
    assert_eq!(queue(&b, Buy, 100), [(1, 4), (2, 10)]);
    assert_eq!(b.best_bid(), level(100, 14, 2));
}

#[test]
fn increasing_size_loses_priority() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 10));
    run(&mut b, limit(2, Buy, 100, 10));
    assert_eq!(
        run(&mut b, modify(1, 100, 11)),
        [
            Modified {
                id: 1,
                price: 100,
                qty: 11
            },
            Rested {
                id: 1,
                side: Buy,
                price: 100,
                qty: 11
            }
        ]
    );
    assert_eq!(queue(&b, Buy, 100), [(2, 10), (1, 11)]);
}

#[test]
fn moving_the_price_loses_priority_and_can_trade() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 105, 3));
    run(&mut b, limit(2, Buy, 100, 5));
    run(&mut b, limit(3, Buy, 101, 5));
    assert_eq!(
        run(&mut b, modify(2, 105, 5)),
        [
            Modified {
                id: 2,
                price: 105,
                qty: 5
            },
            trade(2, 1, Buy, 105, 3),
            Rested {
                id: 2,
                side: Buy,
                price: 105,
                qty: 2
            },
        ]
    );
    assert_eq!(b.best_bid(), level(105, 2, 1));
    assert!(b.best_ask().is_none());
}

#[test]
fn invalid_commands_are_rejected_without_side_effects() {
    let mut b = book();
    run(&mut b, limit(1, Buy, 100, 5));
    let cases = [
        (limit(2, Buy, 100, 0), InvalidQuantity),
        (limit(2, Buy, 0, 1), PriceOutOfRange),
        (limit(2, Sell, 10_001, 1), PriceOutOfRange),
        (limit(2, Sell, Price::MIN, 1), PriceOutOfRange),
        (limit(1, Sell, 100, 1), DuplicateOrderId),
        (market(2, Sell, 0), InvalidQuantity),
        (market(1, Sell, 1), DuplicateOrderId),
        (modify(1, 100, 0), InvalidQuantity),
        (modify(1, 10_001, 1), PriceOutOfRange),
        (modify(7, 100, 1), UnknownOrder),
    ];
    for (command, reason) in cases {
        assert_eq!(
            run(&mut b, command),
            [Rejected {
                id: command.id(),
                reason
            }],
            "{command:?}"
        );
    }
    assert_eq!(queue(&b, Buy, 100), [(1, 5)]);
    assert_eq!(b.order_count(), 1);
}

#[test]
fn full_book_rejects_new_limits_but_still_trades_and_cancels() {
    let mut b = OrderBook::new(BookConfig {
        max_orders: 2,
        ..CFG
    });
    run(&mut b, limit(1, Buy, 100, 5));
    run(&mut b, limit(2, Sell, 110, 5));
    assert_eq!(
        run(&mut b, limit(3, Buy, 99, 1)),
        [Rejected {
            id: 3,
            reason: BookFull
        }]
    );
    assert_eq!(
        run(&mut b, market(4, Buy, 2)),
        [Accepted { id: 4 }, trade(4, 2, Buy, 110, 2)]
    );
    run(&mut b, cancel(1));
    assert_eq!(run(&mut b, limit(3, Buy, 99, 1))[0], Accepted { id: 3 });
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

    let events = run(&mut b, market(100, Sell, 7));
    let trade_prices: Vec<Price> = events
        .iter()
        .filter_map(|e| match e {
            Trade { price, .. } => Some(*price),
            _ => None,
        })
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
fn order_lookup_reports_open_quantity() {
    let mut b = book();
    run(&mut b, limit(1, Sell, 100, 10));
    run(&mut b, market(2, Buy, 4));
    let info = b.order(1).unwrap();
    assert_eq!((info.side, info.price, info.qty), (Sell, 100, 6));
    assert!(b.order(2).is_none());
}
