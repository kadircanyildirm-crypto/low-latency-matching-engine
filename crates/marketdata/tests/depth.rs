//! The depth kept from the events against the book's own, after every command of flows that
//! use every command kind: limits of every time in force and icebergs, market orders,
//! cancels, modifies, stops, mass cancels, and calls that end in an uncross.

use std::collections::BTreeMap;

use marketdata::{Depth, Level};
use orderbook::workload::{Mix, TifMix, Workload, WorkloadConfig};
use orderbook::{BookConfig, OrderBook, Price, Side};

/// The book's depth of one side, best price first.
fn book_levels(book: &OrderBook, side: Side) -> Vec<(Price, Level)> {
    book.depth(side)
        .map(|level| {
            (
                level.price,
                Level {
                    qty: level.qty,
                    orders: level.orders,
                },
            )
        })
        .collect()
}

fn levels(depth: &Depth, side: Side) -> Vec<(Price, Level)> {
    depth.levels(side).collect()
}

fn flow(seed: u64, band: Option<u32>) -> (BookConfig, Workload) {
    let workload = WorkloadConfig {
        seed,
        min_price: 0,
        max_price: 2_000,
        initial_mid: 1_000,
        max_live: 300,
        owners: 8,
        mix: Mix {
            passive_limit: 40,
            aggressive_limit: 12,
            market: 12,
            cancel: 16,
            mass_cancel: 1,
            stop: 6,
            modify: 9,
            session: 4,
        },
        tif: TifMix {
            ioc: 20,
            fok: 10,
            post_only: 10,
        },
        iceberg: 25,
        ..WorkloadConfig::default()
    };
    let config = BookConfig {
        price_protection: None,
        price_band: band,
        reference_price: Some(workload.initial_mid),
        auction_on_band: band.is_some(),
        ..workload.book_config()
    };
    (config, Workload::new(workload))
}

#[test]
fn the_depth_follows_the_book() {
    for seed in 0..40 {
        let band = if seed % 2 == 0 { Some(10) } else { None };
        let (config, mut workload) = flow(seed, band);
        let mut book = OrderBook::new(config);
        let mut depth = Depth::new();
        let mut events = Vec::new();
        let mut uncrossed = 0;
        for step in 0..3_000 {
            let command = workload.next_command();
            let before: BTreeMap<(bool, Price), Level> = [Side::Buy, Side::Sell]
                .into_iter()
                .flat_map(|side| {
                    levels(&depth, side)
                        .into_iter()
                        .map(move |(p, l)| ((side == Side::Buy, p), l))
                })
                .collect();
            events.clear();
            book.process(command, &mut events);
            for event in &events {
                workload.observe(event);
                depth.apply(event);
            }
            if events
                .iter()
                .any(|e| matches!(e, orderbook::Event::PhaseChanged { .. }))
            {
                uncrossed += 1;
            }
            for side in [Side::Buy, Side::Sell] {
                assert_eq!(
                    levels(&depth, side),
                    book_levels(&book, side),
                    "seed {seed}, step {step}, {side:?} after {command:?}: {events:?}"
                );
            }
            // Every level that changed is reported, once, as it is now.
            let changes: Vec<_> = depth.changes().collect();
            let mut reported = std::collections::HashSet::new();
            for update in &changes {
                assert!(reported.insert((update.side == Side::Buy, update.price)));
                assert_eq!(update.level, depth.level(update.side, update.price));
            }
            for side in [Side::Buy, Side::Sell] {
                for (price, level) in levels(&depth, side) {
                    if before.get(&(side == Side::Buy, price)) != Some(&level) {
                        assert!(
                            reported.contains(&(side == Side::Buy, price)),
                            "seed {seed}, step {step}"
                        );
                    }
                }
            }
            for &(buy, price) in before.keys() {
                let side = if buy { Side::Buy } else { Side::Sell };
                if depth.level(side, price) == Level::default() {
                    assert!(
                        reported.contains(&(buy, price)),
                        "seed {seed}, step {step}: a level that went is reported"
                    );
                }
            }
            // A depth taken from the book now is the same.
            if step % 500 == 499 {
                let fresh = Depth::of(&book);
                for side in [Side::Buy, Side::Sell] {
                    assert_eq!(levels(&fresh, side), levels(&depth, side));
                }
            }
        }
        if band.is_some() {
            assert!(uncrossed > 0, "seed {seed}: the flow went through calls");
        }
    }
}
