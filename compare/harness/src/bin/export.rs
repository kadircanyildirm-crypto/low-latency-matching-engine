//! Records each scenario's command stream into `compare/data/<scenario>.bin`.
//!
//! The participant-like generator of `crates/orderbook/src/workload.rs` drives our book and
//! learns from its events which orders rest, exactly as in the latency benchmark. Each
//! command it issues is first made engine-neutral:
//!
//! - the owner becomes the side (buyers are owner 0, sellers owner 1), so no two orders of
//!   one owner can ever meet and self-trade prevention never fires;
//! - a modify becomes a price move that keeps the order's open quantity. If the generator
//!   kept the price (a size change), the move goes one tick further from the touch instead,
//!   because a pure size change behaves differently across engines (see
//!   `docs/COMPARISON.md`).
//!
//! The neutral command is what our book processes, and its events are what the generator
//! learns from, so the stream stays consistent: every cancel and move targets an order
//! that is resting at that moment. The exporter asserts that nothing is rejected and that
//! no self-trade prevention happened, and stores in the header the trades and the final
//! book every engine must reproduce.
//!
//! Env: `CMP_COMMANDS` measured commands per stream (default 2,000,000), `CMP_SCENARIOS`
//! comma-separated subset of `baseline,sweep,deep,modify`, `CMP_DATA` output directory.

use std::time::Instant;

use harness::scenarios::{self, Scenario};
use harness::stream::{self, Header, Kind, Record, Summary};
use harness::{data_dir, env_count, scenario_selected};
use orderbook::workload::Workload;
use orderbook::{CancelReason, Command, Event, OrderBook, Side, TimeInForce};

fn main() {
    let measured = env_count("CMP_COMMANDS", 2_000_000);
    let dir = data_dir();
    std::fs::create_dir_all(&dir).expect("create data dir");
    for scenario in scenarios::all() {
        if scenario_selected(scenario.name) {
            export(&scenario, measured, &dir);
        }
    }
}

fn export(scenario: &Scenario, measured: usize, dir: &std::path::Path) {
    let started = Instant::now();
    let total = scenario.warmup + measured;
    let book_cfg = scenarios::book_config(&scenario.workload);
    let mut book = OrderBook::new(book_cfg);
    let mut workload = Workload::new(scenario.workload);
    let mut events: Vec<Event> = Vec::with_capacity(4096);
    let mut records = Vec::with_capacity(total);
    let mut expect = Summary::default();
    let mut counts = [0u64; 4];
    let (mut max_id, mut max_live) = (0, 0);
    let mut shape = String::new();

    for i in 0..total {
        if i == scenario.warmup {
            shape = format!(
                "{} orders on {} bid / {} ask levels",
                book.order_count(),
                book.depth(Side::Buy).count(),
                book.depth(Side::Sell).count()
            );
        }
        let (record, command) = neutral(workload.next_command(), &book);
        events.clear();
        book.process(command, &mut events);
        for event in &events {
            match *event {
                Event::Rejected { id, reason } => {
                    panic!(
                        "{}: command for order {id} rejected: {reason:?}",
                        scenario.name
                    )
                }
                Event::Cancelled {
                    reason: CancelReason::SelfTrade,
                    ..
                } => panic!("{}: self-trade prevention fired", scenario.name),
                Event::Trade { qty, .. } => {
                    expect.trades += 1;
                    expect.traded_qty += qty;
                }
                _ => {}
            }
            workload.observe(event);
        }
        counts[record.kind as usize] += 1;
        if matches!(record.kind(), Kind::Limit | Kind::Move) {
            assert!((book_cfg.min_price..=book_cfg.max_price).contains(&record.price));
        }
        max_id = max_id.max(record.id);
        max_live = max_live.max(book.order_count() as u64);
        records.push(record);
    }

    for side in [Side::Buy, Side::Sell] {
        for level in book.depth(side) {
            expect.resting_orders += u64::from(level.orders);
            expect.resting_qty += level.qty;
        }
    }
    assert_eq!(expect.resting_orders, book.order_count() as u64);
    expect.best_bid = book.best_bid().map_or(Summary::NO_BID, |l| l.price);
    expect.best_ask = book.best_ask().map_or(Summary::NO_ASK, |l| l.price);

    let header = Header {
        count: total as u64,
        warmup: scenario.warmup as u64,
        min_price: book_cfg.min_price,
        max_price: book_cfg.max_price,
        max_live,
        max_id,
        expect,
        seed: scenario.workload.seed,
    };
    let path = dir.join(format!("{}.bin", scenario.name));
    stream::write(&path, &header, &records).expect("write stream");
    println!(
        "{} -> {} ({:.1}s)\n  {} commands ({} warm-up): {} limit, {} market, {} cancel, \
         {} move\n  after warm-up: {shape}\n  in total: {} trades; at the end: {} orders \
         resting",
        scenario.name,
        std::path::absolute(&path)
            .unwrap_or_else(|_| path.clone())
            .display(),
        started.elapsed().as_secs_f64(),
        total,
        scenario.warmup,
        counts[Kind::Limit as usize],
        counts[Kind::Market as usize],
        counts[Kind::Cancel as usize],
        counts[Kind::Move as usize],
        expect.trades,
        expect.resting_orders,
    );
}

/// The owner every order of `side` belongs to.
fn owner(side: Side) -> u32 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn side_code(side: Side) -> u8 {
    match side {
        Side::Buy => stream::BUY,
        Side::Sell => stream::SELL,
    }
}

fn record(kind: Kind, side: Side, qty: u64, id: u64, price: i64) -> Record {
    Record {
        kind: kind as u8,
        side: side_code(side),
        reserved: 0,
        qty: u32::try_from(qty).expect("quantity fits in u32"),
        id,
        price,
    }
}

/// The engine-neutral form of a generated command, as a record and as the command our
/// book processes.
fn neutral(command: Command, book: &OrderBook) -> (Record, Command) {
    match command {
        Command::Limit {
            id,
            side,
            price,
            qty,
            tif: TimeInForce::Gtc,
            display: None,
            ..
        } => (
            record(Kind::Limit, side, qty, id, price),
            Command::Limit {
                id,
                owner: owner(side),
                side,
                price,
                qty,
                tif: TimeInForce::Gtc,
                display: None,
            },
        ),
        Command::Market { id, side, qty, .. } => (
            record(Kind::Market, side, qty, id, 0),
            Command::Market {
                id,
                owner: owner(side),
                side,
                qty,
            },
        ),
        Command::Cancel { id, .. } => {
            let order = book.order(id).expect("cancels target resting orders");
            (
                record(Kind::Cancel, order.side, 0, id, 0),
                Command::Cancel {
                    id,
                    owner: owner(order.side),
                },
            )
        }
        Command::Modify { id, price, .. } => {
            let order = book.order(id).expect("modifies target resting orders");
            let price = if price != order.price {
                price
            } else {
                match order.side {
                    Side::Buy => price - 1,
                    Side::Sell => price + 1,
                }
            };
            // Total quantity unchanged, so the open quantity is too.
            let total = order.leaves + order.filled;
            (
                record(Kind::Move, order.side, total, id, price),
                Command::Modify {
                    id,
                    owner: owner(order.side),
                    price,
                    qty: total,
                },
            )
        }
        other => panic!("not part of the common subset: {other:?}"),
    }
}
