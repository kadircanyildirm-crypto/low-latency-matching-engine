//! Throughput of the matching core under synthetic order flow, tracked by Criterion so that
//! every optimisation gets a before/after comparison against the saved baseline.
//!
//! Run: `cargo bench --bench throughput`

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use orderbook::workload::{Mix, Workload, WorkloadConfig};
use orderbook::{Event, OrderBook};

const BATCH: u64 = 10_000;
const WARMUP: u64 = 200_000;

fn scenarios() -> [(&'static str, WorkloadConfig); 2] {
    [
        ("mixed", WorkloadConfig::default()),
        (
            "passive_add_cancel",
            WorkloadConfig {
                mix: Mix {
                    passive_limit: 60,
                    aggressive_limit: 0,
                    market: 0,
                    cancel: 40,
                    mass_cancel: 0,
                    stop: 0,
                    modify: 0,
                },
                ..WorkloadConfig::default()
            },
        ),
    ]
}

fn matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("matching");
    group.throughput(Throughput::Elements(BATCH));
    for (name, cfg) in scenarios() {
        let mut book = OrderBook::new(cfg.book_config());
        let mut workload = Workload::new(cfg);
        let mut events = Vec::with_capacity(1024);
        for _ in 0..WARMUP {
            step(&mut book, &mut workload, &mut events);
        }
        group.bench_function(name, |b| {
            b.iter(|| {
                for _ in 0..BATCH {
                    step(&mut book, &mut workload, &mut events);
                }
            });
        });
        black_box(&book);
    }
    group.finish();
}

/// One command through the book, with the events fed back to the generator. The generator's
/// share of the time is constant across engine changes, so comparisons stay meaningful; the
/// `latency` bench reports the engine alone.
#[inline]
fn step(book: &mut OrderBook, workload: &mut Workload, events: &mut Vec<Event>) {
    events.clear();
    book.process(workload.next_command(), events);
    events.iter().for_each(|e| workload.observe(e));
}

criterion_group!(benches, matching);
criterion_main!(benches);
