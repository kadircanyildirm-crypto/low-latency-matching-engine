//! Proves the hot path is allocation-free: a counting global allocator wraps the system
//! allocator, and a million commands of realistic flow must not touch it once.
//!
//! The counter is thread-local so allocations by the test harness or by tests running on
//! other threads cannot leak into the measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use orderbook::workload::{EventCounts, Mix, TifMix, Workload, WorkloadConfig};
use orderbook::{BookConfig, Event, EventSink, OrderBook};

struct CountingAllocator;

thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn count() {
    // `try_with` because the allocator can run while thread-locals are being torn down.
    let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
}

fn allocations() -> u64 {
    ALLOCATIONS.with(Cell::get)
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn assert_no_allocations(cfg: WorkloadConfig, warmup: usize, measured: usize) -> EventCounts {
    assert_no_allocations_in(cfg, cfg.book_config(), warmup, measured)
}

fn assert_no_allocations_in(
    cfg: WorkloadConfig,
    book_cfg: BookConfig,
    warmup: usize,
    measured: usize,
) -> EventCounts {
    let before_setup = allocations();
    let mut book = OrderBook::new(book_cfg);
    let mut workload = Workload::new(cfg);
    assert!(
        allocations() > before_setup,
        "counter is not wired up: construction should allocate"
    );

    // Events go to a reused output buffer, as they will in the pipeline.
    let mut events: Vec<Event> = Vec::with_capacity(1024);
    let mut counts = EventCounts::default();
    let mut step = |counts: &mut EventCounts| {
        events.clear();
        book.process(workload.next_command(), &mut events);
        for event in &events {
            workload.observe(event);
            counts.on_event(*event);
        }
    };

    for _ in 0..warmup {
        step(&mut counts);
    }
    let before = allocations();
    for _ in 0..measured {
        step(&mut counts);
    }
    let allocated = allocations() - before;
    assert_eq!(allocated, 0, "hot path allocated {allocated} times");

    // A replica compares digests often, so computing one must not allocate either.
    let before = allocations();
    std::hint::black_box(book.digest());
    assert_eq!(allocations(), before, "digest allocated");

    assert!(
        counts.trades > 0
            && counts.cancelled > 0
            && counts.modified > 0
            && counts.rested > 0
            && counts.self_trade_cancels > 0,
        "{counts:?}"
    );
    counts
}

#[test]
fn processing_commands_never_allocates() {
    assert_no_allocations(WorkloadConfig::default(), 100_000, 1_000_000);
}

/// A tiny book that is permanently full: every new order first cancels an old one, so the
/// id index sees maximum churn at maximum occupancy, the worst case for its tombstones.
#[test]
fn a_permanently_full_book_never_allocates() {
    let cfg = WorkloadConfig {
        max_live: 64,
        owners: 8,
        ..WorkloadConfig::default()
    };
    assert_no_allocations(cfg, 10_000, 1_000_000);
}

/// Mass cancels sort the owner's orders in a buffer reserved at construction, and churn the
/// owner table, whose index has the same tombstone worst case as the order id index.
#[test]
fn mass_cancels_never_allocate() {
    let cfg = WorkloadConfig {
        owners: 8,
        mix: Mix {
            cancel: 23,
            mass_cancel: 2,
            ..WorkloadConfig::default().mix
        },
        ..WorkloadConfig::default()
    };
    let counts = assert_no_allocations(cfg, 100_000, 1_000_000);
    assert!(
        counts.mass_cancels > 10_000 && counts.mass_cancelled_orders > 100_000,
        "{counts:?}"
    );
}

/// Fill-or-kill orders walk the book before matching, iceberg tranches move to the back of
/// their queue, and stops trigger in cascades; none of it may allocate.
#[test]
fn every_order_type_never_allocates() {
    let cfg = WorkloadConfig {
        tif: TifMix {
            ioc: 30,
            fok: 30,
            post_only: 30,
        },
        iceberg: 30,
        mix: Mix {
            cancel: 20,
            stop: 5,
            ..WorkloadConfig::default().mix
        },
        ..WorkloadConfig::default()
    };
    let counts = assert_no_allocations(cfg, 100_000, 1_000_000);
    assert!(
        counts.ioc_cancels > 1_000 && counts.fok_kills > 1_000 && counts.replenishes > 1_000,
        "{counts:?}"
    );
    assert!(counts.stops_triggered > 1_000, "{counts:?}");
}

/// Calls, halts and the close, uncrosses that walk the crossed part of the book twice to
/// find their price and then trade it, and calls started by the price band: none of it
/// needs a buffer, so none of it may allocate.
#[test]
fn phase_changes_and_uncrosses_never_allocate() {
    let cfg = WorkloadConfig {
        tif: TifMix {
            ioc: 20,
            fok: 10,
            post_only: 10,
        },
        iceberg: 20,
        mix: Mix {
            cancel: 18,
            stop: 5,
            session: 2,
            ..WorkloadConfig::default().mix
        },
        ..WorkloadConfig::default()
    };
    let book_cfg = BookConfig {
        price_protection: None,
        price_band: Some(10),
        reference_price: Some(cfg.initial_mid),
        auction_on_band: true,
        ..cfg.book_config()
    };
    let counts = assert_no_allocations_in(cfg, book_cfg, 100_000, 1_000_000);
    assert!(
        counts.calls > 1_000 && counts.phase_changes > 10_000,
        "{counts:?}"
    );
    assert!(
        counts.band_cancels > 100 && counts.phase_cancels > 100,
        "{counts:?}"
    );
}

/// A deep book (100k resting orders), where the index and the pool are large.
#[test]
fn a_deep_book_never_allocates() {
    let cfg = WorkloadConfig {
        max_live: 100_000,
        passive_depth: 2_000,
        ..WorkloadConfig::default()
    };
    assert_no_allocations(cfg, 400_000, 500_000);
}
