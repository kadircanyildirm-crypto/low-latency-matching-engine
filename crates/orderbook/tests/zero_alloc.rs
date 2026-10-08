//! Proves the hot path is allocation-free: a counting global allocator wraps the system
//! allocator, and a million commands of realistic flow must not touch it once.
//!
//! The counter is thread-local so allocations by the test harness or by tests running on
//! other threads cannot leak into the measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use orderbook::workload::{EventCounts, Workload, WorkloadConfig};
use orderbook::{Event, EventSink, OrderBook};

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
    let before_setup = allocations();
    let mut book = OrderBook::new(cfg.book_config());
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
