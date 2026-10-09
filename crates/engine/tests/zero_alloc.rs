//! Journaling adds no heap allocation to the hot path: a counting global allocator sees
//! none while the engine journals, syncs and applies commands on the real file system,
//! under both sync policies, one at a time and in batches. Segment rolls and snapshots,
//! which create files, are outside the measurement.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use engine::{Engine, EngineConfig, SyncPolicy};
use orderbook::{Event, EventSink};

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

/// Counts events without storing them.
struct Count(u64);

impl EventSink for Count {
    fn on_event(&mut self, _: Event) {
        self.0 += 1;
    }
}

#[test]
fn journaling_does_not_allocate() {
    let (book, commands) = common::flow(9, 6_000);
    let dir = std::env::temp_dir().join(format!("engine-zero-alloc-{}", std::process::id()));
    for (sync, batch) in [
        (SyncPolicy::Os, 1),
        (SyncPolicy::Os, 100),
        (SyncPolicy::Always, 1),
        (SyncPolicy::Always, 100),
    ] {
        let _ = std::fs::remove_dir_all(&dir);
        let config = EngineConfig {
            sync,
            ..EngineConfig::new(book)
        };
        let (mut engine, _) = Engine::open(&dir, config).unwrap();
        let mut sink = Count(0);
        // Warm up: the book's scratch space and the journal's buffer reach their size.
        let (warmup, measured) = commands.split_at(1_000);
        for chunk in warmup.chunks(batch) {
            engine.submit_batch(chunk, &mut sink).unwrap();
        }
        let before = allocations();
        for chunk in measured.chunks(batch) {
            engine.submit_batch(chunk, &mut sink).unwrap();
        }
        let allocated = allocations() - before;
        assert_eq!(allocated, 0, "{sync:?}, batches of {batch}");
        assert!(sink.0 > 10_000);
        drop(engine);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
