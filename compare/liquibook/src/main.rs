//! liquibook (github.com/enewhuis/liquibook, C++, header-only) replaying the comparison
//! streams through `shim.cpp`.
//!
//! The book is liquibook's plain `OrderBook` (not the depth-tracking `DepthOrderBook`, so
//! it does not maintain aggregated market data, which our engine does not do per command
//! either), with no listeners attached. Fills are consumed by overriding `on_fill`, which
//! liquibook calls from its callback queue after every command.
//!
//! Commands map one to one: `add` (GTC limit), `add` with price 0 and immediate-or-cancel
//! (market), `cancel`, and `replace(order, 0, new_price)` for a move, which re-adds the
//! order with its open quantity at the back of the new level, trading first if it
//! crosses. The replay loops and the per-command timer run inside the shim, so no FFI call
//! is timed.
//!
//! Run: `cargo run --release --manifest-path compare/Cargo.toml --bin run-liquibook`
//! (after `compare/run.sh fetch`).

#[cfg(liquibook)]
mod ffi {
    use std::ffi::c_void;

    use harness::stream::{Record, Summary};

    unsafe extern "C" {
        pub fn lb_new(max_id: u64) -> *mut c_void;
        pub fn lb_free(ctx: *mut c_void);
        pub fn lb_replay(ctx: *mut c_void, records: *const Record, n: usize);
        pub fn lb_replay_timed(ctx: *mut c_void, records: *const Record, n: usize, ticks: *mut u64);
        pub fn lb_summary(ctx: *const c_void, out: *mut Summary);
    }
}

#[cfg(liquibook)]
mod engine {
    use std::ffi::c_void;

    use harness::run::Engine;
    use harness::stream::{Header, Record, Summary};

    use crate::ffi;

    // `Summary` crosses the boundary as a C struct of six 64-bit fields.
    const _: () = assert!(std::mem::size_of::<Summary>() == 48);

    pub struct Liquibook {
        ctx: *mut c_void,
    }

    impl Drop for Liquibook {
        fn drop(&mut self) {
            // SAFETY: `ctx` came from `lb_new` and is freed exactly once.
            unsafe { ffi::lb_free(self.ctx) }
        }
    }

    impl Engine for Liquibook {
        const NAME: &'static str = "liquibook";

        fn new(header: &Header) -> Self {
            // SAFETY: plain allocation on the C++ side; a null result is checked.
            let ctx = unsafe { ffi::lb_new(header.max_id) };
            assert!(!ctx.is_null(), "lb_new failed");
            Self { ctx }
        }

        fn apply(&mut self, record: &Record) {
            self.replay(std::slice::from_ref(record));
        }

        fn replay(&mut self, records: &[Record]) {
            // SAFETY: `records` is a valid slice of `repr(C)` records with the layout the
            // shim declares, and every id is at most the header's `max_id`.
            unsafe { ffi::lb_replay(self.ctx, records.as_ptr(), records.len()) }
        }

        fn replay_timed(&mut self, records: &[Record], ticks: &mut [u64]) {
            assert!(ticks.len() >= records.len());
            // SAFETY: as in `replay`; `ticks` has room for one value per record.
            unsafe {
                ffi::lb_replay_timed(
                    self.ctx,
                    records.as_ptr(),
                    records.len(),
                    ticks.as_mut_ptr(),
                )
            }
        }

        fn summary(&self) -> Summary {
            let mut out = Summary::default();
            // SAFETY: `out` is a valid `repr(C)`-compatible destination.
            unsafe { ffi::lb_summary(self.ctx, &mut out) };
            out
        }

        fn describe() -> String {
            "(liquibook @ 2427613, plain OrderBook<Order*>, MSVC/c++ -O2/-O3; on_fill consumer)"
                .to_string()
        }
    }
}

#[cfg(liquibook)]
fn main() {
    harness::run::main::<engine::Liquibook>();
}

#[cfg(not(liquibook))]
fn main() {
    eprintln!(
        "run-liquibook was built without liquibook's sources: run `compare/run.sh fetch`, \
         then build again"
    );
    std::process::exit(2);
}
