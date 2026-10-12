//! The playground's benchmark, run natively: the same flow, the same warm-up and the same
//! five timed rounds as the page, so the figure the page compares a browser with is the one
//! this machine reaches without WebAssembly.
//!
//! Usage: `cargo run --release -p playground --example native`, with `LAT_CORE=<n>` to pin
//! it to that logical core (a performance core on a hybrid processor).

use std::time::Instant;

use playground::{digest_high, digest_low, expected_high, expected_low, measure, prepare, warm_up};

const ROUNDS: usize = 5;

fn main() {
    if let Some(core) = std::env::var("LAT_CORE").ok().and_then(|c| c.parse().ok()) {
        let pinned = core_affinity::set_for_current(core_affinity::CoreId { id: core });
        println!("pinned to logical core {core}: {pinned}");
    }
    prepare();
    let mut rates = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        warm_up();
        let started = Instant::now();
        let commands = measure();
        rates.push(f64::from(commands) / started.elapsed().as_secs_f64());
    }
    let digest = (u64::from(digest_high()) << 32) | u64::from(digest_low());
    let expected = (u64::from(expected_high()) << 32) | u64::from(expected_low());
    assert_eq!(digest, expected, "the book did not reach the pinned state");
    rates.sort_by(|a, b| b.total_cmp(a));
    let millions: Vec<String> = rates.iter().map(|r| format!("{:.1}M", r / 1e6)).collect();
    println!(
        "median {:.1}M commands a second; rounds, fastest first: {}; digest {digest:#018x}",
        rates[ROUNDS / 2] / 1e6,
        millions.join(" · ")
    );
}
