//! Per-command service-time distribution of the matching core.
//!
//! What this measures: the time `OrderBook::process` takes for one command, on one pinned
//! thread, with commands issued back to back (closed loop). There is no network, queueing or
//! arrival schedule involved, so coordinated omission does not apply here; end-to-end latency
//! under an open-loop arrival rate is measured separately once the gateway exists.
//!
//! Method: a client-like generator drives a scratch book once and its commands are recorded.
//! The recording is then replayed into fresh books, so the measured loops contain nothing
//! but the engine (plus the timer, in the latency pass). Because the engine is
//! deterministic, every replay reproduces the recorded run exactly.
//!
//! Run: `cargo bench --bench latency`
//! Env: `LAT_COMMANDS` (default 3,000,000), `LAT_WARMUP` (default 500,000).
//! Writes HdrHistogram percentile files to `target/latency/*.hgrm`
//! (plot at https://hdrhistogram.github.io/HdrHistogram/plotFiles.html).

use std::fmt::Write as _;
use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use hdrhistogram::Histogram;
use orderbook::workload::{EventCounts, Workload, WorkloadConfig};
use orderbook::{Command, Event, EventSink, OrderBook, Side};

const KINDS: [&str; 5] = ["all", "limit", "market", "cancel", "modify"];

fn main() {
    let commands = env_count("LAT_COMMANDS", 3_000_000);
    let warmup = env_count("LAT_WARMUP", 500_000);

    let core = pin_to_last_core();
    let clock = Clock::calibrate();
    let cfg = WorkloadConfig::default();

    let stream = record(cfg, warmup + commands);
    let (warm, measured) = stream.split_at(warmup);
    let mut events: Vec<Event> = Vec::with_capacity(1024);

    // Throughput pass: no per-command timers.
    let mut book = OrderBook::new(cfg.book_config());
    replay(&mut book, warm, &mut events);
    let (resting, bid_levels, ask_levels) = (
        book.order_count(),
        book.depth(Side::Buy).count(),
        book.depth(Side::Sell).count(),
    );
    let started = Instant::now();
    replay(&mut book, measured, &mut events);
    let untimed = started.elapsed();
    let final_orders = book.order_count();

    // Latency pass: same commands into a fresh book, each one timed.
    let mut hists: Vec<Histogram<u64>> = KINDS
        .iter()
        .map(|_| Histogram::new_with_bounds(1, 100_000_000, 3).expect("histogram bounds"))
        .collect();
    let overhead = clock.overhead_ns();
    let mut counts = EventCounts::default();
    let mut book = OrderBook::new(cfg.book_config());
    replay(&mut book, warm, &mut events);
    for &command in measured {
        events.clear();
        let start = clock.start();
        book.process(command, &mut events);
        let end = clock.stop();
        let ns = (clock.to_ns(end.wrapping_sub(start)).round() as u64).max(1);
        hists[0].saturating_record(ns);
        hists[kind_index(&command)].saturating_record(ns);
        events.iter().for_each(|&e| counts.on_event(e));
    }
    black_box(&book);
    assert_eq!(
        book.order_count(),
        final_orders,
        "replays of the same stream diverged"
    );

    println!("matching core service time (single thread, closed loop, replayed stream)");
    println!(
        "  pinned core     : {}",
        core.map_or("none".to_string(), |c| c.to_string())
    );
    println!("  clock           : {}", clock.describe());
    println!("  timer overhead  : ~{overhead:.0} ns per measurement (included below)");
    println!(
        "  stream          : {warmup} warm-up + {commands} measured commands ({:.0} MB)",
        (stream.len() * size_of::<Command>()) as f64 / 1e6
    );
    println!(
        "  book after warm : {resting} resting orders, {bid_levels} bid / {ask_levels} ask levels"
    );
    println!(
        "  throughput      : {:.2} M commands/s ({:.1} ns/command, untimed)",
        commands as f64 / untimed.as_secs_f64() / 1e6,
        untimed.as_nanos() as f64 / commands as f64
    );
    println!(
        "  measured events : {} trades, {} rests, {} cancels, {} modifies, {} rejects",
        counts.trades, counts.rested, counts.cancelled, counts.modified, counts.rejected
    );
    println!();
    println!(
        "  {:<7} {:>10} {:>7} {:>7} {:>7} {:>7} {:>8} {:>9} {:>9}   (ns)",
        "command", "count", "mean", "p50", "p90", "p99", "p99.9", "p99.99", "max"
    );
    for (name, hist) in KINDS.iter().zip(&hists) {
        if hist.is_empty() {
            continue;
        }
        println!(
            "  {:<7} {:>10} {:>7.0} {:>7} {:>7} {:>7} {:>8} {:>9} {:>9}",
            name,
            hist.len(),
            hist.mean(),
            hist.value_at_quantile(0.50),
            hist.value_at_quantile(0.90),
            hist.value_at_quantile(0.99),
            hist.value_at_quantile(0.999),
            hist.value_at_quantile(0.9999),
            hist.max()
        );
    }

    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/latency");
    fs::create_dir_all(&dir).expect("create output dir");
    for (name, hist) in KINDS.iter().zip(&hists) {
        if !hist.is_empty() {
            fs::write(
                dir.join(format!("{name}.hgrm")),
                percentile_distribution(hist),
            )
            .expect("write .hgrm");
        }
    }
    println!();
    println!(
        "  percentile files: {}",
        dir.canonicalize().unwrap_or(dir).display()
    );
}

/// Drives a scratch book with the client-like generator, feeding every event back to it,
/// and returns the commands it issued.
fn record(cfg: WorkloadConfig, n: usize) -> Vec<Command> {
    let mut book = OrderBook::new(cfg.book_config());
    let mut workload = Workload::new(cfg);
    let mut events = Vec::with_capacity(1024);
    let mut stream = Vec::with_capacity(n);
    for _ in 0..n {
        let command = workload.next_command();
        events.clear();
        book.process(command, &mut events);
        events.iter().for_each(|e| workload.observe(e));
        stream.push(command);
    }
    stream
}

fn replay(book: &mut OrderBook, commands: &[Command], events: &mut Vec<Event>) {
    for &command in commands {
        events.clear();
        book.process(command, events);
    }
}

fn kind_index(command: &Command) -> usize {
    match command {
        Command::Limit { .. } => 1,
        Command::Market { .. } => 2,
        Command::Cancel { .. } => 3,
        Command::Modify { .. } => 4,
    }
}

fn env_count(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.replace('_', "").parse().ok())
        .unwrap_or(default)
}

fn pin_to_last_core() -> Option<usize> {
    let core = core_affinity::get_core_ids()?.pop()?;
    core_affinity::set_for_current(core).then_some(core.id)
}

/// HdrHistogram's text percentile format, values in microseconds.
fn percentile_distribution(hist: &Histogram<u64>) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:>12} {:>14} {:>10} {:>14}\n",
        "Value", "Percentile", "TotalCount", "1/(1-Percentile)"
    );
    let mut total = 0u64;
    for v in hist.iter_quantiles(5) {
        total += v.count_since_last_iteration();
        let q = v.quantile_iterated_to();
        let value_us = v.value_iterated_to() as f64 / 1_000.0;
        if q < 1.0 {
            let _ = writeln!(
                out,
                "{value_us:12.3} {q:14.12} {total:10} {:14.2}",
                1.0 / (1.0 - q)
            );
        } else {
            let _ = writeln!(out, "{value_us:12.3} {q:14.12} {total:10}");
        }
    }
    let _ = writeln!(
        out,
        "#[Mean    = {:12.3}, StdDeviation   = {:12.3}]",
        hist.mean() / 1_000.0,
        hist.stdev() / 1_000.0
    );
    let _ = writeln!(
        out,
        "#[Max     = {:12.3}, Total count    = {:12}]",
        hist.max() as f64 / 1_000.0,
        hist.len()
    );
    out
}

/// Cycle-accurate timestamps from the TSC, converted to nanoseconds via a calibration
/// against the OS monotonic clock. `Instant` alone is too coarse on some platforms (100 ns
/// ticks on Windows) for operations that take tens of nanoseconds.
#[cfg(target_arch = "x86_64")]
struct Clock {
    ns_per_cycle: f64,
}

#[cfg(target_arch = "x86_64")]
#[allow(unused_unsafe)]
impl Clock {
    fn calibrate() -> Self {
        let wall = Instant::now();
        let c0 = Self::read_start();
        while wall.elapsed().as_millis() < 250 {
            std::hint::spin_loop();
        }
        let c1 = Self::read_stop();
        let ns = wall.elapsed().as_nanos() as f64;
        Self {
            ns_per_cycle: ns / (c1 - c0) as f64,
        }
    }

    /// `lfence` keeps earlier instructions from drifting past the timestamp.
    #[inline(always)]
    fn read_start() -> u64 {
        use std::arch::x86_64::{_mm_lfence, _rdtsc};
        unsafe {
            _mm_lfence();
            let t = _rdtsc();
            _mm_lfence();
            t
        }
    }

    /// `rdtscp` waits for the measured work to retire; `lfence` keeps later work out.
    #[inline(always)]
    fn read_stop() -> u64 {
        use std::arch::x86_64::{__rdtscp, _mm_lfence};
        let mut aux = 0u32;
        unsafe {
            let t = __rdtscp(&mut aux);
            _mm_lfence();
            t
        }
    }

    #[inline(always)]
    fn start(&self) -> u64 {
        Self::read_start()
    }

    #[inline(always)]
    fn stop(&self) -> u64 {
        Self::read_stop()
    }

    fn to_ns(&self, cycles: u64) -> f64 {
        cycles as f64 * self.ns_per_cycle
    }

    fn describe(&self) -> String {
        format!("TSC @ {:.3} GHz", 1.0 / self.ns_per_cycle)
    }

    /// Median cost of an empty start/stop pair.
    fn overhead_ns(&self) -> f64 {
        let mut samples: Vec<u64> = (0..10_001)
            .map(|_| {
                let s = self.start();
                self.stop().wrapping_sub(s)
            })
            .collect();
        samples.sort_unstable();
        self.to_ns(samples[samples.len() / 2])
    }
}

/// Fallback for non-x86 targets: the OS monotonic clock, in nanoseconds.
#[cfg(not(target_arch = "x86_64"))]
struct Clock {
    origin: Instant,
}

#[cfg(not(target_arch = "x86_64"))]
impl Clock {
    fn calibrate() -> Self {
        Self {
            origin: Instant::now(),
        }
    }

    fn start(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }

    fn stop(&self) -> u64 {
        self.origin.elapsed().as_nanos() as u64
    }

    fn to_ns(&self, ticks: u64) -> f64 {
        ticks as f64
    }

    fn describe(&self) -> String {
        "std::time::Instant".to_string()
    }

    fn overhead_ns(&self) -> f64 {
        let mut samples: Vec<u64> = (0..10_001)
            .map(|_| {
                let s = self.start();
                self.stop() - s
            })
            .collect();
        samples.sort_unstable();
        samples[samples.len() / 2] as f64
    }
}
