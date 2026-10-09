//! Per-command service-time distribution of the matching core, over several order-flow
//! scenarios.
//!
//! What this measures: the time `OrderBook::process` takes for one command, on one pinned
//! thread, with commands issued back to back (closed loop). There is no network, queueing or
//! arrival schedule involved, so coordinated omission does not apply here; end-to-end latency
//! under an open-loop arrival rate is measured separately once the gateway exists.
//!
//! Method: per scenario, a participant-like generator drives a scratch book once and its
//! commands are recorded. The recording is then replayed into fresh books, several times, so
//! the measured loops contain nothing but the engine (plus the timer, in the latency pass).
//! The engine is deterministic, so every replay reproduces the recorded run exactly.
//!
//! Run: `cargo bench --bench latency`
//! Env: `LAT_COMMANDS` measured commands per run (default 2,000,000), `LAT_RUNS` (default 3),
//!      `LAT_SCENARIOS` comma-separated subset of `baseline,deep,sweep,protected,sessions`,
//!      `LAT_CORE` logical core to pin to (default the last one; on a hybrid CPU that can be
//!      an efficiency core).
//! Writes merged HdrHistogram percentile files to `target/latency/<scenario>.hgrm`
//! (plot at https://hdrhistogram.github.io/HdrHistogram/plotFiles.html).

use std::fmt::Write as _;
use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::time::Instant;

use hdrhistogram::Histogram;
use orderbook::workload::{EventCounts, Mix, TifMix, Workload, WorkloadConfig};
use orderbook::{BookConfig, Command, Event, EventSink, OrderBook, Side};

const KINDS: [&str; 8] = [
    "all", "limit", "market", "cancel", "modify", "mass", "stop", "phase",
];

struct Scenario {
    name: &'static str,
    about: &'static str,
    workload: WorkloadConfig,
    book: BookConfig,
    warmup: usize,
}

impl Scenario {
    /// A scenario on the book its workload asks for.
    fn new(
        name: &'static str,
        about: &'static str,
        workload: WorkloadConfig,
        warmup: usize,
    ) -> Self {
        Self {
            name,
            about,
            workload,
            book: workload.book_config(),
            warmup,
        }
    }
}

fn scenarios() -> Vec<Scenario> {
    let protected = WorkloadConfig {
        owners: 4,
        mix: Mix {
            passive_limit: 50,
            aggressive_limit: 5,
            market: 20,
            cancel: 16,
            mass_cancel: 0,
            stop: 4,
            modify: 5,
            session: 0,
        },
        tif: TifMix {
            ioc: 30,
            fok: 20,
            post_only: 20,
        },
        iceberg: 20,
        ..WorkloadConfig::default()
    };
    let sessions = WorkloadConfig {
        mix: Mix {
            cancel: 23,
            session: 2,
            ..WorkloadConfig::default().mix
        },
        ..WorkloadConfig::default()
    };
    vec![
        Scenario::new(
            "baseline",
            "~6k resting orders near the touch; fits in cache",
            WorkloadConfig::default(),
            500_000,
        ),
        Scenario::new(
            "deep",
            "~1M resting orders over ~10k levels; working set far exceeds L2",
            WorkloadConfig {
                max_live: 1_000_000,
                passive_depth: 5_000,
                ..WorkloadConfig::default()
            },
            4_000_000,
        ),
        Scenario::new(
            "sweep",
            "40% aggressive/market flow, sizes up to 1000; multi-level sweeps",
            WorkloadConfig {
                max_qty: 1_000,
                mix: Mix {
                    passive_limit: 40,
                    aggressive_limit: 20,
                    market: 20,
                    cancel: 15,
                    mass_cancel: 0,
                    stop: 0,
                    modify: 5,
                    session: 0,
                },
                ..WorkloadConfig::default()
            },
            500_000,
        ),
        // CancelResting keeps the book two-sided. Under CancelIncoming, stale orders left
        // behind by the generator's random-walking mid anchor the protection band, and one
        // side of the book stays empty for long stretches.
        Scenario {
            name: "protected",
            about: "2-tick protection, 4 owners, every order type; every path",
            workload: protected,
            book: BookConfig {
                price_protection: Some(2),
                ..protected.book_config()
            },
            warmup: 500_000,
        },
        // Phase changes two commands in a hundred, and a band that interrupts trading with
        // a call whenever it stops a market order: what calls and uncrosses cost.
        Scenario {
            name: "sessions",
            about: "calls, halts and the close; 10-tick band with interruptions",
            workload: sessions,
            book: BookConfig {
                price_protection: None,
                price_band: Some(10),
                reference_price: Some(sessions.initial_mid),
                auction_on_band: true,
                ..sessions.book_config()
            },
            warmup: 500_000,
        },
    ]
}

fn main() {
    let commands = env_count("LAT_COMMANDS", 2_000_000);
    let runs = env_count("LAT_RUNS", 3).max(1);
    let only = std::env::var("LAT_SCENARIOS").ok();

    let core = pin();
    let clock = Clock::calibrate();
    println!("matching core service time (single thread, closed loop, replayed streams)");
    println!("  cpu            : {}", cpu_brand());
    println!(
        "  os / cores     : {} / {} logical, pinned to core {}",
        std::env::consts::OS,
        std::thread::available_parallelism().map_or(0, |n| n.get()),
        core.map_or("none".to_string(), |c| c.to_string())
    );
    println!("  clock          : {}", clock.describe());
    println!(
        "  timer overhead : ~{:.0} ns per measurement, included below",
        clock.overhead_ns()
    );
    println!("  per scenario   : {runs} runs x {commands} measured commands");

    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/latency");
    fs::create_dir_all(&dir).expect("create output dir");

    for scenario in scenarios() {
        if only
            .as_deref()
            .is_some_and(|names| !names.split(',').any(|n| n.trim() == scenario.name))
        {
            continue;
        }
        run_scenario(&scenario, commands, runs, &clock, &dir);
    }
    println!(
        "\n  percentile files: {}",
        dir.canonicalize().unwrap_or(dir).display()
    );
}

fn run_scenario(scenario: &Scenario, commands: usize, runs: usize, clock: &Clock, dir: &Path) {
    let book_cfg = scenario.book;
    let stream = record(scenario.workload, book_cfg, scenario.warmup + commands);
    let (warm, measured) = stream.split_at(scenario.warmup);
    let mut events: Vec<Event> = Vec::with_capacity(4096);

    let mut merged: Vec<Histogram<u64>> = KINDS.iter().map(|_| new_histogram()).collect();
    let mut summary = String::new();
    let mut counts = EventCounts::default();
    let mut shape = String::new();
    let mut final_orders = None;

    for run in 0..runs {
        // Throughput: no per-command timers.
        let mut book = OrderBook::new(book_cfg);
        replay(&mut book, warm, &mut events);
        if run == 0 {
            shape = format!(
                "{} resting orders, {} bid / {} ask levels",
                book.order_count(),
                book.depth(Side::Buy).count(),
                book.depth(Side::Sell).count()
            );
        }
        let started = Instant::now();
        replay(&mut book, measured, &mut events);
        let untimed = started.elapsed();
        let orders = book.order_count();
        assert!(
            final_orders.is_none_or(|n| n == orders),
            "replays of the same stream diverged"
        );
        final_orders = Some(orders);
        drop(book);

        // Latency: the same commands into a fresh book, each one timed.
        let mut hists: Vec<Histogram<u64>> = KINDS.iter().map(|_| new_histogram()).collect();
        let mut book = OrderBook::new(book_cfg);
        replay(&mut book, warm, &mut events);
        for &command in measured {
            events.clear();
            let start = clock.start();
            book.process(command, &mut events);
            let end = clock.stop();
            let ns = (clock.to_ns(end.wrapping_sub(start)).round() as u64).max(1);
            hists[0].saturating_record(ns);
            hists[kind_index(&command)].saturating_record(ns);
            if run == 0 {
                events.iter().for_each(|&e| counts.on_event(e));
            }
        }
        black_box(&book);
        assert_eq!(
            book.order_count(),
            orders,
            "replays of the same stream diverged"
        );

        let all = &hists[0];
        let _ = writeln!(
            summary,
            "    run {}: {:>6.2} M cmd/s | p50 {:>4} | p99 {:>5} | p99.9 {:>5} | p99.99 {:>6} | max {:>8}",
            run + 1,
            commands as f64 / untimed.as_secs_f64() / 1e6,
            all.value_at_quantile(0.50),
            all.value_at_quantile(0.99),
            all.value_at_quantile(0.999),
            all.value_at_quantile(0.9999),
            all.max()
        );
        for (m, h) in merged.iter_mut().zip(&hists) {
            m.add(h).expect("same bounds");
        }
    }

    println!("\n== {} ({})", scenario.name, scenario.about);
    println!("  book after warm-up : {shape}");
    println!(
        "  events per run     : {} trades, {} rests, {} cancels ({} self-trade, {} protection, {} band), {} modifies, {} rejects, {} calls",
        counts.trades,
        counts.rested,
        counts.cancelled,
        counts.self_trade_cancels,
        counts.protection_cancels,
        counts.band_cancels,
        counts.modified,
        counts.rejected,
        counts.calls
    );
    println!("  per run (ns):");
    print!("{summary}");
    println!("  all runs merged (ns):");
    println!(
        "    {:<7} {:>10} {:>6} {:>6} {:>6} {:>6} {:>7} {:>8} {:>9}",
        "command", "count", "mean", "p50", "p90", "p99", "p99.9", "p99.99", "max"
    );
    for (name, hist) in KINDS.iter().zip(&merged) {
        if hist.is_empty() {
            continue;
        }
        println!(
            "    {:<7} {:>10} {:>6.0} {:>6} {:>6} {:>6} {:>7} {:>8} {:>9}",
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
    fs::write(
        dir.join(format!("{}.hgrm", scenario.name)),
        percentile_distribution(&merged[0]),
    )
    .expect("write .hgrm");
}

fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 100_000_000, 3).expect("histogram bounds")
}

/// Drives a scratch book with the participant-like generator, feeding every event back to
/// it, and returns the commands it issued.
fn record(cfg: WorkloadConfig, book_cfg: BookConfig, n: usize) -> Vec<Command> {
    let mut book = OrderBook::new(book_cfg);
    let mut workload = Workload::new(cfg);
    let mut events = Vec::with_capacity(4096);
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
        Command::CancelAll { .. } => 5,
        Command::Stop { .. } => 6,
        Command::SetPhase { .. } => 7,
    }
}

fn env_count(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.replace('_', "").parse().ok())
        .unwrap_or(default)
}

/// Pins the thread to `$LAT_CORE`, or to the last core, and returns the core's id.
fn pin() -> Option<usize> {
    let cores = core_affinity::get_core_ids()?;
    let core = match std::env::var("LAT_CORE").ok().and_then(|c| c.parse().ok()) {
        Some(id) => *cores.iter().find(|c| c.id == id)?,
        None => *cores.last()?,
    };
    core_affinity::set_for_current(core).then_some(core.id)
}

/// The processor brand string from CPUID, so results are tied to the hardware that
/// produced them.
#[cfg(target_arch = "x86_64")]
#[allow(unused_unsafe)]
fn cpu_brand() -> String {
    use std::arch::x86_64::__cpuid;
    let mut bytes = Vec::with_capacity(48);
    for leaf in 0x8000_0002u32..=0x8000_0004 {
        let r = unsafe { __cpuid(leaf) };
        for reg in [r.eax, r.ebx, r.ecx, r.edx] {
            bytes.extend_from_slice(&reg.to_le_bytes());
        }
    }
    String::from_utf8_lossy(&bytes)
        .trim_matches(char::from(0))
        .trim()
        .to_string()
}

#[cfg(not(target_arch = "x86_64"))]
fn cpu_brand() -> String {
    std::env::consts::ARCH.to_string()
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
