//! The replay driver shared by every Rust-side adapter.
//!
//! Per scenario, in one process pinned to one core:
//!
//! 1. Load the stream. Nothing below reads files or allocates for the harness.
//! 2. Warm-up pass: replay the whole stream into a fresh engine, untimed, and check its
//!    outcome. This warms caches, the allocator and, for a JIT, the code.
//! 3. For each run (`CMP_RUNS`, default 1):
//!    - throughput pass: a fresh engine replays the warm-up prefix, then the measured part
//!      with no per-command timers, timed in chunks of [`CHUNK`] commands. The row records
//!      the overall rate (everything included) and the median chunk's rate, which an
//!      occasional preemption by another process cannot move;
//!    - latency pass: another fresh engine replays the warm-up prefix, then the measured
//!      part with a TSC read before and after every command.
//!
//!    Both passes must end in exactly the trades and the book recorded in the stream's
//!    header, or the row is marked unverified.
//!
//! Each run appends one row to `compare/results/results.csv`; `report` summarises them.
//!
//! Env: `CMP_SCENARIOS`, `CMP_DATA` (see [`crate::data_dir`]), `CMP_RUNS`, `CMP_ROUND` (a
//! label for the row), `CMP_RESULTS` (CSV path), `CMP_CORE` (core to pin to; default the
//! last one, as in the latency benchmark).

use std::fmt::Write as _;
use std::fs::OpenOptions;
use std::hint::black_box;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::clock::{self, Clock};
use crate::scenarios;
use crate::stream::{self, Header, Kind, Record, Summary};
use crate::{data_dir, env_count, scenario_selected};

/// Commands per timed chunk of the throughput pass.
pub const CHUNK: usize = 100_000;

/// An engine under test, driven one record at a time.
pub trait Engine {
    /// Name in reports.
    const NAME: &'static str;
    /// Whether the engine moves an order to a new price natively; streams with moves are
    /// skipped otherwise.
    const MOVES: bool = true;

    /// A fresh, empty book for `header`'s stream.
    fn new(header: &Header) -> Self;

    /// Processes one command and consumes the engine's output for it.
    fn apply(&mut self, record: &Record);

    /// Trades so far and the current book.
    fn summary(&self) -> Summary;

    /// Processes `records` back to back.
    fn replay(&mut self, records: &[Record]) {
        for record in records {
            self.apply(record);
        }
    }

    /// Processes `records`, storing in `ticks` how long each one took.
    fn replay_timed(&mut self, records: &[Record], ticks: &mut [u64]) {
        for (record, slot) in records.iter().zip(ticks.iter_mut()) {
            let start = clock::start();
            self.apply(record);
            *slot = clock::stop().wrapping_sub(start);
        }
    }

    /// One line on how the engine is configured, for the log.
    fn describe() -> String {
        String::new()
    }
}

/// Runs every selected scenario through `E` and records the results.
pub fn main<E: Engine>() {
    let runs = env_count("CMP_RUNS", 1).max(1);
    let round = std::env::var("CMP_ROUND").unwrap_or_else(|_| "0".to_string());
    let core = pin();
    let clock = Clock::calibrate();
    let overhead = clock.overhead_ns();
    println!("== {} {}", E::NAME, E::describe());
    println!(
        "  cpu {} | core {} | {} | timer overhead ~{overhead:.0} ns (included)",
        clock::cpu_brand(),
        core.as_deref().unwrap_or("not pinned"),
        clock.describe(),
    );

    for scenario in scenarios::all() {
        if !scenario_selected(scenario.name) {
            continue;
        }
        let path = data_dir().join(format!("{}.bin", scenario.name));
        if !path.exists() {
            println!(
                "  {:<9} skipped: no stream at {} (run the exporter)",
                scenario.name,
                path.display()
            );
            continue;
        }
        let (header, records) = stream::read(&path).expect("read stream");
        if !E::MOVES && records.iter().any(|r| r.kind() == Kind::Move) {
            println!(
                "  {:<9} skipped: {} has no native price move",
                scenario.name,
                E::NAME
            );
            continue;
        }
        let row = RowContext {
            engine: E::NAME,
            scenario: scenario.name,
            round: &round,
            core: core.as_deref().unwrap_or("none"),
            overhead,
        };
        run_scenario::<E>(&header, &records, runs, &clock, &row);
    }
}

struct RowContext<'a> {
    engine: &'a str,
    scenario: &'a str,
    round: &'a str,
    core: &'a str,
    overhead: f64,
}

fn run_scenario<E: Engine>(
    header: &Header,
    records: &[Record],
    runs: usize,
    clock: &Clock,
    ctx: &RowContext<'_>,
) {
    let (warm, measured) = records.split_at(header.warmup as usize);

    let mut engine = E::new(header);
    engine.replay(records);
    check(ctx, "warm-up pass", header, &engine.summary());
    drop(black_box(engine));

    let mut ticks = vec![0u64; measured.len()];
    for run in 0..runs {
        let mut engine = E::new(header);
        engine.replay(warm);
        let mut chunks = Vec::with_capacity(measured.len().div_ceil(CHUNK));
        for chunk in measured.chunks(CHUNK) {
            let started = Instant::now();
            engine.replay(chunk);
            chunks.push((chunk.len(), started.elapsed().as_secs_f64()));
        }
        let mut verified = check(ctx, "throughput pass", header, &engine.summary());
        drop(black_box(engine));

        let mut engine = E::new(header);
        engine.replay(warm);
        engine.replay_timed(measured, &mut ticks);
        verified &= check(ctx, "latency pass", header, &engine.summary());
        drop(black_box(engine));

        let stats = Stats::new(&mut ticks, clock);
        let elapsed: f64 = chunks.iter().map(|&(_, secs)| secs).sum();
        let throughput = Throughput {
            overall: measured.len() as f64 / elapsed / 1e6,
            chunk_median: chunk_median(&chunks),
        };
        println!(
            "  {:<9} run {}: {:>6.2} M cmd/s ({:>6.2} median chunk) | p50 {:>5.0} | p90 {:>5.0} | p99 {:>6.0} | \
             p99.9 {:>6.0} | p99.99 {:>7.0} | max {:>8.0} ns | {}",
            ctx.scenario,
            run + 1,
            throughput.overall,
            throughput.chunk_median,
            stats.p50,
            stats.p90,
            stats.p99,
            stats.p999,
            stats.p9999,
            stats.max,
            if verified { "verified" } else { "MISMATCH" }
        );
        append_row(ctx, run + 1, measured.len(), &throughput, &stats, verified);
    }
}

/// Throughput of one run, in millions of commands per second.
struct Throughput {
    /// All measured commands over the sum of the chunk times.
    overall: f64,
    /// The median chunk's rate.
    chunk_median: f64,
}

/// Median rate, in M cmd/s, of `(commands, seconds)` chunks.
fn chunk_median(chunks: &[(usize, f64)]) -> f64 {
    let mut rates: Vec<f64> = chunks
        .iter()
        .map(|&(n, secs)| n as f64 / secs / 1e6)
        .collect();
    rates.sort_by(f64::total_cmp);
    let n = rates.len();
    if n % 2 == 1 {
        rates[n / 2]
    } else {
        (rates[n / 2 - 1] + rates[n / 2]) / 2.0
    }
}

/// Compares an engine's outcome with the one recorded from ours.
fn check(ctx: &RowContext<'_>, pass: &str, header: &Header, got: &Summary) -> bool {
    let ok = *got == header.expect;
    if !ok {
        println!(
            "  {:<9} {pass}: outcome differs from the recorded one\n      expected {:?}\n      got      {:?}",
            ctx.scenario, header.expect, got
        );
    }
    ok
}

/// Exact percentiles of one run, in nanoseconds (nearest rank).
pub struct Stats {
    /// Mean.
    pub mean: f64,
    /// Median.
    pub p50: f64,
    /// 90th percentile.
    pub p90: f64,
    /// 99th percentile.
    pub p99: f64,
    /// 99.9th percentile.
    pub p999: f64,
    /// 99.99th percentile.
    pub p9999: f64,
    /// Maximum.
    pub max: f64,
}

impl Stats {
    /// Sorts `ticks` and reads the percentiles from it.
    pub fn new(ticks: &mut [u64], clock: &Clock) -> Self {
        ticks.sort_unstable();
        let at = |ppm: u64| clock.to_ns(ticks[nearest_rank(ticks.len(), ppm)]);
        let sum: u128 = ticks.iter().map(|&t| u128::from(t)).sum();
        Self {
            mean: clock.to_ns((sum / ticks.len().max(1) as u128) as u64),
            p50: at(500_000),
            p90: at(900_000),
            p99: at(990_000),
            p999: at(999_000),
            p9999: at(999_900),
            max: at(1_000_000),
        }
    }
}

/// Index of the quantile `ppm` (in parts per million) in `n` sorted samples: the smallest
/// value with at least `ppm * n / 10^6` samples at or below it. Integer arithmetic, so no
/// rounding error moves a rank; the Java harness uses the same formula.
pub fn nearest_rank(n: usize, ppm: u64) -> usize {
    let rank = (u128::from(ppm) * n as u128).div_ceil(1_000_000) as usize;
    rank.clamp(1, n.max(1)) - 1
}

/// Column names of `results.csv`.
pub const CSV_HEADER: &str = "engine,scenario,round,run,commands,throughput_mcmd_s,\
chunk_median_mcmd_s,mean_ns,p50_ns,p90_ns,p99_ns,p99_9_ns,p99_99_ns,max_ns,timer_overhead_ns,verified,core";

/// Where rows go: `$CMP_RESULTS`, or `compare/results/results.csv`.
pub fn results_path() -> PathBuf {
    std::env::var_os("CMP_RESULTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../results/results.csv"))
}

fn append_row(
    ctx: &RowContext<'_>,
    run: usize,
    commands: usize,
    throughput: &Throughput,
    s: &Stats,
    verified: bool,
) {
    let path = results_path();
    let mut line = String::new();
    let _ = write!(
        line,
        "{},{},{},{run},{commands},{:.4},{:.4},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{:.1},{},{}",
        ctx.engine,
        ctx.scenario,
        ctx.round,
        throughput.overall,
        throughput.chunk_median,
        s.mean,
        s.p50,
        s.p90,
        s.p99,
        s.p999,
        s.p9999,
        s.max,
        ctx.overhead,
        if verified { "yes" } else { "no" },
        ctx.core.replace(',', ";"),
    );
    append_line(&path, &line).expect("append results row");
}

/// Appends `line` to the CSV at `path`, writing the column names first if it is new.
pub fn append_line(path: &Path, line: &str) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let fresh = std::fs::metadata(path).map_or(true, |m| m.len() == 0);
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if fresh {
        writeln!(file, "{CSV_HEADER}")?;
    }
    writeln!(file, "{line}")
}

/// Pins the thread to `$CMP_CORE`, or to the last core like the latency benchmark, and
/// describes where it landed.
fn pin() -> Option<String> {
    let cores = core_affinity::get_core_ids()?;
    let core = match std::env::var("CMP_CORE").ok().and_then(|c| c.parse().ok()) {
        Some(id) => *cores.iter().find(|c| c.id == id)?,
        None => *cores.last()?,
    };
    if !core_affinity::set_for_current(core) {
        return None;
    }
    Some(match clock::core_type() {
        Some(kind) => format!("{} ({kind})", core.id),
        None => core.id.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::nearest_rank;

    #[test]
    fn nearest_rank_bounds() {
        assert_eq!(nearest_rank(1, 500_000), 0);
        assert_eq!(nearest_rank(4, 500_000), 1);
        assert_eq!(nearest_rank(5, 500_000), 2);
        assert_eq!(nearest_rank(4, 1_000_000), 3);
        assert_eq!(nearest_rank(1000, 990_000), 989);
        assert_eq!(nearest_rank(2_000_000, 999_900), 1_999_799);
    }
}
