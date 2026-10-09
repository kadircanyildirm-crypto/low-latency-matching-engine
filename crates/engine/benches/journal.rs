//! What journaling costs: per-command latency and throughput of `Engine::submit` under each
//! sync policy and batch size, against the book alone; how fast recovery replays; and how
//! long a snapshot takes to write and load.
//!
//! Runs on the real file system, in a directory under the system's temporary directory
//! (`JOURNAL_DIR` overrides it, to measure another disk). The order flow is the latency
//! benchmark's baseline scenario, recorded once and replayed.
//!
//! Run: `cargo bench -p engine --bench journal`
//! Env: `JOURNAL_DIR`, `JOURNAL_CORE` (logical core to pin to; default the last one).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use engine::{Engine, EngineConfig, SyncPolicy};
use hdrhistogram::Histogram;
use orderbook::workload::{Workload, WorkloadConfig};
use orderbook::{BookConfig, Command, Event, EventSink, OrderBook};

/// Counts events, so the book's work is not optimised away and nothing allocates.
#[derive(Default)]
struct Count(u64);

impl EventSink for Count {
    #[inline]
    fn on_event(&mut self, _: Event) {
        self.0 += 1;
    }
}

fn record(len: usize) -> (BookConfig, Vec<Command>) {
    let workload = WorkloadConfig::default();
    let config = workload.book_config();
    let mut generator = Workload::new(workload);
    let mut book = OrderBook::new(config);
    let mut events = Vec::new();
    let commands = (0..len)
        .map(|_| {
            let command = generator.next_command();
            events.clear();
            book.process(command, &mut events);
            events.iter().for_each(|event| generator.observe(event));
            command
        })
        .collect();
    (config, commands)
}

fn fresh_dir(root: &Path, name: &str) -> PathBuf {
    let dir = root.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

struct Run {
    label: String,
    batch: usize,
    /// Latency of each submit call, in ns.
    calls: Histogram<u64>,
    commands: usize,
    elapsed: Duration,
}

impl Run {
    fn print(&self) {
        let per = |q: f64| self.calls.value_at_quantile(q) as f64 / self.batch as f64;
        println!(
            "  {:<32} {:>9.1} {:>10.0} {:>10.0} {:>10.0} {:>11.0} {:>12.0}",
            self.label,
            self.commands as f64 / self.elapsed.as_secs_f64() / 1e3,
            per(0.5),
            per(0.99),
            per(0.999),
            self.calls.value_at_quantile(0.5) as f64,
            self.calls.value_at_quantile(0.99) as f64,
        );
    }
}

/// Submits `commands` in batches of `batch` through an engine in `dir`, after `warmup`
/// unmeasured ones.
fn measure(
    label: &str,
    dir: &Path,
    config: EngineConfig,
    commands: &[Command],
    warmup: usize,
    batch: usize,
) -> Run {
    let (mut engine, _) = Engine::open(dir, config).unwrap();
    let mut sink = Count::default();
    for chunk in commands[..warmup].chunks(batch) {
        engine.submit_batch(chunk, &mut sink).unwrap();
    }
    let measured = &commands[warmup..];
    let mut calls = Histogram::<u64>::new(3).unwrap();
    let start = Instant::now();
    for chunk in measured.chunks(batch) {
        let t = Instant::now();
        engine.submit_batch(chunk, &mut sink).unwrap();
        calls.record(t.elapsed().as_nanos() as u64).unwrap();
    }
    let elapsed = start.elapsed();
    std::hint::black_box(sink.0);
    Run {
        label: label.to_owned(),
        batch,
        calls,
        commands: measured.len(),
        elapsed,
    }
}

/// The book alone, for comparison: the same commands with no journal.
fn measure_book(config: BookConfig, commands: &[Command], warmup: usize) -> Run {
    let mut book = OrderBook::new(config);
    let mut sink = Count::default();
    for &command in &commands[..warmup] {
        book.process(command, &mut sink);
    }
    let mut calls = Histogram::<u64>::new(3).unwrap();
    let start = Instant::now();
    for &command in &commands[warmup..] {
        let t = Instant::now();
        book.process(command, &mut sink);
        calls.record(t.elapsed().as_nanos() as u64).unwrap();
    }
    let elapsed = start.elapsed();
    std::hint::black_box(sink.0);
    Run {
        label: "book alone, no journal".to_owned(),
        batch: 1,
        calls,
        commands: commands.len() - warmup,
        elapsed,
    }
}

fn main() {
    let root = std::env::var_os("JOURNAL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("journal-bench-{}", std::process::id()))
        });
    std::fs::create_dir_all(&root).unwrap();
    let core = pin();
    println!("journal cost (real file system, one thread)");
    println!("  directory : {}", root.display());
    println!(
        "  pinned to : {}",
        core.map_or("none".into(), |c| c.to_string())
    );
    println!("  os        : {}", std::env::consts::OS);

    let (book, commands) = record(1_100_000);
    let base = EngineConfig::new(book);
    let warmup = 100_000;

    println!();
    println!(
        "  {:<32} {:>9} {:>10} {:>10} {:>10} {:>11} {:>12}",
        "", "k cmd/s", "p50 ns/cmd", "p99 ns/cmd", "p99.9", "p50 ns/call", "p99 ns/call"
    );
    measure_book(book, &commands, warmup).print();
    let os = EngineConfig {
        sync: SyncPolicy::Os,
        ..base
    };
    for batch in [1, 64] {
        let dir = fresh_dir(&root, &format!("os-{batch}"));
        measure(
            &format!("os, batches of {batch}"),
            &dir,
            os,
            &commands,
            warmup,
            batch,
        )
        .print();
    }
    // Syncing every command is slow; fewer commands keep the run short.
    for (batch, len) in [(1, 5_000), (8, 20_000), (64, 100_000), (512, 400_000)] {
        let dir = fresh_dir(&root, &format!("always-{batch}"));
        measure(
            &format!("always, batches of {batch}"),
            &dir,
            base,
            &commands[..warmup + len],
            warmup,
            batch,
        )
        .print();
    }

    // Recovery: replaying a million journaled commands, then a snapshot of the book they
    // leave, written and loaded.
    let dir = fresh_dir(&root, "replay");
    {
        let (mut engine, _) = Engine::open(&dir, os).unwrap();
        let mut sink = Count::default();
        for chunk in commands[..1_000_000].chunks(4_096) {
            engine.submit_batch(chunk, &mut sink).unwrap();
        }
        engine.sync().unwrap();
    }
    let start = Instant::now();
    let (mut engine, report) = Engine::open(&dir, os).unwrap();
    let elapsed = start.elapsed();
    println!();
    println!(
        "  replay    : {} commands in {:.0} ms, {:.2} M cmd/s",
        report.journal.replayed,
        elapsed.as_secs_f64() * 1e3,
        report.journal.replayed as f64 / elapsed.as_secs_f64() / 1e6
    );
    let orders = engine.book().order_count();
    let start = Instant::now();
    engine.snapshot().unwrap();
    let written = start.elapsed();
    drop(engine);
    let start = Instant::now();
    let (_, report) = Engine::open(&dir, os).unwrap();
    let loaded = start.elapsed();
    assert_eq!(report.journal.replayed, 0);
    println!(
        "  snapshot  : {orders} orders written in {:.2} ms, loaded in {:.2} ms",
        written.as_secs_f64() * 1e3,
        loaded.as_secs_f64() * 1e3
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// Pins the thread to `$JOURNAL_CORE`, or to the last core.
fn pin() -> Option<usize> {
    let cores = core_affinity::get_core_ids()?;
    let core = match std::env::var("JOURNAL_CORE")
        .ok()
        .and_then(|c| c.parse().ok())
    {
        Some(id) => *cores.iter().find(|c| c.id == id)?,
        None => *cores.last()?,
    };
    core_affinity::set_for_current(core).then_some(core.id)
}
