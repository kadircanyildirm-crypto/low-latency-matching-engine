//! The ring against the standard library's bounded channel and crossbeam's, between two
//! threads pinned to two cores:
//!
//! - throughput: one thread sends 20 million `u64`s as fast as it can, the other receives
//!   them, one at a time or, for the ring, in batches;
//! - latency: a ping-pong through two queues, one each way, a million round trips, with
//!   the distribution of round-trip times.
//!
//! `RING_CORES=a,b` picks the cores; by default the last two logical cores. Which two
//! matters: two hyperthreads of one core share its caches, two cores of a cluster share an
//! L2, and others meet only in the L3. Every queue holds 1,024 items and every side spins
//! while it waits.

use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use hdrhistogram::Histogram;
use ring::{Wait, channel};

const ITEMS: u64 = 20_000_000;
const ROUND_TRIPS: u64 = 1_000_000;
const CAPACITY: usize = 1_024;

/// One row of a table.
type Run<T> = Box<dyn Fn() -> T>;

/// The mean round trip and the distribution of single ones.
type RoundTrips = (f64, Histogram<u64>);

/// One side of a queue, as the benchmark uses it.
trait Send1: Send + 'static {
    fn send(&mut self, value: u64);
}

trait Recv1: Send + 'static {
    fn recv(&mut self) -> u64;
}

impl Send1 for ring::Producer<u64> {
    fn send(&mut self, value: u64) {
        while self.try_push(value).is_err() {
            std::hint::spin_loop();
        }
    }
}

impl Recv1 for ring::Consumer<u64> {
    fn recv(&mut self) -> u64 {
        self.pop(Wait::Spin).expect("a value")
    }
}

impl Send1 for mpsc::SyncSender<u64> {
    fn send(&mut self, mut value: u64) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(mpsc::TrySendError::Full(back)) => value = back,
                Err(error) => panic!("{error}"),
            }
            std::hint::spin_loop();
        }
    }
}

impl Recv1 for mpsc::Receiver<u64> {
    fn recv(&mut self) -> u64 {
        loop {
            match self.try_recv() {
                Ok(value) => return value,
                Err(mpsc::TryRecvError::Empty) => std::hint::spin_loop(),
                Err(error) => panic!("{error}"),
            }
        }
    }
}

impl Send1 for crossbeam_channel::Sender<u64> {
    fn send(&mut self, mut value: u64) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(crossbeam_channel::TrySendError::Full(back)) => value = back,
                Err(error) => panic!("{error}"),
            }
            std::hint::spin_loop();
        }
    }
}

impl Recv1 for crossbeam_channel::Receiver<u64> {
    fn recv(&mut self) -> u64 {
        loop {
            match self.try_recv() {
                Ok(value) => return value,
                Err(crossbeam_channel::TryRecvError::Empty) => std::hint::spin_loop(),
                Err(error) => panic!("{error}"),
            }
        }
    }
}

fn cores() -> (Option<core_affinity::CoreId>, Option<core_affinity::CoreId>) {
    let all = core_affinity::get_core_ids().unwrap_or_default();
    let pick = |id: usize| all.iter().copied().find(|c| c.id == id);
    match std::env::var("RING_CORES").ok() {
        Some(list) => {
            let ids: Vec<usize> = list
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            (
                ids.first().and_then(|&id| pick(id)),
                ids.get(1).and_then(|&id| pick(id)),
            )
        }
        None if all.len() >= 2 => (Some(all[all.len() - 2]), Some(all[all.len() - 1])),
        None => (None, None),
    }
}

fn pin(core: Option<core_affinity::CoreId>) {
    if let Some(core) = core {
        core_affinity::set_for_current(core);
    }
}

/// Items per second from one thread to another, one item at a time.
fn throughput(mut sender: impl Send1, mut receiver: impl Recv1) -> f64 {
    let (a, b) = cores();
    let started = Instant::now();
    let producer = thread::spawn(move || {
        pin(a);
        for value in 0..ITEMS {
            sender.send(value);
        }
    });
    pin(b);
    for expected in 0..ITEMS {
        assert_eq!(receiver.recv(), expected);
    }
    producer.join().unwrap();
    ITEMS as f64 / started.elapsed().as_secs_f64()
}

/// The ring's batched operations: the producer writes up to 64 items per publish, the
/// consumer takes whatever is waiting.
fn ring_batched() -> f64 {
    let (mut producer, mut consumer) = channel::<u64>(CAPACITY);
    let (a, b) = cores();
    let started = Instant::now();
    let sender = thread::spawn(move || {
        pin(a);
        let mut next = 0;
        while next < ITEMS {
            let mut items = next..(next + 64).min(ITEMS);
            let pushed = producer.push_from(&mut items);
            if pushed == 0 {
                std::hint::spin_loop();
            }
            next += pushed as u64;
        }
    });
    pin(b);
    let mut expected = 0;
    while expected < ITEMS {
        consumer.wait(Wait::Spin);
        for value in consumer.drain(usize::MAX) {
            assert_eq!(value, expected);
            expected += 1;
        }
    }
    sender.join().unwrap();
    ITEMS as f64 / started.elapsed().as_secs_f64()
}

/// The mean round-trip time in nanoseconds of a value sent one way and back the other, from
/// the whole run, and the distribution of single round trips, as precise as the clock: on
/// Windows, 100 ns.
fn ping_pong(
    (mut ping, mut pinged): (impl Send1, impl Recv1),
    (mut pong, mut ponged): (impl Send1, impl Recv1),
) -> RoundTrips {
    let (a, b) = cores();
    let echo = thread::spawn(move || {
        pin(b);
        for _ in 0..ROUND_TRIPS {
            let value = pinged.recv();
            pong.send(value);
        }
    });
    pin(a);
    let mut histogram = Histogram::<u64>::new(3).unwrap();
    let started = Instant::now();
    for value in 0..ROUND_TRIPS {
        let sent = Instant::now();
        ping.send(value);
        assert_eq!(ponged.recv(), value);
        histogram.saturating_record(sent.elapsed().as_nanos() as u64);
    }
    let mean = started.elapsed().as_nanos() as f64 / ROUND_TRIPS as f64;
    echo.join().unwrap();
    (mean, histogram)
}

fn main() {
    let (a, b) = cores();
    println!(
        "two threads on cores {:?} and {:?}; queues of {CAPACITY}; {ITEMS} items, {ROUND_TRIPS} round trips",
        a.map(|c| c.id),
        b.map(|c| c.id)
    );
    println!();
    println!("{:<34} {:>14}", "throughput", "items/s");
    let rows: [(&str, Run<f64>); 4] = [
        ("ring, batches", Box::new(ring_batched)),
        (
            "ring, one at a time",
            Box::new(|| {
                let (p, c) = channel(CAPACITY);
                throughput(p, c)
            }),
        ),
        (
            "crossbeam-channel bounded",
            Box::new(|| {
                let (s, r) = crossbeam_channel::bounded(CAPACITY);
                throughput(s, r)
            }),
        ),
        (
            "std::sync::mpsc::sync_channel",
            Box::new(|| {
                let (s, r) = mpsc::sync_channel(CAPACITY);
                throughput(s, r)
            }),
        ),
    ];
    for (name, run) in rows {
        println!("{name:<34} {:>12.1} M", run() / 1e6);
    }

    println!();
    println!(
        "{:<34} {:>6} {:>6} {:>6} {:>6} {:>6} {:>8}",
        "round trip, ns", "mean", "p50", "p90", "p99", "p99.9", "max"
    );
    let rows: [(&str, Run<RoundTrips>); 3] = [
        (
            "ring",
            Box::new(|| ping_pong(channel(CAPACITY), channel(CAPACITY))),
        ),
        (
            "crossbeam-channel bounded",
            Box::new(|| {
                ping_pong(
                    crossbeam_channel::bounded(CAPACITY),
                    crossbeam_channel::bounded(CAPACITY),
                )
            }),
        ),
        (
            "std::sync::mpsc::sync_channel",
            Box::new(|| ping_pong(mpsc::sync_channel(CAPACITY), mpsc::sync_channel(CAPACITY))),
        ),
    ];
    for (name, run) in rows {
        let (mean, h) = run();
        println!(
            "{name:<34} {mean:>6.0} {:>6} {:>6} {:>6} {:>6} {:>8}",
            h.value_at_quantile(0.5),
            h.value_at_quantile(0.9),
            h.value_at_quantile(0.99),
            h.value_at_quantile(0.999),
            h.max()
        );
    }
}
