//! Feeds a recorded command stream into an engine until the stream runs out or the process
//! is killed, printing `durable <seq>` after every batch: the highest sequence number on
//! stable storage at that moment. `tests/kill.rs` kills it at random points and checks
//! what recovery makes of the directory.
//!
//! Usage: `engine-soak <dir> <stream> <always|os> <segment capacity> <snapshot every, 0 for
//! never> <seed>`
//!
//! The stream file holds an encoded snapshot of the empty book, which carries the book
//! configuration, preceded by its length as a little-endian u32, and then the commands, each
//! encoded in `COMMAND_SIZE` bytes. The seed picks the batch sizes.

use std::io::Write;

use engine::codec::{COMMAND_SIZE, decode_command, decode_snapshot};
use engine::{Engine, EngineConfig, SyncPolicy};
use orderbook::workload::SplitMix64;
use orderbook::{Command, Event};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let [_, dir, stream, sync, capacity, every, seed] = &args[..] else {
        eprintln!(
            "usage: engine-soak <dir> <stream> <always|os> <segment capacity> <snapshot every> <seed>"
        );
        std::process::exit(2);
    };
    let bytes = std::fs::read(stream).expect("read the stream");
    let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
    let config = decode_snapshot(&bytes[4..4 + len])
        .expect("decode the configuration")
        .config;
    let commands: Vec<Command> = bytes[4 + len..]
        .chunks_exact(COMMAND_SIZE)
        .map(|chunk| decode_command(chunk.try_into().unwrap()).expect("decode a command"))
        .collect();

    let every: u64 = every.parse().expect("snapshot interval");
    let config = EngineConfig {
        sync: match sync.as_str() {
            "always" => SyncPolicy::Always,
            "os" => SyncPolicy::Os,
            other => panic!("unknown sync policy {other}"),
        },
        segment_capacity: capacity.parse().expect("segment capacity"),
        snapshot_every: (every > 0).then_some(every),
        ..EngineConfig::new(config)
    };
    let (mut engine, _) = Engine::open(dir, config).expect("open the engine");
    let mut rng = SplitMix64::new(seed.parse().expect("seed"));
    let mut events: Vec<Event> = Vec::new();
    let mut next = engine.last_seq() as usize;
    let mut out = std::io::stdout().lock();
    while next < commands.len() {
        let n = (1 + rng.below(16) as usize).min(commands.len() - next);
        engine
            .submit_batch(&commands[next..next + n], &mut events)
            .expect("submit");
        events.clear();
        next += n;
        writeln!(out, "durable {}", engine.durable_seq()).expect("report");
        out.flush().expect("report");
    }
}
