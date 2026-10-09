//! The acceptance test of Phase 2: a process killed at a random point returns, through
//! recovery, to exactly the state its journaled commands lead to.
//!
//! A child process (`engine-soak`) feeds a recorded command stream into an engine on the real
//! file system and reports what is durable after every batch. The test kills it after a
//! random delay, sometimes in the middle of a write, a sync, a segment roll or a snapshot,
//! then opens the directory itself and checks that recovery kept at least everything the
//! child had reported durable, rebuilt exactly the state after the commands it kept, and
//! that finishing the stream from there ends in the state of a run that was never killed.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command as Process, Stdio};
use std::time::Duration;

use engine::codec::{COMMAND_SIZE, encode_command, encode_snapshot};
use engine::{Engine, EngineConfig, SyncPolicy};
use orderbook::workload::SplitMix64;
use orderbook::{BookConfig, Command, OrderBook};

/// A directory under the system's temporary directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path = std::env::temp_dir().join(format!("engine-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Writes the stream file `engine-soak` reads.
fn write_stream(path: &Path, config: BookConfig, commands: &[Command]) {
    let mut snapshot = Vec::new();
    encode_snapshot(&OrderBook::new(config).snapshot(), &mut snapshot);
    let mut bytes = (snapshot.len() as u32).to_le_bytes().to_vec();
    bytes.extend_from_slice(&snapshot);
    for command in commands {
        let mut encoded = [0; COMMAND_SIZE];
        encode_command(command, &mut encoded);
        bytes.extend_from_slice(&encoded);
    }
    std::fs::write(path, bytes).unwrap();
}

#[test]
fn a_killed_process_recovers_exactly() {
    const LEN: usize = 60_000;
    let (book, commands) = common::flow(7, LEN);
    let digests = common::digests(book, &commands);
    let temp = TempDir::new("kill");
    let stream = temp.0.join("stream.bin");
    write_stream(&stream, book, &commands);

    let mut rng = SplitMix64::new(0x4B11);
    let mut killed_mid_stream = 0;
    for run in 0..16 {
        let dir = temp.0.join(format!("run-{run}"));
        let sync = if run % 2 == 0 { "always" } else { "os" };
        let capacity = [64, 1_000, 4_096][run % 3];
        let every = [0, 500, 5_000][(run / 3) % 3];
        let config = EngineConfig {
            sync: if sync == "always" {
                SyncPolicy::Always
            } else {
                SyncPolicy::Os
            },
            segment_capacity: capacity,
            snapshot_every: (every > 0).then_some(every),
            ..EngineConfig::new(book)
        };
        // Kill the same directory's process several times, recovering in between.
        let mut reported = 0;
        for kill in 0..3 {
            let mut child = Process::new(env!("CARGO_BIN_EXE_engine-soak"))
                .args([
                    dir.to_str().unwrap(),
                    stream.to_str().unwrap(),
                    sync,
                    &capacity.to_string(),
                    &every.to_string(),
                    &(run * 3 + kill).to_string(),
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            std::thread::sleep(Duration::from_millis(20 + rng.below(250)));
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            if let Some(last) = stdout
                .lines()
                .filter_map(|line| line.strip_prefix("durable "))
                .filter_map(|seq| seq.parse::<usize>().ok())
                .next_back()
            {
                reported = reported.max(last);
            }

            let (engine, report) = Engine::open(&dir, config)
                .unwrap_or_else(|e| panic!("run {run}, kill {kill}: {e}"));
            let kept = engine.last_seq() as usize;
            assert!(
                kept >= reported,
                "run {run}, kill {kill}: reported {reported} durable, recovered {kept}"
            );
            assert_eq!(
                engine.book().digest(),
                digests[kept],
                "run {run}, kill {kill}: the state after {kept} commands ({report:?})"
            );
            killed_mid_stream += usize::from(kept < LEN);
        }

        // Finish the stream in this process.
        let (mut engine, _) = Engine::open(&dir, config).unwrap();
        let mut events = Vec::new();
        let start = engine.last_seq() as usize;
        for chunk in commands[start..].chunks(256) {
            engine.submit_batch(chunk, &mut events).unwrap();
            events.clear();
        }
        assert_eq!(engine.book().digest(), digests[LEN], "run {run}");
    }
    // Most kills land before the stream ends, or the test checks little.
    assert!(
        killed_mid_stream > 24,
        "{killed_mid_stream} of 48 kills mid-stream"
    );
}
