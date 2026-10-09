# Low-Latency Matching Engine

[![CI](https://github.com/kadircanyildirm-crypto/low-latency-matching-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/kadircanyildirm-crypto/low-latency-matching-engine/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A low-latency exchange matching engine in Rust, modelled on the LMAX architecture:
single-threaded deterministic matching, event sourcing, and a pipeline of pinned stages.

**Status:** Phase 1 of 7 (the matching core) is complete and hardened. See
[docs/ROADMAP.md](docs/ROADMAP.md). The reasoning behind every design decision is in
[docs/DESIGN.md](docs/DESIGN.md).

## Phase 1: the order book

`crates/orderbook` is a single-instrument limit order book with price-time priority.

| Property | How |
|---|---|
| Deterministic | Single-threaded, no clocks, no randomness. CI checks pinned fingerprints of the output on Linux, Windows and macOS (ARM). |
| No heap allocation on the hot path | All memory is reserved at construction. A counting global allocator verifies it, including the worst case for the id index. |
| No overflow by construction | `max_orders × max_order_qty` must fit in a `u64`, so no quantity sum can wrap. |
| No `unsafe` | `#![forbid(unsafe_code)]` in the library. |

### Semantics

| Command | Behaviour |
|---|---|
| `Limit` | Trades at the limit price or better, at each resting order's price. Time in force decides the rest: **GTC** rests the remainder, **IOC** cancels it, **FOK** fills completely or not at all (self-trade prevention included), and **post-only** never takes liquidity, not even after a modify. |
| Iceberg | A limit order with a display quantity rests showing only that much. Each used-up tranche is replenished at the back of the queue, losing time priority; market data never sees the hidden part. |
| `Stop` | Waits off the book until a trade reaches its trigger, then becomes a market order or, with a limit price, a GTC limit order. Any price the command traded at counts, and released stops can trigger more in a fixed, documented order. |
| `Market` | Trades at any price within price protection; the remainder is cancelled. Never rests. |
| `Cancel` | Removes a resting order. Only its owner may cancel it. |
| `Modify` | FIX cancel/replace on **total** quantity, so a modify that races a fill can never over-fill. Shrinking at the same price keeps queue priority; anything else re-enters at the back. Only the owner may modify. |
| `CancelAll` | Cancels every resting order of one owner, as on a session disconnect. It costs O(k log k) in that owner's k orders, however large the rest of the book. |
| `SetPhase` | Moves the book between continuous trading, call phases, halts and the close. In a call, limit orders rest without matching and the book may cross; leaving it uncrosses the book at one price: the most volume, then the least surplus, then market pressure, then the closest to the reference price. Halts and the close accept only cancels. |

Risk controls built into the core:

- **Static price band:** prices outside it are rejected.
- **Maximum order size.**
- **Price protection:** market orders stop a configurable number of ticks beyond the
  opposite best price; limit orders priced further through are rejected.
- **Price band:** the same, measured from the last trade price instead, so stale orders
  cannot move it. A measured comparison of both is in
  [DESIGN.md §6](docs/DESIGN.md#6-risk-controls-in-the-core). Optionally, a market order
  the band stops starts a call, whose uncross re-anchors the band; measured in
  [DESIGN.md §7](docs/DESIGN.md#7-trading-phases-and-auctions).
- **Self-trade prevention:** cancel the resting order, or cancel the incoming one. The
  uncross does not apply it.
- **Ownership checks:** cancels and modifies of another participant's order are
  rejected, in a way that does not reveal the order exists. Owner ids are dense
  participant indices assigned by the gateway, so per-owner state needs no hashing.

Every trade carries a gap-free trade id and both sides' remaining open quantity, so
participants can track their orders from events alone.

### Snapshots and state digest

`snapshot()` captures the book's complete state, and `restore()` rebuilds an identical
book from it without replaying commands. A property test takes a snapshot at a random
point, restores it, and requires the copy to emit exactly the original's events from then
on, so nothing the book depends on can be left out. `digest()` hashes the same state into
64 bits that are identical on every platform, without allocating. A replica or a replay
can compare it against the live book. Details are in
[DESIGN.md §10](docs/DESIGN.md#10-snapshots-and-state-digest).

### Data structures

- **Price ladder:** each side is a dense array of levels indexed by
  `price - min_price`.
- **Occupancy bitset:** a two-level bitset finds the next best level when one empties.
- **Order queues:** orders sit in a preallocated slab, linked into intrusive FIFO queues
  by `u32` slot. Insert, cancel and fill are all O(1).

The trade-offs, including the ladder's memory limit on very wide price bands, are in
[DESIGN.md §3](docs/DESIGN.md#3-book-structure).

### Verification

| Layer | What it shows |
|---|---|
| Scenario tests | One rule per test, with the exact expected event sequence. |
| Differential property test | On random configurations and command sequences, the engine matches a deliberately naive reference book event for event and order for order. |
| Specification checker | After every command, checks the outcome against the rules without relying on a second implementation: price-time priority, no trading through the limit or protection cap, no self-trades, maximal fills, quantity conservation, and untouched orders unchanged. A command must be rejected exactly when a rule requires it, with that rule's reason. FOK orders fill exactly when the book could fill them; post-only orders never trade; icebergs trade only what they show; exactly the stops a command reaches trigger, in the right order; each uncross trades at one price no candidate beats under the auction rules, in priority order on both sides; no command's work exceeds what the book in front of it allows. |
| Invariant checker tests | `validate()` itself is tested: every corruption it checks for, from broken queue links to owner lists out of step with the book, must be detected. |
| Soak tests | Hundreds of thousands of commands of multi-participant flow under both self-trade policies, price protection, a price band, and trading sessions with calls, uncrosses, halts and the close. |
| Snapshot tests | A book restored from a snapshot taken at a random point continues exactly like the original; the digest changes with every field of the state. |
| Zero-allocation tests | Normal flow, a permanently full book, a deep book, mass cancels, every time in force, icebergs, stop cascades, phase changes and uncrosses, and computing the digest. |
| Fuzzing | [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (libFuzzer) steers inputs toward code they have not reached yet. Three targets: the differential test over any configuration, bands out to the ends of `i64` included; the snapshot round trip; and `restore` on arbitrary snapshots, which must never panic and must accept exactly the snapshots the documented rules allow. Debug assertions and overflow checks stay on. Each target runs 30 seconds on every push and 20 minutes every week. |
| Formal proofs | [Kani](https://github.com/model-checking/kani) checks every input within stated bounds: the bitset searches agree with a linear scan, the order pool's free list is a LIFO stack of exactly the free slots, the iceberg fill arithmetic is exact, and each owner's list holds its orders in the order they started resting. |
| Coverage | The tests run **99.86%** of the engine's lines (3 of 2,076 missed) and **96.1%** of its branches (13 of 334 missed), measured with [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov) in CI. What they miss is defensive code, such as `validate()`'s overflow error that the capacity rule makes unreachable. |
| Mutation testing | [`cargo-mutants`](https://mutants.rs) injects small faults into the engine; see [results](#mutation-testing). |

Random inputs are biased toward where bugs live: duplicate and unknown ids, shared
owners, prices at bitset word boundaries and band edges, and quantities at `0`,
`max_order_qty`, `max_order_qty + 1` and `u64::MAX`. What each fuzz target and proof
checks, its bounds, and how to run it are in
[DESIGN.md §11](docs/DESIGN.md#fuzzing-and-formal-verification).

### Mutation testing

[`cargo-mutants`](https://mutants.rs) makes small changes to the engine, such as `<`
to `<=`, `+` to `-`, or a function body replaced with a default value. It then runs the
whole test suite against each changed version. A mutant that leaves every test passing
points at behaviour the tests do not pin down.

| Outcome | Mutants | Meaning |
|---|---:|---|
| Caught | 296 | A test failed. |
| Timed out | 23 | The mutant made the tests hang, so it was detected too. For example, if `level_emptied` does nothing, an empty level stays the best price and the matching loop never leaves it. |
| Unviable | 21 | The mutant does not compile. |
| **Missed** | **0** | |

Every one of the 319 mutants that compile was detected in the last full run, which
includes the snapshot module's own run. `src/workload.rs`, the benchmark's order-flow
generator, is excluded because it is not part of the engine. Every change since, up to
stop orders, has been mutation-tested on the lines it touches (`cargo mutants --in-diff`),
and none has left a mutant alive. Trading phases have not been through a run yet. A full
run is repeated at the end of each milestone, and a scheduled workflow repeats it every
week, failing on any missed mutant.

The first full run missed two mutants. Both changed the bound in a `.min(last)` clamp in
`protection_cap`. The clamp turned out to have no effect: the matcher compares levels
against the cap, and no level lies beyond the last one. The clamp was removed, and a
rerun of `protection_cap`'s remaining mutants caught all of them.

### Performance

Service time of `OrderBook::process` per command, on one thread pinned to a core, with
commands issued back to back. It is measured with the TSC and HdrHistogram. For each
scenario, a participant-like generator records a command stream, which is then replayed
into fresh books: 3 runs × 2M measured commands after warm-up.

Numbers from a development laptop (Intel Core i5-12450H, Windows 11, untuned, no core
isolation), pinned to a performance core (`LAT_CORE=2`). The ~14 ns timer overhead is
included. Throughput comes from a separate pass without per-command timers.

| Scenario | Book after warm-up | p50 | p90 | p99 | p99.9 | p99.99 | Throughput |
|---|---|---:|---:|---:|---:|---:|---:|
| baseline | 7.7k orders, 243 levels | 58 ns | 83 ns | 130 ns | 199 ns | 1.1 µs | 21.8–24.6M cmd/s |
| sweep: 40% aggressive flow, multi-level fills | 1.6k orders, 165 levels | 58 ns | 90 ns | 136 ns | 187 ns | 0.6 µs | 23.1–23.8M cmd/s |
| deep | 1M orders, 10.2k levels | 165 ns | 296 ns | 798 ns | 1.1 µs | 10.9 µs | 6.3–6.7M cmd/s |
| protected: 2-tick protection, 4 owners, every order type | 6.5k orders, 182 levels | 63 ns | 104 ns | 254 ns | 661 ns | 1.6 µs | 19.2–20.1M cmd/s |
| sessions: calls, halts and the close | 3.5k orders, 243 levels | 51 ns | 68 ns | 128 ns | 1.9 µs | 4.1 µs | 28.4–29.2M cmd/s |

The protected scenario runs the paths the others never reach: IOC, FOK, post-only,
icebergs and stops. About 7% of its commands are rejected, about a quarter of its market
orders stop at the protection band, and self-trade prevention removes about 270k resting
orders per run. The sessions scenario changes phase two commands in a hundred, under a
10-tick band that starts a call whenever it stops a market order: about 15,000 calls per
run. A phase change takes 36 ns at p50 and 3.2 µs at p99, uncross included, and
those uncrosses set the scenario's p99.9.

On the last logical core, an efficiency core (E-core), the same session measured 89 ns at
p50 and 191 ns at p99 for baseline (15.7–16.0M cmd/s) and 222 ns and 868 ns for deep
(3.3–4.7M cmd/s).

By command, in the baseline scenario:

| Command | p50 | p99 | p99.9 |
|---|---:|---:|---:|
| limit | 58 ns | 124 ns | 185 ns |
| market | 65 ns | 169 ns | 239 ns |
| cancel | 57 ns | 87 ns | 191 ns |
| modify | 94 ns | 158 ns | 276 ns |

- **The deep book is slower because it does not fit in cache.** A million 48-byte order
  nodes take about 48 MB, and the id index another 32 MB, against a 12 MB L3. Cancels and
  modifies touch arbitrary orders and pay for the misses. A custom id index that finds
  an order in one cache line instead of two
  ([DESIGN.md §13](docs/DESIGN.md#13-performance-work)) cut the deep scenario's p50 by
  about a fifth.
- **The tail is the machine, not the engine.** p99.99 moves by a factor of two or three
  from run to run, and the maximum reaches milliseconds: preemption by the OS. Even
  medians move by several percent between sessions on this laptop, so only
  same-session comparisons are meaningful. Measurements on an isolated Linux core are
  part of Phase 5.
- **What this does not measure:** network, serialization, journaling or queueing. This
  is the matching core alone, in a closed loop. End-to-end, open-loop latency (with
  coordinated-omission correction) arrives with the gateway and pipeline phases.

### Compared with other engines

[docs/COMPARISON.md](docs/COMPARISON.md) replays one recorded order flow (GTC limit,
market, cancel and price-move commands only, our extra controls off) through this engine,
[exchange-core](https://github.com/exchange-core/exchange-core) (Java, its matching core),
[liquibook](https://github.com/enewhuis/liquibook) (C++) and
[OrderBook-rs](https://github.com/joaquinbejar/OrderBook-rs) (Rust), and checks that all
four produce exactly the same trades and final book. On an otherwise idle laptop, every
engine pinned to the same P-core, median of three rounds:

| Scenario | ours | exchange-core | liquibook | OrderBook-rs |
|---|---:|---:|---:|---:|
| baseline | 27.6M cmd/s, p50 55 ns | 16.3M (0.59x) | 3.48M (0.13x) | 0.94M (0.03x) |
| deep (1M orders) | 7.47M cmd/s, p50 143 ns | 5.09M (0.68x) | 0.27M (0.04x) | 0.04M (0.01x) |

exchange-core comes closest on the million-order book, where both engines are bound by
cache misses. The method, all four scenarios, latency percentiles and caveats are in the
document; `compare/run.sh` reruns everything.

## Running

```sh
cargo test                                # all tests
cargo bench --bench latency               # latency per scenario; writes target/latency/*.hgrm
LAT_CORE=2 cargo bench --bench latency    # the same, pinned to logical core 2 instead of the last
cargo bench --bench throughput            # Criterion before/after comparison (includes generator cost)
cargo mutants -p orderbook --exclude crates/orderbook/src/workload.rs   # mutation testing
cargo +nightly fuzz run differential -s none -a -- -max_total_time=60 -len_control=0   # fuzzing
(cd crates/orderbook && cargo kani)                                     # proofs (Linux, macOS)
cargo +nightly llvm-cov -p orderbook --branch --ignore-filename-regex 'workload\.rs' --summary-only   # coverage
compare/run.sh fetch && compare/run.sh export && compare/run.sh all   # comparison with other engines
```

The fuzz targets are `differential`, `snapshot_roundtrip` and `restore`. Each needs its
tool first: `cargo install cargo-fuzz`, `cargo install --locked kani-verifier && cargo kani
setup`, or `cargo install cargo-llvm-cov` with the nightly `llvm-tools-preview` component.
Windows needs a different fuzzing setup, described in
[DESIGN.md §11](docs/DESIGN.md#fuzzing-and-formal-verification).

The `.hgrm` files can be plotted with the
[HdrHistogram plotter](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html)
(values are in microseconds).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
