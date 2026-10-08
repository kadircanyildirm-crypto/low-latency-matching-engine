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
| No heap allocation on the hot path | All memory is reserved at construction. A counting global allocator verifies it, including the worst case for the hash index. |
| No overflow by construction | `max_orders × max_order_qty` must fit in a `u64`, so no quantity sum can wrap. |
| No `unsafe` | `#![forbid(unsafe_code)]` in the library. |

### Semantics

| Command | Behaviour |
|---|---|
| `Limit` | Good-till-cancelled. Trades at the limit price or better, at each resting order's price; the remainder rests. |
| `Market` | Trades at any price within price protection; the remainder is cancelled. Never rests. |
| `Cancel` | Removes a resting order. Only its owner may cancel it. |
| `Modify` | FIX cancel/replace on **total** quantity, so a modify that races a fill can never over-fill. Shrinking at the same price keeps queue priority; anything else re-enters at the back. Only the owner may modify. |

Risk controls built into the core:

- **Static price band:** prices outside it are rejected.
- **Maximum order size.**
- **Price protection:** market orders stop a configurable number of ticks beyond the
  opposite best price; limit orders priced further through are rejected.
- **Self-trade prevention:** cancel the resting order, or cancel the incoming one.
- **Ownership checks:** cancels and modifies of another participant's order are
  rejected, in a way that does not reveal the order exists.

Every trade carries a gap-free trade id and both sides' remaining open quantity, so
participants can track their orders from events alone.

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
| Specification checker | After every command, checks the outcome against the rules without relying on a second implementation: price-time priority, no trading through the limit or protection cap, no self-trades, maximal fills, quantity conservation, and untouched orders unchanged. A command must be rejected exactly when a rule requires it, with that rule's reason. |
| Invariant checker tests | `validate()` itself is tested: each of 11 kinds of corrupted state must be detected. |
| Soak tests | Hundreds of thousands of commands of multi-participant flow under both self-trade policies. |
| Zero-allocation tests | Normal flow, a permanently full book, and a deep book. |
| Mutation testing | [`cargo-mutants`](https://mutants.rs) injects small faults into the engine; see [results](#mutation-testing). |

Random inputs are biased toward where bugs live: duplicate and unknown ids, shared
owners, prices at bitset word boundaries and band edges, and quantities at `0`,
`max_order_qty`, `max_order_qty + 1` and `u64::MAX`.

### Mutation testing

[`cargo-mutants`](https://mutants.rs) makes small changes to the engine, such as `<`
to `<=`, `+` to `-`, or a function body replaced with a default value. It then runs the
whole test suite against each changed version. A mutant that leaves every test passing
points at behaviour the tests do not pin down.

| Outcome | Mutants | Meaning |
|---|---:|---|
| Caught | 274 | A test failed. |
| Timed out | 23 | The mutant made the tests hang, so it was detected too. For example, if `level_emptied` does nothing, an empty level stays the best price and the matching loop never leaves it. |
| Unviable | 19 | The mutant does not compile. |
| **Missed** | **0** | |

Every one of the 297 mutants that compile is detected. `src/workload.rs`, the benchmark's
order-flow generator, is excluded because it is not part of the engine.

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
isolation). The ~25 ns timer overhead is included. Throughput comes from a separate pass
without per-command timers.

| Scenario | Book after warm-up | p50 | p90 | p99 | p99.9 | p99.99 | Throughput |
|---|---|---:|---:|---:|---:|---:|---:|
| baseline | 7.7k orders, 243 levels | 79 ns | 120 ns | 193 ns | 497 ns | 3.9 µs | 16–18M cmd/s |
| sweep: 40% aggressive flow, multi-level fills | 1.6k orders, 165 levels | 78 ns | 129 ns | 205 ns | 693 ns | 2.4 µs | 16–17M cmd/s |
| deep | 1M orders, 10k levels | 343 ns | 794 ns | 2.3 µs | 4.7 µs | 41 µs | 3.0–3.3M cmd/s |
| protected: 2-tick protection, 4 owners | 2.6k orders, 170 levels | 78 ns | 120 ns | 191 ns | 284 ns | 2.3 µs | 19–20M cmd/s |

The protected scenario runs the paths the others never reach. About 5% of its commands are
rejected, about a quarter of its market orders stop at the protection band, and
self-trade prevention removes about 220k resting orders per run.

By command, in the baseline scenario:

| Command | p50 | p99 | p99.9 |
|---|---:|---:|---:|
| limit | 71 ns | 171 ns | 334 ns |
| market | 97 ns | 222 ns | 491 ns |
| cancel | 94 ns | 199 ns | 698 ns |
| modify | 129 ns | 268 ns | 798 ns |

- **The deep book is slower because it does not fit in cache.** A million 48-byte order
  nodes take about 48 MB, on top of the id index, against a 12 MB L3. Cancels and
  modifies touch arbitrary orders and pay for the misses.
- **The tail is the machine, not the engine.** p99.99 moves between 2.5 and 7 µs from run
  to run in the baseline scenario, and the maximum reaches tens of milliseconds:
  preemption by the OS. Measurements on an isolated Linux core are part of Phase 5.
- **What this does not measure:** network, serialization, journaling or queueing. This
  is the matching core alone, in a closed loop. End-to-end, open-loop latency (with
  coordinated-omission correction) arrives with the gateway and pipeline phases.

## Running

```sh
cargo test                                # all tests
cargo bench --bench latency               # latency per scenario; writes target/latency/*.hgrm
cargo bench --bench throughput            # Criterion before/after comparison (includes generator cost)
cargo mutants -p orderbook --exclude crates/orderbook/src/workload.rs   # mutation testing
```

The `.hgrm` files can be plotted with the
[HdrHistogram plotter](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html)
(values are in microseconds).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
