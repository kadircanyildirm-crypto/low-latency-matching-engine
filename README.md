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
| Deterministic | Single-threaded, no clocks, no randomness. CI checks a pinned fingerprint of the output on Linux, Windows and macOS (ARM). |
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
| Specification checker | After every command, checks the outcome against the rules without relying on a second implementation: price-time priority, no trading through the limit or protection cap, no self-trades, maximal fills, quantity conservation, untouched orders unchanged, and every rejection justified. |
| Invariant checker tests | `validate()` itself is tested: each of 11 kinds of corrupted state must be detected. |
| Soak tests | Hundreds of thousands of commands of multi-participant flow under both self-trade policies. |
| Zero-allocation tests | Normal flow, a permanently full book, and a deep book. |
| Mutation testing | [`cargo-mutants`](https://mutants.rs) injects small faults into the engine; see [results](#mutation-testing). |

Random inputs are biased toward where bugs live: duplicate and unknown ids, shared
owners, prices at bitset word boundaries and band edges, and quantities at `0`,
`max_order_qty`, `max_order_qty + 1` and `u64::MAX`.

### Mutation testing

RESULTS_PLACEHOLDER

### Performance

PERFORMANCE_PLACEHOLDER

## Running

```sh
cargo test                                # all tests
cargo bench --bench latency               # latency per scenario; writes target/latency/*.hgrm
cargo bench --bench throughput            # Criterion before/after comparison (includes generator cost)
cargo mutants -p orderbook --exclude src/workload.rs   # mutation testing
```

The `.hgrm` files can be plotted with the
[HdrHistogram plotter](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html)
(values are in microseconds).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
