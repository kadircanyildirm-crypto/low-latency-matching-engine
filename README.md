# Low-Latency Matching Engine

[![CI](https://github.com/kadircanyildirm-crypto/low-latency-matching-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/kadircanyildirm-crypto/low-latency-matching-engine/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A low-latency exchange matching engine in Rust, modelled on the LMAX architecture:
single-threaded deterministic matching, event sourcing, and a pipeline of pinned stages.

**Status:** Phase 1 of 7 (the matching core) is complete. See [docs/ROADMAP.md](docs/ROADMAP.md).

## Phase 1: the order book

`crates/orderbook` is a single-instrument limit order book with price-time priority.

| Property | How |
|---|---|
| Deterministic | Single-threaded; the event stream is a pure function of the command stream. |
| No heap allocation on the hot path | All memory is reserved at construction; verified by a counting global allocator. |
| No `unsafe` | `#![forbid(unsafe_code)]` in the library. |
| Integer prices | Prices are tick counts (`i64`); no floating point in the book. |

### Semantics

| Command | Behaviour |
|---|---|
| `Limit` | Good-till-cancelled. Trades at the limit price or better, at the resting order's price; the remainder rests. |
| `Market` | Trades at any price; the unfilled remainder is cancelled (`NoLiquidity`). Never rests. |
| `Cancel` | Removes a resting order. |
| `Modify` | Same price with smaller or equal size keeps queue priority. Any other change re-enters at the back of the queue and may trade. |

Commands that fail validation are rejected with no side effects: `InvalidQuantity`,
`PriceOutOfRange`, `DuplicateOrderId`, `UnknownOrder`, `BookFull`.

### Data structures

```
 price ladder (one per side)          order slab (preallocated)
 index = price - min_price            free list: LIFO, cache-warm reuse
┌──────┬──────┬──────┬──────┐        ┌────┬────┬────┬────┬────┐
│ ...  │ L100 │ L101 │ ...  │        │ #7 │ #3 │ #9 │free│ #4 │
└──────┴──┬───┴──────┴──────┘        └────┴────┴────┴────┴────┘
          │ head/tail                  ▲ prev/next u32 links
          └──► #7 ⇄ #3 ⇄ #9  (FIFO) ────┘

 occupancy bitset: 1 bit per level + 1 summary bit per 64 words
 id index: FxHashMap<OrderId, slot>, reserved at 2x capacity
```

- **Finding a level:** one subtraction (`price - min_price`). The price band works like
  an exchange's static price collar.
- **Next best level** when one empties: a two-level bitset. This avoids scanning
  thousands of empty levels in a sparse book.
- **Insert, cancel and fill:** O(1) via intrusive doubly linked queues addressed by
  `u32` slot.
- **Id index:** reserved at twice the order capacity. Cleaning up tombstones is then
  always an in-place rehash, never a reallocation.

### Testing

| Test | What it proves |
|---|---|
| `tests/scenarios.rs` | One rule per test, asserting the exact event sequence. |
| `tests/differential.rs` | Property test: on 1,000 random command sequences, the engine emits exactly the events of a naive `BTreeMap`-based reference book and holds exactly the same orders in the same queue order. |
| `OrderBook::validate()` | Checked after every command in the differential test: queue links, per-level aggregates, bitset, best prices, id index, uncrossed book. |
| `tests/soak.rs` | 200k commands of realistic flow against the reference; a client rebuilds the set of resting orders from events alone. Also checks determinism. |
| `tests/zero_alloc.rs` | A counting global allocator observes **zero** allocations over 1,000,000 commands. |
| `src/bitset.rs` | Bitset search properties against `BTreeSet`. |

To check that the tests can actually catch bugs, a one-character fault was injected
into the engine's crossing check (`<=` → `<`). The differential test caught it, and
proptest shrank the failure to a 2-command counterexample.

### Performance

Service time of `OrderBook::process` per command, measured with the TSC and HdrHistogram
on a recorded stream replayed into a fresh book (warm-up 500k, measured 3M).

The book holds about 6.5k resting orders over a few hundred levels. The command mix is
roughly 55% passive limits, 10% aggressive limits, 5% market orders, 25% cancels and
5% modifies, plus extra cancels to keep the book size bounded.

Preliminary numbers on a development laptop (Windows 11, untuned, no core isolation;
the ~25 ns timer overhead is included):

| command | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|
| all | 72 ns | 105 ns | 156 ns | 242 ns | 2.4 µs |
| limit | 66 ns | 92 ns | 143 ns | 209 ns | 2.2 µs |
| market | 86 ns | 130 ns | 185 ns | 270 ns | 2.4 µs |
| cancel | 80 ns | 99 ns | 126 ns | 275 ns | 2.5 µs |
| modify | 116 ns | 148 ns | 210 ns | 369 ns | 3.2 µs |

Throughput without per-command timers: about **22M commands/s** (45 ns/command).

The p99.99 and max values are dominated by OS scheduling and interrupts on an untuned
machine. Measurements on an isolated Linux core are part of Phase 5.

What this does **not** measure: network, serialization, journaling or queueing. This is
the matching core only, in a closed loop. End-to-end, open-loop latency (with
coordinated-omission correction) arrives with the gateway and pipeline phases.

## Running

```sh
cargo test                                # all tests
cargo bench --bench latency               # latency histogram; writes target/latency/*.hgrm
cargo bench --bench throughput            # Criterion before/after comparison (includes generator cost)
```

The `.hgrm` files can be plotted with the
[HdrHistogram plotter](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html)
(values are in microseconds).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
