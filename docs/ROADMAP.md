# Roadmap

Each phase produces a result that can be shown on its own. A phase only counts as done
once all of its acceptance criteria are met. Two phases that are finished, measured and
documented are worth more than five half-finished ones.

| Phase | Topic | Status |
|-------|-------|--------|
| 1 | Matching core + test infrastructure + benchmarks | ✅ Done |
| 2 | Event sourcing: journal + replay | ⏳ Next |
| 3 | Binary protocol + TCP gateway | — |
| 4 | Pipeline: gateway → sequencer → matcher → publisher | — |
| 5 | End-to-end measurement and optimisation on Linux | — |
| 6 | Hot standby replication and failover | — |
| 7 | Extensions (optional) | — |

---

## Phase 1 — Matching core ✅

**Goal:** a single-threaded, deterministic order book with price-time priority that never
allocates on the hot path.

**Delivered**
- Commands: limit orders (GTC, IOC, fill-or-kill, post-only), market, cancel, and modify
  with FIX cancel/replace semantics on total quantity. Shrinking at the same price keeps
  queue priority; any other change goes to the back of the queue.
- Mass cancel: `CancelAll` pulls every order of one owner in O(k log k) in that owner's
  orders, through per-owner lists keyed by dense owner ids.
- Risk controls in the core: static price band, maximum order size, price protection,
  self-trade prevention (`CancelResting` / `CancelIncoming`), owner checks on cancel and
  modify, and a capacity limit that never refuses an order that can trade.
- Overflow-free by construction: `max_orders × max_order_qty` must fit in a `u64`.
- Stop and stop-limit orders: triggered by any price a command traded at, released in a
  fixed order, cascading; stop-limits rest in the slot the stop held.
- Iceberg orders: only the display quantity shows; each new tranche goes to the back of
  the queue; the number of tranches per order is bounded.
- Price band around the last trade price, alongside price protection; a committed
  experiment measures how each behaves under a harsh flow.
- Snapshots and a state digest: `snapshot()` / `restore()` rebuild an identical book
  without replay, and `digest()` is a platform-independent hash of the complete state.
- Data structures: a dense price ladder per side with a two-level occupancy bitset,
  intrusive FIFO queues in a preallocated slab, and an `FxHashMap` id index reserved at
  twice the capacity.
- Verification:
  - Scenario tests, one rule per test, with the exact expected event sequence.
  - A differential property test against a deliberately naive reference book.
  - A specification checker that verifies every command against the rules without a
    second implementation, including that each command is accepted or rejected exactly
    as the rules require.
  - Tests that `validate()` detects each kind of corrupted state.
  - Soak tests under both self-trade policies, and golden fingerprints that CI checks on
    Linux, Windows and macOS.
  - Zero-allocation tests with a counting global allocator.
  - Mutation testing: every mutant that compiles is detected.
- Measurement: per-command latency with the TSC and HdrHistogram over four scenarios
  (`.hgrm` output), and Criterion throughput tracking.

**Acceptance criteria:** all tests green, no clippy warnings, zero allocations on the hot
path, latency report in the README. All met.

---

## Phase 2 — Event sourcing: journal + replay

**Goal:** rebuild the system's exact state from the command log alone.

- A sequencer that gives every command an increasing sequence number.
- An append-only journal file: a fixed header plus length and CRC32 for each record.
- Crash recovery: detect a half-written final record and truncate it.
- Snapshots: write the book's state at intervals; on startup, load the snapshot and replay
  the tail of the log. The book already provides `snapshot()` / `restore()`; this phase
  adds the on-disk format and the schedule.
- State hash: after a replay, the book's `digest()` must equal the live run's.
- `fsync` policies (every command / group commit / leave it to the OS) and a benchmark of
  each one's effect on latency.

**Acceptance criteria:** a test proving that a process killed at a random point returns to
exactly the same state through replay; the cost of journal writes measured.

---

## Phase 3 — Binary protocol + TCP gateway

**Goal:** accept orders from the outside world.

- Fixed-length, little-endian messages: `NewOrder`, `Cancel`, `Modify`,
  `ExecutionReport`, `Reject`, `Heartbeat`.
- Framing, sessions (login / heartbeat / logout), mapping client order ids to exchange
  order ids, and accounts, each mapped to one of the book's dense owner ids.
- Exchange-assigned sequential order ids, which let the book replace its hash-map id index
  with a directly indexed table.
- Cancel-on-disconnect: the gateway sends the book's `CancelAll` when a session drops.
- Per-session pre-trade risk: order count limits and throttling, so one participant cannot
  fill the book.
- Fuzzing the protocol decoder with `cargo-fuzz`.
- A simple load-generating client.

**Acceptance criteria:** no crashes under fuzzing; end-to-end order flow driven by the load
generator.

---

## Phase 4 — Pipeline

**Goal:** an LMAX-style staged architecture, with each stage on its own core.

- Our own SPSC ring buffer: cache-line padding, no false sharing, batched reads.
- Stages: gateway → sequencer/journal → matcher → publisher.
- One command can emit any number of events (a sweep emits one per order it reaches), so
  the matcher-to-publisher path must handle batches of any size.
- Market data publisher: L2 snapshots plus incremental updates.
- CPU pinning, busy-spin waiting, backpressure.

**Acceptance criteria:** separate tests and a benchmark for the ring buffer; the pipeline
runs end to end.

---

## Phase 5 — Measurement and optimisation (Linux)

**Goal:** real, defensible latency numbers.

- Open-loop load testing (at a fixed arrival rate) with coordinated-omission correction.
- `isolcpus`, `nohz_full`, IRQ affinity settings.
- `perf` + flamegraphs and cache-miss analysis, with a before/after chart for every
  optimisation.
- **Research question:** a comparison of order book data structures (ladder + bitset,
  `BTreeMap`, sorted `Vec`) by cache misses and tail latency, for an academic report. It
  also decides the design for markets with very wide price bands.

**Acceptance criteria:** a reproducible benchmark report with the hardware and settings
documented.

---

## Phase 6 — Hot standby

**Goal:** if the primary node fails, a standby continues from the same state.

- The primary streams its journal to the standby, which replays the same commands.
- Failure detection by heartbeat, split-brain prevention with epochs and fencing.
- Measuring failover time.

**Acceptance criteria:** a test that kills the primary; the standby's state hash equals the
primary's.

---

## Phase 7 — Extensions (optional)

- Market states: trading halts, opening and closing auctions.
- Per-order self-trade prevention instructions.
- Multiple instruments (a shard / thread per instrument).
- A network layer on `io_uring`.
- A web-based live depth view for demos.
