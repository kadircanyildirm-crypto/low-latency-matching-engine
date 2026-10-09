# Roadmap

Each phase produces a result that can be shown on its own. A phase only counts as done
once all of its acceptance criteria are met. Two phases that are finished, measured and
documented are worth more than five half-finished ones.

| Phase | Topic | Status |
|-------|-------|--------|
| 1 | Matching core + test infrastructure + benchmarks | ✅ Done |
| 2 | Event sourcing: journal + replay | ✅ Done |
| 3 | Binary protocol + TCP gateway | ⏳ Next |
| 4 | Pipeline: gateway → sequencer → matcher → publisher, market data | — |
| 5 | Public web demo: paper trading against bots, live order book | — |
| 6 | End-to-end measurement and optimisation on Linux | — |
| 7 | Users: accounts, bot API and competitions, several instruments, open-source release | — |
| 8 | Hot standby replication and failover | — |
| 9 | Extensions (optional) | — |

Phases 3 to 6 build something to show: a running exchange anyone can open in a browser,
with defensible numbers behind it. Phases 7 and 8 turn it into something people use and can
rely on. Real money is out of scope: it needs a licence, customer identification and
custody.
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
- Market states: continuous trading, call phases that end in a single-price uncross (most
  volume, least surplus, market pressure, closest to the reference price), trading halts
  and the close, switched by commands from the sequencer. Optionally a volatility
  interruption: a band breach starts a call whose uncross re-anchors the band; the
  experiment measures that it keeps a banded book alive, and what it costs in time spent
  in calls.
- Snapshots and a state digest: `snapshot()` / `restore()` rebuild an identical book
  without replay, and `digest()` is a platform-independent hash of the complete state.
- Data structures: a dense price ladder per side with a two-level occupancy bitset,
  intrusive FIFO queues in a preallocated slab, and an id index of 64-byte lines sized
  once at twice the capacity.
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
  - Fuzzing, Kani proofs of the bitset, order pool and owner lists, and line and branch
    coverage in CI.
- Measurement: per-command latency with the TSC and HdrHistogram over four scenarios
  (`.hgrm` output), and Criterion throughput tracking.

**Acceptance criteria:** all tests green, no clippy warnings, zero allocations on the hot
path, latency report in the README. All met.

---

## Phase 2 — Event sourcing: journal + replay ✅

**Goal:** rebuild the system's exact state from the command log alone.

**Delivered** (`crates/engine`, [DESIGN.md §14](DESIGN.md#14-the-journal-and-recovery))
- A sequencer: `Engine::submit` gives every command the next sequence number, journals it,
  syncs as the policy says, and only then applies it to the book. Events carry their
  command's sequence number; a consumer that says where it stands gets exactly the events
  after that on recovery, or is told it has seen commands a power failure took back.
- An append-only journal of segment files written full of zeros before use and prepared
  ahead under a temporary name: a checksummed header per segment, and 64-byte records,
  each with its length, CRC-32, sequence number and a strict 40-byte encoding of the
  command. An OS lock keeps a second engine out of the directory.
- Crash recovery: a torn or half-written tail is cut and zeroed, so stale records cannot
  return after a later crash. Each record carries the highest sequence number synced when
  it was written, so recovery tells a crash from damage to synced data and refuses the
  latter instead of dropping acknowledged commands. Records the last sync may not have
  written are written again, the directory is synced, and nothing is changed on disk until
  everything recovery relies on has been checked.
- Snapshots written atomically (temporary file, sync, rename) on a schedule or on request,
  read back and checked before anything older is deleted; recovery loads the newest intact
  one, falls back to an older one if it is damaged, and replays the journal after it.
- State hash: every snapshot records the book's digest, checked after loading, and every
  start replays the journal from the snapshot before the newest and checks that it
  reaches the newest one's digest.
- Format and matching-rules versions in every file: a newer format or other rules are
  refused, never deleted.
- Sync policies: every call (`Always`; with `submit_batch`, group commit) or left to the OS
  (`Os`), with a benchmark of each one's cost. A failed write or sync, or a panic in the
  book, poisons the engine; a failed snapshot does not stop trading.
- Zero allocation on the journaling path, shown with a counting allocator.
- Verification: the process killed at every single change a run makes to the disk, on a
  simulated disk that loses power (torn and reordered writes, lost directory changes,
  failed syncs that drop pages) and flips bits; a test per way the files can be damaged;
  a fuzz target combining all of it; and fault injection showing that each safety
  mechanism is needed. Two adversarial reviews found thirteen defects, each fixed with a
  test that fails without the fix.

**Acceptance criteria:** a test proving that a process killed at a random point returns to
exactly the same state through replay (`crates/engine/tests/kill.rs`: 48 kills on Linux,
Windows and macOS); the cost of journal writes measured (README and DESIGN.md §14). Both
met.

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

## Phase 4 — Pipeline and market data

**Goal:** an LMAX-style staged architecture, with each stage on its own core.

- Our own SPSC ring buffer: cache-line padding, no false sharing, batched reads.
- Stages: gateway → sequencer/journal → matcher → publisher. The journal leaves the
  matching thread, taking segment rolls and syncs with it.
- One command can emit any number of events (a sweep emits one per order it reaches), so
  the matcher-to-publisher path must handle batches of any size.
- Market data publisher: L2 snapshots plus incremental updates, and consumers that resume
  from a sequence number, holding journal retention back while they need it.
- CPU pinning, busy-spin waiting, backpressure.

**Acceptance criteria:** separate tests and a benchmark for the ring buffer; the pipeline
runs end to end.

---

## Phase 5 — Public web demo

**Goal:** a link anyone can open to watch and use a live market, for a CV and a first
audience. Paper money only: running a real-money exchange needs a licence (in Turkey from
the Capital Markets Board, SPK), customer identification and custody, and is out of scope.

- A WebSocket gateway next to the binary one, speaking JSON to browsers.
- A browser interface: live order book depth, trades, a price chart, order entry and
  cancel, the visitor's own orders and fills.
- Paper-trading accounts with a starting balance, and position and balance checks before
  an order is accepted.
- Bots that make the market lively: market makers, noise traders, a trend follower.
- A live performance panel: commands per second and the engine's latency percentiles.
- Deployed on a small Linux server, with the journal and snapshots surviving restarts.

**Acceptance criteria:** the public link works; a visitor can trade against the bots; the
market keeps running across a restart of the server.

---

## Phase 6 — Measurement and optimisation (Linux)

**Goal:** real, defensible latency numbers.

- Open-loop load testing (at a fixed arrival rate) with coordinated-omission correction.
- `isolcpus`, `nohz_full`, IRQ affinity settings.
- `perf` + flamegraphs and cache-miss analysis, with a before/after chart for every
  optimisation.
- The journal on Linux: `fdatasync` cost on drives with and without power-loss protection,
  and `io_uring`.
- **Research question:** a comparison of order book data structures (ladder + bitset,
  `BTreeMap`, sorted `Vec`) by cache misses and tail latency, for an academic report. It
  also decides the design for markets with very wide price bands.

**Acceptance criteria:** a reproducible benchmark report with the hardware and settings
documented.

---

## Phase 7 — Users

**Goal:** people who come back, and developers who build on it.

- Accounts that persist, with history and statistics.
- A bot API (WebSocket and the binary protocol) with documentation and example bots in a
  few languages.
- A bot arena: competitions with a leaderboard, for algorithmic-trading clubs and
  courses.
- Several instruments, one book per instrument, sharded across cores.
- The engine released as an open-source crate on crates.io, with a documentation site.
- Reaching people: write-ups of the design and the measurements, university clubs,
  algorithmic-trading and Rust communities.

**Acceptance criteria:** outside users trading or running bots; the crate published.

---

## Phase 8 — Hot standby

**Goal:** if the primary node fails, a standby continues from the same state.

- The primary streams its journal to the standby, which replays the same commands.
- Failure detection by heartbeat, split-brain prevention with epochs and fencing.
- Measuring failover time.

**Acceptance criteria:** a test that kills the primary; the standby's state hash equals the
primary's.

---

## Phase 9 — Extensions (optional)

- Auction extensions: market orders in calls, published imbalances, auction price collars.
- Per-order self-trade prevention instructions.
- A network layer on `io_uring`.
