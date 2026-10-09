# Roadmap

Each phase produces a result that can be shown on its own. A phase only counts as done
once all of its acceptance criteria are met. Two phases that are finished, measured and
documented are worth more than five half-finished ones.

| Phase | Topic | Status |
|-------|-------|--------|
| 1 | Matching core + test infrastructure + benchmarks | ✅ Done |
| 2 | Event sourcing: journal + replay | ✅ Done |
| 3 | Binary protocol + TCP gateway | ✅ Done |
| 4 | Pipeline: gateway → sequencer → matcher → publisher, market data | ✅ Done |
| 5 | Public web demo: paper trading against bots, live order book | 🚀 Built; waits for a server |
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

## Phase 3 — Binary protocol + TCP gateway ✅

**Goal:** accept orders from the outside world.

**Delivered** (`crates/protocol`, `crates/gateway`,
[DESIGN.md §15](DESIGN.md#15-the-gateway))
- A wire protocol of fixed-length, little-endian messages: login, logout, heartbeat, new
  order (limit, market, stop, with time in force and iceberg display), cancel, modify and
  mass cancel in; login accepted or rejected, logout with a reason, heartbeat, a gateway
  reject and one report per event of the book out. Decoding is strict: unknown types,
  wrong lengths, out-of-range codes and non-zero padding are errors, and a connection that
  sends one is logged out.
- Sessions: a login with an account and its token first, one session per account,
  heartbeats to quiet clients, logout of silent ones.
- Accounts from a plain text file, each id also the book's owner id for its orders.
- Exchange-assigned order ids: an order's id is the sequence number of the command that
  places it, unique, increasing, and recovered with the journal. The client's own
  reference is echoed in every report about the order.
- Cancel-on-disconnect: a connection that closes, is logged out, or falls too far behind
  reading its reports has its account's orders cancelled. Stopping the gateway leaves
  them, as a crash would, and a restart attributes them to their accounts again.
- Per-account pre-trade risk: a limit on open orders and pending stops, counting those
  not yet applied, and a token bucket per session, enforced before anything is journaled.
- A single-threaded `mio` event loop with group commit: the commands of one round of reads
  share a journal sync, and reports go out only after it.
- Fuzzing: the decoders on arbitrary byte streams, and the gateway's sessions on byte
  streams and well-formed messages from several connections that come and go.
- A load generator whose clients trade against each other and measure the time from
  sending each order to its acknowledgement.

The directly indexed id table this phase once listed was not built. Order ids are now
sequence numbers, which grow without bound while a good-till-cancelled order may rest
indefinitely, so a table indexed by id needs to handle collisions: that is what the book's
index already does, and it puts consecutive ids into shared cache lines.

**Acceptance criteria:** no crashes under fuzzing (both targets on every push, and for
twenty minutes each a week); end-to-end order flow driven by the load generator
(`crates/gateway/tests/server.rs`, on Linux, Windows and macOS). Both met.

---

## Phase 4 — Pipeline and market data ✅

**Goal:** an LMAX-style staged architecture, with each stage on its own core.

**Delivered** (`crates/ring`, `crates/marketdata`, `crates/gateway/src/pipeline.rs`,
[DESIGN.md §16](DESIGN.md#16-the-pipeline-and-market-data))
- Our own SPSC ring buffer: positions on separate cache-line pairs, cached copies of the
  other side's position, batched writes and reads that publish once, bounded with
  backpressure, spinning or backing off while it waits. Checked by Miri, loom and a model
  test; benchmarked against `crossbeam-channel` and `std::sync::mpsc`.
- The engine split into a writer and a matcher, and a pipeline that runs them on threads
  of their own behind the network thread: gateway → writer (journal, segment rolls,
  syncs, retention) → matcher (book, snapshots) → back to the network thread, which routes
  reports and publishes market data. Write-ahead holds across the threads, group commit
  happens by itself, and a failure on any thread stops the server cleanly.
- Events of any number per command: the matcher waits for room rather than drop or buffer
  them, and the network thread always drains them, so the waits cannot form a cycle.
- Market data: the depth by price level kept from the events alone, published as a
  snapshot on subscription and then trades and changed levels after every round, each with
  the sequence number of the last command it reflects.
- CPU pinning (`--cores network,writer,matcher`), busy-spin or backoff waiting
  (`--wait`), and backpressure from the rings to the clients' sockets.

Changed from the plan: market data consumers recover by a new snapshot rather than by
replaying from a sequence number, so nothing holds journal retention back for them; a
replay of a client's own reports moves to Phase 7 with the user-facing API.

Measured honestly, the pipeline does not raise throughput on the laptop: the network
thread bounds both modes, and the pipeline adds two hops per round trip. Phase 6 measures
on Linux, open-loop, with timestamps per stage.

**Acceptance criteria:** separate tests and a benchmark for the ring buffer
(`crates/ring`: Miri, loom, a model test, `cargo bench -p ring`); the pipeline runs end to
end (`crates/gateway/tests/server.rs`: the load generator trades through it over TCP, on
Linux, Windows and macOS). Both met.

---

## Phase 5 — Public web demo 🚀

**Goal:** a link anyone can open to watch and use a live market, for a CV and a first
audience. Paper money only: running a real-money exchange needs a licence (in Turkey from
the Capital Markets Board, SPK), customer identification and custody, and is out of scope.

**Delivered** (`crates/gateway`: `web/`, `wallet.rs`, `recovery.rs`, `bots.rs`, `web/*.html`,
`deploy/`, [DESIGN.md §17](DESIGN.md#17-the-web-demo))
- A web gateway on the gateway's own event loop: the page over HTTP, and sessions over
  WebSocket speaking JSON, with the same login, risk limits, reports and market data as
  binary ones; strict HTTP, WebSocket and JSON decoders, fuzzed.
- A browser interface: live depth, trades, a price chart, order entry and cancel, the
  visitor's open orders and fills, the paper account with profit.
- Paper-trading accounts created on a visitor's first visit, with a starting balance, and
  balance and position checks before an order is accepted, with holds and settlement.
- Bots: market makers, noise traders, a trend follower.
- A live performance panel: commands per second, and how long the engine's turns take.
- The market keeps running across restarts: the exchange's state is checkpointed and
  rebuilt from the journal after the checkpoint, which the engine now replays with its
  commands.
- An image, a compose file with HTTPS in front, and a guide (`deploy/README.md`); CI builds
  the image.

**Acceptance criteria:** the public link works; a visitor can trade against the bots; the
market keeps running across a restart of the server. The last two are met locally and in
tests (`crates/gateway/tests/web.rs`, `tests/recovery.rs`); the first waits for a server.

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
