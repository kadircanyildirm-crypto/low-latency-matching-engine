# Head-to-head comparison with other matching engines

This compares the matching core in `crates/orderbook` with three well-known open-source
order books on exactly the same order flow, and checks that every engine produced exactly
the same trades and the same final book. The harness lives in [`compare/`](../compare); one
command per engine reruns everything ([Reproducing](#reproducing)).

> **The numbers below are preliminary.** They were taken on a development laptop while
> three other build-and-test jobs were running on it, two interleaved rounds per engine.
> Treat the ratios as indicative and the absolute values as noisy. The harness is meant to
> be rerun on an idle machine, which will replace these numbers.

## Competitors

| Engine | Language | Version measured | What exactly is measured |
|---|---|---|---|
| **ours** | Rust | this repository (`crates/orderbook`) | `OrderBook::process`, every event materialised into a `Vec<Event>` |
| [exchange-core](https://github.com/exchange-core/exchange-core) | Java | commit `2f8548749839e9095c8dc597e4b61521d259fa5d` (master, May 2022; `0.5.4-SNAPSHOT`, 41 commits after the 0.5.3 tag) | `OrderBookDirectImpl`, its performance order book, through `IOrderBook.processCommand`, as its `MatchingEngineRouter` calls it |
| [liquibook](https://github.com/enewhuis/liquibook) | C++ | commit `2427613b32f1667abae68a01df6af9ba8270f8e7` (master, Dec 2022; 24 commits after 2.0.0) | `liquibook::book::OrderBook<Order*>`, the plain book without depth tracking |
| [OrderBook-rs](https://github.com/joaquinbejar/OrderBook-rs) | Rust | `orderbook-rs` 0.15.0 with `pricelevel` 0.10.2, from crates.io | `OrderBook<()>` as in the crate's own latency benchmarks |

Why these: exchange-core (about 2,600 GitHub stars) is the best-known open-source exchange
core and follows the same LMAX architecture this project does; liquibook (about 1,500
stars) is a widely used C++ order book; OrderBook-rs (about 540 stars, 74k downloads) was
the most downloaded actively maintained order book engine in a crates.io search on
2026-10-09. Our engine is `crates/orderbook` as of commit `0a5f459`, which the comparison
does not change.

Toolchains: Rust 1.96.0 with the main workspace's release profile (fat LTO, one codegen
unit) for both Rust engines; liquibook built with MSVC 19.44 (`/O2`, through the `cc`
crate), baseline x86-64 like the Rust code; exchange-core built and run with Eclipse
Temurin 17.0.20.1 (`-Xms1g -Xmx2g -XX:+UseParallelGC`; the parallel collector was the
default of Java 8, exchange-core's reference platform).

## Method

### One recorded order flow for everyone

The streams come from the participant-like generator the latency benchmark uses
(`crates/orderbook/src/workload.rs`): it learns from the engine's execution reports which
orders rest, so cancels and modifies target live orders, and the book reaches a steady
state. The exporter (`compare/harness/src/bin/export.rs`) drives our book with it once and
records every command into a simple binary file: a 128-byte header and 24-byte
little-endian records (limit, market, cancel, move), documented byte for byte in
[`compare/harness/src/stream.rs`](../compare/harness/src/stream.rs). Each adapter loads the
whole file into memory before anything is timed and replays it.

| Scenario | Flow | Commands | Book after warm-up |
|---|---|---|---|
| `baseline` | 55% passive limits, 10% aggressive limits, 5% market, 30% cancels | 0.5M warm-up + 2M measured | 4.6k orders on 224 levels |
| `sweep` | 40% passive, 20% aggressive, 20% market, 20% cancels, sizes up to 1,000 | 0.5M + 2M | 621 orders on 102 levels |
| `deep` | as `baseline`, up to 1M resting orders spread over ±5,000 ticks | 4M + 2M | 846k orders on 10k levels |
| `modify` | the generator's default mix: 25% cancels and 5% modifies, which become price moves | 0.5M + 2M | 6.5k orders on 281 levels |

### Fairness decisions

- **Common subset only.** GTC limit orders, market orders and cancels, plus price moves in
  one scenario. No IOC/FOK/post-only limits, icebergs, stops or mass cancels: not every
  engine has them.
- **Our extra controls are off.** No price protection, no price band. Self-trade
  prevention cannot be switched off in our book, so the streams make it impossible
  instead: every buy order belongs to owner 0 and every sell order to owner 1, so two
  orders of one owner never meet. Our book still compares owners on every match, a few
  instructions the others do not execute; it never fires (the exporter asserts this).
- **Market orders.** liquibook and exchange-core have no unpriced market order that
  cannot rest: liquibook gets price 0 (its market price) with immediate-or-cancel, and
  exchange-core an IOC order at the extreme price. Both behave exactly like a market
  order of the others.
- **Moves, not size changes.** A modify in the stream is a cancel/replace to a new price
  that keeps the open quantity: the order goes to the back of the new level and trades
  first if it crosses. All four engines have that natively (our `Modify` with the same
  total quantity, exchange-core's `MOVE_ORDER`, liquibook's `replace` with a new price,
  OrderBook-rs's `UpdatePrice`). Pure size reductions are left out on purpose:
  ours and exchange-core keep the order's queue position, but liquibook's `replace`
  re-inserts the order at the back of its level even when only the size shrinks, so the
  books would diverge after the first one. When the generator keeps the price, the
  exporter moves the order one tick further from the touch instead.
- **Same work, verified.** The stream header records what our engine produced: the
  number of trades, the traded quantity, the resting orders and quantity at the end, and
  the final best bid and ask. Every pass of every engine is checked against it, and a
  mismatch would mark the row unverified. **Every engine matched exactly, in every
  scenario**, so all four did identical matching work: same fills, same priority, same
  final book. The check earned its place: liquibook first disagreed on `sweep` by two
  fills out of 1.5 million. Stepping it in lockstep with our engine showed that its market
  orders were not really immediate-or-cancel, so a remainder rested at the market price
  and traded later. liquibook's `OrderTracker` ignores an order's own
  `immediate_or_cancel()` (it reads it only under `LIQUIBOOK_ORDER_KNOWS_CONDITIONS`, and
  then ORs it into a constructor parameter, not the member); the adapter now passes the
  condition to `add()`, the supported way.
- **Every engine consumes its own output.** Ours materialises every event (accept, trade,
  rest, cancel, modify) into a `Vec<Event>`, as in the latency benchmark, and the harness
  counts trades from it; exchange-core's harness walks each command's matcher event chain;
  liquibook delivers fills through its callback queue to an `on_fill` override;
  OrderBook-rs delivers them to a trade listener.
- **Each engine as its authors configure it for speed**, with one change each where the
  default would charge a cost unrelated to matching: OrderBook-rs gets its `StubClock` (the
  clock it provides for replay) instead of reading the wall clock on every order;
  exchange-core runs without its pipeline (below); liquibook gets raw pointers to orders
  indexed directly by id rather than the `shared_ptr` of its examples.
- **OrderBook-rs gets the generator's 64 participants.** It keeps a `Vec` of order ids per
  user and removes from it with a linear search. A first version of the adapter submitted
  everything under the crate's default user, so every cancel and fill scanned the whole
  book: `deep` did not finish a pass in ten minutes, and `baseline` ran at 0.18M instead of
  0.55M cmd/s. Each order now belongs to the participant our generator assigned it to
  (one of 64, as in the latency benchmark's flow). Its self-trade prevention is off, so
  this changes no fill.

### exchange-core: the core, not the pipeline

exchange-core is a full exchange: an LMAX Disruptor pipeline with a risk engine,
journaling and result handlers around the order book. Running the whole pipeline would add
inter-thread hand-offs and a risk check per order that none of the other engines has, so
the harness measures the layer that corresponds to them, `OrderBookDirectImpl`, called
through `IOrderBook.processCommand` exactly as the router calls it, with the router's
object-pool sizes and event helper (`EVENTS_POOLING` is off in exchange-core, so trade
events are allocated in its pipeline too). exchange-core's own order book benchmark
(`ITOrderBookBase`) measures the same layer.

### Measurement protocol

Every adapter runs the same protocol (`compare/harness/src/run.rs`; the Java harness mirrors
it), in its own process, with the measuring thread pinned to one core:

1. Load the stream. Nothing timed reads files or allocates for the harness.
2. Warm-up pass: replay the whole stream into a fresh book, untimed, and verify it. This
   warms caches and the allocator and lets the JIT compile everything.
3. Throughput pass: a fresh book replays the warm-up prefix, then the 2M measured
   commands with no per-command timer, timed in chunks of 100k commands. Two figures come
   out: the overall rate (all commands over the total time, everything included, the
   figure to use on an idle machine) and the median chunk's rate, which an occasional
   preemption by another process cannot move. On this busy laptop a single interval was
   not usable: a first attempt measured our `deep` scenario at 0.49M cmd/s while its own
   latency pass, run right after, had a median of 290 ns. Engines are compared on the chunk
   median here.
4. Latency pass: another fresh book replays the warm-up prefix, then the measured
   commands with a timestamp before and after each one. Percentiles are exact (nearest
   rank over all 2M samples).
5. A driver script runs the engines in interleaved rounds (ours, OrderBook-rs, liquibook,
   exchange-core, then the next round starting one engine later), and the report takes the
   median over rounds and shows the range.

Timers: the Rust and C++ adapters read the TSC exactly as the latency benchmark does
(`lfence; rdtsc; lfence` before, `rdtscp; lfence` after; ~25 ns per pair, included). The
replay loop and the timer of liquibook run inside C++, so no FFI call is timed. Java has
no cycle counter: exchange-core is timed with `System.nanoTime()`, which on Windows ticks
in **100 ns steps**, so its latency percentiles are quantised to 100 ns and its medians
cannot be compared with the others below that resolution. Its throughput needs no
per-command timer and is directly comparable. The JVM's GC is triggered before each timed
pass, as in exchange-core's own benchmarks; collections during a pass are included.

Pinning: by default the measuring thread is pinned to the last logical core, as the latency
benchmark does. **On the i5-12450H that is an E-core** (Gracemont, CPUID leaf 0x1A), so the
numbers here, like the README's, are from an efficiency core; `CMP_CORE` selects another.
The JVM pins only its main thread (the JIT and GC threads run elsewhere). OpenHFT Affinity
3.2.2, which exchange-core ships, cannot pin on Windows, so the harness calls
`SetThreadAffinityMask` through JNA itself. This matters: unpinned, exchange-core ran
more than twice as fast as when pinned, because Windows put it on a P-core.

## What each engine does per command beyond the matching itself

| Engine | Extra work per command | Effect |
|---|---|---|
| ours | Owner comparison per match (self-trade prevention that never fires), per-owner order lists (for O(k) mass cancel), validation of every field, full event stream materialised | Small; all of it is in the numbers |
| exchange-core | Allocates a `MatcherTradeEvent` per trade and per cancel/reject (garbage for the GC); adaptive radix trees (ART) for price levels and the order index; object pools for orders and buckets | Young collections show up in its tail |
| liquibook | A `std::multimap` node allocation per resting order; a cancel or replace finds the order by walking its price level linearly; every command goes through a callback vector dispatched with virtual calls inside a `try`/`catch`; all-or-none bookkeeping on every match | Cost grows with orders per level (the `deep` scenario) |
| OrderBook-rs | Concurrent data structures (lock-free skip lists of levels, a concurrent id map, atomics, a submit gate) used single-threaded; about 1 to 3 KB allocated per passive add (its own measurement); a UUID v5 (SHA-1) trade id per execution; `u128` prices; a `TradeResult` with a symbol string per trade batch; removal from the owner's id list is linear in that owner's orders | It is built for many threads touching one book; a single-threaded replay pays for that without using it. The per-owner list makes `deep` (16k orders per owner) much slower than the small books |

## Results (preliminary)

Two interleaved rounds per engine on 2026-10-09, every thread pinned to logical core 11
(an E-core), 2M measured commands per run; liquibook's two `sweep` runs were redone
after the fix described above, about 20 minutes later. **Every row is verified.**

Throughput is the median 100k-command chunk, in millions of commands per second; latency
is the median per command.

| Scenario | ours | exchange-core | liquibook | OrderBook-rs |
|---|---:|---:|---:|---:|
| `baseline` | **16.7M/s**, 84 ns | 8.7M/s (0.52x), 100 ns* | 2.05M/s (0.12x), 427 ns | 0.54M/s (0.03x), 1.5 µs |
| `sweep` | **16.6M/s**, 80 ns | 8.4M/s (0.50x), 100 ns* | 3.6M/s (0.22x), 302 ns | 0.50M/s (0.03x), 1.5 µs |
| `deep` | **4.15M/s**, 196 ns | 3.1M/s (0.74x), 300 ns* | 0.20M/s (0.05x), 2.5 µs | 0.03M/s (0.01x), 8.9 µs |
| `modify` | **15.9M/s**, 84 ns | 8.5M/s (0.54x), 100 ns* | 1.8M/s (0.11x), 442 ns | 0.45M/s (0.03x), 1.8 µs |

\* exchange-core's latencies are in 100 ns steps (Java's timer on Windows), so its medians
mean "100-200 ns" and cannot be compared below that resolution.

The full report (`compare/run.sh report`): median over the runs, range in brackets;
"Throughput" is all commands over their total time, "Chunk median" the robust figure
above; latency in ns, timer overhead (~25 ns, Java ~0-100 ns) included.

**baseline** (~5k resting orders on ~220 levels near the touch)

| Engine | Runs | Throughput | Chunk median | vs ours | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours | 2 | 16.28 [15.97-16.59] | 16.72 [16.70-16.74] | 1.00x | 84 | 119 | 190 | 382 | 2565 |
| exchange-core | 2 | 8.26 [8.24-8.27] | 8.66 [8.54-8.79] | 0.52x | 100 | 200 | 450 | 900 | 5700 |
| liquibook | 2 | 2.04 [2.04-2.05] | 2.05 [2.05-2.05] | 0.12x | 427 | 946 | 2024 | 4121 | 37412 |
| OrderBook-rs | 2 | 0.52 [0.52-0.52] | 0.54 [0.52-0.55] | 0.03x | 1507 | 3138 | 6755 | 12525 | 91393 |

**sweep** (40% aggressive and market flow, sizes up to 1,000; multi-level fills)

| Engine | Runs | Throughput | Chunk median | vs ours | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours | 2 | 16.35 [16.03-16.67] | 16.61 [16.55-16.67] | 1.00x | 80 | 118 | 190 | 260 | 1917 |
| exchange-core | 2 | 8.26 [8.16-8.37] | 8.36 [8.32-8.40] | 0.50x | 100 | 200 | 500 | 950 | 5700 |
| liquibook | 2 | 3.31 [3.04-3.58] | 3.62 [3.61-3.64] | 0.22x | 302 | 371 | 487 | 663 | 5790 |
| OrderBook-rs | 2 | 0.49 [0.47-0.50] | 0.50 [0.49-0.52] | 0.03x | 1465 | 3916 | 8313 | 14094 | 70058 |

**deep** (850k-1M resting orders on ~10k levels; far beyond the caches)

| Engine | Runs | Throughput | Chunk median | vs ours | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours | 2 | 4.21 [4.04-4.39] | 4.15 [4.10-4.21] | 1.00x | 196 | 469 | 990 | 3461 | 11452 |
| exchange-core | 2 | 3.11 [2.81-3.40] | 3.08 [2.82-3.34] | 0.74x | 300 | 500 | 1000 | 3850 | 13700 |
| liquibook | 2 | 0.20 [0.19-0.21] | 0.20 [0.20-0.20] | 0.05x | 2456 | 12315 | 21003 | 44747 | 301579 |
| OrderBook-rs | 2 | 0.04 [0.04-0.04] | 0.03 [0.03-0.04] | 0.01x | 8862 | 65506 | 143083 | 375384 | 2643646 |

**modify** (baseline flow with 5% price moves)

| Engine | Runs | Throughput | Chunk median | vs ours | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours | 2 | 15.29 [14.70-15.88] | 15.86 [15.65-16.07] | 1.00x | 84 | 126 | 185 | 272 | 2442 |
| exchange-core | 2 | 7.15 [5.90-8.39] | 8.53 [8.17-8.90] | 0.54x | 100 | 200 | 500 | 950 | 12800 |
| liquibook | 2 | 1.81 [1.77-1.85] | 1.81 [1.76-1.85] | 0.11x | 442 | 1144 | 2202 | 3525 | 27350 |
| OrderBook-rs | 2 | 0.44 [0.44-0.44] | 0.45 [0.45-0.46] | 0.03x | 1847 | 4921 | 15129 | 39345 | 186836 |

**One run on a P-core** (logical core 2, `CMP_CORE=2`; a spot check, not part of the rounds):

| Scenario | ours | exchange-core |
|---|---:|---:|
| `baseline` | 29.3M/s, p50 54 ns, p99 111 ns | 13.5M/s (0.46x), p50 100 ns, p99 300 ns |
| `deep` | 5.6M/s, p50 157 ns, p99 888 ns | 5.0M/s (0.89x), p50 200 ns, p99 900 ns |

### What the numbers say

- **Ours is the fastest in every scenario measured.** On the small books (`baseline`,
  `sweep`, `modify`) it does about twice the work of exchange-core per second and has the
  lowest latency at every percentile the timers can resolve. Where the book fits in
  cache, the dense price ladder (one subtraction to find a level), the intrusive queues in
  a preallocated slab and the absence of allocation and garbage collection pay off.
- **exchange-core comes close on the deep book, and on a P-core it nearly catches up:**
  0.74x on the E-core and 0.89x in the P-core spot check, with the same p99 (about 1 µs)
  and a similar p99.9. With a million resting orders both engines are dominated by cache
  misses: cancels and fills touch arbitrary old orders. exchange-core finds orders through
  an adaptive radix tree, which suits the stream's sequential order ids; ours goes through
  a hash map (`FxHashMap`) into a 48 MB slab. Replacing that hash map
  with a directly indexed table once the gateway assigns sequential ids is already on the
  roadmap ([DESIGN.md §11](DESIGN.md#11-known-limitations-and-deliberate-deferrals)); this
  comparison is evidence for doing it.
- **exchange-core gains more from a P-core than ours does on `deep`** (+62% against +35%),
  so the core type matters for the ranking's margins; the final numbers should come from
  the machine and core the project cares about.
- **liquibook** is 5 to 20 times slower. A cancel or replace finds the order by walking its
  price level linearly (`find_on_market`), every resting order is a node allocation in a
  `std::multimap`, and every command goes through its callback queue. On `deep`, with
  about 85 orders per level, the walk dominates: 2.5 µs median, 21 µs p99. `sweep`, with
  short queues and many market orders, is its best case.
- **OrderBook-rs** is 30 times slower on the small books, over 100 times on `deep`, and has
  the longest tails. It is designed for
  many threads sharing one book, and a single thread pays for that design without
  benefiting from it: concurrent skip lists and maps, atomics, about 1-3 KB allocated per
  resting order, a SHA-1-based UUID per trade, and per-user id lists that are searched
  linearly (16k orders per user on `deep`). Its own published figures (Apple M5 Max:
  mixed workload p50 0.54 µs, p99 15.7 µs) are consistent with these.
- These numbers are close to, but not the same as, the README's: the streams differ (no
  self-trade prevention firing, no price protection, a different mix), and the machine
  was busy.

## Caveats

- **Noisy, shared machine.** Other agents were compiling and testing on the same laptop
  throughout; the coordinator re-measures on an idle machine. Tails (p99.9 and beyond) are
  dominated by that noise and by Windows scheduling, for every engine.
- **E-core.** All engines ran on the same efficiency core. A P-core is faster for all of
  them, but not by the same factor (see the spot check above: ours +75% and exchange-core
  +56% on `baseline`, +35% and +62% on `deep`).
- **Java's timer.** exchange-core's percentiles are in 100 ns steps on Windows; a median of
  100 ns means "between 0 and 200 ns". Its throughput is unaffected.
- **One flow family.** All four scenarios come from one generator. Books with very
  different shapes (thousands of orders per level, very sparse prices, huge sweeps) could
  rank the engines differently. In particular our dense price ladder pays memory for the
  width of the price band (here 200,001 ticks), which the tree-based books do not.
- **Single-threaded only.** OrderBook-rs is designed for concurrent access and
  exchange-core for a multi-core pipeline; neither advantage is exercised here, by design.
- **No end-to-end latency.** Like the README's numbers, this is the matching core in a
  closed loop: no network, no serialisation, no journaling, no queueing.
- **Versions.** exchange-core and liquibook are measured at the latest commits of their
  default branches, which have seen little change since 2022/2023. OrderBook-rs moves
  fast; 0.15.0 was its latest release when this was written.

## Reproducing

Prerequisites: Rust (stable), bash with `git` and `curl` (Git Bash on Windows), and a C++
compiler (MSVC on Windows, found automatically, or `c++` elsewhere). A JDK 17 and Maven are
downloaded into `compare/vendor/tools` with checksum verification unless `JAVA_HOME` and
`MVN` are set. Nothing is installed system-wide; everything downloaded stays in the
git-ignored `compare/vendor`.

```sh
compare/run.sh fetch           # liquibook and exchange-core at the pinned commits, JDK 17 + Maven
compare/run.sh export          # record the four streams into compare/data (~320 MB)

compare/run.sh ours            # one round of one engine, appended to compare/results/results.csv
compare/run.sh orderbook-rs
compare/run.sh liquibook
compare/run.sh exchange-core

compare/run.sh all 5           # 5 interleaved rounds of all engines into a fresh results.csv,
                               # then the Markdown report
compare/run.sh report          # the report alone
```

Useful settings: `CMP_CORE=2` pins to another core (on the i5-12450H, logical cores 0-7 are
P-cores and 8-11 E-cores), `CMP_SCENARIOS=baseline,deep` restricts the scenarios,
`CMP_RUNS=3` adds runs per process, `JAVA_OPTS` overrides the JVM options. The comparison is
its own Cargo workspace, so the main workspace's `cargo build` and `cargo test` never
compile it. On this laptop a round of all engines and scenarios takes about 8 minutes,
most of it OrderBook-rs.
