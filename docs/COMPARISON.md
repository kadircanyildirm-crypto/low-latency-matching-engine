# Head-to-head comparison with other matching engines

This compares the matching core in `crates/orderbook` with three well-known open-source
order books on exactly the same order flow, and checks that every engine produced exactly
the same trades and the same final book. The harness lives in [`compare/`](../compare); one
command per engine reruns everything ([Reproducing](#reproducing)).

> The numbers below come from a development laptop, idle apart from the harness, with
> every engine pinned to the same performance core: three interleaved rounds per engine.

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
2026-10-09. Our engine is `crates/orderbook` as of commit `7123fcc`, which the comparison
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
   preemption by another process cannot move. On a busy laptop a single interval is not
   usable: a first attempt, with other builds running, measured our `deep` scenario at
   0.49M cmd/s while its own latency pass, run right after, had a median of 290 ns. Engines
   are compared on the chunk median; on the idle machine the two figures agree within a
   few percent.
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
runs below set `CMP_CORE=2`, a performance core, as the README's latency figures do with
`LAT_CORE=2`.
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

## Results

Three interleaved rounds per engine on 2026-10-09, on a laptop otherwise idle, every
measuring thread pinned to logical core 2 (a P-core), 2M measured commands per run. Our
engine is at commit `7123fcc`. **Every row is verified.**

Throughput is the median 100k-command chunk, in millions of commands per second; latency
is the median per command.

| Scenario | ours | exchange-core | liquibook | OrderBook-rs |
|---|---:|---:|---:|---:|
| `baseline` | **27.6M/s**, 55 ns | 16.3M/s (0.59x), 100 ns* | 3.48M/s (0.13x), 238 ns | 0.94M/s (0.03x), 858 ns |
| `sweep` | **24.8M/s**, 55 ns | 13.6M/s (0.55x), 100 ns* | 6.16M/s (0.25x), 182 ns | 0.86M/s (0.03x), 837 ns |
| `deep` | **7.47M/s**, 143 ns | 5.09M/s (0.68x), 200 ns* | 0.27M/s (0.04x), 1.7 µs | 0.04M/s (0.01x), 6.0 µs |
| `modify` | **26.1M/s**, 56 ns | 15.8M/s (0.61x), 100 ns* | 2.87M/s (0.11x), 253 ns | 0.76M/s (0.03x), 972 ns |

\* exchange-core's latencies are in 100 ns steps (Java's timer on Windows), so its medians
mean "100-200 ns" and cannot be compared below that resolution.

The full report (`compare/run.sh report`): median over the runs, range in brackets;
"Throughput" is all commands over their total time, "Chunk median" the robust figure
above; latency in ns, timer overhead (~14 ns, Java ~0-100 ns) included.

**baseline** (~5k resting orders on ~220 levels near the touch)

| Engine | Runs | Throughput | Chunk median | vs ours | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours | 3 | 26.58 [25.28-28.33] | 27.61 [27.10-28.32] | 1.00x | 55 | 69 | 113 | 163 | 612 |
| exchange-core | 3 | 15.99 [14.63-16.50] | 16.30 [16.02-16.56] | 0.59x | 100 | 100 | 300 | 600 | 2100 |
| liquibook | 3 | 3.51 [3.09-3.54] | 3.48 [3.28-3.50] | 0.13x | 238 | 560 | 1130 | 1667 | 13662 |
| OrderBook-rs | 3 | 0.94 [0.93-0.94] | 0.94 [0.94-0.94] | 0.03x | 858 | 1764 | 3872 | 12700 | 76584 |

**sweep** (40% aggressive and market flow, sizes up to 1,000; multi-level fills)

| Engine | Runs | Throughput | Chunk median | vs ours | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours | 3 | 24.17 [23.39-25.09] | 24.80 [23.98-25.21] | 1.00x | 55 | 80 | 130 | 182 | 698 |
| exchange-core | 3 | 13.55 [12.16-13.84] | 13.57 [12.09-13.76] | 0.55x | 100 | 100 | 300 | 700 | 6400 |
| liquibook | 3 | 6.04 [6.00-6.15] | 6.16 [6.13-6.18] | 0.25x | 182 | 231 | 306 | 454 | 8515 |
| OrderBook-rs | 3 | 0.84 [0.82-0.86] | 0.86 [0.85-0.87] | 0.03x | 837 | 2239 | 4660 | 12344 | 83247 |

**deep** (850k-1M resting orders on ~10k levels; far beyond the caches)

| Engine | Runs | Throughput | Chunk median | vs ours | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours | 3 | 7.39 [6.84-7.64] | 7.47 [6.93-7.49] | 1.00x | 143 | 275 | 784 | 1022 | 13561 |
| exchange-core | 3 | 5.04 [4.86-5.12] | 5.09 [4.82-5.28] | 0.68x | 200 | 400 | 900 | 1400 | 20000 |
| liquibook | 3 | 0.29 [0.28-0.29] | 0.27 [0.27-0.27] | 0.04x | 1679 | 9215 | 15863 | 40162 | 151625 |
| OrderBook-rs | 3 | 0.05 [0.05-0.05] | 0.04 [0.04-0.05] | 0.01x | 5964 | 43168 | 100304 | 224213 | 1121757 |

**modify** (baseline flow with 5% price moves)

| Engine | Runs | Throughput | Chunk median | vs ours | p50 | p90 | p99 | p99.9 | p99.99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ours | 3 | 25.93 [25.11-26.25] | 26.08 [25.98-26.35] | 1.00x | 56 | 77 | 119 | 169 | 576 |
| exchange-core | 3 | 15.38 [15.34-16.02] | 15.78 [15.50-16.18] | 0.61x | 100 | 100 | 300 | 600 | 3600 |
| liquibook | 3 | 2.91 [2.87-3.07] | 2.87 [2.85-3.02] | 0.11x | 253 | 750 | 1433 | 2034 | 25154 |
| OrderBook-rs | 3 | 0.75 [0.73-0.78] | 0.76 [0.75-0.78] | 0.03x | 972 | 2538 | 6649 | 16287 | 98178 |

Latency ranges over the runs are in the report; medians up to p99 moved by at most a few
percent between runs, the p99.99 of every engine by up to a factor of two or three.

**An earlier, preliminary pass** ran two rounds on the last logical core (an E-core) while
three build-and-test jobs shared the laptop, before our engine replaced its order-id hash
map with the cache-line index. It ranked the engines the same way: ours at 16.7M/s on
`baseline` and 4.15M/s on `deep`, exchange-core at 0.52x and 0.74x, liquibook at 0.12x and
0.05x, OrderBook-rs at 0.03x and 0.01x. A one-run P-core spot check from that pass put
exchange-core at 0.46x on `baseline` and 0.89x on `deep`; with the new index it is 0.68x.

### What the numbers say

- **Ours is the fastest in every scenario measured.** On the small books (`baseline`,
  `sweep`, `modify`) it does 1.6 to 1.8 times the work of exchange-core per second and has
  the lowest latency at every percentile the timers can resolve. Where the book fits in
  cache, the dense price ladder (one subtraction to find a level), the intrusive queues in
  a preallocated slab and the absence of allocation and garbage collection pay off.
- **exchange-core comes closest on the deep book: 0.68x,** with a p99 of 900 ns against
  our 784 ns. With a million resting orders both engines are dominated by cache misses:
  cancels and fills touch arbitrary old orders. exchange-core finds orders through an
  adaptive radix tree, which suits the stream's sequential order ids. Ours used a
  general-purpose hash map (`FxHashMap`) until this comparison showed the deep book to be
  its weakest case; it now uses an id index that normally finds an order in one cache line
  ([DESIGN.md §13](DESIGN.md#13-performance-work)). exchange-core reached 0.89x of ours
  on `deep` in the preliminary P-core spot check, with the hash map, and 0.68x here.
- **liquibook** is 4 to 25 times slower. A cancel or replace finds the order by walking its
  price level linearly (`find_on_market`), every resting order is a node allocation in a
  `std::multimap`, and every command goes through its callback queue. On `deep`, with
  about 85 orders per level, the walk dominates: 1.7 µs median, 16 µs p99. `sweep`, with
  short queues and many market orders, is its best case.
- **OrderBook-rs** is about 30 times slower on the small books, over 150 times on `deep`,
  and has the longest tails. It is designed for many threads sharing one book, and a
  single thread pays for that design without benefiting from it: concurrent skip lists and
  maps, atomics, about 1-3 KB allocated per resting order, a SHA-1-based UUID per trade,
  and per-user id lists that are searched linearly (16k orders per user on `deep`). Its
  own published figures (Apple M5 Max: mixed workload p50 0.54 µs, p99 15.7 µs) are
  consistent with these.
- These numbers are close to, but not the same as, the README's: the streams differ (no
  self-trade prevention firing, no price protection, a different mix).

## Caveats

- **A laptop, not an isolated core.** The machine was otherwise idle, but it runs Windows
  with its usual background services and no tuning. Tails (p99.99 and beyond) are
  dominated by scheduling, for every engine.
- **Core type.** All engines ran on the same performance core. On an efficiency core all
  of them are slower, and not by the same factor: in the preliminary pass, a P-core gave
  ours +75% and exchange-core +56% on `baseline`, but +35% and +62% on `deep`. The ranking
  did not change.
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
compile it. On this laptop's P-core a round of all engines and scenarios takes about
5 minutes, most of it OrderBook-rs.
