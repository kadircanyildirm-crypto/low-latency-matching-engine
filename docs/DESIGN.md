# Design of the matching engine

This document explains what the order book in `crates/orderbook`, the journal and recovery
in `crates/engine` around it, the gateway in `crates/protocol` and `crates/gateway` in
front of them, and the pipeline and market data of `crates/ring` and `crates/marketdata`,
do, how, and why. It also says what they deliberately do not do yet. Every claim here is
backed by a test: the [verification](#11-verification) section says which one for the
book, [§14](#how-it-is-verified) for the engine, [§15](#how-it-is-verified-1) for the
gateway, [§16](#how-it-is-verified-2) for the pipeline and [§17](#how-it-is-verified-3) for
the web demo.

## 1. Goals

| Goal | Consequence |
|---|---|
| Deterministic | One thread, no clocks, no randomness, no iteration over hash maps. The event stream is a pure function of the command stream, so state can be rebuilt by replay (Phase 2) and mirrored by a standby (Phase 8). |
| Predictable latency | No heap allocation after construction, O(1) work per order touched, cache-friendly layout. |
| Exchange-grade semantics | Price-time priority, owner checks, FIX-style modifies, self-trade prevention, price protection, explicit rejections. |
| Safe | No `unsafe` code; quantity sums cannot overflow for any input (§9); property tests feed extreme values (`u64::MAX` quantities, `i64::MIN`/`MAX` prices) without a panic. |

## 2. Data model

- **Prices** are `i64` tick counts and **quantities** are `u64` lots. No floating point.
  The conversion from decimal prices belongs to the gateway (Phase 3).
- **Order ids** are assigned upstream and must be unique among resting orders. An id can
  be reused once its order is gone.
- **Owners** identify participants. They drive self-trade prevention, cancel/modify
  authorization and mass cancels. Owner ids are dense indices `0..max_owners` that the
  gateway assigns, the way exchanges number their participants internally; a new order
  from any other id is rejected (`InvalidOwner`). Dense ids let the book keep per-owner
  state in plain arrays instead of hash maps (see §3).
- **Trade ids** come from a per-book counter: 1, 2, 3, ... They are gap-free and part of
  the deterministic state.

## 3. Book structure

```
 price ladder (one per side)          order slab (preallocated)
 index = price - min_price            free list: LIFO, cache-warm reuse
┌──────┬──────┬──────┬──────┐        ┌────┬────┬────┬────┬────┐
│ ...  │ L100 │ L101 │ ...  │        │ #7 │ #3 │ #9 │free│ #4 │
└──────┴──┬───┴──────┴──────┘        └────┴────┴────┴────┴────┘
          │ head/tail                  ▲ prev/next u32 links
          └──► #7 ⇄ #3 ⇄ #9  (FIFO) ────┘

 occupancy bitset: 1 bit per level + 1 summary bit per 64 words
 id index: hash table of cache lines, 5 (id, slot) pairs each, room for 2x capacity
 stop ladders: two more ladders of the same shape, keyed by trigger level; buy stops
               run like asks (lowest trigger first), sell stops like bids
 iceberg parts: (display, visible) per slot, beside the slab; a level's total counts
                what its orders show, and the level counts its icebergs
 owner lists: one list per owner, head/tail in a table indexed by owner id,
              prev/next in a links array indexed by order slot
```

| Operation | Cost | How |
|---|---|---|
| Find the level for a price | O(1) | `price - min_price` |
| Add an order at a level | O(1) | append to the level's queue and the owner's list |
| Cancel an order anywhere in a queue | O(1) + hash lookup | doubly linked lists |
| Fill against the best level | O(1) per order touched | pop from the head |
| Find the next best level after one empties | O(levels / 4096) worst case | two-level bitset |
| Best bid / ask | O(1) | cached index |
| Cancel all of one owner's k orders | O(k log k) | walk the owner's list, sort into book order |

**Why a dense ladder.** Near the touch, real books are dense. Indexing an array beats
walking a tree: there is no pointer chasing and no rebalancing, and the hot levels share
cache lines. Each level is 24 bytes and each order node 48 bytes. Both sizes are checked
at compile time, so an accidental size increase breaks the build.

**Its limit.** Memory is `2 × 24 bytes × (max_price - min_price + 1)`. A 200k-tick band
needs about 10 MB, which is fine. But BTC with a $0.01 tick and a $0–$1M band would need
about 4.8 GB. Wide bands need a different structure: a ladder window that re-centres
around the touch, or a ladder near the touch with a tree beyond it. Phase 6 benchmarks
the ladder against `BTreeMap` and a sorted `Vec`, and that comparison decides the design
for wide markets.

**Why a custom id index.** Cancels and modifies look orders up by id. In a book far
larger than the cache, a general-purpose map pays two misses in a row per lookup: first
its control bytes, then the bucket. The index (`src/index.rs`) is a table of 64-byte
lines, each holding five ids with their slots, sized once for `2 × max_orders` and never
grown. An id lives in its home line unless that line is full, so a lookup normally reads
one cache line. Four consecutive ids share a home line: ids are usually handed out in
sequence, so a new order's duplicate check and insertion hit the line its predecessor
just used. An id that overflows goes to the next line with room, and the lines it passes
count it; a lookup moves on from a line only while that count is non-zero. Removal clears
the entry and lowers the counts, so there are no tombstones and never a rehash, which in a
general-purpose map stalls whichever command happens to trigger it. A test hammers a
permanently full book to show it never allocates. A lookup also stops when it gets back to
its home line: bursts of colliding ids at different times can leave every line with a
non-zero count at once, and without that stop an absent id would be searched for forever.

**Why owner lists, and why mass cancels sort.** Cancel-on-disconnect has to pull every
order of one participant at once. Scanning the book would cost time proportional to the
whole book, paid by every other participant waiting behind that command. With one list per
owner, the cost depends only on that owner's orders.

The lists are kept cheap in two ways, both measured on the baseline benchmark:

- **No hashing.** A first version looked owners up in a hash map and cost 12 ns per
  command at the median: 79 → 91 ns, and 17% less throughput. Dense owner ids make the
  owner table a plain array, which brought the cost down to about 3 ns.
- **Links outside the order node.** An order's owner links (8 bytes) sit in a separate
  array indexed by its slot. Matching never reads them, so the 48-byte node stays as it
  was.

The events of a mass cancel come out in book order (bids best price first, then asks,
each level in time priority), the same canonical order a snapshot uses. An owner's list
holds its orders in the order they started resting, which is not book order. But an order
joins its owner's list exactly when it joins the back of its level's queue, so within
one level the two orders agree. Sorting the owner's orders by side and price, with list
position as the tie-break, therefore gives book order, in a buffer reserved at
construction. The cross-level order of an owner's list is never observable, so a snapshot
does not need to record it.

## 4. Matching rules

- An incoming order trades against the opposite side while it crosses its limit. Best
  price comes first; within a price, the oldest order comes first.
- Each trade executes at the **maker's** price and is as large as possible:
  `min(taker leaves, maker leaves)`.
- A maker is left partially filled only when the taker is completely filled.

### Events

| Command | Events |
|---|---|
| `Limit` / `Market` | `Rejected` alone, or `Accepted`, then `Trade`s (with `Cancelled{SelfTrade}` for resting orders removed by self-trade prevention, and `Replenished` right after a trade that uses up an iceberg's tranche), then at most one of `Rested` or `Cancelled` for the remainder. A fill-or-kill order that cannot fill emits `Accepted` and `Cancelled{FillOrKill}` only. In a call phase a limit order emits `Accepted` and `Rested` only. A market order that the band stops, with `auction_on_band`, is followed by `PhaseChanged{Auction}` |
| `Cancel` | `Cancelled{Requested}` or `Rejected` |
| `Modify` | `Rejected`, or `Modified` followed, if priority is lost, by the events of a new limit order |
| `Stop` | `Rejected`, or `Accepted` and `StopPlaced` |
| `CancelAll` | `Cancelled{MassCancel}` for each of the owner's resting orders in book order, then each of its pending stops in trigger order, then `MassCancelled{owner, count}`. Never rejected; `count` is zero if the owner had nothing |
| `SetPhase` | Leaving a call phase: the uncross's `Trade`s, each followed by `Replenished` for the icebergs whose tranche it used up (the buy order's first). Then `PhaseChanged{phase}`. Never rejected |
| any command that trades | then, for each stop its trades reached, `Triggered` and the events of the order the stop becomes |

Every `Trade` carries the trade id and both sides' remaining open quantity (`leaves`).
An uncross trade has no aggressor; it reports the buy order as the `taker`.
A participant can therefore track its orders from events alone; the soak test checks
exactly that. Sequence numbers are deliberately absent. The sequencer numbers commands
when it journals them (Phase 2), and the publisher numbers outgoing messages (Phase 4).
The engine itself has no notion of time or transport.

## 5. Order semantics

| Command | Behaviour |
|---|---|
| `Limit`, GTC | Trades up to its price; the remainder rests at its price until cancelled. |
| `Limit`, IOC | Trades up to its price; the remainder is cancelled (`ImmediateOrCancel`, or `SelfTrade` under `CancelIncoming`). Never rests. |
| `Limit`, FOK | Trades its whole quantity up to its price, or nothing at all (`FillOrKill`). Never rests. |
| `Limit`, post-only | Rests without trading. Refused (`PostOnlyWouldCross`) if it would trade on arrival, and so is any later modify that would make it trade. |
| `Limit` with a display quantity | An iceberg: GTC or post-only, resting with only `display` lots on show. See below. |
| `Market` | Trades at any price within price protection and the price band. The remainder is cancelled with `NoLiquidity`, `PriceProtection`, `PriceBand` or `SelfTrade`. Never rests. |
| `Cancel` | Removes the order. Only its owner may cancel it. |
| `Modify` | FIX cancel/replace on **total** quantity; see below. Only the owner may modify. |
| `Stop` | Waits off the book, invisible to market data, until a trade reaches its trigger; then works as a market order, or as a GTC limit order if it has a limit price. See below. |
| `CancelAll` | Removes every resting order and pending stop of one owner, as on a session disconnect. |
| `SetPhase` | Moves the book to another trading phase; leaving a call phase uncrosses it. See §7. |

**Modify uses total quantity, as FIX does.** Suppose a participant sends "reduce 10 to 8"
while 5 lots are filling:

- If `qty` meant the new open quantity, the order would end up with 5 filled plus 8 open:
  13 lots, more than the participant ever wanted.
- With total-quantity semantics it ends with 5 filled and 3 open.

The cases:

| New total `qty` | Result |
|---|---|
| `qty <= filled` | Nothing is left to work: the order is removed (`Modified{leaves: 0}`) |
| same price, `qty <= current total` | Open quantity shrinks in place; **queue priority is kept** |
| anything else | Cancel/replace: back of the queue at the new price; may trade |

**Icebergs show one tranche at a time.** A resting iceberg shows at most `display` lots;
market data (`depth`, `best_bid`, `Rested{visible}`) sees only those, while its owner sees
the whole open quantity. A trade takes at most what the order shows. When a tranche is used
up, the next one, `min(display, leaves)`, goes to the **back** of the level's queue and
loses time priority, and `Replenished` reports it. A taker that keeps going therefore meets
the other orders at that price before it meets the iceberg again. An incoming iceberg
trades with its whole quantity; only the part that rests is hidden. Modifies cut the hidden
part first, so shrinking an iceberg keeps its tranche on show and its priority.

**An iceberg may be cut into at most `max_iceberg_tranches` tranches** (default 10): its
display times that must cover its total quantity. The rule exists because the random tests
found what happens without it. An iceberg showing 3 lots of 2 × 10¹⁸, met by an equally
large taker, made one command produce a trade and a new tranche for every 3 lots. In
practice it never finished. Exchanges bound this with a minimum display quantity. A ratio
bound also holds when `max_order_qty` is huge, and it makes the work of any command
proportional to the orders it reaches. The specification checker now enforces exactly
that for every command.

A modify that re-enters the book works like a GTC order. A post-only order keeps its
restriction, though: the flag is stored with the resting order and its snapshot, so a
market maker's order can never take liquidity, whatever it is modified to.

**Fill-or-kill decides before it trades.** "Fill completely or not at all" must hold under
self-trade prevention too. Under `CancelResting`, the owner's own orders in the way would
be cancelled, not traded, so they contribute nothing. Under `CancelIncoming`, matching
would stop at the first of them, so nothing behind it counts. So the engine walks the
opposite side in matching order, without changing anything, and sums what the order could
really take. Icebergs add a twist: at a level without an own order, a taker cycles through
every tranche and gets the hidden quantity too. Under `CancelIncoming`, though, new
tranches land behind the owner's order that stops the match, so they are out of reach. If that is enough it matches normally, otherwise it is killed and the book is
untouched. The walk stops as soon as the quantity is covered, so it costs no more than the
match it precedes. The reference book does not reimplement this rule: it runs the match on
a copy of itself and keeps the copy only if every lot traded. The two must agree on every
random sequence.

**Stops trigger on what traded, in a fixed order.**

- **When.** A buy stop triggers when a trade happens at or above its trigger, a sell stop
  at or below. Every price the command traded at counts, not just the last. A buy that
  trades at 99 and then 101 has reached a sell stop at 99, even though it ended above it.
- **Not at once.** A stop whose trigger the last trade price has already reached is
  refused (`StopWouldTrigger`); before the first trade, any trigger waits.
- **In which order.** Released stops trade, and their trades can reach further stops, so
  release repeats until none is left. Buy stops go before sell stops; within a side, the
  trigger the price passed first goes first, and at one trigger the oldest stop.
- **Becoming an order.** A stop-market becomes a market order with its caps measured at
  that moment. A stop-limit faces price protection and the band as they stand then; it was
  accepted long ago, so it is cancelled rather than rejected if it falls outside. What a
  stop-limit cannot fill rests in the slot the stop already held, at the back of its level.
- **Capacity.** A pending stop holds a slot of `max_orders`, sits in the id index and its
  owner's list, can be cancelled and mass-cancelled, but not modified (`PendingStop`).

## 6. Risk controls in the core

| Control | Rule |
|---|---|
| Static price band | Prices outside `[min_price, max_price]` are rejected (`PriceOutOfRange`). |
| Maximum order size | `qty` must be in `1..=max_order_qty` (`InvalidQuantity`). |
| Price protection | Measured from the opposite best price when the order arrives. A market order stops trading `price_protection` ticks beyond it (`Cancelled{PriceProtection}`). A limit or modify priced further through is rejected (`PriceOutsideProtection`). If the opposite side is empty, there is nothing to protect against. |
| Price band | Measured from the reference price: the last trade, or the configured `reference_price` (a previous close, say) before the first one. A limit or modify priced more than `price_band` ticks through it is rejected (`PriceOutsideBand`), and a market order stops there (`Cancelled{PriceBand}`). With both controls on, a market order stops at the tighter cap and names it; a tie names price protection. The reference is part of the state, in snapshots and the digest. |
| Self-trade prevention | Two orders of the same owner never trade in continuous trading. `CancelResting` removes the resting order and keeps matching. `CancelIncoming` cancels the rest of the incoming order and leaves the book untouched. The uncross does not apply it (§7). |
| Owner ids | A new order must come from an owner id below `max_owners` (`InvalidOwner`). This is the first check, before quantity and price. |
| Ownership | Cancels and modifies of someone else's order are rejected as `UnknownOrder`, exactly like a missing order, so others' order ids do not leak. |

**What each price control is good for, measured.** Price protection guards against fat
fingers relative to the book as it stands. But a stale order far from the market anchors
it: orders priced at the market are then rejected as if they were mistakes. A price band
relative to the last trade cannot be moved by stale orders, but it has the opposite
weakness. Once the market drifts more than the band away from the last trade without
trading, every order priced at the new market falls outside the band, nothing trades, and
the reference never moves again. `tests/market_health.rs` measures both under a harsh flow,
whose participants price off a fair value that drifts on its own, with four owners under
`CancelIncoming`, which leaves stale orders behind:

| Control | Time one-sided | Trades |
|---|---:|---:|
| none | 0% | 199,807 |
| protection 1 / 2 / 4 ticks | 22% / 25% / 24% | 105k / 106k / 113k |
| band 1 / 2 / 4 / 8 / 50 ticks | 70% / 90% / 83% / 62% / 46% | 216 / 621 / 9k / 34k / 96k |

Pulling the band's reference into the current spread barely helped (72-85% one-sided at
1-4 ticks), and it let a stale quote above the last trade move the reference, the very
weakness the band is meant to avoid, so it was dropped. Exchanges re-anchor a band
differently: a breach halts trading, and a reopening auction sets a new reference. The
book can do that too (`auction_on_band`, §7), and the same experiment shows it keeps the
book alive at a price in time spent in calls.

## 7. Trading phases and auctions

| Phase | Accepts | Trades |
|---|---|---|
| `Continuous` | everything | on arrival, as in §4 |
| `Auction`: a call (opening, closing, reopening) | limit orders, stops, modifies, cancels; refuses market, IOC and FOK orders (`AuctionCall`) | nothing until the call ends, so the book may cross |
| `Halted` | cancels and mass cancels only (`TradingHalted`) | nothing |
| `Closed` | cancels and mass cancels only (`MarketClosed`) | nothing |

`SetPhase` moves the book between phases. The sequencer decides when, so phases are part
of the command stream and replay like any other command. It is never rejected, and setting
the phase the book is already in only reports it. Continuous trading pays one branch on the
phase per order.

**In a call phase nothing trades on arrival.** So price protection and the band, which
guard against what an order trades on arrival, do not apply: the uncross is what finds the
new price, and a reopening call held to the band could never move it. Post-only keeps its
rule not to cross the opposite side. And since no match frees a slot, a full book refuses
every new order, crossing or not.

**The uncross.** Every way out of a call executes everything that can trade, at one price.
The candidates are the prices of the orders on the book and the reference price. At each,
the executable quantity is the smaller of the bids at or above it and the asks at or below
it, hidden iceberg quantity included. The rules, each breaking the ties the previous one
leaves:

1. the most executable quantity;
2. the least surplus, the quantity left over on the larger side;
3. market pressure: if every remaining price leaves its surplus on the buy side, the
   highest; if every one leaves it on the sell side, the lowest;
4. the closest to the reference price;
5. the lowest.

No price between two candidates executes more: there, the bids add up to what they do at
the candidate above and the asks to what they do at the candidate below. Rule 4 never
ties. The reference is itself a candidate, and when it lies between two tied prices it ties
too, because the bid sum only falls and the ask sum only rises with the price. So the
closest tied price is simply the reference clamped to the tied range, and rule 5 decides
only when there is no reference price. The random tests found this: "the lower of two
equally close" never happened, and the proof explains why.

Execution follows price-time priority on both sides. The oldest order at the best bid
trades with the oldest at the best ask, at the auction price, as much as both show, until
one side has nothing left at or through that price. Icebergs go one tranche at a time,
each new tranche to the back of its level, as in continuous trading. One side fills
completely, and the other in priority order, so the last order it reaches may fill in part.
At the volume-maximizing price this always leaves the book uncrossed: a bid and an ask left
crossing each other would mark a price that executes more. The auction price becomes the
reference price and so re-anchors the band.

**Cost.** The price search walks the occupied levels between the best ask and the best bid
and reads each level's total in O(1); only a level where an iceberg rests has its queue
summed, for the hidden quantity. Each level counts its icebergs, in four bytes that were
padding. The first version summed every order in the crossed range, and the latency bench
caught it: phase changes took 23 µs on average and 4 ms at p99.9. Now they take 0.6 µs on
average, 5.5 µs at p99 and 10 µs at p99.9. The uncross allocates nothing.

**Decisions.**

- *Market, IOC and FOK orders are refused in a call.* They exist to trade at once. Many
  exchanges accept market orders into auctions, with priority at the auction price; that
  is a deferral (§12).
- *No self-trade prevention in the uncross.* Removing an order that crosses the auction
  price can leave the rest crossed at another price, which one price cannot clear. Take
  one owner's bid of 10 at 101 and ask of 10 at 99, and another owner's ask of 5 at 100.
  The auction price is 99, where the first owner's two orders match exactly. Cancel the
  ask to prevent the self-trade, and the bid stays crossed with the ask at 100. So an
  owner whose orders cross each other at the auction price trades with itself.
- *Stops.* The uncross's trades reach triggers like any others, and the stops they reach
  are released once the new phase is in force. A released stop becomes its order under that
  phase: in continuous trading it trades; in a call a stop-limit rests and a stop-market is
  cancelled (`TradingPhase`); while halted or closed, both are cancelled. A halt leaves
  pending stops pending, since nothing trades, and they can still be cancelled.
- *Invariant.* A crossed book exists only in a call phase. `validate()` checks it, and
  `restore()` refuses a crossed snapshot in any other phase.

**A volatility interruption keeps a banded book alive, measured.** With `auction_on_band`,
a market order that the band stops short of liquidity beyond it moves the book into a call.
The market-health experiment (§6) adds rows in which the experiment, as the sequencer,
reopens with an uncross 100 or 1,000 commands later:

| Control | One-sided | In a call | Calls | Trades |
|---|---:|---:|---:|---:|
| band 1 / 2 / 4 / 8 / 50 ticks, alone | 70 / 90 / 83 / 62 / 46% | 0% | 0 | 216 / 621 / 9k / 34k / 96k |
| the same, reopened after 100 commands | 0% | 84 / 73 / 60 / 33 / 6% | 4,152 ... 255 | 152k / 144k / 155k / 168k / 196k |
| the same, reopened after 1,000 commands | 0% | 91 / 84 / 71 / 49 / 5% | 456 ... 28 | 137k / 156k / 147k / 164k / 196k |

Every reopening re-anchors the band, so the book never freezes: no sample finds it
one-sided, and it trades 69-98% as much as with no control at all. The price is time in
calls. A band tight against the flow's drift is breached by the next market order after
nearly every reopening, so at 1-2 ticks the book spends 73-91% of its time in calls, where
the flow's market orders are refused. At 50 ticks it is in a call 5-6% of the time and
trades within 2% of an unrestricted book. A halt is therefore a remedy for a band that is
rarely breached, not a substitute for choosing its width.

## 8. Capacity and admission

The book holds at most `max_orders` resting orders and pending stops. A new stop needs a
free slot. When the book is full, a new GTC or
post-only order is refused with `BookFull` **only if it does not cross**. IOC and FOK
orders never rest, so they are never refused. A crossing order always finds a slot:

- Its first match either fills the taker completely (nothing needs to rest), or
- it removes a resting order, by filling it or by self-trade prevention, which frees a
  slot.

Under `CancelIncoming` the remainder is cancelled instead of resting. A modify frees its
own slot before it re-enters. So the pool can never be exhausted mid-command. `alloc`
still asserts this, so a bug fails loudly instead of corrupting state. In a call phase
nothing matches, so a full book refuses every new order that would rest (§7).

## 9. Overflow safety

`BookConfig` requires `max_orders × max_order_qty ≤ u64::MAX` and asserts it at
construction. Every quantity sum in the book is a sum over at most `max_orders` orders,
each at most `max_order_qty`, so no sum can overflow for any input. `validate()` adds
with checked arithmetic, so an overflow would surface as an error rather than a
plausible-looking wrapped number.

This rule exists because an earlier version had no limit. Two orders of `u64::MAX / 2 + 1`
lots at one price made the level total wrap to 1 in release builds, and the old
`validate()` wrapped the same way and reported success.

## 10. Snapshots and state digest

`snapshot()` returns the book's complete state:
- its configuration;
- the trade counter, the reference price and the trading phase;
- every resting order with its owner, price, and open and filled quantity;
- every pending stop.

Orders come in canonical order: bids best price first, then asks best price first, and
each level in time priority. `restore()` rebuilds a book from a snapshot directly,
without replaying commands. It refuses snapshots the engine could never have produced:
too many orders, duplicate ids, orders that could not rest, a crossed book outside a call
phase, or a trade count above 2⁶³ − 1, which keeps at least 2⁶³ trade ids for the
restored book: at a billion trades a second, 292 years.

Two properties make snapshots safe to build on:

- **Complete.** A property test takes a snapshot at a random point of a random command
  sequence, restores it, and feeds the rest of the sequence to both books. Their events
  must be identical. Any state the snapshot left out (the trade counter, a fill history, a
  queue position) would sooner or later make them drift apart.
- **Canonical.** Two books are in the same state exactly when their snapshots are equal.

`digest()` is a 64-bit FNV-1a hash over a fixed little-endian encoding of the same state,
computed without allocating. A replica or a replay compares digests instead of shipping
whole snapshots. The encoding names every enum value explicitly, so reordering a
declaration cannot change it by accident. One test changes each field in turn and requires
the digest to change. Digests pinned in the golden tests are checked on Linux, Windows and
macOS. The digest detects accidental divergence; it is not a cryptographic hash.

Snapshots are plain data. Writing them to disk, and deciding when, belongs to Phase 2.

## 11. Verification

| Layer | What it shows |
|---|---|
| `tests/scenarios.rs` | One rule per test, with the exact expected event sequence. |
| `tests/differential.rs` | Over random configurations and command sequences, the engine and a deliberately naive reference (`BTreeMap` + `VecDeque`, no shared code) produce identical events and books, with `validate()` checked after every command. Two runs in three change phases; the reference finds the auction price by scoring every candidate and filtering the list rule by rule. A second strategy builds small calls whose sums tie, so every auction rule gets to decide. |
| `tests/properties.rs` | Specification checks that do not rely on a second implementation, after every command: each trade is with the next order in price-time priority, at the maker's price, within the limit or protection cap, never between the same owner, and as large as possible; leaves, trade ids and quantity add up; a remainder rests only when nothing more can trade; orders the command did not reach are unchanged; rejected commands change nothing; icebergs trade only what they show and replenish at the back of the queue; no command emits more events than its rules allow for the orders on the book; exactly the stops the command's trades reached trigger, in the order the rules release them, and become what the phase lets them; in a call phase orders rest without trading; each uncross trades at one price that no candidate beats under the auction rules, pairs both sides in priority order and leaves the book uncrossed; and a command is rejected exactly when a rule requires it, with that rule's reason. The events are replayed against a copy of the opposite side, so the checker follows iceberg tranches as they move. Each part of a command, its own effect and then each stop it releases, yields the state the rules say it must leave, and the engine's book must equal the end of that chain. The last check runs in both directions: an order that should have been refused but was accepted can leave a perfectly healthy-looking book, so acceptance has to be justified too. |
| `tests/soak.rs` | Hundreds of thousands of commands of realistic multi-participant flow against the reference, under both self-trade policies; participants rebuild the book from events alone. |
| `tests/soak.rs` (golden) | Pinned fingerprints of all events, and pinned digests of the final book, for four flows that between them cover both self-trade policies, protection and band stops, rejections, and trading phases with their uncrosses and volatility interruptions. CI runs them on Linux, Windows and macOS, which shows the output is identical across platforms. |
| `tests/market_health.rs` | An ignored experiment, run by hand: how long each price control leaves one side of the book empty under a harsh flow, with and without volatility interruptions (§6, §7). |
| `tests/snapshot.rs` | A book restored from a snapshot taken at a random point continues exactly like the original; the digest changes with every field; every kind of impossible snapshot is refused. |
| `tests/zero_alloc.rs` | A counting global allocator sees zero allocations in normal flow, in a permanently full book (worst case for the id index), in a deep book, under frequent mass cancels, and through phase changes and uncrosses, and none when computing the digest. |
| `src/bitset.rs` | Bitset searches agree with `BTreeSet`. |
| `fuzz/` | Coverage-guided fuzzing of the differential test, of the snapshot round trip, and of `restore` on arbitrary snapshots ([below](#fuzzing-and-formal-verification)). |
| Kani proofs | The bitset, the order pool's free list and iceberg arithmetic, and the owner lists, for every input within stated bounds ([below](#fuzzing-and-formal-verification)). |
| Coverage | CI measures line and branch coverage of the engine with cargo-llvm-cov ([numbers](../README.md#verification)). |
| Mutation testing | `cargo mutants` injects 758 small faults into the engine, and the tests detect every one of the 725 that compile ([results](../README.md#mutation-testing)). A weekly workflow repeats the full run. |

Random inputs are biased toward where bugs live: few ids (duplicates, unknown ids), few
owners (self-trades), prices at bitset word and summary boundaries, band edges and just
outside, quantities at 0, at `max_order_qty`, just above it, and at `u64::MAX`, with
`max_order_qty` itself either small or at the overflow limit.

### Fuzzing and formal verification

Property tests sample a fixed distribution. A fuzzer steers its inputs toward code they
have not reached yet, and a model checker covers every input within a bound. Both add to
the layers above; neither replaces them.

**Fuzzing** (`fuzz/`, cargo-fuzz with libFuzzer). The fuzz crate is a workspace of its
own, so normal builds never compile libFuzzer. The fuzzer's bytes are decoded with
`arbitrary` into a configuration and commands. The decoding always yields a valid
configuration, and it is biased like the property tests' strategies: few ids and
owners, prices at band edges and bitset word boundaries, quantities at the limits. But
the fuzzer also picks every configuration value: bands of up to 12,289 levels anywhere in
the `i64` range, 1 to 64 orders, 1 to 5 owners or 1,024, any number of protection and
band ticks up to `u32::MAX`, up to 255 iceberg tranches, either self-trade policy, and
whether a band stop starts a call. Phase changes are commands like the others, mostly to
continuous trading or a call, so books cross in calls and uncross when they end. An input
runs at most 512 commands. Tranches stay few because each one is a trade: with
`u32::MAX` of them, one command could legally emit billions of events, which only
exhausts the fuzzer's memory.

| Target | What every run checks |
|---|---|
| `differential` | The engine matches the reference book event for event, and in its orders, stops, trade count, reference price and phase, with `validate()` after every command. At the end, `digest()` equals the snapshot's digest. Bands keep 2³³ ticks away from the ends of `i64`, because the reference adds tick counts to prices in plain `i64` arithmetic. |
| `snapshot_roundtrip` | A snapshot taken at a cut the fuzzer chooses restores to a healthy book with the same snapshot and digest, which then emits exactly the original's events, with `validate()` on both books after every command. Bands may reach `i64::MIN` and `i64::MAX`. |
| `restore` | Up to 64 commands build a live book. Up to 16 edits then turn its snapshot into anything from valid to garbage: entries made from scratch, any field changed, entries removed, duplicated or moved, any trade count, reference price and phase. `restore` must not panic. It must accept exactly the snapshots that keep the rules `SnapshotError` documents, which the target restates independently of the implementation, and a refusal must name a rule the snapshot breaks. An accepted snapshot must yield a healthy book holding exactly its orders and stops, each level's queue in snapshot order, whose own snapshot restores to the same state. The book then runs up to 512 commands in step with the reference book loaded from the same snapshot. |

The targets are built with debug assertions and overflow checks, so an arithmetic
overflow anywhere fails a run: §9's claim is tested on inputs the property tests never
produce. They run without a sanitizer, because the library forbids `unsafe`. CI fuzzes
each target for 30 seconds on every push, and a weekly workflow fuzzes each for 20
minutes on every core of the runner. Both start from a corpus cached between runs, plus
long random inputs, so their time adds up.

To run a target locally (needs a nightly toolchain and `cargo install cargo-fuzz`):

```sh
cargo +nightly fuzz run differential -s none -a -- -max_total_time=60 -len_control=0 -max_len=4096
cargo +nightly fuzz run differential -s none -a fuzz/artifacts/differential/<file>   # reproduce a failure
cargo +nightly fuzz fmt differential fuzz/artifacts/differential/<file>              # print it as data
cargo +nightly fuzz tmin differential -s none -a fuzz/artifacts/differential/<file>  # shrink it
```

`-len_control=0` lets inputs grow to full length at once, so even a short run reaches long
command sequences; by default libFuzzer lengthens inputs slowly, and a one-minute run
stays at a few commands per input. On Windows with MSVC, linking needs AddressSanitizer:
leave out `-s none`, and put the directory holding `clang_rt.asan_dynamic-x86_64.dll`
(`VC\Tools\MSVC\<version>\bin\HostX64\x64` in Visual Studio) on `PATH`.

**Proofs** (Kani). Proof harnesses sit in a `#[cfg(kani)] mod proofs` at the end of
`src/bitset.rs`, `src/pool.rs` and `src/owners.rs`. Kani checks each harness for every
input within its bounds, and also that nothing in it panics, overflows or indexes out of
bounds.

| Harness | Proves | Bounds |
|---|---|---|
| `bitset::proofs::low_bits_through_…`, `highest_bit_…` | The two bit helpers of the searches. | Every bit position, every non-zero word. |
| `bitset::proofs::dense_sets_…` | `next_at_or_after` and `prev_at_or_before`, from any start including `usize::MAX`, equal a linear scan over a boolean array, after any insert and remove. | Every subset of bands of 0, 63, 64, 128 and 130 levels: none, a partial word, one and two full words, a partial third word. |
| `bitset::proofs::sparse_sets_in_…_summary_words` | The same searches return the nearest element on each side when it lies summary words away. | Up to 2 elements anywhere, any one index removed, in bands of 4,097 levels (a second summary word with one level), 8,192 (two full summary words) and 8,193 (two full ones and a third with one level, so a search can skip a whole empty summary word). |
| `pool::proofs::free_slots_form_a_lifo_stack` | `alloc` returns the slot freed most recently; the free list holds exactly the free slots, each with no quantity, and ends in `NIL`; `is_full` and `live` agree with it. `validate()` and `alloc` rely on this. | 3 slots, any 6 allocations and frees. |
| `pool::proofs::fills_trade_what_the_order_shows`, `shrinking_takes_the_cut_from_the_hidden_part_first` | The quantity arithmetic of fills, iceberg replenishment and in-place shrinks: amounts, what the order shows afterwards, no underflow. | Every quantity of a plain order or an iceberg. |
| `owners::proofs::lists_hold_each_owners_orders_in_link_order` | Each owner's list holds exactly that owner's slots in link order, which mass cancels rely on, with consistent back links, tail and count; owners outside the table read as empty. | 2 owners, 4 slots, any 5 links and unlinks, including the unlink and relink an iceberg's new tranche causes. |

The bounds include every shape the code treats differently: a partial and a full last
word, searches that cross a word or a summary word, an empty and a full pool, an owner
with no, one or several orders. Band lengths and the pool's capacity are fixed per
harness, while everything else stays symbolic: a symbolic length makes every vector
symbolic in size, and those harnesses exhausted the CI runner's memory. CI proves the bit
helpers, the order pool and the owner lists on every push, each in under a minute. The
search proofs take far longer (`sparse_sets_in_two_summary_words` took 25 minutes), so
the weekly workflow runs them. Kani does not run on Windows. On Linux or macOS:

```sh
cargo install --locked kani-verifier && cargo kani setup
cd crates/orderbook
cargo kani                                                # every proof
cargo kani --harness dense_sets_in_two_full_words        # one proof
```

cargo-mutants skips `#[cfg(test)]` code but not `#[cfg(kani)]` code, which ordinary
builds never compile, so `.cargo/mutants.toml` excludes the `proofs` modules.

**Coverage.** A CI job runs the test suite under cargo-llvm-cov and reports line and
branch coverage of `crates/orderbook/src`, unit-test modules included and the benchmarks'
workload generator left out. The summary is in the job log, and `lcov.info` is uploaded as
an artifact. The numbers are in the [README](../README.md#verification). What the tests
leave unrun is defensive code: `validate()`'s level-overflow error, which §9 makes
unreachable; single conditions within `validate()`'s corruption checks; the guards of
`BookConfig::new` and `OrderBook::new` against zero or `u32::MAX` orders; a zero-capacity
pool; and the failure branch of a debug assertion. Branch coverage needs nightly. To
measure it locally:

```sh
rustup component add llvm-tools-preview --toolchain nightly
cargo install cargo-llvm-cov
cargo +nightly llvm-cov --workspace --branch --ignore-filename-regex '(workload|engine-soak)\.rs' --summary-only
```

## 12. Known limitations and deliberate deferrals

| Limitation | Plan |
|---|---|
| The book has no per-participant limits: one owner can fill it and block others with `BookFull` | Enforced in front of it: the gateway limits each account's open orders and each session's message rate (§15) |
| The id index's hash is not keyed: a caller choosing ids adversarially could crowd many into one home line, and lookups would then scan several lines | Through the gateway, ids are sequence numbers and participants never choose them (§15). A directly indexed table was considered: ids grow without bound while an order may rest indefinitely, so it would need the collision handling the index already has |
| Ladder memory grows with band width (see §3) | Phase 6: benchmark alternatives and add a windowed or hybrid ladder |
| Without `auction_on_band`, a price band never re-anchors on its own: if the market moves away without trading, it freezes (measured in §6). With it, a band tight against the market's moves keeps the book in calls most of the time (§7) | The band's width is a configuration choice; widening the band during a call, as some exchanges do, is not implemented |
| Calls refuse market orders; there are no auction-only order types, no published imbalance, and no collar on the auction price besides the static band. Market data can read `indicative_uncross()` | Phase 9 |
| The uncross does not prevent self-trades (§7) | Phase 9, with per-order self-trade instructions |
| Calls end only when the sequencer says so; the engine has no timers or random call ends | Phase 4: the sequencer schedules phase changes |
| An uncross sums the queues of the crossed levels where icebergs rest, so its cost grows with those orders | Measure first; a per-level hidden quantity would remove it at 8 bytes per level per side |
| One instrument per book | Phase 7: one book per instrument, sharded across cores |
| Self-trade policy is per book, not per order | Phase 9: per-order STP instruction |
| Pending stops cannot be modified, and there are no trailing stops | Later: modify of a pending stop's trigger, limit and quantity; trailing stops |
| Records vouch only for what was synced before they were written: damage to the last synced batch, with nothing written after it, is cut like a torn write (§14) | Phase 8: a standby holds a second copy to compare against |
| A segment roll stalls the journal for 8 to 23 ms on this laptop and a sync costs about a millisecond (§14). The pipeline (§16) takes both off the matching thread, but acknowledgements still wait for them, since the book applies only journaled commands | Phase 6: measure Linux, `io_uring` and drives with power-loss protection |
| Snapshots are taken on the matching thread, which stalls while the book is encoded, written and read back, and opening replays one snapshot interval a second time to verify it | Phase 4 or 6: take snapshots from a copy, such as the standby's book |
| Records carry no timestamp: the sequencer assigns none yet, and the 64-byte record has no room for one | Phase 6, with per-stage timestamps for the latency breakdown: a second record format version |
| One network thread reads, routes, publishes market data and writes every socket. On this laptop it, not the matcher, bounds throughput (§16) | Phase 6: measure on Linux first; then split the publisher's writes off, or shard sessions over network threads |
| Plain TCP; tokens travel and are stored in the clear, and are compared in variable time | Phase 5 puts the public demo behind TLS; Phase 7 brings real account management |
| A client that reconnects cannot ask what it missed: it is told its open orders on login, but there is no replay of its reports from a sequence number, and orders placed after the last checkpoint lose their client references in a restart. Market data recovers by a new snapshot, not by replay | Phase 7, with the user-facing API: a report history per account |
| Market data is one book's levels and trades over the order-entry protocol: no per-order (L3) feed, no multicast, and subscribers must log in | Per-order feeds when an instrument needs them |
| Paper accounts place limit orders and cancel them, nothing else: market and stop orders have no price to hold against, and a modify could need more than is held while it waits | Holds at the price band's edge for market orders, and modifies checked against what the order already holds |
| A visitor's account lives in the browser's local storage: clearing it loses the account, and nothing ties an account to a person | Phase 7: real accounts |
| A command that makes the book panic does so again on every replay, so the engine cannot recover past it on its own | Operational: recovery up to a given sequence number, and Phase 8's standby to compare against |
| Retention does not know where consumers stand: a consumer further behind than the oldest kept snapshot cannot resume, and recovery refuses with `MissingJournal`. The gateway's own consumers do not need it: they recover from the book (§15, §16) | When an outside consumer of the journal appears, such as Phase 8's standby |

## 13. Performance work

Each attempt was measured against the commit before it with interleaved A/B runs of
`cargo bench --bench latency` (base, candidate, base, candidate, ...) on a laptop busy
with other builds, so only same-session medians are compared. Numbers are medians of the
per-invocation p50 and p99 (ns) and of the throughput (M cmd/s). The deep scenario is the
target: its cancels and modifies miss the cache on almost every access.

| Attempt | Deep: p50, p99, throughput | Other scenarios | Verdict |
|---|---|---|---|
| Linear-probing index, Fibonacci hash, (id, slot) entries side by side, 2× capacity, backward-shift deletion | 328 → 306, 1280 → 1272, 2.74 → 3.13; cancel 470 → 366, but limit 220 → 238, market 279 → 325 | not measured | Rejected: a new id's probe runs to an empty entry, which at half load often leaves the line, while the hash map's 4 MB of control bytes likely stay mostly in L3 |
| The same at 4× capacity | 302 → 288 and 334 → 319 in two sessions; cancel 335–373, limit 229–249 | not measured | Rejected: new orders still slower than before |
| Linear probing with four consecutive ids per 64-byte line, 2× and 4× capacity | 334 → 289 (2×), 334 → 275 (4×); limit 219 → 117–129, cancel back to 418–474 | not measured | Rejected: 45% of removals' backward shift reads the next line, a second miss in a row |
| Lines of five (id, slot) pairs with overflow counts, four consecutive ids per home line | 305 → 245, 1149 → 1047, 3.08 → 3.96; limit 213 → 122, cancel 452 → 345 | baseline p50 93 → 98, protected 95 → 100: in-cache lookups cost ~8 ns more for new orders | Refined below |
| **Kept:** the same with the five ways compared without branching | 328 → 254, 1266 → 1010, 2.46 → 3.70; limit 219 → 119, cancel 491 → 362, modify 615 → 533 | baseline 92 → 93, sweep 91 → 91, protected 95 → 95; p99 lower in all three | Kept (`src/index.rs`) |
| Order nodes aligned to 64 bytes (48 → 64), so none straddles two cache lines, on top of the kept index | 239 → 238, 927 → 918; cancel 342 → 341 | baseline 89 → 89 | Rejected: no measurable effect for a third more memory |

What the kept index changes: a lookup reads one cache line instead of a control-byte group
and then a bucket, new orders find their line already cached because ids arrive in
sequence, and removal never leaves tombstones, so no command ever pays for a rehash. Its
memory for a million orders is about 32 MB, against about 68 MB for the hash map.

Not attempted yet: owner links inside a 64-byte node (cancels would read the owner
neighbours' nodes instead of three entries of the links array), a hot/cold split of the
node, and a smaller level struct.

## 14. The journal and recovery

`crates/engine` puts the book behind a sequencer and a write-ahead journal, so its state
survives the process. The goal of Phase 2: rebuild the exact state from the command log
alone.

### Order of operations

`Engine::submit` gives a command the next sequence number (1, 2, 3, ...), appends it to the
journal, syncs as the [sync policy](#sync-policies-and-what-they-cost) says, and only then
applies it to the book. The book never sees a command the journal does not hold, so
replaying the journal through a fresh book reaches exactly the state the live book was in:
the book is a pure function of its command sequence (§1). Rejected commands are journaled
too; the log records inputs, not outcomes.

The book's events go to an `Output`, each tagged with the sequence number of its command.
They are emitted after the command is journaled, so a crash in between loses them with the
process, and recovery delivers the events of replayed commands again under the same
numbers. A consumer that remembers the last number it has fully handled says so through
`Output::resume_after`: recovery then starts from a snapshot no later than that, so it can
deliver exactly the events after it, and returns `Error::ConsumerAhead` if the consumer
has handled commands the journal no longer holds. That can only happen under
`SyncPolicy::Os`, where a power failure takes back commands whose events were already
delivered. The journal and snapshots must reach back to where the slowest consumer stands.

`Engine::open` takes an exclusive OS lock on a `LOCK` file in the directory (`flock`,
`LockFileEx`), held while the engine is open and released by the OS however the process
ends. Two engines on one directory would append records under the same sequence numbers;
the second gets `Error::Locked`.

### The journal format

The journal is a series of segment files, `journal-<first seq>.log`: a 64-byte header, then
`capacity` slots of 64 bytes, 2¹⁶ by default (4 MiB).

| Bytes | Record field |
|---|---|
| 0..4 | CRC-32 of bytes 4..64 |
| 4 | kind: 1 = command |
| 5 | payload length: 40 |
| 6..8 | zero |
| 8..16 | sequence number |
| 16..24 | the highest sequence number synced when the record was written |
| 24..64 | the command, in the strict encoding of `engine::codec` |

The header holds a magic number, the format version, the record size, the segment's first
sequence number, a fingerprint of the book configuration, the capacity, the version of the
matching rules, and its own CRC.

- **Fixed-size records.** The record for `seq` is in slot `seq − first_seq`, so replay
  finds its starting point by arithmetic, and a record never straddles a 4 KiB page: 64
  divides 4,096.
- **Written full of zeros before use.** An all-zero slot is free, so the end of the log is
  where the zeros start, with no length field to keep in step. Writing the zeros, rather
  than only setting the length, makes the file system allocate every block in advance
  (a length alone makes a sparse file on Linux, and on NTFS leaves the valid data length
  to be advanced by every append): appends then overwrite allocated blocks, and a data sync
  has no metadata to commit. It brought a sync on the development laptop from about 2 ms
  down to about 1 in most runs (below), and it overwrites whatever an earlier file left in those blocks, so stale
  records cannot reappear. PostgreSQL fills its WAL segments with zeros for the same
  reasons.
- **Prepared ahead.** The next segment is created as `journal-<seq>.next` when the current
  one takes its first record, and filled with zeros 64 KiB at a time, twice as fast as
  records arrive. Rolling over finishes it, writes the header, syncs it, renames it into
  place and syncs the directory, all before a record goes in. The sync still has to write
  whatever zeros the OS has not written back yet, which is why segments are small: with
  64 MiB segments the slowest call, the one that rolled over, took 130 to 147 ms under the
  OS policy; with 4 MiB, 8 to 23 ms. A failure to prepare, such as a full disk, does not
  stop trading: `Engine::take_failure` reports it, and the roll prepares the segment
  again, failing then only if it still cannot.
- **Strict decoding.** A record is valid only if its checksum matches and its command
  decodes canonically (`engine::codec`: unused fields and padding must be zero), so damage
  that happened to keep the checksum would still have to produce a canonical encoding.
- **Configuration fingerprint and rules version.** A journal written for another book
  configuration is refused (`Error::ConfigMismatch`), and so is replaying records written
  under another version of the matching rules (`orderbook::RULES_VERSION`,
  `Error::RulesMismatch`): the same commands could reach another state.

### Recovery

1. Take the directory lock; delete leftover temporary snapshot files.
2. Choose the starting snapshot: the newest that loads and is no later than what the
   consumer has handled. A read error stops recovery (`Error::Io`): the disk's problem,
   not the file's. A snapshot of another format version stops it too
   (`Error::Unsupported`), untouched. A damaged one is noted and the next older is tried;
   with none left, recovery starts from an empty book.
3. With `verify_replay` (the default), load the snapshot before that one, or start from an
   empty book, replay the journal up to the starting snapshot without changing anything,
   and compare digests: a mismatch is `Error::Divergence`, before anything is repaired or
   delivered. Every start where the journal still reaches back that far thus proves that
   replay is deterministic and that the files belong together; when it does not,
   `RecoveryReport::unverified` says why.
4. Read the segment headers. A header that fails its checksum, in the newest segment with
   no valid record behind it, is damage that cost nothing, since a segment gets its final
   name only once its header is synced. A header whose checksum holds but which this
   version does not write is `Error::Unsupported`; one under the wrong name, or of a
   foreign file, is damage. Headers of segments wholly before the starting point only
   serve older snapshots: damage to them, a read error, or a gap among them does not stop
   recovery.
5. Check that the segments replay needs follow each other and reach the snapshot. A
   journal that ends before its snapshot can only be damaged, since the journal is synced
   before every snapshot: recovery refuses. A segment written under other rules may only
   hold nothing after the snapshot; then the journal is ended at the snapshot and goes on
   in a new segment, which is the upgrade path (below).

   Up to here recovery has changed nothing. From here on it repairs.

6. Delete the segment found torn, any segment still being prepared, and the clean-shutdown
   record; end an old-rules segment at the snapshot.
7. Replay from the record after the snapshot while each record holds the next sequence
   number, delivering each command's events.
8. Cut the log where replay stopped. First read everything after the cut, in this segment
   and the later ones. If any valid record there says it was written after the cut point
   had been synced, the bad record was durable and is now damaged: recovery stops with
   `Error::Corrupt` rather than continue without commands that were acknowledged.
   Otherwise everything after the cut belongs to the unsynced tail of a crash, where a
   power failure may tear writes or drop them in any order: zero those slots and delete
   the later segments.
9. Write again every record nothing vouches for: no later record, and no clean-shutdown
   record (`Engine::close` leaves one, saying everything was synced). Then sync the file,
   then the directory.
10. Rename the damaged snapshots `snapshot-<seq>.damaged`; retention deletes them once
    they are older than every kept snapshot.

Each repair guards against the next failure, not the current one, and each is shown to be
needed by a test that fails without it:

- **Zeroing the cut tail (8).** A stale record from a lost tail could, after the next
  crash, sit right where a new record was expected and replay a command that was never
  acknowledged.
- **Syncing what was recovered (9).** Records that survived only in the OS cache of a
  killed process would be vouched for by new records; a power failure would then lose
  them while the journal says they were durable.
- **Writing the unvouched tail again (9).** After a failed sync, Linux marks the pages
  clean although they never reached the disk; a process that reopens the journal without a
  reboot reads them from the cache, and a plain sync does nothing for them.
- **Syncing the directory (9).** A segment a killed process created may exist only in the
  OS cache; adopted and filled with acknowledged records, it would vanish whole in a power
  failure.

| What happened | What recovery keeps |
|---|---|
| The process was killed, in whatever it was doing | Every command it acknowledged, and possibly part of the batch it was journaling |
| Power failed, `SyncPolicy::Always` | Every acknowledged command |
| Power failed, `SyncPolicy::Os` | Every command up to the last sync (a segment roll, `Engine::sync` or `close`, a snapshot), and possibly more; a consumer that handled lost commands gets `ConsumerAhead` |
| A sync failed, and the process reopened the engine without a reboot | The same as above: the records the failed sync left behind are written again before they count |
| A synced record was damaged and a later record vouches for it | Nothing silently: recovery refuses with `Error::Corrupt` |
| The last synced batch was damaged, and nothing was written after it | After a clean shutdown, nothing silently: the shutdown record vouches for it. Otherwise, the commands before it: such damage cannot be told from a torn write, and is cut like one |
| The newest snapshot was damaged | Everything, from the previous snapshot if one is kept, or from the start if the journal still begins there; otherwise `Error::MissingJournal` |
| Files only older snapshots need were damaged or missing | Everything: recovery does not need them; verification reports that it could not run |
| Files from a newer format | Nothing changes: recovery refuses (`Unsupported`) |
| A journal written under other rules | The state at the old version's last snapshot, if nothing after it needs replaying; otherwise recovery refuses (`RulesMismatch`) |

In every case recovery keeps a prefix of the submitted commands and rebuilds exactly the
state after it: it never invents, reorders or half-applies a command.

### Snapshots

`Engine::snapshot` syncs the journal, then writes the book's snapshot (§10) with a header
holding the sequence number, the book's digest, the rules version and two checksums, to
`snapshot-<seq>.tmp`. It syncs that file, renames it to `snapshot-<seq>.snap` and syncs the
directory, so a file with the final name is always complete. It then reads the snapshot
back, with every check recovery would make, and only if it restores to exactly this book
deletes anything older. Loading checks the header, the checksums, the decoding,
`OrderBook::restore`'s own rules, and that the restored book's digest equals the header's.

With `snapshot_every: Some(n)`, a snapshot is taken before the first batch after `n` more
commands. If it fails, for example on a full disk, the batch goes ahead, the next attempt
waits another `n` commands, and `take_failure` reports why: a snapshot is an optimisation,
and its failure must not stop trading. Only a failed journal sync poisons the engine.

Only the newest `keep_snapshots` are kept. Once there are that many, journal segments that
hold nothing after the oldest kept snapshot are deleted; until then the whole journal
stays. So disk use stays bounded, and with more than one snapshot kept, a damaged newest
snapshot falls back to an older one.

### Sync policies and what they cost

- `SyncPolicy::Always` syncs before `submit` or `submit_batch` returns. A batch shares one
  sync, which is group commit, unless it fills a segment: the full segment is synced
  before the next is put in place.
- `SyncPolicy::Os` syncs only when a segment fills up, on `Engine::sync` and
  `Engine::close`, and before a snapshot; otherwise the OS writes the journal back when it
  chooses.

`cargo bench -p engine --bench journal`, on the development laptop (i5-12450H, Windows 11,
consumer NVMe SSD, P-core 2), the baseline flow, measured with `Instant` (100 ns
resolution on Windows). Syncing on this drive varies a lot between runs, so the figures
are ranges: over five runs of this session for the configurations that sync on every
call, whose cost the syncs dominate whatever the segment size; over the runs with the
current 4 MiB segments for the others, where the size of a segment matters.

| Configuration | Throughput | p50 per command | p50 per call | Slowest call |
|---|---:|---:|---:|---:|
| Book alone, no journal | 10.1–10.5M cmd/s | 100 ns | 100 ns | 0.4 ms |
| `Os`, one command per call | 433k–461k cmd/s | 1.8–1.9 µs | 1.8–1.9 µs | 8–23 ms |
| `Os`, 64 commands per call | 2.5M–4.0M cmd/s | 109–116 ns | 7.0–7.4 µs | 10–19 ms |
| `Always`, one command per call | 0.4k–1.0k cmd/s | 0.97–2.3 ms | 0.97–2.3 ms | |
| `Always`, 8 per call | 3k–8k cmd/s | 121–282 µs | 0.97–2.3 ms | |
| `Always`, 64 per call | 18k–57k cmd/s | 17–43 µs | 1.1–2.7 ms | |
| `Always`, 512 per call | 94k–370k cmd/s | 2.4–10 µs | 1.2–5.1 ms | |

Recovery replayed a million journaled commands in 94 ms in the run with 4 MiB segments
(138–256 ms with 64 MiB ones), from the page cache right after they were written, which
includes writing again those no record vouches for. Recovery after a reboot reads from the
disk and was not measured. A snapshot of the resulting book, 10,000 orders, took 11–31 ms
to write, sync and read back, and opening the engine from it 88–164 ms, which includes
replaying the million commands from the start to verify it.

What the numbers say:

- **A sync costs about a millisecond on this drive**, however little it writes, and two or
  more in a bad run. It cost about two in every run before segments were written full of
  zeros (2.1–2.2 ms per single-command sync, measured the same way), consistent with NTFS
  committing the file's valid data length, which every append into a merely sized file
  advances, on each sync. A consumer NVMe drive without power-loss protection really writes
  to flash on every flush; drives with power-loss protection acknowledge flushes from their
  protected cache much faster, which Phase 6 measures, on Linux.
- **Group commit is what makes durability affordable**: at 512 commands per sync, durable
  journaling costs a few microseconds per command instead of a millisecond. In Phase 4 the
  journal stage takes whatever the ring buffer holds, so batches grow with load by
  themselves.
- **Without syncs, the system call dominates**: about 2 µs per `WriteFile`, twenty times
  the matching. Batching 64 commands per write brings throughput to 2.5–4.0M commands a
  second. Writing every segment full of zeros costs part of that: with segments that were
  only sized, the same configuration ran at 5.3M. It is the price of the cheaper sync, and
  it moves off the matching thread with the journal stage in Phase 4, along with the
  slowest calls, the ones that roll over.

### Where the time goes, live

The engine counts where its time goes, for whoever asks with `take_timings`: the batches
it journaled and how long appending and syncing them took, the commands it applied and how
long that took with the output's handling of their events, and the book's own time on
one command in 64. A batch costs a few readings of the clock, a few tens of nanoseconds
next to a journal write. The book's time cannot be read around every command, since the
reading would cost about as much as the matching; the sampled command's events are instead
collected in space reserved once and handed on after the book is done, so the time is the
book's alone and nothing is allocated. A command with more than 256 events hands the rest
on as they come and is not counted. Handing on afterwards changes nothing the output sees:
the events and their order are the same, as a test checks against a book on its own.

### When something fails

A failed write or sync poisons the engine: every later call returns `Error::Poisoned`
until it is reopened, because the journal may not hold what the engine believes it does.
A failed command was not applied; but if its record reached the disk, recovery will apply
it, so a client must treat an error as an unknown outcome and reconcile through the
sequence numbers of the events it has seen. A panic while a command is applied, in the
book or in the `Output`, poisons the engine too. A failed snapshot, or a failure to prepare
the next segment, does not: `Engine::take_failure` reports it.

### Upgrades

The journal's format version and the rules version change independently. A binary refuses
segments and snapshots of a format it does not know, and changes nothing. A change to the
matching rules raises `orderbook::RULES_VERSION`; to move to it, the old binary takes a
snapshot and closes, and the new one starts from that snapshot: it ends the old journal
there and continues in a new segment under the new rules. Snapshots load under any rules
version, since they hold state rather than commands. Verification cannot replay across the
change, and says so.

### How it is verified

| Test | What it shows |
|---|---|
| `tests/codec.rs` | Every command, over the full range of each field, and snapshots of real books round-trip; arbitrary and altered bytes decode canonically or not at all. |
| `tests/interrupted.rs` | The process dies at every single change a run makes to the disk, in turn (in appends, segment preparation and rolls, snapshot writes and checks, retention), then a kill or a power failure, a recovery that may itself die and be followed by another power failure, and two more rounds of the same at random points. A second test does the same at sampled points with segments prepared in several pieces. Each recovery keeps what was durable (everything acknowledged, after a kill), invents nothing, and rebuilds exactly the state after what it kept; the stream then finishes in the uncrashed state. |
| `tests/recovery.rs` | 400 runs of random settings with four failures each between batches, power failures (in-order or reordered write-back, sectors torn out of writes, lost directory changes) or kills; and 300 runs with a bit flipped anywhere: recovery refuses or rebuilds a prefix that lacks at most the last command. |
| `tests/files.rs` | One test per way the files can contradict the engine: leftover temporary files, snapshots that lie about their sequence number, length, digest, format or configuration, missing, misnamed, short, cut or damaged segments, records in the wrong slot, a journal that ends before its snapshot, two engines on one directory, the exact pace of segment preparation. |
| `tests/operations.rs` | Events with their sequence numbers, redelivered on replay, and consumers resuming where they stood or told they are ahead; replay verified against snapshots before anything changes; failing snapshots and segment preparations that do not stop trading; a failing roll; a failed sync followed by a reopen and a power failure; a clean shutdown; settings that change between runs; the upgrade to new rules and commands under old ones; damage to files recovery does not need; read errors; panics. |
| `tests/kill.rs` | The acceptance test: a child process on the real file system, killed 48 times at random points under 16 combinations of sync policy, segment size and snapshot interval, recovers every command it had reported applied, and exactly the state after them. CI runs it on Linux, Windows and macOS. |
| `tests/zero_alloc.rs` | Journaling, syncing and applying commands on the real file system allocate nothing between segment rolls, the measured commands included. |
| `tests/timings.rs` | Measuring hands on exactly the events, in exactly the order, that a book on its own gives, including for a measured command with more events than are reserved, which is then not counted; every batch and command is counted, and one command in 64 is measured. |
| `fuzz/fuzz_targets/recovery.rs` | Any sequence of commands, syncs, snapshots, power failures, kills, deaths after any number of changes, recoveries that die, and flipped bits, under book configurations the fuzzer picks, segments of 1 to 32 or of 1,024 to 2,816 records, any snapshot interval and either sync policy. |

Each safety mechanism was removed in turn to check that a test notices: zeroing the cut
tail, syncing recovered records, writing the unvouched tail again, syncing the directory at
the end of recovery, syncing a segment before the next is created, syncing a snapshot
before renaming it, and refusing when a later record vouches for a damaged one.

The first version of this phase passed its tests and still had defects. A stricter
damage test found that damage to the one record at a snapshot's sequence number made
recovery delete the journal after it, and that retention left no fallback when a lone
first snapshot was damaged. The fuzzer showed that the vouching guarantee had been stated
too strongly. An adversarial review then found six more: a header from a newer version,
or under the wrong name, made recovery delete the newest segment with its records;
recovery never synced the directory; a reopen after a failed sync called unwritten records
durable; read errors set good snapshots aside; snapshots were deleted before their
successor was known to load; and a failing snapshot blocked every later command. A second
review of the reworked code found that the documented upgrade path could not work, that
verification could loop forever on a missing old segment and ran only after recovery had
already repaired files and delivered events, that the consumer promise failed behind a
snapshot and after a power failure under the OS policy, that a failure to prepare a
segment stopped trading, that every start under the OS policy wrote a whole segment again,
and that preparing segments ahead had moved the stall at a roll rather than removed it.
Each is fixed, with a test that fails without the fix; `interrupted.rs` and
`operations.rs` exist because the earlier tests could not see these: they only failed
between batches, never inside an operation.

## 15. The gateway

`crates/protocol` defines the wire protocol and `crates/gateway` serves it: clients log in
over TCP, enter orders, and receive reports of what happens to them. The goal of Phase 3:
accept orders from the outside world without letting any of it reach the book unchecked.

### The protocol

Every message has a fixed length: a 4-byte header (total length `u16`, type `u8`, a zero
byte) and a body of little-endian integers. The tables are in the `protocol` crate's
documentation; the client sends `Login`, `Logout`, `Heartbeat`, `NewOrder`, `Cancel`,
`Modify` and `MassCancel`, and the exchange sends `LoginAccepted`, `LoginRejected`,
`Logout`, `Heartbeat`, `Reject` and `Report`. A `Report` describes one event of the book
about one order, as its owner sees it: accepted, rejected with the book's reason, filled
(with the trade id, price, quantity and what is left), rested, replenished, cancelled
with a reason, modified, stop placed, triggered; or, about no order in particular, a mass
cancel's count and a phase change. Every report carries the sequence number of the
command that caused it.

Decoding is as strict as the journal's: an unknown type, a length that is not the type's,
a code out of range, a non-zero padding byte or unused field are all errors. A decoder
that accepted variants would give two byte strings one meaning and turn a client's bug
into a guess. Directions are separate: a client cannot send what only the exchange sends.
Fixed lengths make framing a lookup, and a connection's undecoded input never exceeds one
partial message.

### Sessions

A connection's first message must be a `Login` with the protocol version, an account and
its token; anything else ends the session with `Logout(ProtocolError)`. A login is refused
for an unknown version, account or token, or an account already logged in elsewhere, and
the connection is closed: one session per account, so a client cannot race itself. A
second login in a session is refused and the session goes on. `LoginAccepted` carries the
sequence number of the last command the exchange has taken: every report after it carries
a larger one.

A logged-in session that has been sent nothing for a second gets a `Heartbeat`; a session
that has sent nothing for five is logged out as idle. The exchange logic takes the time
as an argument and reads no clock, so these rules are tested, and fuzzed, with time under
the test's control.

Accounts come from a text file, one per line: id, token, open-order limit and message
rate. An account's id is also the book's owner id for its orders, which keeps the book's
per-owner state in plain arrays (§6).

### Order ids

An order's id is the sequence number of the command that places it. Ids are unique and
increasing without coordination, survive restarts with the journal, and tell the order's
place in history. The client chooses only a reference, which every report about the
order echoes. Ids are assigned when a message is accepted into the batch, before it is
journaled, so the id of the next new order is the engine's last sequence number plus the
number of commands waiting, plus one.

A recovered book whose orders have ids above the journal's last sequence number was not
built through a gateway, and a new order could get one of their ids: the gateway refuses
to start on it.

### Pre-trade risk

Two limits apply before a message reaches the batch, and a refusal is a `Reject` that
never reaches the journal:

- **Open orders** per account: orders and pending stops on the book plus new ones waiting
  in the batch. An order counts until its final event: rejected, filled, cancelled, or
  modified down to nothing. Without it, one participant could fill the book and every
  other participant would get `BookFull`.
- **Message rate** per session: a token bucket that refills at the account's rate and
  holds at most one second's worth, so short bursts pass and floods do not. It counts
  order entry (new orders, cancels, modifies, mass cancels); heartbeats and logouts are
  free.

### Routing

The engine's output goes through a router that turns each event into a report for the
session of the account that owns the order, found through a map of live orders to their
account and client reference. A trade becomes two fills, one per side. A `Rejected`
event goes only to the session that sent the command, and only if that session is still
logged in for the command's account: a cancel naming someone else's order must not tell
that order's owner anything, and a disconnected session's slot may already belong to
another account. Phase changes go to every session. When an order's final event passes,
the router retires it from the map and from its account's count.

Reports for an account go to whichever session is logged in for it when they are
produced. A session being logged out is sent nothing more, not even the acceptance of an
order it placed in its last batch; the account's next session hears what becomes of the
order. The gateway fuzzer found this case: the target had assumed every order's first
report is its acceptance.

### The event loop and group commit

One thread runs a `mio` event loop over non-blocking sockets. Each round:

1. Wait up to 10 ms for readiness, or not at all if something is still to do.
2. Accept new connections, up to the session limit; further ones are closed at once.
   Nagle's algorithm is off: replies are small and latency matters.
3. Read each ready connection, at most sixteen 64 KiB chunks per round, decode each
   complete message and hand it over. A client that sends without pause would otherwise
   keep the loop reading it, and fill one huge batch; what is left is read next round.
4. Flush the batch: journal it (one sync under `SyncPolicy::Always`), apply it, and route
   the events.
5. Send heartbeats and log out idle sessions.
6. Write each connection's replies, as far as its socket takes them. A connection with more
   than 4 MiB of replies unwritten is dropped as a slow consumer.

Reports go out only after the batch is journaled, so under `Always` a client never hears of
an order the journal could lose. Everything the clients sent in one round shares one
sync: the more clients send at once, the more commands each sync carries, which is what
makes durability affordable (§14).

### Disconnects, stops and failures

A connection that closes, sends bytes that do not decode, is logged out, or falls behind
as a slow consumer, has its account's orders cancelled: a `CancelAll` for the account goes
into the batch. A client that is gone cannot manage its orders, and an exchange that kept
them would trade on the client's behalf. A session being logged out keeps the connection
for up to a second, to read its `Logout`.

Stopping the gateway logs every session out with `Logout(Shutdown)` but cancels nothing:
the orders stay on the book, as they would after a crash, and both are recovered the same
way. On start, the gateway walks the recovered book's queues and stops and attributes
each order to its account again. Their client references were not journaled and are lost:
reports about them carry zero.

If the engine fails (a write or sync error poisons it, §14), every session is logged out
with `Shutdown` and the server stops: the engine cannot accept commands until reopened,
and recovery decides what survived.

### Cost

`loadgen` runs clients that trade against each other through a running gateway, each
keeping a window of new orders in flight, and measures each order's time from sending to
its acknowledgement. On the Windows laptop of §13, four clients over loopback with 16
orders in flight each, for three seconds:

| Sync policy | Orders/s | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| `Always` | 16,400 | 3.1 ms | 7.8 ms | 13.5 ms |
| `Os` | 267,000 | 165 µs | 780 µs | 17 ms |

Under `Always`, each round waits for a sync of about a millisecond (§14) and carries the
64 orders in flight; under `Os` the round trip is the loopback and the loop. These are
closed-loop numbers on a busy laptop, with the clients on the same machine: Phase 6
measures open-loop on Linux.

### How it is verified

| Test | What it shows |
|---|---|
| `crates/protocol/tests/messages.rs` | Every message round-trips in both directions; every prefix of one is incomplete; any changed byte fails to decode or decodes to what encodes to it; streams decode the same in any pieces; bad headers are named. |
| `crates/gateway/tests/exchange.rs` | The session rules, order ids, routing of every kind of event, refusals only to their sender, the open-order limit counting the batch, the token bucket, heartbeats and idle logouts, cancel-on-disconnect, stops and self-trades, phase changes to everyone, recovery after a stop, refusing a foreign book, an engine failure; and a property test of random sessions, messages, disconnects and ticks: reports reach only the owner, and open-order counts match the book after every flush. |
| `crates/gateway/tests/server.rs` | Real sockets and a real journal: trading, 500 orders in one write, bytes that do not decode before and after login, a client that does not read, the session limit, a stop and restart that keeps the orders, a bad token, and the load generator end to end: every order answered exactly once and the book empty when the clients leave. CI runs it on Linux, Windows and macOS. |
| `fuzz/fuzz_targets/protocol.rs` | Arbitrary byte streams, read as the gateway reads a client and as a client reads the gateway: decoding never panics, every message decoded re-encodes to its bytes, and a stream cut anywhere decodes the same messages up to the cut. |
| `fuzz/fuzz_targets/gateway.rs` | Up to four connections that come and go and send raw bytes or well-formed messages, with time passing and batches flushing: nothing panics, nothing reaches a closed session, all reports about an order go to one account, the one that owns it on the book, and open-order counts match the book and stay within the limit. |

## 16. The pipeline and market data

Phase 4 takes the journal off the matching thread and publishes the market. The gateway
can run the engine as before, on its own thread, or as a pipeline of three threads
connected by ring buffers of our own; either way the exchange keeps the book's depth from
the events and publishes it.

### The ring buffer

`crates/ring` is a bounded, lock-free, single-producer single-consumer queue. Each side
counts the items it has moved, its position, and publishes it with a release store; the
other side reads it with an acquire load. An item's slot is its position modulo the
capacity, a power of two. The design follows from where the cost of a queue is: in cache
lines moving between cores.

- **No false sharing.** The two positions sit 128 bytes apart, so they never share a cache
  line, nor the pair of lines Intel's adjacent-line prefetcher fetches together.
- **Cached positions.** Each side keeps a copy of the other's position and reads the real
  one only when its copy says the ring is full (producer) or empty (consumer). In a steady
  flow neither side touches the other's line except to publish.
- **Batches publish once.** `push_from` writes as many items as fit and then stores its
  position once; `drain` loads the producer's position once and stores its own once, when
  the batch is dropped. A stage that finds a hundred items waiting pays two shared-memory
  operations, not two hundred.
- **Backpressure.** A full ring makes the producer wait: nothing is dropped and nothing
  grows. A side waits by spinning (`Wait::Spin`, for stages pinned to cores of their own)
  or by backing off from spinning to yielding to naps of up to 50 µs (`Wait::Backoff`).

The unsafe code is two functions, each with its contract written down: writing a free
slot, and moving an item out of a full one. Three tools check it:

- **Miri** runs the tests and reports undefined behaviour, data races included.
- **loom** runs two models under every interleaving of the two sides and every order the
  memory model lets them see each other's writes in: items crossing a ring smaller than
  their number, one at a time and in batches, and items left behind when one side goes.
  Weakening the producer's release store to relaxed fails it.
- A **model test** checks every operation against `VecDeque` over thousands of random
  sequences, with exact drop counting through wrap-around and leaked batches, and two
  threads move two million items with mixed single and batched operations.

`cargo bench -p ring --bench ring`, two threads on two P-cores of the i5-12450H laptop,
queues of 1,024 items, every side spinning:

| | Items/s | Round trip, mean | p50 | p99.9 |
|---|---:|---:|---:|---:|
| ring, batches | 372 M | | | |
| ring, one at a time | 91 M | 367 ns | 200 ns | 800 ns |
| `crossbeam-channel`, bounded | 53 M | 613 ns | 400 ns | 1,200 ns |
| `std::sync::mpsc::sync_channel` | 41 M | 539 ns | 300 ns | 1,200 ns |

Percentiles come from a clock of 100 ns resolution on Windows; the means do not. Two
hyperthreads of one core, which share its caches, run every queue about twice as fast.

### The engine, split

`Engine` is now the composition of two halves. The **writer** numbers commands, writes and
syncs them, and removes journal segments; it holds the directory lock, and a failed write
or sync poisons it. The **matcher** applies journaled commands in sequence, takes
snapshots and deletes old ones, and returns the sequence number through which the writer
may remove segments. `Engine::split` separates them; `Engine` itself runs them in step and
behaves exactly as before, every Phase 2 test unchanged. The rule that the journal must
reach a snapshot before the snapshot exists is enforced where the halves meet:
`Matcher::snapshot` takes the writer's durable sequence number and panics if it falls
short. Without that rule, recovery from the snapshot itself would still be right, as a test
showed when the sync was dropped; what it guarantees is that the journal and an older
snapshot can rebuild the newer one too.

### The pipeline

```text
  network thread           writer thread             matcher thread
  sessions, risk,  ──A──▶  journal: write, sync, ──B──▶  book: apply,  ──┐
  routing, md      ◀──────────────────────C──────────── snapshots      ◀─┘
```

- **A** carries commands, numbered by the exchange in the order the writer journals them
  (an order's id is still its command's sequence number; the writer checks it).
- **B** carries each command with its sequence number once it is journaled, and synced
  under `SyncPolicy::Always`: the book never sees a command the journal could lose, and
  since reports come from the book's events, no client hears of one either.
- **C** carries the events back. The network thread routes them, keeps the depth, and
  publishes.

The writer takes whatever waits in A as one batch, so the slower the disk, the larger each
sync's batch: group commit happens by itself. Segment rolls and syncs happen on the
writer's thread. The matcher waits for the disk only for a snapshot under `SyncPolicy::Os`:
it asks the writer for a sync through the snapshot and waits for the writer's durable
position to reach it. The writer, which owns the journal, also removes the segments a
snapshot freed.

Every ring is bounded, and the waits cannot form a cycle: the writer waits for room in B,
the matcher for room in C, but the network thread never blocks on a ring and always drains
C. When A is full it keeps the rest of the batch and stops reading its sockets, so clients
wait instead of the batch growing. While the writer waits for room in B it still serves
the matcher's sync requests, or a matcher waiting for a sync would never get room.

A failure ends its thread, and the rings close behind it: the matcher applies what the
writer journaled before it stopped, the network thread delivers what the matcher applied,
then joins the threads and reports why, which logs every session out and stops the server.

For the exchange to work with events that come later, from another thread, it no longer
holds the engine. It numbers commands as they enter the batch, hands the batch over, and
keeps the commands in flight with their sender, owner and whether they place an order,
until a later command's events show theirs are all in. The server is generic over a
`Core`: an engine on its own thread is one, the pipeline another.

The test that runs the same sessions through the pipeline and through an engine on one
thread, and requires exactly the same replies, found a lost wake-up. The network thread
asked whether events might still come by checking the event ring and then the matcher's
progress; between the two, the matcher could push the last events of a batch and publish
its progress, and the network thread would wait a whole tick before delivering them. It
now reads the progress first: the matcher publishes it after the events, so the ring is
checked after everything it covers is in.

### Market data

`crates/marketdata` keeps a book's depth from its events alone: every resting order's side,
price, place in its level's queue, what it shows and what is left, and per level the
quantity shown and the number of orders. Orders join the back of their level's queue
when they rest. Trades take from what the resting orders show (both of them in an
uncross); an iceberg shows its next tranche with `Replenished`, at the back of the queue
again; a cancel removes the rest; a modify that keeps the price and adds nothing shrinks
the order in place, keeping its place, its hidden part first, and any other modify takes
it off, to rest again with a `Rested` of its own. So the depth can tell, order by order,
what is ahead of any order in its queue: market data by order, as a browser's queue view
shows it. Since it needs only the events, it runs on the network thread, not the
matcher.

A session that sends `Subscribe` gets a `BookSnapshot` with the number of levels that
follow, the levels as `LevelUpdate`s, bids best first and then asks, and from then on,
after every round, the trades as `TradeTick`s and every level that changed, once each, as
it now is. Everything carries the sequence number of the last command it reflects. The
subscription first publishes what is pending to the others and then sends the book under
that publication's number, so the updates that follow start exactly from it. A client
that loses track subscribes again and starts over: recovery is by snapshot, not replay.

Keeping the depth from the events found a defect that four phases of tests had missed. When
an iceberg's tranche ran out in an uncross, `Replenished` gave the auction price instead of
the iceberg's own, which differ for any iceberg priced through the auction price. The
reference book of the differential tests and fuzzer made the same mistake, and a property
encoded it, so nothing disagreed. Both are fixed, a scenario pins the case, and the rules
version rose to 2: a command now emits other events than before.

### Cost

`loadgen` against the gateway on the same laptop, four clients over loopback, the network,
writer and matcher threads pinned to separate P-cores and spinning, three seconds each:

| Engine | Sync | Orders in flight | Orders/s | p50 | p99 |
|---|---|---:|---:|---:|---:|
| one thread | `Always` | 64 | 13,800 | 4.6 ms | 11 ms |
| pipeline | `Always` | 64 | 13,800 | 4.1 ms | 13 ms |
| one thread | `Os` | 64 | 344,000 | 96 µs | 600 µs |
| pipeline | `Os` | 64 | 197,000 | 163 µs | 2.7 ms |
| one thread | `Os` | 512 | 676,000 | 364 µs | 15 ms |
| pipeline | `Os` | 512 | 629,000 | 298 µs | 16 ms |
| pipeline, backing off | `Os` | 512 | 245,000 | 1.2 ms | 18 ms |

The pipeline does not raise throughput here, and the numbers say why: the network thread,
which reads, routes and writes every socket, is the bottleneck in both modes, and the
pipeline adds two hops between threads to every order's round trip. With few orders in
flight those hops are the round trip's cost; with many, both modes saturate the network
thread. Under `Always` both wait for the same sync. What the pipeline changes is where the
disk's stalls land: a roll or a sync now stops the writer, not the thread that reads
sockets and matches. Backing off instead of spinning costs most: a stage that naps wakes
late. These are closed-loop numbers on a laptop with the clients beside the server; Phase
6 measures open-loop on Linux, with timestamps per stage.

### How it is verified

| Test | What it shows |
|---|---|
| `crates/ring/tests/ring.rs`, `tests/loom.rs`, Miri | As above: a model test with drop counting, two threads at full speed, loom models of every interleaving, and Miri over all of it. |
| `crates/engine/tests/split.rs` | The halves driven as a pipeline would, the writer ahead by any number of commands and freed segments removed later, under both sync policies and both crash models: recovery keeps every durable command and nothing unjournaled, and the matcher's events are the whole engine's. A snapshot without a durable journal panics. |
| `crates/gateway/tests/pipeline.rs` | Sessions sending the same messages through the pipeline and through an engine on one thread get exactly the same replies and market data, with snapshots and retention on the threads, rings of 1 to 64 items and both ways of waiting, and the files left recover to the same book. A disk that fails stops the pipeline with every reported command recoverable; a power failure while it runs loses nothing reported. |
| `crates/marketdata/tests/depth.rs` | 40 flows of 3,000 commands of every kind, half through calls that end in an uncross: after every command the depth equals the book's, level by level and order by order in queue priority, every changed level is reported, and a depth taken from the book matches the one kept. |
| `crates/gateway/tests/exchange.rs`, `tests/server.rs` | A client that subscribes in the middle of trading rebuilds exactly the book's depth from the snapshot and the updates; market data reaches a subscriber over TCP through either core; the load generator trades through the pipeline, which also stops and restarts with its orders. |
| `fuzz/fuzz_targets/gateway.rs` | Also subscribes sessions and publishes: the depth kept equals the book's after every flush, and market data reaches only logged-in sessions. |

## 17. The web demo

Phase 5 makes the exchange something anyone can open in a browser and trade on, with paper
money, against bots, and keeps the market running across restarts of the server.

### Browsers as sessions

The server can listen on a second port for browsers. A connection there speaks HTTP until
it asks for a file or upgrades to a WebSocket: the page and its three files are built into
the binary and served under a content security policy that allows nothing from
elsewhere, anything else gets a 404 or a 400, and a request not complete within ten
seconds is dropped. An upgraded connection becomes a session like any other, with the
same login, risk limits, reports and market data; its messages are JSON objects in
WebSocket text frames, mapped one to one onto the binary protocol's, and the server's
replies are encoded the same way.

The three decoders are strict, for the same reason the binary protocol's is (§15): HTTP
takes only `GET` without a body and the handshake of RFC 6455; WebSocket frames must be
masked, whole, text or control frames, within a size limit, with lengths in their shortest
form; JSON must have a known `"type"`. A `web` fuzz target runs arbitrary bytes through
all three.

Two things differ for browsers. A browser's orders stay on the book when its connection
goes, since reloading the page should not cancel them; it is told its open orders when it
logs in again, as every session now is. And a browser may ask for an account: `register`
creates one with the lowest free id of a range kept for guests and a random token, one per
connection, and appends it to a file, synced, before answering, so that an account a
visitor was told of survives a crash.

### Paper money

An account can start with cash, in price ticks times lots, and a position, in lots: it is
then a paper-trading account, and the gateway checks its orders against what it owns
before they reach the batch. A buy order holds its open quantity times its limit price in
cash, a sell order holds its open quantity in lots, and an order that would hold more than
the account has free is refused with `InsufficientFunds`. Holds follow the order's open
quantity through the book's events: it rests, trades, is cancelled or rejected. A trade
settles at its own price, which for a buy order is never above its limit, so what was held
for the quantity that traded covers what it cost, and the rest comes back. Paper accounts
place limit orders and cancel them, nothing else (`NotAllowed`): a market or stop order has
no price to hold against, and a modify could need more than is held while it waits to be
applied. Accounts without funds, such as the bots', are not checked. A session is told its
balance on login and after every round in which it changed.

### Checkpoints

The engine rebuilds the book after a restart (§14). The exchange's own state, paper money
above all, was lost; now it is a projection of the journal, snapshotted like the book. A
checkpoint holds the paper accounts' cash and positions and every open order's account,
client reference, side, price and open quantity, as of a sequence number at which every
command's events have been delivered; what orders hold follows from the orders, and the
depth from the book. The server saves one every `--checkpoint-every` commands (10,000 by
default) and when it stops, written to a temporary file, synced, renamed into place and the
directory synced, keeping the one before.

On start, `recovery::open` finds the newest checkpoint that reads and opens the engine with
the checkpoint's sequence number as where its consumer stands (`Output::resume_after`). The
engine starts from a snapshot no later than that and replays the commands and events after
it, now with each command before its events (`Output::on_command`), so the exchange learns
who placed the orders it has not seen. It then requires the orders it knows to be exactly
those on the recovered book, with the same open quantities, and refuses to start
otherwise. The journal must reach back to the checkpoint, which is why checkpoints come at
most every `--snapshot-every` commands: retention keeps the journal after the oldest kept
snapshot.

### Bots and the page

Bots are ordinary clients of the binary protocol with accounts of their own, watching the
market through its market data: market makers quote ten levels each side of a fair price
that takes random steps and is pulled back towards the start and the last trade, with more
size further out, replacing their quotes every interval; noise traders cross the spread with small IOC orders; trend
followers trade with the recent move. They reconnect when the gateway restarts.

The page has no dependencies and no build step, and loads nothing from elsewhere: no
fonts, no scripts, no trackers. It keeps in local storage the account, and the size and
time of the orders it placed, which the reports after a reload do not tell; it
reconnects with backoff, sends heartbeats so it is not logged out as idle, and draws the
book and tape at most once a frame. Its chart opens on the last hour, not empty: the
exchange keeps the trades it delivers as five-second candles, an hour of them in memory,
and a browser that subscribes is sent them after the book. The trades after that come as
market data, and the server's clock, sent with the statistics, places them in the right
candle. The candles are not saved: after a restart they start again. Every second the
server tells it the commands per second, how long the turns that handed commands to the
engine took, as percentiles and as counts in buckets that double from a microsecond, the
sessions and resting orders, and its clock.

A subscribed browser is also shown the engine at work. It gets the statistics of the last
two minutes when it subscribes, so its charts of them start full. Five times a second it gets the
best ten levels of each side order by order, with each order's id and the quantity it
shows in queue priority, when they changed since the last time; and the commands the
engine sequenced since then, the latest twelve, each with what came of it: trades and
the quantity traded, what rested, what was cancelled, why it was refused. A command is
told once all its events have come, which is once a later command's arrive or when the
engine has nothing left to send back; the exchange keeps the last 256 commands not yet
told, so a server with no browser does not grow. Every second it gets the paper accounts
that traded, ranked by what they made with both what they hold and what they started
with valued at the last trade price, and its own place among them. Order ids and
account numbers are all a browser learns of other traders, as on a market-by-order feed.

### Deployment

`deploy/` holds an image built from source with the gateway and the bots, and a compose
file that runs them with Caddy in front for HTTPS; only ports 80 and 443 are reachable from
outside, and the exchange's directory is a volume. CI builds the image on every push.

### How it is verified

| Test | What it shows |
|---|---|
| `crates/gateway/src/web/*` | The handshake's accept key against the RFC's example; every refusal of the HTTP parser and frame decoder; frames of every length class and every prefix of them; the JSON in both directions for every message. |
| `crates/gateway/tests/web.rs` | Over real sockets: the page and nothing else; a browser that registers, logs in, subscribes, places an order, sees a binary client trade against it, cancels, keeps its orders across a reconnect, and finds the guest ids running out; the last hour as candles after each subscription, the trade in its own at the time it happened, and none for a browser that subscribes before logging in; its order in its queue under its id, the engine log's entries for it and for the trade against it, and its place at the top of the leaderboard; statistics every second, with the server's clock; a guest's money and orders, with their references, across a stop and restart. |
| `crates/gateway/src/candles.rs` | Trades fall into their five-second interval, one after a clock that went back into the newest, and only the last hour is kept. |
| `crates/gateway/src/web/json.rs` | The queues of the best levels, order by order, with those past the first 24 summed; every command kind in the engine log with each part of its outcome; the leaderboard, a browser's rank, and which bucket a turn of any length falls in. |
| `crates/gateway/tests/exchange.rs` | Holds, refunds, refusals and release on disconnect for a paper account; and four paper accounts trading only with each other through random flows: money and lots in total never change, each wallet equals what its fills say, none holds more than it owns, and each holds exactly what its orders on the book need. Dropping settlement fails it. Trades go into candles at the wall-clock time the exchange's clock stands for, and only a logged-in session that subscribed counts as subscribed. The engine log tells each command once, in sequence, only after all its events came, with its trades, what rested, a refusal, and what a mass cancel took off; paper accounts that traded are ranked by profit at the last trade price. |
| `crates/gateway/tests/recovery.rs` | 40 random runs with checkpoints now and then and a power failure, in order and out of order, on the simulated disk: the exchange comes back exactly, only the client references of orders placed after the checkpoint lost. A damaged newest checkpoint falls back to the one before; one that contradicts the book is refused. |
| `crates/engine/tests/operations.rs` | Commands reach an output before their events, live and on replay from where it stands. |
| `crates/gateway/tests/server.rs` | Bots make a market within a second: both sides quoted, trades. |
| `fuzz/fuzz_targets/web.rs` | Arbitrary bytes through the HTTP parser, the frame decoder and the JSON: nothing panics, frames take exactly their bytes, and a stream cut anywhere decodes the same frames up to the cut. |
