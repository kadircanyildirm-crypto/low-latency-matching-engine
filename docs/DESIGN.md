# Design of the matching core

This document explains what the order book in `crates/orderbook` does, how, and why. It
also says what it deliberately does not do yet. Every claim here is backed by a test.
The [verification](#11-verification) section says which one.

## 1. Goals

| Goal | Consequence |
|---|---|
| Deterministic | One thread, no clocks, no randomness, no iteration over hash maps. The event stream is a pure function of the command stream, so state can be rebuilt by replay (Phase 2) and mirrored by a standby (Phase 6). |
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
around the touch, or a ladder near the touch with a tree beyond it. Phase 5 benchmarks
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
phase, or an exhausted trade counter.

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
| Mutation testing | Before trading phases, `cargo mutants` injected 340 small faults into the engine, and the tests detected every one of the 319 that compile ([results](../README.md#mutation-testing)). The phase code has not been through a run yet. |

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
cargo +nightly llvm-cov --package orderbook --branch --ignore-filename-regex 'workload\.rs' --summary-only
```

## 12. Known limitations and deliberate deferrals

| Limitation | Plan |
|---|---|
| No per-participant limits: one owner can fill the book and block others with `BookFull` | Phase 3: pre-trade risk in the gateway (per-session order limits, throttling) |
| The id index's hash is not keyed: a participant choosing ids adversarially could crowd many into one home line, and lookups would then scan several lines | Phase 3: the gateway assigns order ids, so participants never choose them; with sequential ids, a directly indexed table could replace the hash altogether |
| One command can emit any number of events: a market order that sweeps the book emits one per order it reaches | Phase 4: the publisher and its ring buffers must accept a batch of any size |
| Ladder memory grows with band width (see §3) | Phase 5: benchmark alternatives and add a windowed or hybrid ladder |
| Without `auction_on_band`, a price band never re-anchors on its own: if the market moves away without trading, it freezes (measured in §6). With it, a band tight against the market's moves keeps the book in calls most of the time (§7) | The band's width is a configuration choice; widening the band during a call, as some exchanges do, is not implemented |
| Calls refuse market orders; there are no auction-only order types, no published imbalance, and no collar on the auction price besides the static band. Market data can read `indicative_uncross()` | Phase 7 |
| The uncross does not prevent self-trades (§7) | Phase 7, with per-order self-trade instructions |
| Calls end only when the sequencer says so; the engine has no timers or random call ends | Phase 4: the sequencer schedules phase changes |
| An uncross sums the queues of the crossed levels where icebergs rest, so its cost grows with those orders | Measure first; a per-level hidden quantity would remove it at 8 bytes per level per side |
| One instrument per book | Phase 7: one book per instrument, sharded across cores |
| Self-trade policy is per book, not per order | Phase 7: per-order STP instruction |
| Pending stops cannot be modified, and there are no trailing stops | Later: modify of a pending stop's trigger, limit and quantity; trailing stops |
| Trade ids come from a `u64` counter, and the trade that would get id `u64::MAX` overflows it: a debug build panics, a release build wraps. A live book needs 2⁶⁴ − 1 trades to get there, but `restore` accepts any trade count below `u64::MAX`, so a hand-made snapshot gets there in one trade. The `restore` fuzz target keeps clear of it | Phase 2, when snapshots come from disk: decide what an exhausted counter means (halt trading, or refuse such snapshots with a margin) |

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
