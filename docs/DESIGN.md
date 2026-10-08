# Design of the matching core

This document explains what the order book in `crates/orderbook` does, how, and why. It
also says what it deliberately does not do yet. Every claim here is backed by a test.
The [verification](#9-verification) section says which one.

## 1. Goals

| Goal | Consequence |
|---|---|
| Deterministic | One thread, no clocks, no randomness, no iteration over hash maps. The event stream is a pure function of the command stream, so state can be rebuilt by replay (Phase 2) and mirrored by a standby (Phase 6). |
| Predictable latency | No heap allocation after construction, O(1) work per order touched, cache-friendly layout. |
| Exchange-grade semantics | Price-time priority, owner checks, FIX-style modifies, self-trade prevention, price protection, explicit rejections. |
| Safe | No `unsafe` code; quantity sums cannot overflow for any input (§8); property tests feed extreme values (`u64::MAX` quantities, `i64::MIN`/`MAX` prices) without a panic. |

## 2. Data model

- **Prices** are `i64` tick counts and **quantities** are `u64` lots. No floating point.
  The conversion from decimal prices belongs to the gateway (Phase 3).
- **Order ids** are assigned upstream and must be unique among resting orders. An id can
  be reused once its order is gone.
- **Owners** (`u32`) identify participants. They drive self-trade prevention and
  cancel/modify authorization.
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
 id index: FxHashMap<OrderId, slot>, reserved at 2x capacity
```

| Operation | Cost | How |
|---|---|---|
| Find the level for a price | O(1) | `price - min_price` |
| Add an order at a level | O(1) | append to the level's intrusive list |
| Cancel an order anywhere in a queue | O(1) + hash lookup | doubly linked list |
| Fill against the best level | O(1) per order touched | pop from the head |
| Find the next best level after one empties | O(levels / 4096) worst case | two-level bitset |
| Best bid / ask | O(1) | cached index |

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

**Why an id index at 2× capacity.** Hash-map deletes leave tombstones. When the map runs
out of growth room, it either rehashes in place (no allocation) or grows (allocation).
With capacity reserved for `2 × max_orders`, live entries never exceed half the table, so
it always takes the in-place path. A test hammers a permanently full book to show this
empirically.

## 4. Matching rules

- An incoming order trades against the opposite side while it crosses its limit. Best
  price comes first; within a price, the oldest order comes first.
- Each trade executes at the **maker's** price and is as large as possible:
  `min(taker leaves, maker leaves)`.
- A maker is left partially filled only when the taker is completely filled.

### Events

| Command | Events |
|---|---|
| `Limit` / `Market` | `Rejected` alone, or `Accepted`, then `Trade`s (with `Cancelled{SelfTrade}` for resting orders removed by self-trade prevention), then at most one of `Rested` or `Cancelled` for the remainder |
| `Cancel` | `Cancelled{Requested}` or `Rejected` |
| `Modify` | `Rejected`, or `Modified` followed, if priority is lost, by the events of a new limit order |

Every `Trade` carries the trade id and both sides' remaining open quantity (`leaves`).
A participant can therefore track its orders from events alone; the soak test checks
exactly that. Sequence numbers are deliberately absent. The sequencer numbers commands
when it journals them (Phase 2), and the publisher numbers outgoing messages (Phase 4).
The engine itself has no notion of time or transport.

## 5. Order semantics

| Command | Behaviour |
|---|---|
| `Limit` | Good-till-cancelled. Trades up to its price; the remainder rests at its price. |
| `Market` | Trades at any price within price protection. The remainder is cancelled with `NoLiquidity`, `PriceProtection` or `SelfTrade`. Never rests. |
| `Cancel` | Removes the order. Only its owner may cancel it. |
| `Modify` | FIX cancel/replace on **total** quantity; see below. Only the owner may modify. |

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

## 6. Risk controls in the core

| Control | Rule |
|---|---|
| Static price band | Prices outside `[min_price, max_price]` are rejected (`PriceOutOfRange`). |
| Maximum order size | `qty` must be in `1..=max_order_qty` (`InvalidQuantity`). |
| Price protection | Measured from the opposite best price when the order arrives. A market order stops trading `price_protection` ticks beyond it (`Cancelled{PriceProtection}`). A limit or modify priced further through is rejected (`PriceOutsideProtection`). If the opposite side is empty, there is nothing to protect against. |
| Self-trade prevention | Two orders of the same owner never trade. `CancelResting` removes the resting order and keeps matching. `CancelIncoming` cancels the rest of the incoming order and leaves the book untouched. |
| Ownership | Cancels and modifies of someone else's order are rejected as `UnknownOrder`, exactly like a missing order, so others' order ids do not leak. |

## 7. Capacity and admission

The book holds at most `max_orders` resting orders. When it is full, a new limit order is
refused with `BookFull` **only if it does not cross**. A crossing order always finds a
slot:

- Its first match either fills the taker completely (nothing needs to rest), or
- it removes a resting order, by filling it or by self-trade prevention, which frees a
  slot.

Under `CancelIncoming` the remainder is cancelled instead of resting. A modify frees its
own slot before it re-enters. So the pool can never be exhausted mid-command. `alloc`
still asserts this, so a bug fails loudly instead of corrupting state.

## 8. Overflow safety

`BookConfig` requires `max_orders × max_order_qty ≤ u64::MAX` and asserts it at
construction. Every quantity sum in the book is a sum over at most `max_orders` orders,
each at most `max_order_qty`, so no sum can overflow for any input. `validate()` adds
with checked arithmetic, so an overflow would surface as an error rather than a
plausible-looking wrapped number.

This rule exists because an earlier version had no limit. Two orders of `u64::MAX / 2 + 1`
lots at one price made the level total wrap to 1 in release builds, and the old
`validate()` wrapped the same way and reported success.

## 9. Verification

| Layer | What it shows |
|---|---|
| `tests/scenarios.rs` | One rule per test, with the exact expected event sequence (32 tests). |
| `tests/differential.rs` | Over random configurations and command sequences, the engine and a deliberately naive reference (`BTreeMap` + `VecDeque`, no shared code) produce identical events and books, with `validate()` checked after every command. |
| `tests/properties.rs` | Specification checks that do not rely on a second implementation, after every command: each trade is with the next order in price-time priority, at the maker's price, within the limit or protection cap, never between the same owner, and as large as possible; leaves, trade ids and quantity add up; a remainder rests only when nothing more can trade; orders the command did not reach are unchanged; rejected commands change nothing; and each rejection reason actually applies. |
| `tests/soak.rs` | Hundreds of thousands of commands of realistic multi-participant flow against the reference, under both self-trade policies; participants rebuild the book from events alone. |
| `tests/soak.rs` (golden) | A pinned fingerprint of all events. CI runs it on Linux, Windows and macOS, which shows the output is identical across platforms. |
| `tests/zero_alloc.rs` | A counting global allocator sees zero allocations in normal flow, in a permanently full book (worst case for the id index), and in a deep book. |
| `src/bitset.rs` | Bitset searches agree with `BTreeSet`. |
| Mutation testing | `cargo mutants` injects hundreds of small faults into the engine; the test suite must catch them. |

Random inputs are biased toward where bugs live: few ids (duplicates, unknown ids), few
owners (self-trades), prices at bitset word and summary boundaries, band edges and just
outside, quantities at 0, at `max_order_qty`, just above it, and at `u64::MAX`, with
`max_order_qty` itself either small or at the overflow limit.

## 10. Known limitations and deliberate deferrals

| Limitation | Plan |
|---|---|
| Ladder memory grows with band width (see §3) | Phase 5: benchmark alternatives and add a windowed or hybrid ladder |
| No per-participant limits: one owner can fill the book and block others with `BookFull` | Phase 3: pre-trade risk in the gateway (per-session order limits, throttling) |
| Price protection is measured from the opposite best at arrival, not from a reference or last-trade price; no dynamic bands for limit orders resting away from the market | Later: reference price and dynamic bands |
| Self-trade policy is per book, not per order | Later: per-order STP instruction |
| Only GTC limit and market orders | Phase 7: IOC, FOK, post-only |
| One instrument per book | Phase 7: one book per instrument, sharded across cores |
