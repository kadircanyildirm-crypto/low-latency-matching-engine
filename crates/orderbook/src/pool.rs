//! Fixed-capacity storage for resting orders, plus the iceberg part of those that show only
//! some of their quantity.

use crate::types::{OrderId, OwnerId, Qty, Side};

/// Null link for the intrusive lists.
pub(crate) const NIL: u32 = u32::MAX;

/// What a node holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrderKind {
    /// An order resting in a price level's queue.
    Resting,
    /// A stop that becomes a market order when it triggers.
    StopMarket,
    /// A stop that becomes a GTC limit order at `limit` when it triggers.
    StopLimit,
}

/// A resting order, or a pending stop, plus its links in the FIFO queue of its level.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OrderNode {
    pub id: OrderId,
    /// Open quantity ("leaves").
    pub remaining: Qty,
    /// Total order quantity: filled + open. Kept for FIX-style modifies.
    pub total: Qty,
    /// Index of the price level within its ladder: the limit price of a resting order, the
    /// trigger price of a pending stop.
    pub level: u32,
    /// Limit level of a pending stop-limit; `NIL` otherwise.
    pub limit: u32,
    pub prev: u32,
    pub next: u32,
    pub owner: OwnerId,
    pub side: Side,
    /// Post-only orders keep the restriction while they rest: a modify may not make them
    /// trade.
    pub post_only: bool,
    /// Whether the slot's [`IcebergPart`] is in use. A plain order shows all it has.
    pub iceberg: bool,
    pub kind: OrderKind,
}

/// What an iceberg order shows: at most `display` lots at a time, of which `visible` are
/// still on display. Kept beside the node rather than in it, so plain orders do not pay
/// for it in cache space.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IcebergPart {
    pub display: Qty,
    pub visible: Qty,
}

// The node's size is part of the cache budget; growing it should be a deliberate decision.
const _: () = assert!(size_of::<OrderNode>() == 48);

impl OrderNode {
    /// An order about to rest. Its links are set when the book places it.
    #[inline]
    pub fn new(
        id: OrderId,
        owner: OwnerId,
        side: Side,
        level: u32,
        remaining: Qty,
        total: Qty,
        post_only: bool,
    ) -> Self {
        Self {
            id,
            remaining,
            total,
            level,
            limit: NIL,
            prev: NIL,
            next: NIL,
            owner,
            side,
            post_only,
            iceberg: false,
            kind: OrderKind::Resting,
        }
    }

    /// A pending stop, triggering at `trigger` and then working as a market order, or as a
    /// limit order at `limit`.
    #[inline]
    pub fn stop(
        id: OrderId,
        owner: OwnerId,
        side: Side,
        trigger: u32,
        limit: Option<u32>,
        qty: Qty,
    ) -> Self {
        Self {
            limit: limit.unwrap_or(NIL),
            kind: match limit {
                None => OrderKind::StopMarket,
                Some(_) => OrderKind::StopLimit,
            },
            ..Self::new(id, owner, side, trigger, qty, qty, false)
        }
    }

    /// Whether the node is a stop waiting for its trigger.
    #[inline]
    pub fn is_stop(&self) -> bool {
        self.kind != OrderKind::Resting
    }
}

/// Slab of order nodes addressed by `u32` slot, allocated once up front.
///
/// Free slots form a LIFO list threaded through `next`, so the most recently freed slot
/// (the one most likely still in cache) is reused first.
pub(crate) struct OrderPool {
    nodes: Vec<OrderNode>,
    /// Iceberg part of each slot; meaningful only while the node's `iceberg` flag is set.
    icebergs: Vec<IcebergPart>,
    free_head: u32,
    live: usize,
}

impl OrderPool {
    pub fn with_capacity(capacity: u32) -> Self {
        assert!(
            capacity < NIL,
            "capacity must leave room for the NIL sentinel"
        );
        let nodes = (0..capacity)
            .map(|slot| OrderNode {
                id: 0,
                remaining: 0,
                total: 0,
                level: NIL,
                limit: NIL,
                prev: NIL,
                next: if slot + 1 < capacity { slot + 1 } else { NIL },
                owner: 0,
                side: Side::Buy,
                post_only: false,
                iceberg: false,
                kind: OrderKind::Resting,
            })
            .collect();
        let unused = IcebergPart {
            display: 0,
            visible: 0,
        };
        Self {
            nodes,
            icebergs: vec![unused; capacity as usize],
            free_head: if capacity == 0 { NIL } else { 0 },
            live: 0,
        }
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.free_head == NIL
    }

    /// Slots in the slab, free or not.
    pub fn capacity(&self) -> usize {
        self.nodes.len()
    }

    /// First free slot, or `NIL`. Free slots are linked through `next`.
    pub fn free_head(&self) -> u32 {
        self.free_head
    }

    #[inline]
    pub fn live(&self) -> usize {
        self.live
    }

    /// Stores `node` in a free slot.
    ///
    /// # Panics
    ///
    /// If the pool is full. The book's admission rules make that unreachable; reaching it
    /// anyway is a bug, and failing loudly beats corrupting the free list.
    #[inline]
    pub fn alloc(&mut self, node: OrderNode) -> u32 {
        let slot = self.free_head;
        assert_ne!(slot, NIL, "order pool exhausted");
        self.free_head = self.nodes[slot as usize].next;
        self.nodes[slot as usize] = node;
        self.live += 1;
        slot
    }

    #[inline]
    pub fn free(&mut self, slot: u32) {
        let node = &mut self.nodes[slot as usize];
        node.remaining = 0;
        node.next = self.free_head;
        self.free_head = slot;
        self.live -= 1;
    }

    #[inline]
    pub fn get(&self, slot: u32) -> &OrderNode {
        &self.nodes[slot as usize]
    }

    #[inline]
    pub fn get_mut(&mut self, slot: u32) -> &mut OrderNode {
        &mut self.nodes[slot as usize]
    }

    /// Turns the order in `slot` into an iceberg showing `visible` of at most `display`.
    #[inline]
    pub fn make_iceberg(&mut self, slot: u32, display: Qty, visible: Qty) {
        self.nodes[slot as usize].iceberg = true;
        self.icebergs[slot as usize] = IcebergPart { display, visible };
    }

    /// The iceberg part of the order in `slot`, if it is an iceberg.
    #[inline]
    pub fn iceberg(&self, slot: u32) -> Option<IcebergPart> {
        self.nodes[slot as usize]
            .iceberg
            .then(|| self.icebergs[slot as usize])
    }

    /// Quantity the order in `slot` shows on the book: all of it, unless it is an iceberg.
    #[inline]
    pub fn visible(&self, slot: u32) -> Qty {
        let node = &self.nodes[slot as usize];
        if node.iceberg {
            self.icebergs[slot as usize].visible
        } else {
            node.remaining
        }
    }

    /// Trades up to `qty` against what the order in `slot` shows. Returns the quantity
    /// traded and the order's open quantity afterwards, hidden part included.
    #[inline]
    pub fn fill(&mut self, slot: u32, qty: Qty) -> (Qty, Qty) {
        let node = &mut self.nodes[slot as usize];
        let fill = if node.iceberg {
            let part = &mut self.icebergs[slot as usize];
            let fill = qty.min(part.visible);
            part.visible -= fill;
            fill
        } else {
            qty.min(node.remaining)
        };
        node.remaining -= fill;
        (fill, node.remaining)
    }

    /// Shows the next tranche of an iceberg whose visible part is used up, and returns it.
    #[inline]
    pub fn replenish(&mut self, slot: u32) -> Qty {
        let remaining = self.nodes[slot as usize].remaining;
        let part = &mut self.icebergs[slot as usize];
        part.visible = part.display.min(remaining);
        part.visible
    }

    /// Lowers the order's open quantity to `leaves` and its total to `total`, taking the
    /// cut out of the hidden part first. Returns how much less the order now shows.
    #[inline]
    pub fn shrink(&mut self, slot: u32, leaves: Qty, total: Qty) -> Qty {
        let node = &mut self.nodes[slot as usize];
        let shown_less = if node.iceberg {
            let part = &mut self.icebergs[slot as usize];
            let before = part.visible;
            part.visible = before.min(leaves);
            before - part.visible
        } else {
            node.remaining - leaves
        };
        node.remaining = leaves;
        node.total = total;
        shown_less
    }

    #[cfg(test)]
    pub fn iceberg_mut(&mut self, slot: u32) -> &mut IcebergPart {
        &mut self.icebergs[slot as usize]
    }
}

/// Kani proofs over every input within the stated bounds. Run with
/// `cargo kani -p orderbook`.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// Slots in the pool of the free-list proof.
    const CAPACITY: u32 = 3;

    /// Allocations and frees in the free-list proof.
    const STEPS: usize = 6;

    fn order(id: OrderId) -> OrderNode {
        OrderNode::new(id, 0, Side::Buy, 0, 1, 1, false)
    }

    /// Any interleaving of allocations and frees of live slots, on a pool of [`CAPACITY`]
    /// slots: the free slots form a LIFO stack. `alloc` returns the slot freed most
    /// recently (initially slot 0, then 1, ...), the free list holds exactly the free slots
    /// in stack order, each with no quantity, and ends in `NIL`, and `is_full` and `live`
    /// agree with the stack. `validate()` and `alloc` both rely on this. The capacity is
    /// fixed: a symbolic one makes the slab symbolic in size, which exhausts the model
    /// checker's memory.
    #[kani::proof]
    #[kani::unwind(7)]
    fn free_slots_form_a_lifo_stack() {
        let capacity = CAPACITY;
        let mut pool = OrderPool::with_capacity(capacity);
        // The model: free slots bottom to top, and which slots are live.
        let mut stack = [NIL; CAPACITY as usize];
        let mut depth = capacity as usize;
        for (k, slot) in stack.iter_mut().enumerate().take(depth) {
            *slot = capacity - 1 - k as u32;
        }
        let mut live = [false; CAPACITY as usize];

        for step in 0..STEPS {
            if kani::any() {
                if depth > 0 {
                    let slot = pool.alloc(order(step as OrderId));
                    depth -= 1;
                    assert_eq!(slot, stack[depth]);
                    live[slot as usize] = true;
                }
            } else {
                let slot: u32 = kani::any();
                if slot < capacity && live[slot as usize] {
                    pool.free(slot);
                    live[slot as usize] = false;
                    stack[depth] = slot;
                    depth += 1;
                }
            }

            assert_eq!(pool.capacity(), capacity as usize);
            assert_eq!(pool.live(), capacity as usize - depth);
            assert_eq!(pool.is_full(), depth == 0);
            let mut cur = pool.free_head();
            for k in (0..CAPACITY as usize).rev() {
                if k < depth {
                    assert_eq!(cur, stack[k]);
                    assert_eq!(pool.get(cur).remaining, 0);
                    cur = pool.get(cur).next;
                }
            }
            assert_eq!(cur, NIL);
        }
        kani::cover!(depth == 0 && capacity == CAPACITY, "the pool fills up");
    }

    /// A pool holding one order with any quantities a resting order can have: plain, or an
    /// iceberg showing between one lot and the smaller of its display and its open
    /// quantity.
    fn one_order() -> (OrderPool, u32, Option<IcebergPart>) {
        let mut pool = OrderPool::with_capacity(1);
        let remaining: Qty = kani::any_where(|&remaining| remaining > 0);
        let total: Qty = kani::any_where(|&total| total >= remaining);
        let slot = pool.alloc(OrderNode::new(1, 0, Side::Sell, 0, remaining, total, false));
        let iceberg = kani::any::<bool>().then(|| {
            let display: Qty = kani::any();
            let visible: Qty = kani::any_where(|&visible| {
                visible > 0 && visible <= display && visible <= remaining
            });
            pool.make_iceberg(slot, display, visible);
            IcebergPart { display, visible }
        });
        (pool, slot, iceberg)
    }

    /// A fill trades the smaller of the quantity asked for and what the order shows, takes
    /// it from both the open and the shown quantity, and never underflows. An iceberg whose
    /// tranche runs out shows its next one: the smaller of its display and what is left,
    /// at least one lot while anything is left.
    #[kani::proof]
    fn fills_trade_what_the_order_shows() {
        let (mut pool, slot, iceberg) = one_order();
        let remaining = pool.get(slot).remaining;
        let shown = pool.visible(slot);
        assert_eq!(shown, iceberg.map_or(remaining, |part| part.visible));
        assert_eq!(pool.iceberg(slot), iceberg);

        let qty: Qty = kani::any();
        let (fill, leaves) = pool.fill(slot, qty);
        assert_eq!(fill, qty.min(shown));
        assert_eq!(leaves, remaining - fill);
        assert_eq!(pool.get(slot).remaining, leaves);
        assert_eq!(pool.visible(slot), shown - fill);

        if let Some(part) = iceberg {
            if leaves > 0 && pool.visible(slot) == 0 {
                let next = pool.replenish(slot);
                assert_eq!(next, part.display.min(leaves));
                assert!(next > 0 && next <= leaves);
                assert_eq!(pool.visible(slot), next);
            }
        }
    }

    /// Shrinking an order's open quantity, as a modify that keeps priority does, takes the
    /// cut from the hidden part first and reports exactly how much less the order shows.
    #[kani::proof]
    fn shrinking_takes_the_cut_from_the_hidden_part_first() {
        let (mut pool, slot, _) = one_order();
        let remaining = pool.get(slot).remaining;
        let shown = pool.visible(slot);
        let leaves: Qty = kani::any_where(|&leaves| leaves > 0 && leaves <= remaining);
        let total: Qty = kani::any();

        let shown_less = pool.shrink(slot, leaves, total);
        assert_eq!(pool.visible(slot), shown.min(leaves));
        assert_eq!(shown_less, shown - shown.min(leaves));
        assert_eq!(pool.get(slot).remaining, leaves);
        assert_eq!(pool.get(slot).total, total);
    }
}
