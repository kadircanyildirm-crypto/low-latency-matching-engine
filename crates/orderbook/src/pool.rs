//! Fixed-capacity storage for resting orders.

use crate::types::{OrderId, OwnerId, Qty, Side};

/// Null link for the intrusive lists.
pub(crate) const NIL: u32 = u32::MAX;

/// A resting order plus its links in the FIFO queue of its price level.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OrderNode {
    pub id: OrderId,
    /// Open quantity ("leaves").
    pub remaining: Qty,
    /// Total order quantity: filled + open. Kept for FIX-style modifies.
    pub total: Qty,
    /// Index of the price level within its half-book.
    pub level: u32,
    pub prev: u32,
    pub next: u32,
    pub owner: OwnerId,
    pub side: Side,
}

// The node's size is part of the cache budget; growing it should be a deliberate decision.
const _: () = assert!(size_of::<OrderNode>() == 48);

/// Slab of order nodes addressed by `u32` slot, allocated once up front.
///
/// Free slots form a LIFO list threaded through `next`, so the most recently freed slot
/// (the one most likely still in cache) is reused first.
pub(crate) struct OrderPool {
    nodes: Vec<OrderNode>,
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
                prev: NIL,
                next: if slot + 1 < capacity { slot + 1 } else { NIL },
                owner: 0,
                side: Side::Buy,
            })
            .collect();
        Self {
            nodes,
            free_head: if capacity == 0 { NIL } else { 0 },
            live: 0,
        }
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.free_head == NIL
    }

    #[inline]
    pub fn live(&self) -> usize {
        self.live
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.nodes.len()
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
}
