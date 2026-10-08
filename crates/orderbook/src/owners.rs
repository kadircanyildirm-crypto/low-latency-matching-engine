//! Each owner's resting orders, linked into one list per owner, so a mass cancel touches
//! only that owner's orders instead of scanning the book.
//!
//! Owner ids are dense: `0..max_owners`, assigned by the gateway the way exchanges number
//! their participants internally. The table is therefore a plain array indexed by owner id,
//! with no hashing on the hot path.
//!
//! An order joins the back of its owner's list when it joins the back of a price level's
//! queue, and both lists only ever append. So, restricted to any one level, an owner's list
//! is in that level's queue order; mass cancels rely on it to emit events in book order.
//!
//! The links live in their own array, indexed by the order's pool slot, rather than in the
//! order nodes. Matching never reads them, so they stay out of the nodes' cache lines, and
//! updating a neighbour's link touches an 8-byte entry instead of a whole node.

use crate::pool::NIL;
use crate::types::OwnerId;

/// One owner's list of resting orders.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerList {
    pub head: u32,
    pub tail: u32,
    pub count: u32,
}

impl OwnerList {
    const EMPTY: OwnerList = OwnerList {
        head: NIL,
        tail: NIL,
        count: 0,
    };
}

/// An order's place in its owner's list.
#[derive(Clone, Copy, Debug)]
pub(crate) struct OwnerLink {
    pub prev: u32,
    pub next: u32,
}

/// Every owner's list, indexed by owner id, and every order slot's owner links, indexed by
/// pool slot. Both are sized up front.
pub(crate) struct Owners {
    lists: Vec<OwnerList>,
    /// Meaningful only while the slot holds an order.
    links: Vec<OwnerLink>,
}

impl Owners {
    pub fn new(max_owners: u32, max_orders: u32) -> Self {
        let unlinked = OwnerLink {
            prev: NIL,
            next: NIL,
        };
        Self {
            lists: vec![OwnerList::EMPTY; max_owners as usize],
            links: vec![unlinked; max_orders as usize],
        }
    }

    /// Appends the order in pool slot `slot` to the back of `owner`'s list.
    #[inline]
    pub fn link(&mut self, slot: u32, owner: OwnerId) {
        let list = &mut self.lists[owner as usize];
        let tail = list.tail;
        self.links[slot as usize] = OwnerLink {
            prev: tail,
            next: NIL,
        };
        if tail == NIL {
            list.head = slot;
        } else {
            self.links[tail as usize].next = slot;
        }
        list.tail = slot;
        list.count += 1;
    }

    /// Removes the order in pool slot `slot` from `owner`'s list.
    #[inline]
    pub fn unlink(&mut self, slot: u32, owner: OwnerId) {
        let OwnerLink { prev, next } = self.links[slot as usize];
        let list = &mut self.lists[owner as usize];
        if prev == NIL {
            list.head = next;
        } else {
            self.links[prev as usize].next = next;
        }
        if next == NIL {
            list.tail = prev;
        } else {
            self.links[next as usize].prev = prev;
        }
        list.count -= 1;
    }

    /// `owner`'s list; empty for an owner id outside the table.
    #[inline]
    pub fn list(&self, owner: OwnerId) -> OwnerList {
        self.lists
            .get(owner as usize)
            .copied()
            .unwrap_or(OwnerList::EMPTY)
    }

    /// The owner links of the order in pool slot `slot`.
    #[inline]
    pub fn link_of(&self, slot: u32) -> OwnerLink {
        self.links[slot as usize]
    }

    /// Every owner's list, by owner id.
    pub fn lists(&self) -> impl Iterator<Item = (OwnerId, &OwnerList)> {
        (0..).zip(&self.lists)
    }

    #[cfg(test)]
    pub fn list_mut(&mut self, owner: OwnerId) -> &mut OwnerList {
        &mut self.lists[owner as usize]
    }

    #[cfg(test)]
    pub fn link_mut(&mut self, slot: u32) -> &mut OwnerLink {
        &mut self.links[slot as usize]
    }
}
