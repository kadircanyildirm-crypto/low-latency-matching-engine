//! The order id index: which pool slot holds each resting order and pending stop.
//!
//! A hash table of cache lines, sized once at construction so it never grows. Each line
//! holds five ids with their slots, and an id is looked up in its home line, chosen by
//! hashing, so a lookup normally reads exactly one cache line. A general-purpose map first
//! reads a group of control bytes and only then the bucket, and in a book far larger than
//! the cache both are misses, one after the other.
//!
//! Four consecutive ids share a home line. Exchanges hand out order ids in sequence, so a
//! new order's id usually lands in the line its predecessor just used, and the duplicate
//! check and insertion of a new order hit the cache.
//!
//! An id whose home line is full goes to the next line with room, and every line it
//! passes counts it. A lookup moves on from a line only while that count is non-zero, and a
//! removal clears the entry and lowers the counts again. So removal leaves no tombstones,
//! nothing ever has to be cleaned up by a rehash, and no single command pays for one.

use crate::pool::NIL;
use crate::types::OrderId;

/// Fibonacci hashing: multiply by 2^64 / φ and keep the top bits, which depend on every
/// bit of the input.
const MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;

/// Entries per line.
const WAYS: usize = 5;

/// One cache line of the table.
#[derive(Clone, Copy, Debug)]
#[repr(C, align(64))]
struct Line {
    ids: [OrderId; WAYS],
    /// Pool slot of each entry; `NIL` marks a free one.
    slots: [u32; WAYS],
    /// Entries stored beyond this line whose home is this line or one before it.
    passed: u32,
}

const EMPTY: Line = Line {
    ids: [0; WAYS],
    slots: [NIL; WAYS],
    passed: 0,
};

const _: () = assert!(size_of::<Line>() == 64);

impl Line {
    /// The way holding `id`, if any. Compares all ways without branching.
    #[inline]
    fn way_of(&self, id: OrderId) -> Option<usize> {
        let mut hits = 0u32;
        for way in 0..WAYS {
            hits |= u32::from((self.ids[way] == id) & (self.slots[way] != NIL)) << way;
        }
        (hits != 0).then(|| hits.trailing_zeros() as usize)
    }

    /// The first free way, if any.
    #[inline]
    fn free_way(&self) -> Option<usize> {
        let mut free = 0u32;
        for way in 0..WAYS {
            free |= u32::from(self.slots[way] == NIL) << way;
        }
        (free != 0).then(|| free.trailing_zeros() as usize)
    }
}

/// Order id -> pool slot, for at most the capacity it was built for.
pub(crate) struct IdIndex {
    /// A power of two in length, with room for at least twice the capacity.
    lines: Vec<Line>,
    /// `lines.len() - 1`.
    mask: usize,
    /// `64 - log2(lines.len())`: the hash's top bits pick an id's home line.
    shift: u32,
    len: usize,
}

impl IdIndex {
    /// An empty index for up to `capacity` ids.
    pub fn with_capacity(capacity: u32) -> Self {
        let lines = (capacity as usize * 2)
            .div_ceil(WAYS)
            .next_power_of_two()
            .max(2);
        Self {
            lines: vec![EMPTY; lines],
            mask: lines - 1,
            shift: 64 - lines.trailing_zeros(),
            len: 0,
        }
    }

    /// The line where `id` is stored if there is room.
    #[inline]
    fn home(&self, id: OrderId) -> usize {
        ((id >> 2).wrapping_mul(MULTIPLIER) >> self.shift) as usize
    }

    /// Line and way of `id`, if it is in the index.
    #[inline]
    fn find(&self, id: OrderId) -> Option<(usize, usize)> {
        let mut l = self.home(id);
        loop {
            let line = &self.lines[l];
            if let Some(way) = line.way_of(id) {
                return Some((l, way));
            }
            if line.passed == 0 {
                return None;
            }
            l = (l + 1) & self.mask;
        }
    }

    /// The slot of `id`, if it is in the index.
    #[inline]
    pub fn get(&self, id: &OrderId) -> Option<&u32> {
        self.find(*id).map(|(l, way)| &self.lines[l].slots[way])
    }

    #[inline]
    pub fn contains_key(&self, id: &OrderId) -> bool {
        self.find(*id).is_some()
    }

    /// Maps `id` to `slot`, replacing any earlier slot of `id`.
    #[inline]
    pub fn insert(&mut self, id: OrderId, slot: u32) {
        debug_assert_ne!(slot, NIL);
        if let Some((l, way)) = self.find(id) {
            self.lines[l].slots[way] = slot;
            return;
        }
        // The table has room for twice the capacity, so a free entry is never far.
        let mut l = self.home(id);
        loop {
            let line = &mut self.lines[l];
            if let Some(way) = line.free_way() {
                line.ids[way] = id;
                line.slots[way] = slot;
                self.len += 1;
                return;
            }
            line.passed += 1;
            l = (l + 1) & self.mask;
        }
    }

    /// Removes `id`, returning its slot if it was in the index.
    #[inline]
    pub fn remove(&mut self, id: &OrderId) -> Option<u32> {
        let (at, way) = self.find(*id)?;
        let slot = std::mem::replace(&mut self.lines[at].slots[way], NIL);
        self.len -= 1;
        let mut l = self.home(*id);
        while l != at {
            self.lines[l].passed -= 1;
            l = (l + 1) & self.mask;
        }
        Some(slot)
    }

    /// Number of ids in the index.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
impl std::ops::Index<&OrderId> for IdIndex {
    type Output = u32;

    fn index(&self, id: &OrderId) -> &u32 {
        self.get(id).expect("id not in the index")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashMap;

    #[derive(Clone, Debug)]
    enum Op {
        Insert(OrderId, u32),
        Remove(OrderId),
        Get(OrderId),
    }

    /// Few distinct ids, so the same ones come back and probe sequences overlap, plus ids
    /// whose hashes collide in a small table.
    fn id() -> impl Strategy<Value = OrderId> {
        prop_oneof![
            3 => 0..40u64,
            1 => (0..8u64).prop_map(|i| i << 60),
            1 => Just(u64::MAX),
        ]
    }

    proptest! {
        #[test]
        fn behaves_like_hashmap(capacity in 1..24u32, ops in prop::collection::vec(
            prop_oneof![
                (id(), 0..1_000u32).prop_map(|(id, slot)| Op::Insert(id, slot)),
                id().prop_map(Op::Remove),
                id().prop_map(Op::Get),
            ],
            1..400,
        )) {
            let mut index = IdIndex::with_capacity(capacity);
            let mut model = HashMap::new();
            for op in ops {
                match op {
                    // Like the book, never more ids than the capacity.
                    Op::Insert(id, slot) if model.len() < capacity as usize || model.contains_key(&id) => {
                        index.insert(id, slot);
                        model.insert(id, slot);
                    }
                    Op::Insert(..) => {}
                    Op::Remove(id) => prop_assert_eq!(index.remove(&id), model.remove(&id)),
                    Op::Get(id) => {
                        prop_assert_eq!(index.get(&id), model.get(&id));
                        prop_assert_eq!(index.contains_key(&id), model.contains_key(&id));
                    }
                }
                prop_assert_eq!(index.len(), model.len());
            }
            for (id, slot) in &model {
                prop_assert_eq!(index.get(id), Some(slot));
            }
        }
    }

    /// Room for at least twice the capacity, in a power of two of lines.
    #[test]
    fn sizing() {
        for (capacity, lines) in [(1, 2), (5, 2), (6, 4), (1_000, 512), (1_000_000, 524_288)] {
            let index = IdIndex::with_capacity(capacity);
            assert_eq!(index.lines.len(), lines, "capacity {capacity}");
            assert_eq!(index.mask, lines - 1);
        }
    }

    /// Four consecutive ids share a line, and sequential ids spread out so evenly that a
    /// table half full of them has no line overflowing.
    #[test]
    fn sequential_ids_stay_home() {
        let mut index = IdIndex::with_capacity(4_096);
        assert_eq!(index.home(4_000), index.home(4_003));
        assert_ne!(index.home(4_003), index.home(4_004));
        for id in 4_000..4_000 + 2_048 {
            index.insert(id, 0);
        }
        assert!(index.lines.iter().all(|line| line.passed == 0));
    }
}
