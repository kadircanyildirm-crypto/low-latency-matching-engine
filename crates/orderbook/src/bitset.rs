//! Two-level bitset marking which price levels are occupied.
//!
//! When the best level empties, the book needs the next occupied level. A linear scan over a
//! sparse ladder could touch thousands of empty levels; here one `u64` per 64 levels holds the
//! occupancy bits and a summary word per 4096 levels says which of those words are non-zero,
//! so a search inspects at most a few words plus one summary word per 4096 levels.

pub(crate) struct LevelBitset {
    len: usize,
    words: Vec<u64>,
    summary: Vec<u64>,
}

impl LevelBitset {
    pub fn new(len: usize) -> Self {
        let words = len.div_ceil(64);
        Self {
            len,
            words: vec![0; words],
            summary: vec![0; words.div_ceil(64)],
        }
    }

    #[inline]
    pub fn insert(&mut self, i: usize) {
        debug_assert!(i < self.len);
        let w = i / 64;
        self.words[w] |= 1 << (i % 64);
        self.summary[w / 64] |= 1 << (w % 64);
    }

    #[inline]
    pub fn remove(&mut self, i: usize) {
        debug_assert!(i < self.len);
        let w = i / 64;
        self.words[w] &= !(1 << (i % 64));
        if self.words[w] == 0 {
            self.summary[w / 64] &= !(1 << (w % 64));
        }
    }

    #[cfg(test)]
    pub fn contains(&self, i: usize) -> bool {
        i < self.len && self.words[i / 64] & (1 << (i % 64)) != 0
    }

    /// Smallest set index `>= i`.
    pub fn next_at_or_after(&self, i: usize) -> Option<usize> {
        if i >= self.len {
            return None;
        }
        let w = i / 64;
        let bits = self.words[w] & (u64::MAX << (i % 64));
        if bits != 0 {
            return Some(w * 64 + bits.trailing_zeros() as usize);
        }
        let w = self.next_word_after(w)?;
        Some(w * 64 + self.words[w].trailing_zeros() as usize)
    }

    /// Largest set index `<= i`.
    pub fn prev_at_or_before(&self, i: usize) -> Option<usize> {
        if self.len == 0 {
            return None;
        }
        let i = i.min(self.len - 1);
        let w = i / 64;
        let bits = self.words[w] & low_bits_through(i % 64);
        if bits != 0 {
            return Some(w * 64 + highest_bit(bits));
        }
        let w = self.prev_word_before(w)?;
        Some(w * 64 + highest_bit(self.words[w]))
    }

    /// First non-empty word after word `w`.
    fn next_word_after(&self, w: usize) -> Option<usize> {
        let start = w + 1;
        if start >= self.words.len() {
            return None;
        }
        let mut s = start / 64;
        let mut bits = self.summary[s] & (u64::MAX << (start % 64));
        loop {
            if bits != 0 {
                return Some(s * 64 + bits.trailing_zeros() as usize);
            }
            s += 1;
            bits = *self.summary.get(s)?;
        }
    }

    /// Last non-empty word before word `w`.
    fn prev_word_before(&self, w: usize) -> Option<usize> {
        let end = w.checked_sub(1)?;
        let mut s = end / 64;
        let mut bits = self.summary[s] & low_bits_through(end % 64);
        loop {
            if bits != 0 {
                return Some(s * 64 + highest_bit(bits));
            }
            s = s.checked_sub(1)?;
            bits = self.summary[s];
        }
    }
}

/// Mask of bits `0..=bit`.
#[inline]
fn low_bits_through(bit: usize) -> u64 {
    u64::MAX >> (63 - bit)
}

#[inline]
fn highest_bit(bits: u64) -> usize {
    63 - bits.leading_zeros() as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    #[derive(Clone, Debug)]
    enum Op {
        Insert(usize),
        Remove(usize),
        Next(usize),
        Prev(usize),
    }

    /// Lengths just below, at and above a word (64) and a summary word (4096), so the last
    /// word is sometimes full and sometimes partial; and one spanning three summary words.
    fn len() -> impl Strategy<Value = usize> {
        prop::sample::select(vec![1, 63, 64, 65, 128, 4_095, 4_096, 4_097, 8_192, 10_000])
    }

    /// Mostly random, with extra weight on word and summary boundaries.
    fn index(len: usize) -> impl Strategy<Value = usize> {
        let boundaries: Vec<usize> = [0, 1, 63, 64, 65, 127, 128, 4_095, 4_096, 4_097, len - 1]
            .into_iter()
            .filter(|&i| i < len)
            .collect();
        prop_oneof![4 => 0..len, 1 => prop::sample::select(boundaries)]
    }

    /// Queries also probe past the end, including `usize::MAX`.
    fn query(len: usize) -> impl Strategy<Value = usize> {
        prop_oneof![10 => 0..len + 100, 1 => Just(usize::MAX)]
    }

    fn ops(len: usize) -> impl Strategy<Value = Vec<Op>> {
        let op = prop_oneof![
            index(len).prop_map(Op::Insert),
            index(len).prop_map(Op::Remove),
            query(len).prop_map(Op::Next),
            query(len).prop_map(Op::Prev),
        ];
        prop::collection::vec(op, 1..300)
    }

    proptest! {
        #[test]
        fn behaves_like_btreeset((len, ops) in len().prop_flat_map(|len| (Just(len), ops(len)))) {
            let mut bits = LevelBitset::new(len);
            let mut model = BTreeSet::new();
            for op in ops {
                match op {
                    Op::Insert(i) => {
                        bits.insert(i);
                        model.insert(i);
                    }
                    Op::Remove(i) => {
                        bits.remove(i);
                        model.remove(&i);
                    }
                    Op::Next(i) => {
                        prop_assert_eq!(bits.next_at_or_after(i), model.range(i..).next().copied());
                    }
                    Op::Prev(i) => {
                        prop_assert_eq!(bits.prev_at_or_before(i), model.range(..=i).next_back().copied());
                    }
                }
            }
            for i in 0..len {
                prop_assert_eq!(bits.contains(i), model.contains(&i));
            }
        }
    }

    #[test]
    fn empty_and_tiny_sets() {
        let bits = LevelBitset::new(0);
        assert_eq!(bits.next_at_or_after(0), None);
        assert_eq!(bits.prev_at_or_before(0), None);

        let mut bits = LevelBitset::new(1);
        bits.insert(0);
        assert_eq!(bits.next_at_or_after(0), Some(0));
        assert_eq!(bits.prev_at_or_before(usize::MAX), Some(0));
        bits.remove(0);
        assert_eq!(bits.prev_at_or_before(0), None);
    }

    #[test]
    fn queries_past_a_full_last_word() {
        // 128 levels fill exactly two words: a query past the end must not step into a
        // third word that does not exist.
        let mut bits = LevelBitset::new(128);
        bits.insert(127);
        assert_eq!(bits.prev_at_or_before(128), Some(127));
        assert_eq!(bits.prev_at_or_before(usize::MAX), Some(127));
        assert_eq!(bits.next_at_or_after(128), None);
    }
}

/// Kani proofs: for every input within the stated bounds, not just sampled ones, the
/// searches agree with a linear scan over a plain model, and nothing panics or overflows,
/// queries at `usize::MAX` included. Run with `cargo kani -p orderbook`.
///
/// Each harness fixes the band's length and leaves everything else symbolic. A symbolic
/// length makes every vector symbolic in size, which exhausts the model checker's memory.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// Elements of a sparse set: enough for one on each side of a query, or two in one
    /// word of which one is removed.
    const ELEMENTS: usize = 2;

    /// For every bit position, every bit of the mask is checked.
    #[kani::proof]
    fn low_bits_through_masks_bits_up_to_and_including_its_argument() {
        let bit: usize = kani::any_where(|&bit| bit < 64);
        let probe: usize = kani::any_where(|&probe| probe < 64);
        assert_eq!(low_bits_through(bit) >> probe & 1 == 1, probe <= bit);
    }

    /// For every non-zero word.
    #[kani::proof]
    fn highest_bit_is_the_highest_set_bit() {
        let bits: u64 = kani::any_where(|&bits| bits != 0);
        let highest = highest_bit(bits);
        assert!(highest < 64);
        assert_eq!(bits >> highest, 1);
    }

    /// Every subset of a band of `LEN` levels, followed by an insert and a remove of any
    /// index: both searches, from any starting index, match a linear scan over a boolean
    /// array. That covers membership too: `next_at_or_after(i)` is `Some(i)` exactly when
    /// `i` is in the set.
    fn dense_sets_agree_with_a_linear_scan<const LEN: usize>() {
        let mut model: [bool; LEN] = kani::any();
        let mut bits = LevelBitset::new(LEN);
        for (i, &set) in model.iter().enumerate() {
            if set {
                bits.insert(i);
            }
        }
        if kani::any() {
            let i = kani::any_where(|&i| i < LEN);
            bits.insert(i);
            model[i] = true;
        }
        if kani::any() {
            let i = kani::any_where(|&i| i < LEN);
            bits.remove(i);
            model[i] = false;
        }

        let i: usize = kani::any();
        // The smallest set index at or after `i`, and the largest at or before it.
        let mut next = None;
        let mut prev = None;
        for (j, &set) in model.iter().enumerate().rev() {
            if set && j >= i {
                next = Some(j);
            }
        }
        for (j, &set) in model.iter().enumerate() {
            if set && j <= i {
                prev = Some(j);
            }
        }
        assert_eq!(bits.next_at_or_after(i), next);
        assert_eq!(bits.prev_at_or_before(i), prev);
        kani::cover!(
            next.is_some_and(|next| next / 64 > i / 64),
            "next is found in a later word"
        );
        kani::cover!(
            prev.is_some_and(|prev| prev / 64 < i.min(LEN.saturating_sub(1)) / 64),
            "prev is found in an earlier word"
        );
    }

    /// No levels at all.
    #[kani::proof]
    fn dense_sets_with_no_levels() {
        dense_sets_agree_with_a_linear_scan::<0>();
    }

    /// 63 levels: one partial word.
    #[kani::proof]
    #[kani::unwind(65)]
    fn dense_sets_in_a_partial_word() {
        dense_sets_agree_with_a_linear_scan::<63>();
    }

    /// 64 levels: exactly one full word, so a search can run off its end.
    #[kani::proof]
    #[kani::unwind(66)]
    fn dense_sets_in_one_full_word() {
        dense_sets_agree_with_a_linear_scan::<64>();
    }

    /// 128 levels: exactly two full words.
    #[kani::proof]
    #[kani::unwind(130)]
    fn dense_sets_in_two_full_words() {
        dense_sets_agree_with_a_linear_scan::<128>();
    }

    /// 130 levels: two full words and a partial third.
    #[kani::proof]
    #[kani::unwind(132)]
    fn dense_sets_in_a_partial_third_word() {
        dense_sets_agree_with_a_linear_scan::<130>();
    }

    /// A set of up to [`ELEMENTS`] anywhere in a band of `len` levels, with any one index
    /// removed: both searches, from any starting index, match the smallest element at or
    /// after it and the largest at or before it.
    fn sparse_sets_agree_with_their_elements(len: usize) {
        let elements: [usize; ELEMENTS] = kani::any();
        let mut present = [true; ELEMENTS];
        let mut bits = LevelBitset::new(len);
        for &element in &elements {
            kani::assume(element < len);
            bits.insert(element);
        }
        if kani::any() {
            let removed = kani::any_where(|&i| i < len);
            bits.remove(removed);
            for k in 0..ELEMENTS {
                if elements[k] == removed {
                    present[k] = false;
                }
            }
        }

        let i: usize = kani::any();
        let mut next: Option<usize> = None;
        let mut prev: Option<usize> = None;
        for k in 0..ELEMENTS {
            let element = elements[k];
            if present[k] && element >= i && next.is_none_or(|next| element < next) {
                next = Some(element);
            }
            if present[k] && element <= i && prev.is_none_or(|prev| element > prev) {
                prev = Some(element);
            }
        }
        assert_eq!(bits.next_at_or_after(i), next);
        assert_eq!(bits.prev_at_or_before(i), prev);
        kani::cover!(
            next.is_some_and(|next| next / 4096 > i / 4096),
            "next is found in a later summary word"
        );
        kani::cover!(
            prev.is_some_and(|prev| prev / 4096 < i.min(len - 1) / 4096),
            "prev is found in an earlier summary word"
        );
    }

    /// 4,097 levels: two summary words, the second holding one word of one level.
    #[kani::proof]
    #[kani::unwind(200)]
    fn sparse_sets_in_two_summary_words() {
        sparse_sets_agree_with_their_elements(4_097);
    }

    /// 8,192 levels: exactly two full summary words, so a search can run off the end.
    #[kani::proof]
    #[kani::unwind(200)]
    fn sparse_sets_in_two_full_summary_words() {
        sparse_sets_agree_with_their_elements(8_192);
    }

    /// 8,193 levels: two full summary words and a third holding one word of one level, so
    /// a search can skip a whole empty summary word.
    #[kani::proof]
    #[kani::unwind(200)]
    fn sparse_sets_in_three_summary_words() {
        sparse_sets_agree_with_their_elements(8_193);
    }
}
