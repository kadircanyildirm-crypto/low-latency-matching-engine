//! The ring against a model, item ownership, and two threads at full speed. Miri runs these
//! too, with fewer items.

#![cfg(not(loom))]

use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::thread;

use ring::{Wait, channel};

/// Items per thread test: fewer under Miri, which is slow.
const ITEMS: u64 = if cfg!(miri) { 2_000 } else { 2_000_000 };

/// A small generator, so that the tests need no dependencies Miri would have to build.
struct Rng(u64);

impl Rng {
    /// SplitMix64.
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> usize {
        (self.next() % n) as usize
    }
}

/// On one thread the ring is a bounded FIFO queue: over random sequences of every
/// operation, it agrees with `VecDeque`, and refuses what does not fit.
#[test]
fn the_ring_is_a_bounded_queue() {
    let cases = if cfg!(miri) { 20 } else { 2_000 };
    for seed in 0..cases {
        let mut rng = Rng(seed);
        let (mut producer, mut consumer) = channel(1 + rng.below(8));
        let capacity = producer.capacity();
        let mut model: VecDeque<u64> = VecDeque::new();
        for _ in 0..rng.below(80) {
            match rng.below(5) {
                0 => {
                    let value = rng.next();
                    let pushed = producer.try_push(value);
                    if model.len() < capacity {
                        assert_eq!(pushed, Ok(()), "seed {seed}");
                        model.push_back(value);
                    } else {
                        assert_eq!(pushed, Err(value), "seed {seed}");
                    }
                }
                1 => {
                    let values: Vec<u64> = (0..rng.below(12)).map(|_| rng.next()).collect();
                    let mut items = values.iter().copied();
                    let pushed = producer.push_from(&mut items);
                    assert_eq!(
                        pushed,
                        values.len().min(capacity - model.len()),
                        "seed {seed}"
                    );
                    assert_eq!(items.len(), values.len() - pushed, "seed {seed}");
                    model.extend(&values[..pushed]);
                }
                2 => assert_eq!(consumer.try_pop(), model.pop_front(), "seed {seed}"),
                3 => {
                    let (max, take) = (rng.below(12), rng.below(12));
                    let mut batch = consumer.drain(max);
                    assert_eq!(batch.len(), model.len().min(max), "seed {seed}");
                    let taken: Vec<u64> = batch.by_ref().take(take).collect();
                    drop(batch);
                    let expected: Vec<u64> = model.drain(..taken.len()).collect();
                    assert_eq!(taken, expected, "seed {seed}");
                }
                _ => assert_eq!(consumer.len(), model.len(), "seed {seed}"),
            }
        }
    }
}

/// Counts its drops.
struct Counted(Rc<Cell<usize>>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

/// Every item is dropped exactly once: by whoever took it out, or by the ring when both
/// sides are gone, also when a batch was leaked or the ring wrapped around.
#[test]
fn every_item_is_dropped_once() {
    let drops = Rc::new(Cell::new(0));
    let item = || Counted(drops.clone());
    let (mut producer, mut consumer) = channel(4);
    for _ in 0..4 {
        assert!(producer.try_push(item()).is_ok());
    }
    // A refused item comes back, and is dropped here.
    drop(producer.try_push(item()));
    assert_eq!(drops.get(), 1);
    drop(consumer.try_pop());
    assert_eq!(drops.get(), 2);
    // Wrap around.
    assert!(producer.try_push(item()).is_ok());
    // A batch that is partly taken leaves the rest in the ring.
    let mut batch = consumer.drain(usize::MAX);
    drop(batch.next());
    drop(batch);
    assert_eq!(drops.get(), 3);
    // A leaked batch: the items it took are the caller's, the rest stay.
    let mut batch = consumer.drain(2);
    let taken = batch.next().unwrap();
    std::mem::forget(batch);
    drop(taken);
    assert_eq!(drops.get(), 4);
    // Two items are left in the ring, which drops them once both sides are gone.
    drop(producer);
    assert_eq!(drops.get(), 4);
    drop(consumer);
    assert_eq!(drops.get(), 6);
}

#[test]
fn each_side_sees_the_other_go() {
    let (mut producer, mut consumer) = channel(2);
    producer.try_push(1).unwrap();
    producer.try_push(2).unwrap();
    assert!(!consumer.is_closed());
    drop(producer);
    // What was written is still read before the ring counts as closed.
    assert!(!consumer.is_closed());
    assert_eq!(consumer.pop(Wait::Spin), Some(1));
    assert_eq!(consumer.drain(usize::MAX).collect::<Vec<_>>(), [2]);
    assert!(consumer.is_closed());
    assert_eq!(consumer.pop(Wait::Backoff), None);
    assert!(!consumer.wait(Wait::Spin));

    let (mut producer, consumer) = channel(1);
    assert!(!producer.is_closed());
    producer.try_push(1).unwrap();
    drop(consumer);
    assert!(producer.is_closed());
    // A full ring whose consumer is gone gives the item back rather than wait for ever.
    assert_eq!(producer.push(2, Wait::Backoff), Err(2));
}

/// Two threads, each mixing single and batched operations, the producer often finding the
/// ring full and the consumer often finding it empty: every item arrives once, in order.
fn transfer(capacity: usize, wait: Wait) {
    let (mut producer, mut consumer) = channel(capacity);
    let sender = thread::spawn(move || {
        let mut next = 0;
        while next < ITEMS {
            if next % 3 == 0 {
                producer.push(next, wait).unwrap();
                next += 1;
            } else {
                let end = (next + 1 + next % 7).min(ITEMS);
                let mut items = next..end;
                next += producer.push_from(&mut items) as u64;
            }
        }
    });
    let mut expected = 0;
    while expected < ITEMS {
        if expected % 5 == 0 {
            assert_eq!(consumer.pop(wait), Some(expected));
            expected += 1;
        } else {
            for item in consumer.drain(1 + expected as usize % 11) {
                assert_eq!(item, expected);
                expected += 1;
            }
        }
    }
    sender.join().unwrap();
    assert!(consumer.is_closed());
}

#[test]
fn items_cross_threads_in_order() {
    transfer(64, Wait::Spin);
}

#[test]
fn a_tiny_ring_with_backoff_loses_nothing() {
    transfer(1, Wait::Backoff);
}
