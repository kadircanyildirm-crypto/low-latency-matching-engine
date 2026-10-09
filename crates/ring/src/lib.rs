//! A bounded, lock-free, single-producer single-consumer ring buffer: the queues between the
//! pipeline's stages.
//!
//! ```
//! let (mut producer, mut consumer) = ring::channel(1024);
//! producer.try_push(1).unwrap();
//! producer.push_from(&mut [2, 3].into_iter());
//! let items: Vec<u64> = consumer.drain(usize::MAX).collect();
//! assert_eq!(items, [1, 2, 3]);
//! ```
//!
//! Each side counts the items it has moved, its *position*, and publishes it with a release
//! store; the other side reads it with an acquire load. Positions only grow, and an item's slot
//! is its position modulo the capacity, a power of two.
//!
//! The common case touches only memory the side owns. Each side keeps a copy of the other's
//! position and rereads the real one only when its copy says the ring is full (for the
//! producer) or empty (for the consumer). The two positions sit 128 bytes apart, so they never
//! share a cache line, nor a pair of lines that Intel's adjacent-line prefetcher fetches
//! together: the sides do not invalidate each other's lines except to publish.
//!
//! Batches publish once. [`Producer::push_from`] writes as many items as fit and then stores
//! its position once; [`Consumer::drain`] reads the producer's position once and stores its
//! own once, when the batch is dropped. A stage that finds a hundred items waiting pays for
//! two shared-memory operations, not two hundred.
//!
//! When the ring is full, the producer waits (backpressure): nothing is dropped and nothing
//! grows. How a side waits is its [`Wait`] strategy.

#![warn(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

use std::mem::MaybeUninit;

use sync::{Arc, AtomicBool, AtomicUsize, Ordering, UnsafeCell};

/// The synchronisation primitives: the standard library's, or loom's when the crate is built
/// for model checking with `--cfg loom`.
mod sync {
    #[cfg(loom)]
    pub use loom::cell::UnsafeCell;
    #[cfg(loom)]
    pub use loom::sync::Arc;
    #[cfg(loom)]
    pub use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[cfg(not(loom))]
    pub use std::sync::Arc;
    #[cfg(not(loom))]
    pub use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// `std::cell::UnsafeCell` with loom's interface.
    #[cfg(not(loom))]
    #[derive(Debug)]
    pub struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    #[cfg(not(loom))]
    impl<T> UnsafeCell<T> {
        pub fn new(value: T) -> Self {
            UnsafeCell(std::cell::UnsafeCell::new(value))
        }

        pub fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}

/// Keeps its contents on cache lines of their own: 128 bytes, since Intel's adjacent-line
/// prefetcher fetches lines in pairs.
#[repr(align(128))]
#[derive(Debug)]
struct Padded<T>(T);

/// What the two sides share.
struct Shared<T> {
    /// Items written so far: the producer's position.
    tail: Padded<AtomicUsize>,
    /// Items read so far: the consumer's position.
    head: Padded<AtomicUsize>,
    /// Whether each side still exists.
    producer_alive: AtomicBool,
    consumer_alive: AtomicBool,
    /// The slots. Those from `head` up to `tail` hold items; the rest are free.
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// The capacity minus one, to turn a position into a slot index.
    mask: usize,
}

// SAFETY: each slot is accessed by one side at a time, as the positions hand it over: the
// producer writes only free slots and the consumer reads only full ones, and the release and
// acquire on the positions order those accesses. Items move between threads, so they must be
// `Send`; they are never shared, so they need not be `Sync`.
unsafe impl<T: Send> Send for Shared<T> {}
// SAFETY: as above.
unsafe impl<T: Send> Sync for Shared<T> {}

impl<T> Shared<T> {
    /// Moves `value` into the slot of `position`.
    ///
    /// # Safety
    ///
    /// The slot must be free and owned by the caller: the producer, with `position` at or
    /// after its published position and less than a capacity ahead of the consumer's.
    unsafe fn write(&self, position: usize, value: T) {
        self.slots[position & self.mask].with_mut(|slot| {
            // SAFETY: the caller owns the slot, so nothing else accesses it.
            unsafe { (*slot).write(value) };
        });
    }

    /// Moves the item out of the slot of `position`, leaving the slot free.
    ///
    /// # Safety
    ///
    /// The slot must hold an item owned by the caller: the consumer, with `position` at or
    /// after its own position and before the producer's.
    unsafe fn read(&self, position: usize) -> T {
        self.slots[position & self.mask].with_mut(|slot| {
            // SAFETY: the caller owns the slot, which holds an initialised item.
            unsafe { (*slot).assume_init_read() }
        })
    }
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        // Both sides are gone, and each published its position as it went: the items left
        // are those between them.
        let mut position = self.head.0.load(Ordering::Acquire);
        let tail = self.tail.0.load(Ordering::Acquire);
        while position != tail {
            // SAFETY: the slot lies between the positions, so it holds an item, and with both
            // sides gone this is the only access.
            drop(unsafe { self.read(position) });
            position = position.wrapping_add(1);
        }
    }
}

/// Creates a ring with room for at least `capacity` items: the next power of two.
///
/// # Panics
///
/// If `capacity` is zero or its next power of two does not fit in a `usize`.
pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(capacity > 0, "a ring needs room for an item");
    let capacity = capacity
        .checked_next_power_of_two()
        .expect("the capacity's next power of two fits in a usize");
    let slots = (0..capacity)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect();
    let shared = Arc::new(Shared {
        tail: Padded(AtomicUsize::new(0)),
        head: Padded(AtomicUsize::new(0)),
        producer_alive: AtomicBool::new(true),
        consumer_alive: AtomicBool::new(true),
        slots,
        mask: capacity - 1,
    });
    let producer = Producer {
        shared: shared.clone(),
        tail: 0,
        head: 0,
    };
    let consumer = Consumer {
        shared,
        head: 0,
        tail: 0,
    };
    (producer, consumer)
}

/// How a side waits for the other: the consumer for items, the producer for room.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wait {
    /// Spin on the CPU without pause: the lowest latency, at the cost of a core kept busy
    /// all the time. For stages pinned to cores of their own.
    Spin,
    /// Spin briefly, then yield the CPU, then sleep in naps of up to 50 µs: for machines
    /// with fewer cores than stages, at the cost of up to a nap's latency after a pause.
    Backoff,
}

/// One wait, for a stage that waits on more than one thing at once: call
/// [`snooze`](Backoff::snooze) each time it finds nothing to do, and
/// [`reset`](Backoff::reset) when it does something.
#[derive(Clone, Debug)]
pub struct Backoff {
    strategy: Wait,
    rounds: u32,
}

impl Backoff {
    /// A wait with `strategy`.
    pub fn new(strategy: Wait) -> Self {
        Backoff {
            strategy,
            rounds: 0,
        }
    }

    /// Starts the wait over: the next snooze spins again.
    pub fn reset(&mut self) {
        self.rounds = 0;
    }

    /// Waits a little, longer the more often it was called since the last reset.
    pub fn snooze(&mut self) {
        #[cfg(loom)]
        loom::thread::yield_now();
        self.rounds = self.rounds.saturating_add(1);
        match self.strategy {
            Wait::Spin => std::hint::spin_loop(),
            Wait::Backoff if self.rounds <= 64 => std::hint::spin_loop(),
            Wait::Backoff if self.rounds <= 128 => std::thread::yield_now(),
            Wait::Backoff => {
                let micros = (self.rounds - 128).min(50);
                std::thread::sleep(std::time::Duration::from_micros(u64::from(micros)));
            }
        }
    }
}

/// The writing side of a ring.
pub struct Producer<T> {
    shared: Arc<Shared<T>>,
    /// Items written: the position.
    tail: usize,
    /// The consumer's position, as last read.
    head: usize,
}

impl<T> Producer<T> {
    /// How many items the ring holds at most.
    pub fn capacity(&self) -> usize {
        self.shared.mask + 1
    }

    /// Whether the consumer is gone: nothing pushed will be read.
    pub fn is_closed(&self) -> bool {
        !self.shared.consumer_alive.load(Ordering::Acquire)
    }

    /// Free slots by the consumer's position as last read, rereading it if that leaves
    /// fewer than `wanted`.
    fn free(&mut self, wanted: usize) -> usize {
        let free = self.capacity() - self.tail.wrapping_sub(self.head);
        if free >= wanted {
            return free;
        }
        self.head = self.shared.head.0.load(Ordering::Acquire);
        self.capacity() - self.tail.wrapping_sub(self.head)
    }

    /// Appends `value` if there is room, or gives it back.
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        if self.free(1) == 0 {
            return Err(value);
        }
        // SAFETY: the slot of `tail` is free, since fewer than a capacity of items are in
        // the ring, and only the producer writes.
        unsafe { self.shared.write(self.tail, value) };
        self.tail = self.tail.wrapping_add(1);
        self.shared.tail.0.store(self.tail, Ordering::Release);
        Ok(())
    }

    /// Appends `value`, waiting while the ring is full. Gives it back if the consumer is
    /// gone.
    pub fn push(&mut self, mut value: T, wait: Wait) -> Result<(), T> {
        let mut waiting = Backoff::new(wait);
        loop {
            match self.try_push(value) {
                Ok(()) => return Ok(()),
                Err(back) if self.is_closed() => return Err(back),
                Err(back) => value = back,
            }
            waiting.snooze();
        }
    }

    /// Appends items taken from `items` while there is room, and publishes them together.
    /// Returns how many it took; the rest stay in the iterator.
    pub fn push_from(&mut self, items: &mut impl Iterator<Item = T>) -> usize {
        let free = self.free(items.size_hint().0.max(1));
        let mut written = 0;
        while written < free {
            let Some(value) = items.next() else { break };
            // SAFETY: as in `try_push`, for each of the `free` slots after `tail`.
            unsafe { self.shared.write(self.tail.wrapping_add(written), value) };
            written += 1;
        }
        if written > 0 {
            self.tail = self.tail.wrapping_add(written);
            self.shared.tail.0.store(self.tail, Ordering::Release);
        }
        written
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // Everything written was published as it was written.
        self.shared.producer_alive.store(false, Ordering::Release);
    }
}

/// The reading side of a ring.
pub struct Consumer<T> {
    shared: Arc<Shared<T>>,
    /// Items read: the position.
    head: usize,
    /// The producer's position, as last read.
    tail: usize,
}

impl<T> Consumer<T> {
    /// How many items the ring holds at most.
    pub fn capacity(&self) -> usize {
        self.shared.mask + 1
    }

    /// How many items are waiting.
    pub fn len(&mut self) -> usize {
        self.tail = self.shared.tail.0.load(Ordering::Acquire);
        self.tail.wrapping_sub(self.head)
    }

    /// Whether no item is waiting.
    pub fn is_empty(&mut self) -> bool {
        self.len() == 0
    }

    /// Whether the producer is gone and every item it wrote has been read.
    pub fn is_closed(&mut self) -> bool {
        // The producer published its last position before it marked itself gone, so once
        // that is seen, the position read after it is final.
        !self.shared.producer_alive.load(Ordering::Acquire) && self.is_empty()
    }

    /// Items waiting by the producer's position as last read, rereading it if there are
    /// none.
    fn available(&mut self) -> usize {
        let available = self.tail.wrapping_sub(self.head);
        if available > 0 {
            return available;
        }
        self.len()
    }

    /// Takes the next item, if there is one.
    pub fn try_pop(&mut self) -> Option<T> {
        if self.available() == 0 {
            return None;
        }
        // SAFETY: the slot of `head` holds an item, since the producer's position is past
        // it, and only the consumer reads.
        let value = unsafe { self.shared.read(self.head) };
        self.head = self.head.wrapping_add(1);
        self.shared.head.0.store(self.head, Ordering::Release);
        Some(value)
    }

    /// Takes the next item, waiting for one. Returns `None` once the producer is gone and
    /// every item has been read.
    pub fn pop(&mut self, wait: Wait) -> Option<T> {
        if !self.wait(wait) {
            return None;
        }
        self.try_pop()
    }

    /// Waits until an item is waiting, and returns true, or until the producer is gone and
    /// every item has been read, and returns false.
    pub fn wait(&mut self, wait: Wait) -> bool {
        let mut waiting = Backoff::new(wait);
        loop {
            if self.available() > 0 {
                return true;
            }
            if self.is_closed() {
                return false;
            }
            waiting.snooze();
        }
    }

    /// Takes up to `max` of the items waiting now, in order. The slots they free are
    /// published together when the returned iterator is dropped; items it did not yield stay
    /// in the ring.
    pub fn drain(&mut self, max: usize) -> Drain<'_, T> {
        let end = self.head.wrapping_add(self.len().min(max));
        Drain {
            consumer: self,
            end,
        }
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // A batch whose iterator was leaked took items without publishing: publish them, so
        // that what remains in the ring is exactly what was never taken.
        self.shared.head.0.store(self.head, Ordering::Release);
        self.shared.consumer_alive.store(false, Ordering::Release);
    }
}

/// A batch of items taken by [`Consumer::drain`].
pub struct Drain<'a, T> {
    consumer: &'a mut Consumer<T>,
    /// The position after the batch's last item.
    end: usize,
}

impl<T> Iterator for Drain<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let consumer = &mut *self.consumer;
        if consumer.head == self.end {
            return None;
        }
        // SAFETY: the slot of `head` holds an item, since it is before `end`, which was at
        // most the producer's position, and only the consumer reads.
        let value = unsafe { consumer.shared.read(consumer.head) };
        consumer.head = consumer.head.wrapping_add(1);
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.end.wrapping_sub(self.consumer.head);
        (left, Some(left))
    }
}

impl<T> ExactSizeIterator for Drain<'_, T> {}

impl<T> Drop for Drain<'_, T> {
    fn drop(&mut self) {
        let consumer = &mut *self.consumer;
        consumer
            .shared
            .head
            .0
            .store(consumer.head, Ordering::Release);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn capacities_round_up_to_powers_of_two() {
        for (asked, got) in [(1, 1), (2, 2), (3, 4), (1000, 1024)] {
            let (producer, consumer) = channel::<u8>(asked);
            assert_eq!((producer.capacity(), consumer.capacity()), (got, got));
        }
    }

    #[test]
    #[should_panic(expected = "room for an item")]
    fn an_empty_ring_is_refused() {
        let _ = channel::<u8>(0);
    }

    #[test]
    fn the_positions_have_cache_lines_of_their_own() {
        assert_eq!(std::mem::align_of::<Padded<AtomicUsize>>(), 128);
        let (producer, _consumer) = channel::<u8>(1);
        let shared = &*producer.shared;
        let tail = &shared.tail as *const _ as usize;
        let head = &shared.head as *const _ as usize;
        assert!(tail.abs_diff(head) >= 128);
    }
}
