//! Model checking with loom: every interleaving of the two sides, and every way the memory
//! model lets one side see the other's writes late, within the bounds below. Run with
//! `RUSTFLAGS="--cfg loom" cargo test -p ring --test loom --release`.

#![cfg(loom)]

use ring::{Wait, channel};

/// Items cross a ring smaller than their number, so the producer waits for room and the
/// consumer for items, one at a time and in batches.
#[test]
fn items_arrive_once_and_in_order() {
    loom::model(|| {
        let (mut producer, mut consumer) = channel(2);
        let sender = loom::thread::spawn(move || {
            producer.push(0, Wait::Spin).unwrap();
            let mut rest = 1..3;
            while rest.len() > 0 {
                if producer.push_from(&mut rest) == 0 {
                    loom::thread::yield_now();
                }
            }
        });
        let mut received = Vec::new();
        while let Some(item) = consumer.pop(Wait::Spin) {
            received.push(item);
            received.extend(consumer.drain(usize::MAX));
        }
        sender.join().unwrap();
        assert_eq!(received, [0, 1, 2]);
    });
}

/// Items the consumer never took are dropped with the ring, whichever side goes last.
#[test]
fn items_left_behind_are_dropped_once() {
    loom::model(|| {
        let item = loom::sync::Arc::new(());
        let (mut producer, mut consumer) = channel(2);
        let copy = item.clone();
        let sender = loom::thread::spawn(move || {
            let _ = producer.push(copy.clone(), Wait::Spin);
            let _ = producer.push(copy, Wait::Spin);
        });
        drop(consumer.pop(Wait::Spin));
        drop(consumer);
        sender.join().unwrap();
        assert_eq!(loom::sync::Arc::strong_count(&item), 1);
    });
}
