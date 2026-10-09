//! Command streams for the engine's tests, and the states they lead to.

#![allow(dead_code)]

use orderbook::workload::{Mix, TifMix, Workload, WorkloadConfig};
use orderbook::{BookConfig, Command, OrderBook};

/// A book configuration and `len` commands of a flow that uses every command kind: limits
/// with every time in force and icebergs, market orders, cancels, modifies, stops, mass
/// cancels and phase changes, under a band that starts calls. The generator learns from a
/// scratch book which orders rest, so most commands are accepted.
pub fn flow(seed: u64, len: usize) -> (BookConfig, Vec<Command>) {
    let workload = WorkloadConfig {
        seed,
        min_price: 0,
        max_price: 2_000,
        initial_mid: 1_000,
        max_live: 300,
        owners: 8,
        mix: Mix {
            passive_limit: 45,
            aggressive_limit: 10,
            market: 15,
            cancel: 17,
            mass_cancel: 1,
            stop: 4,
            modify: 5,
            session: 3,
        },
        tif: TifMix {
            ioc: 20,
            fok: 10,
            post_only: 10,
        },
        iceberg: 20,
        ..WorkloadConfig::default()
    };
    let config = BookConfig {
        price_protection: None,
        price_band: Some(10),
        reference_price: Some(workload.initial_mid),
        auction_on_band: true,
        ..workload.book_config()
    };
    let mut generator = Workload::new(workload);
    let mut book = OrderBook::new(config);
    let mut events = Vec::new();
    let commands = (0..len)
        .map(|_| {
            let command = generator.next_command();
            events.clear();
            book.process(command, &mut events);
            events.iter().for_each(|event| generator.observe(event));
            command
        })
        .collect();
    (config, commands)
}

/// The digest of the book after each prefix of `commands`: `digests[n]` after the first
/// `n`.
pub fn digests(config: BookConfig, commands: &[Command]) -> Vec<u64> {
    let mut book = OrderBook::new(config);
    let mut events = Vec::new();
    let mut digests = Vec::with_capacity(commands.len() + 1);
    digests.push(book.digest());
    for &command in commands {
        events.clear();
        book.process(command, &mut events);
        digests.push(book.digest());
    }
    digests
}
