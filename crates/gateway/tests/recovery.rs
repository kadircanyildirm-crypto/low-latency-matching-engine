//! The exchange across crashes: paper money and every open order's account and client
//! reference come back from the newest checkpoint and the journal after it, exactly as they
//! were; a damaged checkpoint falls back to the one before, and one that disagrees with the
//! book is refused.

use std::path::Path;

use engine::sim::{CrashModel, SimStorage};
use engine::{Engine, EngineConfig};
use gateway::recovery::{self, RecoveryError};
use gateway::{Account, Checkpoint, Exchange, Funds, Mailbox, SessionId, Timing};
use orderbook::workload::SplitMix64;
use orderbook::{BookConfig, Side, TimeInForce};
use protocol::{Inbound, NewOrder, OrderKind, Outbound, VERSION};

const DIR: &str = "data";

struct Nobody;

impl Mailbox for Nobody {
    fn send(&mut self, _: SessionId, _: Outbound) {}
    fn close(&mut self, _: SessionId) {}
}

fn config() -> EngineConfig {
    let book = BookConfig {
        max_owners: 8,
        ..BookConfig::new(1, 200, 512)
    };
    EngineConfig {
        segment_capacity: 32,
        snapshot_every: Some(100),
        ..EngineConfig::new(book)
    }
}

fn accounts() -> Vec<Account> {
    let paper = |id| Account {
        id,
        token: u64::from(id),
        max_open_orders: 30,
        messages_per_second: 1_000_000,
        funds: Some(Funds {
            cash: 50_000,
            position: 300,
        }),
    };
    let mut accounts: Vec<Account> = (1..=3).map(paper).collect();
    accounts.push(Account {
        funds: None,
        ..paper(4)
    });
    accounts
}

fn open(storage: &SimStorage) -> (Engine<SimStorage>, Exchange, recovery::Recovered) {
    recovery::open(
        storage.clone(),
        Path::new(DIR),
        config(),
        &accounts(),
        Timing::default(),
    )
    .unwrap()
}

/// Trades for `steps`, saving a checkpoint now and then; returns the exchange's exact state
/// at the end, and the sequence number of the last checkpoint saved.
fn trade(
    storage: &SimStorage,
    engine: &mut Engine<SimStorage>,
    exchange: &mut Exchange,
    seed: u64,
    steps: u64,
) -> (Checkpoint, Option<u64>) {
    let mut rng = SplitMix64::new(seed);
    for session in 0..4 {
        let account = session as u32 + 1;
        exchange.connect(session, 0);
        let login = Inbound::Login {
            version: VERSION,
            account,
            token: u64::from(account),
        };
        exchange.receive(session, login, 0, &mut Nobody);
    }
    let mut saved = None;
    for step in 0..steps {
        let session = rng.below(4) as usize;
        let message = if rng.below(4) == 0 {
            Inbound::Cancel {
                order_id: 1 + rng.below(engine.last_seq() + 1),
            }
        } else {
            Inbound::NewOrder(NewOrder {
                client_ref: 1_000 + step,
                side: if rng.below(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
                qty: 1 + rng.below(20),
                kind: OrderKind::Limit {
                    price: 90 + rng.below(21) as i64,
                    tif: [TimeInForce::Gtc, TimeInForce::Ioc][rng.below(2) as usize],
                    display: None,
                },
            })
        };
        exchange.receive(session, message, 1, &mut Nobody);
        if rng.below(3) == 0 {
            exchange.flush(engine, &mut Nobody).unwrap();
            if rng.below(8) == 0 {
                let checkpoint = exchange.checkpoint().expect("nothing is in flight");
                recovery::save(&mut storage.clone(), Path::new(DIR), &checkpoint).unwrap();
                saved = Some(checkpoint.seq);
            }
        }
    }
    exchange.flush(engine, &mut Nobody).unwrap();
    (exchange.checkpoint().unwrap(), saved)
}

#[test]
fn the_exchange_comes_back_from_a_crash() {
    let mut recovered_from_checkpoints = 0;
    for seed in 0..40 {
        let storage = SimStorage::new();
        let (mut engine, mut exchange, recovered) = open(&storage);
        assert_eq!(recovered.checkpoint, None);
        let (state, saved) = trade(&storage, &mut engine, &mut exchange, seed, 400);
        let mut rng = SplitMix64::new(seed);
        let model = if seed % 2 == 0 {
            CrashModel::InOrder
        } else {
            CrashModel::AnyOrder
        };
        let crashed = storage.crash(&mut rng, model);
        drop((engine, exchange));
        let (engine, exchange, recovered) = open(&crashed);
        assert_eq!(recovered.checkpoint, saved, "seed {seed}");
        // Every command was synced before its events were delivered.
        assert_eq!(engine.last_seq(), state.seq, "seed {seed}");
        let Some(from) = saved else {
            continue;
        };
        recovered_from_checkpoints += 1;
        assert_eq!(recovered.replayed, state.seq - from);
        let mut expected = state.clone();
        // The orders placed after the checkpoint have lost their client references.
        for order in &mut expected.orders {
            if order.id > from {
                order.client_ref = 0;
            }
        }
        assert_eq!(exchange.checkpoint().unwrap(), expected, "seed {seed}");
        for account in 1..=3 {
            assert!(exchange.wallet(account).is_some());
        }
    }
    assert!(recovered_from_checkpoints > 30);
}

#[test]
fn a_damaged_checkpoint_falls_back_to_the_one_before() {
    let storage = SimStorage::new();
    let (mut engine, mut exchange, _) = open(&storage);
    let (_, _) = trade(&storage, &mut engine, &mut exchange, 7, 50);
    let first = exchange.checkpoint().unwrap();
    recovery::save(&mut storage.clone(), Path::new(DIR), &first).unwrap();
    for session in 0..4 {
        exchange.detach(session);
    }
    let (state, _) = trade(&storage, &mut engine, &mut exchange, 8, 50);
    recovery::save(&mut storage.clone(), Path::new(DIR), &state).unwrap();
    drop((engine, exchange));
    // Two checkpoints are kept: the newest, and the one before.
    let mut kept: Vec<u64> = storage
        .files()
        .iter()
        .filter_map(|(path, _)| {
            let name = path.file_name()?.to_str()?;
            name.strip_prefix("gateway-")?
                .strip_suffix(".json")?
                .parse()
                .ok()
        })
        .collect();
    kept.sort_unstable();
    assert_eq!(kept.len(), 2);
    assert_eq!(kept[1], state.seq);
    assert!(kept[0] >= first.seq);
    let newest = Path::new(DIR).join(format!("gateway-{:020}.json", state.seq));
    storage.flip_bit(&newest, 8 * 3);
    let (engine, exchange, recovered) = open(&storage);
    assert_eq!(recovered.checkpoint, Some(kept[0]));
    assert_eq!(engine.last_seq(), state.seq);
    // Wallets come back exactly, whichever checkpoint they start from.
    assert_eq!(exchange.checkpoint().unwrap().wallets, state.wallets);
}

#[test]
fn a_checkpoint_that_disagrees_with_the_book_is_refused() {
    let storage = SimStorage::new();
    let (mut engine, mut exchange, _) = open(&storage);
    let (mut state, _) = trade(&storage, &mut engine, &mut exchange, 9, 80);
    drop((engine, exchange));
    // An order the book never had.
    state.orders.push(gateway::exchange::SavedOrder {
        id: 1,
        account: 1,
        client_ref: 0,
        buy: true,
        price: Some(1),
        leaves: 999,
    });
    state.orders.sort_by_key(|o| o.id);
    state.orders.dedup_by_key(|o| o.id);
    if let Some(order) = state.orders.iter_mut().find(|o| o.id == 1) {
        order.leaves = 999;
    }
    recovery::save(&mut storage.clone(), Path::new(DIR), &state).unwrap();
    match recovery::open(
        storage,
        Path::new(DIR),
        config(),
        &accounts(),
        Timing::default(),
    ) {
        Err(RecoveryError::Inconsistent(detail)) => assert!(!detail.is_empty()),
        Err(other) => panic!("{other}"),
        Ok(_) => panic!("a checkpoint the book contradicts was used"),
    }
}
