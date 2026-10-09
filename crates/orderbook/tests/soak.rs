//! Long runs of realistic synthetic flow: differential against the reference book, and
//! determinism checks.

mod common;

use common::reference::ReferenceBook;
use common::{Fnv, snapshot};
use orderbook::workload::{EventCounts, Mix, TifMix, Workload, WorkloadConfig};
use orderbook::{BookConfig, Command, Event, EventSink, OrderBook, Phase, SelfTradePolicy};

fn config() -> WorkloadConfig {
    WorkloadConfig {
        min_price: 0,
        max_price: 2_000,
        initial_mid: 1_000,
        max_live: 2_000,
        ..WorkloadConfig::default()
    }
}

/// What a run did in its call phases.
#[derive(Debug, Default)]
struct Sessions {
    /// Uncrosses that traded, and their trades.
    uncrosses: u64,
    uncross_trades: u64,
    /// Calls the price band started.
    interruptions: u64,
}

impl Sessions {
    fn tally(&mut self, command: Command, events: &[Event]) {
        if let Command::SetPhase { .. } = command {
            // An uncross's trades come before the phase change.
            let trades = events
                .iter()
                .take_while(|event| !matches!(event, Event::PhaseChanged { .. }))
                .filter(|event| matches!(event, Event::Trade { .. }))
                .count() as u64;
            self.uncrosses += u64::from(trades > 0);
            self.uncross_trades += trades;
        } else {
            let call = Event::PhaseChanged {
                phase: Phase::Auction,
            };
            self.interruptions += u64::from(events.contains(&call));
        }
    }
}

fn soak(cfg: WorkloadConfig, book_cfg: BookConfig, steps: usize) -> (EventCounts, Sessions) {
    let mut engine = OrderBook::new(book_cfg);
    let mut reference = ReferenceBook::new(book_cfg);
    let mut workload = Workload::new(cfg);
    let mut counts = EventCounts::default();
    let mut sessions = Sessions::default();
    let (mut got, mut want) = (Vec::new(), Vec::new());

    for step in 0..steps {
        let command = workload.next_command();
        got.clear();
        want.clear();
        engine.process(command, &mut got);
        reference.process(command, &mut want);
        assert_eq!(got, want, "events differ at step {step}: {command:?}");
        sessions.tally(command, &got);
        for event in &got {
            counts.on_event(*event);
            workload.observe(event);
        }
        if step % 5_000 == 0 {
            engine.validate().unwrap();
            assert_eq!(snapshot(&engine), reference.snapshot(), "step {step}");
            // The participants rebuilt the set of resting orders from events alone.
            assert_eq!(workload.live_orders(), engine.order_count(), "step {step}");
        }
    }
    engine.validate().unwrap();
    assert_eq!(snapshot(&engine), reference.snapshot());
    assert_eq!(workload.live_orders(), engine.order_count());
    (counts, sessions)
}

#[test]
fn synthetic_flow_matches_reference() {
    let cfg = config();
    let (counts, _) = soak(cfg, cfg.book_config(), 200_000);

    // Participants only cancel and modify orders they know are resting and stay under the
    // order limit, so nothing is rejected...
    assert_eq!(counts.rejected, 0, "{counts:?}");
    // ...and the run exercised every path, self-trade prevention included. Its protection
    // band is too wide to stop anything; `protected_config` covers protection.
    assert!(counts.trades > 10_000, "{counts:?}");
    assert!(counts.rested > 10_000, "{counts:?}");
    assert!(counts.cancelled > 10_000, "{counts:?}");
    assert!(counts.modified > 1_000, "{counts:?}");
    assert!(counts.self_trade_cancels > 100, "{counts:?}");
}

/// Few owners for frequent self-trades under `CancelIncoming`, many market orders against
/// a one-tick protection band for frequent protection stops and rejections, mass cancels,
/// every time in force, icebergs, and stop orders.
fn protected_config() -> (WorkloadConfig, BookConfig) {
    let cfg = WorkloadConfig {
        owners: 4,
        seed: 7,
        mix: Mix {
            passive_limit: 50,
            aggressive_limit: 5,
            market: 20,
            cancel: 15,
            mass_cancel: 1,
            stop: 4,
            modify: 5,
            session: 0,
        },
        tif: TifMix {
            ioc: 30,
            fok: 20,
            post_only: 20,
        },
        iceberg: 20,
        ..config()
    };
    let book_cfg = BookConfig {
        self_trade: SelfTradePolicy::CancelIncoming,
        price_protection: Some(1),
        ..cfg.book_config()
    };
    (cfg, book_cfg)
}

#[test]
fn synthetic_flow_matches_reference_under_cancel_incoming() {
    let (cfg, book_cfg) = protected_config();
    let (counts, _) = soak(cfg, book_cfg, 100_000);
    assert!(counts.self_trade_cancels > 100, "{counts:?}");
    assert!(counts.protection_cancels > 100, "{counts:?}");
    // Aggressive limits priced more than a tick through the opposite best.
    assert!(counts.rejected > 100, "{counts:?}");
    assert!(counts.mass_cancels > 100, "{counts:?}");
    assert!(counts.mass_cancelled_orders > 1_000, "{counts:?}");
    assert!(
        counts.ioc_cancels > 100 && counts.fok_kills > 100,
        "{counts:?}"
    );
    assert!(counts.replenishes > 100, "{counts:?}");
    assert!(counts.stops_triggered > 100, "{counts:?}");
}

/// A tight price band around the last trade and no price protection, so the band does all
/// the stopping and rejecting.
fn banded_config() -> (WorkloadConfig, BookConfig) {
    let cfg = WorkloadConfig {
        seed: 11,
        mix: Mix {
            passive_limit: 50,
            aggressive_limit: 10,
            market: 15,
            cancel: 20,
            mass_cancel: 0,
            stop: 0,
            modify: 5,
            session: 0,
        },
        ..config()
    };
    let book_cfg = BookConfig {
        price_protection: None,
        price_band: Some(10),
        reference_price: Some(cfg.initial_mid),
        ..cfg.book_config()
    };
    (cfg, book_cfg)
}

#[test]
fn synthetic_flow_matches_reference_under_a_price_band() {
    let (cfg, book_cfg) = banded_config();
    let (counts, _) = soak(cfg, book_cfg, 100_000);
    assert!(counts.band_cancels > 100, "{counts:?}");
    assert!(counts.rejected > 100, "{counts:?}");
    assert!(counts.trades > 1_000, "{counts:?}");
}

/// Trading sessions: the exchange's schedule moves the book into and out of calls, halts
/// and the close, about one command in fifty, and a 10-tick band interrupts trading with a
/// call whenever it stops a market order. Participants do not adapt, so every phase sees
/// orders it refuses; stops, icebergs and every time in force trade through it all.
fn session_config() -> (WorkloadConfig, BookConfig) {
    let cfg = WorkloadConfig {
        owners: 8,
        seed: 13,
        mix: Mix {
            passive_limit: 45,
            aggressive_limit: 10,
            market: 15,
            cancel: 18,
            mass_cancel: 1,
            stop: 4,
            modify: 5,
            session: 2,
        },
        tif: TifMix {
            ioc: 20,
            fok: 10,
            post_only: 10,
        },
        iceberg: 20,
        ..config()
    };
    let book_cfg = BookConfig {
        price_protection: None,
        price_band: Some(10),
        reference_price: Some(cfg.initial_mid),
        auction_on_band: true,
        ..cfg.book_config()
    };
    (cfg, book_cfg)
}

#[test]
fn synthetic_flow_matches_reference_through_trading_sessions() {
    let (cfg, book_cfg) = session_config();
    let (counts, sessions) = soak(cfg, book_cfg, 100_000);
    // About 1,000 calls, 400 of them started by the band, end in 700 uncrosses that trade
    // 3,700 times; halts and the close refuse 29,000 orders.
    assert!(counts.calls > 500, "{counts:?}");
    assert!(
        sessions.uncrosses > 400 && sessions.uncross_trades > 2_000,
        "{sessions:?}"
    );
    assert!(sessions.interruptions > 200, "{sessions:?}");
    assert!(counts.rejected_phase > 10_000, "{counts:?}");
    assert!(counts.phase_cancels > 50, "{counts:?}");
    assert!(counts.stops_triggered > 500, "{counts:?}");
    assert!(counts.replenishes > 1_000, "{counts:?}");
}

/// Fingerprint of every event and the final book after 100k commands of synthetic flow,
/// and the final book's [`OrderBook::digest`].
fn fingerprint(cfg: WorkloadConfig, book_cfg: BookConfig) -> (u64, u64) {
    let mut book = OrderBook::new(book_cfg);
    let mut workload = Workload::new(cfg);
    let mut events = Vec::new();
    let mut hash = Fnv::new();
    for _ in 0..100_000 {
        events.clear();
        book.process(workload.next_command(), &mut events);
        for event in &events {
            workload.observe(event);
            hash.event(event);
        }
    }
    for (price, queue) in snapshot(&book).iter().flatten() {
        hash.write_u64(*price as u64);
        for order in queue {
            [order.id, u64::from(order.owner), order.leaves, order.filled]
                .iter()
                .for_each(|&v| hash.write_u64(v));
        }
    }
    (hash.finish(), book.digest())
}

#[test]
fn same_commands_produce_the_same_events() {
    let cfg = config();
    assert_eq!(
        fingerprint(cfg, cfg.book_config()),
        fingerprint(cfg, cfg.book_config())
    );
}

/// Pinned values: CI runs this on Linux, Windows and macOS, so a match proves the engine and
/// its state digest are deterministic across platforms, which replay on a standby machine
/// depends on. Between them the four runs cover both self-trade policies, protection and
/// band stops, rejections, and trading phases with their uncrosses. They also flag any
/// change in behaviour; if a change is intended, update the constants.
#[test]
fn output_matches_the_golden_fingerprints() {
    let cfg = config();
    let (events, digest) = fingerprint(cfg, cfg.book_config());
    assert_eq!(events, GOLDEN_DEFAULT, "default flow: got {events:#018x}");
    assert_eq!(
        digest, GOLDEN_DEFAULT_DIGEST,
        "default flow digest: got {digest:#018x}"
    );

    let (cfg, book_cfg) = protected_config();
    let (events, digest) = fingerprint(cfg, book_cfg);
    assert_eq!(
        events, GOLDEN_PROTECTED,
        "protected flow: got {events:#018x}"
    );
    assert_eq!(
        digest, GOLDEN_PROTECTED_DIGEST,
        "protected flow digest: got {digest:#018x}"
    );
    let (cfg, book_cfg) = banded_config();
    let (events, digest) = fingerprint(cfg, book_cfg);
    assert_eq!(events, GOLDEN_BANDED, "banded flow: got {events:#018x}");
    assert_eq!(
        digest, GOLDEN_BANDED_DIGEST,
        "banded flow digest: got {digest:#018x}"
    );
    let (cfg, book_cfg) = session_config();
    let (events, digest) = fingerprint(cfg, book_cfg);
    assert_eq!(events, GOLDEN_SESSIONS, "session flow: got {events:#018x}");
    assert_eq!(
        digest, GOLDEN_SESSIONS_DIGEST,
        "session flow digest: got {digest:#018x}"
    );
}

const GOLDEN_DEFAULT: u64 = 0xa83b_9f96_80cf_a652;
const GOLDEN_DEFAULT_DIGEST: u64 = 0x2994_b8d1_188e_075b;
const GOLDEN_PROTECTED: u64 = 0x1c6e_12e1_87ef_3f44;
const GOLDEN_PROTECTED_DIGEST: u64 = 0xf167_5321_c238_1e08;
const GOLDEN_BANDED: u64 = 0xdbe4_19eb_3a15_01ed;
const GOLDEN_BANDED_DIGEST: u64 = 0x7c1f_5993_3e23_a1f2;
const GOLDEN_SESSIONS: u64 = 0x6d48_751a_7a35_71c9;
const GOLDEN_SESSIONS_DIGEST: u64 = 0x9a3a_9c2e_d3e9_235c;
