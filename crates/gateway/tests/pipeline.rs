//! The pipeline against the engine on one thread: the same sessions sending the same
//! messages get exactly the same replies, while the writer journals on a thread of its own,
//! the matcher applies and takes snapshots on another, and the writer removes the segments
//! they free. A failing disk stops the pipeline with nothing reported that was not
//! journaled, and a power failure while it runs loses nothing it reported.

use std::path::Path;

use engine::sim::{CrashModel, SimStorage};
use engine::{Discard, Engine, EngineConfig, SyncPolicy};
use gateway::{Account, Core, Exchange, Mailbox, Pipeline, PipelineConfig, SessionId, Timing};
use orderbook::workload::SplitMix64;
use orderbook::{BookConfig, Side, TimeInForce};
use protocol::{Inbound, NewOrder, OrderKind, Outbound, VERSION};
use ring::Wait;

const DIR: &str = "data";
const SESSIONS: usize = 4;

/// Everything the exchange did, in order.
#[derive(Debug, Default, PartialEq)]
struct Mail(Vec<(SessionId, Option<Outbound>)>);

impl Mailbox for Mail {
    fn send(&mut self, session: SessionId, message: Outbound) {
        self.0.push((session, Some(message)));
    }

    fn close(&mut self, session: SessionId) {
        self.0.push((session, None));
    }
}

fn book() -> BookConfig {
    BookConfig {
        max_owners: 8,
        price_band: Some(15),
        reference_price: Some(100),
        auction_on_band: true,
        ..BookConfig::new(1, 200, 256)
    }
}

fn accounts() -> Vec<Account> {
    (1..=SESSIONS as u32)
        .map(|id| Account {
            id,
            token: u64::from(id),
            max_open_orders: 40,
            messages_per_second: 1_000_000,
            funds: None,
        })
        .collect()
}

/// What one step of a scenario does.
enum Step {
    Send(SessionId, Inbound),
    /// A connection drops and comes back, logged in again.
    Reconnect(SessionId),
    /// The batch so far is handed over, and its replies awaited.
    Flush,
}

/// A random scenario: orders of every kind, cancels and modifies of recent ids, mass
/// cancels, reconnects, subscriptions to market data.
fn scenario(seed: u64, len: usize) -> Vec<Step> {
    let mut rng = SplitMix64::new(seed);
    let mut next_ref = 0;
    (0..len)
        .map(|step| {
            let session = rng.below(SESSIONS as u64) as usize;
            let price = 85 + rng.below(31) as i64;
            let side = if rng.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let qty = 1 + rng.below(9);
            next_ref += 1;
            let order = |kind| {
                Inbound::NewOrder(NewOrder {
                    client_ref: next_ref,
                    side,
                    qty,
                    kind,
                })
            };
            let recent = (step as u64).saturating_sub(rng.below(30)).max(1);
            match rng.below(40) {
                0..=13 => Step::Send(
                    session,
                    order(OrderKind::Limit {
                        price,
                        tif: TimeInForce::Gtc,
                        display: (rng.below(6) == 0).then_some(1),
                    }),
                ),
                14..=16 => Step::Send(
                    session,
                    order(OrderKind::Limit {
                        price,
                        tif: TimeInForce::Ioc,
                        display: None,
                    }),
                ),
                17..=18 => Step::Send(session, order(OrderKind::Market)),
                19 => Step::Send(
                    session,
                    order(OrderKind::Stop {
                        trigger: price,
                        limit: (rng.below(2) == 0).then_some(price),
                    }),
                ),
                20..=26 => Step::Send(session, Inbound::Cancel { order_id: recent }),
                27..=29 => Step::Send(
                    session,
                    Inbound::Modify {
                        order_id: recent,
                        price,
                        qty,
                    },
                ),
                30 => Step::Send(session, Inbound::MassCancel),
                31 => Step::Reconnect(session),
                32 => Step::Send(session, Inbound::Subscribe),
                _ => Step::Flush,
            }
        })
        .collect()
}

fn login(exchange: &mut Exchange, session: SessionId, mail: &mut Mail) {
    exchange.connect(session, 0);
    let account = session as u32 + 1;
    let token = u64::from(account);
    let login = Inbound::Login {
        version: VERSION,
        account,
        token,
    };
    exchange.receive(session, login, 0, mail);
}

/// Plays `steps` against `exchange`, with `flush` handing each batch to the engine and
/// waiting for its replies. Returns how many messages had been sent at each flush: a reply
/// that came a round late would move it.
fn play(
    steps: &[Step],
    exchange: &mut Exchange,
    mail: &mut Mail,
    mut flush: impl FnMut(&mut Exchange, &mut Mail),
) -> Vec<usize> {
    let mut rounds = Vec::new();
    for session in 0..SESSIONS {
        login(exchange, session, mail);
    }
    for step in steps {
        match step {
            Step::Send(session, message) => exchange.receive(*session, *message, 0, mail),
            Step::Reconnect(session) => {
                exchange.disconnect(*session);
                login(exchange, *session, mail);
            }
            Step::Flush => {
                flush(exchange, mail);
                rounds.push(mail.0.len());
            }
        }
    }
    flush(exchange, mail);
    rounds.push(mail.0.len());
    rounds
}

/// Turns the pipeline until every command handed over has been applied and its events
/// delivered.
fn settle(pipeline: &mut Pipeline<SimStorage>, exchange: &mut Exchange, mail: &mut Mail) {
    loop {
        pipeline.turn(exchange, mail).unwrap();
        if !pipeline.busy() && !exchange.has_batch() {
            return;
        }
        std::thread::yield_now();
    }
}

fn open(storage: &SimStorage, config: EngineConfig) -> Engine<SimStorage> {
    Engine::open_with(storage.clone(), Path::new(DIR), config, &mut Discard)
        .unwrap()
        .0
}

#[test]
fn the_pipeline_replies_as_the_engine_does() {
    for seed in 0..24 {
        let config = EngineConfig {
            sync: if seed % 2 == 0 {
                SyncPolicy::Always
            } else {
                SyncPolicy::Os
            },
            // Small segments roll, and sync, often; with large ones, a snapshot under
            // SyncPolicy::Os has to ask the writer for its sync.
            segment_capacity: if seed % 4 < 2 { 16 } else { 4_096 },
            snapshot_every: Some(40),
            keep_snapshots: 2,
            ..EngineConfig::new(book())
        };
        let steps = scenario(seed, 600);

        // The engine on this thread.
        let mut engine = open(&SimStorage::new(), config);
        let mut exchange = Exchange::new(
            engine.book(),
            engine.last_seq(),
            &accounts(),
            Timing::default(),
        )
        .unwrap();
        let mut direct = Mail::default();
        let direct_rounds = play(&steps, &mut exchange, &mut direct, |exchange, mail| {
            exchange.flush(&mut engine, mail).unwrap();
            exchange.publish(mail);
        });
        assert_eq!(exchange.last_seq(), engine.last_seq());

        // The pipeline.
        let storage = SimStorage::new();
        let engine_b = open(&storage, config);
        let mut exchange = Exchange::new(
            engine_b.book(),
            engine_b.last_seq(),
            &accounts(),
            Timing::default(),
        )
        .unwrap();
        let pipeline_config = PipelineConfig {
            capacity: 1 + seed as usize % 64,
            wait: if seed % 3 == 0 {
                Wait::Spin
            } else {
                Wait::Backoff
            },
            ..PipelineConfig::default()
        };
        let mut pipeline = Pipeline::start(engine_b, pipeline_config).unwrap();
        let mut piped = Mail::default();
        let piped_rounds = play(&steps, &mut exchange, &mut piped, |exchange, mail| {
            settle(&mut pipeline, exchange, mail);
            exchange.publish(mail);
        });
        if let Some(at) =
            (0..piped.0.len().max(direct.0.len())).find(|&i| piped.0.get(i) != direct.0.get(i))
        {
            panic!(
                "seed {seed}: message {at} differs: piped {:?}, direct {:?}",
                &piped.0[at.saturating_sub(3)..(at + 3).min(piped.0.len())],
                &direct.0[at.saturating_sub(3)..(at + 3).min(direct.0.len())]
            );
        }
        assert_eq!(piped_rounds, direct_rounds, "seed {seed}");
        assert_eq!(pipeline.applied(), engine.last_seq());
        if config.sync == SyncPolicy::Always {
            assert_eq!(pipeline.durable(), engine.last_seq());
        }
        // The depth kept for market data is the book's.
        let kept: Vec<_> = exchange.depth().levels(Side::Buy).collect();
        let book: Vec<_> = engine.book().depth(Side::Buy).map(|l| l.price).collect();
        assert_eq!(
            kept.iter().map(|&(price, _)| price).collect::<Vec<_>>(),
            book
        );

        // What the pipeline journaled recovers to the engine's book, and its snapshots
        // and retention left the files a whole engine would have.
        let (writer, _) = pipeline.stop().unwrap();
        writer.close().unwrap();
        let reopened = open(&storage, config);
        assert_eq!(reopened.last_seq(), engine.last_seq(), "seed {seed}");
        assert_eq!(
            reopened.book().digest(),
            engine.book().digest(),
            "seed {seed}"
        );
        assert!(reopened.last_snapshot() > 0, "seed {seed}");
        let segments = storage
            .files()
            .iter()
            .filter(|(name, _)| name.extension().is_some_and(|e| e == "log"))
            .count();
        assert!(
            segments < engine.last_seq() as usize / 16,
            "seed {seed}: {segments} segments"
        );
    }
}

/// A disk that fails stops the pipeline: the server is told, and every command whose
/// events it delivered had been journaled, so recovery has it.
#[test]
fn a_failing_disk_stops_the_pipeline() {
    for seed in 0..8 {
        let config = EngineConfig {
            segment_capacity: 16,
            ..EngineConfig::new(book())
        };
        let storage = SimStorage::new();
        let engine = open(&storage, config);
        let mut exchange = Exchange::new(
            engine.book(),
            engine.last_seq(),
            &accounts(),
            Timing::default(),
        )
        .unwrap();
        let mut pipeline = Pipeline::start(engine, PipelineConfig::default()).unwrap();
        let mut mail = Mail::default();
        let steps = scenario(100 + seed, 400);
        let fail_at = 50 + seed as usize * 40;
        let mut failure = None;
        for session in 0..SESSIONS {
            login(&mut exchange, session, &mut mail);
        }
        for (index, step) in steps.iter().enumerate() {
            if index == fail_at {
                storage.set_failing(true);
            }
            match step {
                Step::Send(session, message) => exchange.receive(*session, *message, 0, &mut mail),
                Step::Reconnect(_) => {}
                Step::Flush => {
                    if let Err(error) = pipeline.turn(&mut exchange, &mut mail) {
                        failure = Some(error);
                        break;
                    }
                }
            }
        }
        // The failure reaches the network thread.
        while failure.is_none() {
            match pipeline.turn(&mut exchange, &mut mail) {
                Ok(()) => std::thread::yield_now(),
                Err(error) => failure = Some(error),
            }
        }
        assert!(
            matches!(failure, Some(engine::Error::Io(_))),
            "seed {seed}: {failure:?}"
        );
        assert!(matches!(
            pipeline.turn(&mut exchange, &mut mail),
            Err(engine::Error::Poisoned)
        ));
        assert!(pipeline.stop().is_err());
        let reported = mail
            .0
            .iter()
            .filter_map(|(_, message)| match message {
                Some(Outbound::Report(report)) => Some(report.seq),
                _ => None,
            })
            .max()
            .unwrap_or(0);
        storage.set_failing(false);
        let crashed = storage.crash(&mut SplitMix64::new(seed), CrashModel::AnyOrder);
        let recovered = open(&crashed, config);
        assert!(
            recovered.last_seq() >= reported,
            "seed {seed}: {} < {reported}",
            recovered.last_seq()
        );
    }
}

/// The power fails while the pipeline runs: under `SyncPolicy::Always`, every command a
/// client heard about survives.
#[test]
fn a_power_failure_loses_nothing_reported() {
    for seed in 0..12 {
        let config = EngineConfig {
            segment_capacity: 1 + seed as u32 * 3,
            snapshot_every: Some(25),
            ..EngineConfig::new(book())
        };
        let storage = SimStorage::new();
        let engine = open(&storage, config);
        let mut exchange = Exchange::new(
            engine.book(),
            engine.last_seq(),
            &accounts(),
            Timing::default(),
        )
        .unwrap();
        let mut pipeline = Pipeline::start(engine, PipelineConfig::default()).unwrap();
        let mut mail = Mail::default();
        let steps = scenario(200 + seed, 500);
        for session in 0..SESSIONS {
            login(&mut exchange, session, &mut mail);
        }
        let mut rng = SplitMix64::new(seed);
        let crash_at = rng.below(steps.len() as u64) as usize;
        let mut crashed = None;
        let mut reported = 0;
        for (index, step) in steps.iter().enumerate() {
            match step {
                Step::Send(session, message) => exchange.receive(*session, *message, 0, &mut mail),
                Step::Reconnect(_) => {}
                Step::Flush => pipeline.turn(&mut exchange, &mut mail).unwrap(),
            }
            if index == crash_at {
                // Whatever the threads are doing at this instant.
                reported = mail
                    .0
                    .iter()
                    .filter_map(|(_, message)| match message {
                        Some(Outbound::Report(report)) => Some(report.seq),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(0);
                crashed = Some(storage.crash(&mut rng, CrashModel::AnyOrder));
            }
        }
        settle(&mut pipeline, &mut exchange, &mut mail);
        drop(pipeline.stop().unwrap());
        let recovered = open(&crashed.unwrap(), config);
        assert!(
            recovered.last_seq() >= reported,
            "seed {seed}: {} < {reported}",
            recovered.last_seq()
        );
    }
}

/// Stopping a pipeline whose rings are full of events nobody read still ends it: stop
/// drains the events while the matcher finishes.
#[test]
fn a_pipeline_stops_with_events_unread() {
    let config = EngineConfig::new(book());
    let engine = open(&SimStorage::new(), config);
    let mut exchange = Exchange::new(
        engine.book(),
        engine.last_seq(),
        &accounts(),
        Timing::default(),
    )
    .unwrap();
    let pipeline_config = PipelineConfig {
        capacity: 1,
        ..PipelineConfig::default()
    };
    let mut pipeline = Pipeline::start(engine, pipeline_config).unwrap();
    let mut mail = Mail::default();
    login(&mut exchange, 0, &mut mail);
    for client_ref in 0..50 {
        let order = Inbound::NewOrder(NewOrder {
            client_ref,
            side: Side::Buy,
            qty: 1,
            kind: OrderKind::Limit {
                price: 90 + client_ref as i64 % 10,
                tif: TimeInForce::Gtc,
                display: None,
            },
        });
        exchange.receive(0, order, 0, &mut mail);
    }
    // One turn hands a ring's worth over and finds no events yet; the matcher then fills
    // its ring with the first order's events and waits for room.
    pipeline.turn(&mut exchange, &mut mail).unwrap();
    let (writer, matcher) = pipeline.stop().unwrap();
    assert!(matcher.last_seq() >= 1);
    drop(writer);
}
