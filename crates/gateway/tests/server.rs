//! The gateway over real sockets and a real journal: order flow, protocol errors, slow
//! consumers, session limits, cancel-on-disconnect, and shutdown.

use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use engine::{Discard, Engine, EngineConfig};
use gateway::client::Client;
use gateway::load::{self, LoadConfig};
use gateway::{Account, Core, Exchange, Pipeline, PipelineConfig, Server, ServerConfig, Timing};
use orderbook::{BookConfig, CancelReason, Side, TimeInForce};
use protocol::{
    Inbound, LevelUpdate, LogoutReason, NewOrder, OrderKind, Outbound, Report, ReportKind,
    TradeTick,
};

/// A directory removed when the test ends.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path = std::env::temp_dir().join(format!("gateway-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn account(id: u32) -> Account {
    Account {
        id,
        token: 100 + u64::from(id),
        max_open_orders: 1_000,
        messages_per_second: 100_000,
        funds: None,
    }
}

/// Serves until `stop` is set, and gives the core back.
fn serve<C: Core>(
    exchange: Exchange,
    core: C,
    addr: SocketAddr,
    config: ServerConfig,
    started: &mpsc::Sender<SocketAddr>,
    stop: &AtomicBool,
) -> Result<C, String> {
    let mut server = Server::bind(exchange, core, addr, config).map_err(|e| e.to_string())?;
    started.send(server.local_addr().unwrap()).unwrap();
    server.run(stop).map_err(|e| e.to_string())?;
    Ok(server.into_parts().1)
}

/// A running gateway.
struct Gateway {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    /// Returns the number of orders left on the book.
    thread: Option<JoinHandle<Result<usize, String>>>,
}

impl Gateway {
    /// A gateway with the engine on its own thread.
    fn start(dir: &TempDir, config: ServerConfig) -> Gateway {
        Gateway::start_with(dir, config, false)
    }

    /// A gateway with the engine on threads of its own, in a pipeline.
    fn pipelined(dir: &TempDir, config: ServerConfig) -> Gateway {
        Gateway::start_with(dir, config, true)
    }

    fn start_with(dir: &TempDir, config: ServerConfig, pipelined: bool) -> Gateway {
        let path = dir.0.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let (tx, rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            let config_book = BookConfig {
                max_owners: 8,
                ..BookConfig::new(1, 1_000, 1_024)
            };
            let engine = Engine::open(&path, EngineConfig::new(config_book), &mut Discard)
                .map_err(|e| e.to_string())?
                .0;
            let accounts = [account(1), account(2), account(3)];
            let exchange = Exchange::new(
                engine.book(),
                engine.last_seq(),
                &accounts,
                Timing::default(),
            )
            .map_err(|e| e.to_string())?;
            let addr = "127.0.0.1:0".parse().unwrap();
            if pipelined {
                let pipeline = Pipeline::start(engine, PipelineConfig::default())
                    .map_err(|e| e.to_string())?;
                let pipeline = serve(exchange, pipeline, addr, config, &tx, &stopping)?;
                let (writer, matcher) = pipeline.stop().map_err(|e| e.to_string())?;
                writer.close().map_err(|e| e.to_string())?;
                Ok(matcher.book().order_count())
            } else {
                let engine = serve(exchange, engine, addr, config, &tx, &stopping)?;
                let left = engine.book().order_count();
                engine.close().map_err(|e| e.to_string())?;
                Ok(left)
            }
        });
        let addr = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the gateway starts");
        Gateway {
            addr,
            stop,
            thread: Some(thread),
        }
    }

    fn login(&self, account: u32) -> Client {
        let (mut client, _) = Client::login(self.addr, account, 100 + u64::from(account)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        client
    }

    /// Stops the gateway, and returns the number of orders left on the book.
    fn stop(mut self) -> usize {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap().unwrap()
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn limit(client_ref: u64, side: Side, price: i64, qty: u64) -> Inbound {
    Inbound::NewOrder(NewOrder {
        client_ref,
        side,
        qty,
        kind: OrderKind::Limit {
            price,
            tif: TimeInForce::Gtc,
            display: None,
        },
    })
}

fn report(client: &mut Client) -> Report {
    match client.receive().unwrap() {
        Outbound::Report(report) => report,
        other => panic!("not a report: {other:?}"),
    }
}

/// Reads until the gateway closes the connection, and returns what came before.
fn until_closed(client: &mut Client) -> Vec<Outbound> {
    let mut messages = Vec::new();
    loop {
        match client.receive() {
            Ok(message) => messages.push(message),
            Err(error) => {
                assert!(
                    matches!(
                        error.kind(),
                        io::ErrorKind::UnexpectedEof
                            | io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                    ),
                    "{error}"
                );
                return messages;
            }
        }
    }
}

#[test]
fn orders_trade_over_tcp() {
    let dir = TempDir::new("flow");
    let gateway = Gateway::start(&dir, ServerConfig::default());
    let mut seller = gateway.login(1);
    let mut buyer = gateway.login(2);
    seller.send(&limit(7, Side::Sell, 100, 10)).unwrap();
    assert_eq!(report(&mut seller).kind, ReportKind::Accepted);
    assert!(matches!(
        report(&mut seller).kind,
        ReportKind::Rested { .. }
    ));
    buyer.send(&limit(9, Side::Buy, 100, 4)).unwrap();
    let accepted = report(&mut buyer);
    assert_eq!((accepted.order_id, accepted.client_ref), (2, 9));
    let fill = ReportKind::Fill {
        trade_id: 1,
        side: Side::Buy,
        price: 100,
        qty: 4,
        leaves: 0,
    };
    assert_eq!(report(&mut buyer).kind, fill);
    let made = report(&mut seller);
    assert_eq!((made.seq, made.order_id, made.client_ref), (2, 1, 7));
    assert!(matches!(made.kind, ReportKind::Fill { leaves: 6, .. }));
    // Heartbeats are answered by nothing; a logout by a logout.
    seller.send(&Inbound::Heartbeat).unwrap();
    seller.send(&Inbound::Logout).unwrap();
    let reason = LogoutReason::Requested;
    assert_eq!(until_closed(&mut seller), [Outbound::Logout { reason }]);
    // Logging out cancelled the seller's order.
    buyer.send(&limit(10, Side::Buy, 100, 1)).unwrap();
    let kinds: Vec<ReportKind> = (0..2).map(|_| report(&mut buyer).kind).collect();
    assert!(matches!(kinds[1], ReportKind::Rested { .. }), "{kinds:?}");
    // Stopping leaves the buyer's order on the book.
    assert_eq!(gateway.stop(), 1);
}

#[test]
fn many_orders_in_one_write_are_all_answered() {
    let dir = TempDir::new("pipelined");
    let gateway = Gateway::start(&dir, ServerConfig::default());
    let mut client = gateway.login(1);
    for client_ref in 0..500 {
        client.queue(&limit(
            client_ref,
            Side::Buy,
            1 + (client_ref % 50) as i64,
            1,
        ));
    }
    client.flush().unwrap();
    for client_ref in 0..500 {
        let accepted = report(&mut client);
        assert_eq!(accepted.client_ref, client_ref);
        assert_eq!(accepted.kind, ReportKind::Accepted);
        assert!(matches!(
            report(&mut client).kind,
            ReportKind::Rested { .. }
        ));
    }
    client.send(&Inbound::MassCancel).unwrap();
    for _ in 0..500 {
        assert!(matches!(
            report(&mut client).kind,
            ReportKind::Cancelled {
                reason: CancelReason::MassCancel,
                ..
            }
        ));
    }
    assert_eq!(
        report(&mut client).kind,
        ReportKind::MassCancelled { count: 500 }
    );
    assert_eq!(gateway.stop(), 0);
}

#[test]
fn bad_bytes_end_the_session_and_cancel_its_orders() {
    let dir = TempDir::new("garbage");
    let gateway = Gateway::start(&dir, ServerConfig::default());
    let mut client = gateway.login(1);
    client.send(&limit(1, Side::Sell, 100, 1)).unwrap();
    report(&mut client);
    report(&mut client);
    let mut raw = client.stream().try_clone().unwrap();
    raw.write_all(&[4, 0, 99, 0]).unwrap();
    let reason = LogoutReason::ProtocolError;
    assert_eq!(until_closed(&mut client), [Outbound::Logout { reason }]);
    // Before a login too.
    let mut stranger = Client::connect(gateway.addr).unwrap();
    stranger
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stranger.send(&Inbound::Heartbeat).unwrap();
    assert_eq!(until_closed(&mut stranger), [Outbound::Logout { reason }]);
    // The order is gone: a market order finds nothing.
    let mut other = gateway.login(2);
    let market = Inbound::NewOrder(NewOrder {
        client_ref: 5,
        side: Side::Buy,
        qty: 1,
        kind: OrderKind::Market,
    });
    other.send(&market).unwrap();
    report(&mut other);
    assert_eq!(
        report(&mut other).kind,
        ReportKind::Cancelled {
            qty: 1,
            reason: CancelReason::NoLiquidity
        }
    );
    assert_eq!(gateway.stop(), 0);
}

#[test]
fn a_client_that_does_not_read_is_dropped() {
    let dir = TempDir::new("slow");
    let config = ServerConfig {
        max_output: 1_000,
        ..ServerConfig::default()
    };
    let gateway = Gateway::start(&dir, config);
    let mut client = gateway.login(1);
    // Twenty orders in one write make forty reports in one round: more than 1,000 bytes.
    for client_ref in 0..20 {
        client.queue(&limit(client_ref, Side::Buy, 10, 1));
    }
    client.flush().unwrap();
    until_closed(&mut client);
    // The account's orders went with it.
    let mut again = gateway.login(1);
    again.send(&Inbound::MassCancel).unwrap();
    assert_eq!(
        report(&mut again).kind,
        ReportKind::MassCancelled { count: 0 }
    );
    assert_eq!(gateway.stop(), 0);
}

#[test]
fn sessions_beyond_the_limit_are_closed() {
    let dir = TempDir::new("limit");
    let config = ServerConfig {
        max_sessions: 1,
        ..ServerConfig::default()
    };
    let gateway = Gateway::start(&dir, config);
    let _first = gateway.login(1);
    let mut second = Client::connect(gateway.addr).unwrap();
    second
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    // The login may or may not reach a socket that is already closed.
    let _ = second.send(&Inbound::Heartbeat);
    assert!(until_closed(&mut second).is_empty());
    drop(_first);
    // The slot frees once the first connection goes.
    let mut third = None;
    for _ in 0..100 {
        if let Ok((client, _)) = Client::login(gateway.addr, 2, 102) {
            third = Some(client);
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(third.is_some());
    gateway.stop();
}

#[test]
fn stopping_logs_everyone_out_and_keeps_the_orders() {
    let dir = TempDir::new("stop");
    let gateway = Gateway::start(&dir, ServerConfig::default());
    let mut client = gateway.login(1);
    client.send(&limit(1, Side::Sell, 100, 1)).unwrap();
    report(&mut client);
    report(&mut client);
    let stop = gateway.stop.clone();
    stop.store(true, Ordering::Relaxed);
    let reason = LogoutReason::Shutdown;
    assert_eq!(until_closed(&mut client), [Outbound::Logout { reason }]);
    assert_eq!(gateway.stop(), 1);
    // The next start finds the order, and the client its id.
    let gateway = Gateway::start(&dir, ServerConfig::default());
    let mut client = gateway.login(1);
    client.send(&Inbound::Cancel { order_id: 1 }).unwrap();
    let cancelled = report(&mut client);
    assert_eq!((cancelled.seq, cancelled.order_id), (2, 1));
    assert!(matches!(cancelled.kind, ReportKind::Cancelled { .. }));
    assert_eq!(gateway.stop(), 0);
}

#[test]
fn a_connection_can_be_refused_by_a_bad_token() {
    let dir = TempDir::new("token");
    let gateway = Gateway::start(&dir, ServerConfig::default());
    let error = Client::login(gateway.addr, 1, 5).err().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    // A raw socket that connects and leaves does no harm.
    drop(TcpStream::connect(gateway.addr).unwrap());
    let _ = gateway.login(1);
    gateway.stop();
}

/// The load generator's clients trade with each other until the time is up; every order is
/// answered, and they leave nothing on the book.
#[test]
fn the_load_generator_trades_end_to_end() {
    let dir = TempDir::new("load");
    let gateway = Gateway::start(&dir, ServerConfig::default());
    let config = LoadConfig {
        duration: Duration::from_millis(500),
        window: 8,
        mid: 500,
        max_resting: 50,
        ..LoadConfig::default()
    };
    let accounts = [account(1), account(2), account(3)];
    let report = load::run(gateway.addr, &accounts, config).unwrap();
    assert!(report.orders > 0);
    assert_eq!(report.latencies.len() as u64, report.orders);
    assert_eq!(
        report.accepted + report.rejected + report.refused,
        report.orders
    );
    assert_eq!(report.refused, 0);
    assert!(report.fills > 0);
    assert!(report.percentile(0.5) <= report.percentile(1.0));
    assert_eq!(gateway.stop(), 0);
}

/// The same through the pipeline: the writer and the matcher on threads of their own.
#[test]
fn the_load_generator_trades_through_the_pipeline() {
    let dir = TempDir::new("pipelined-load");
    let gateway = Gateway::pipelined(&dir, ServerConfig::default());
    let config = LoadConfig {
        duration: Duration::from_millis(500),
        window: 8,
        mid: 500,
        max_resting: 50,
        ..LoadConfig::default()
    };
    let accounts = [account(1), account(2), account(3)];
    let report = load::run(gateway.addr, &accounts, config).unwrap();
    assert!(report.orders > 0);
    assert_eq!(report.latencies.len() as u64, report.orders);
    assert_eq!(
        report.accepted + report.rejected + report.refused,
        report.orders
    );
    assert!(report.fills > 0);
    assert_eq!(gateway.stop(), 0);
}

/// A pipelined gateway stops and starts again with the orders where they were.
#[test]
fn a_pipelined_gateway_keeps_its_orders_across_a_restart() {
    let dir = TempDir::new("pipelined-restart");
    let gateway = Gateway::pipelined(&dir, ServerConfig::default());
    let mut client = gateway.login(1);
    client.send(&limit(1, Side::Sell, 100, 1)).unwrap();
    report(&mut client);
    report(&mut client);
    assert_eq!(gateway.stop(), 1);
    let gateway = Gateway::pipelined(&dir, ServerConfig::default());
    let mut client = gateway.login(2);
    let market = Inbound::NewOrder(NewOrder {
        client_ref: 2,
        side: Side::Buy,
        qty: 1,
        kind: OrderKind::Market,
    });
    client.send(&market).unwrap();
    assert_eq!(report(&mut client).kind, ReportKind::Accepted);
    assert!(matches!(
        report(&mut client).kind,
        ReportKind::Fill { leaves: 0, .. }
    ));
    assert_eq!(gateway.stop(), 0);
}

/// A subscriber over TCP gets the book, then the trades and level changes, through either
/// core.
#[test]
fn market_data_reaches_subscribers() {
    for pipelined in [false, true] {
        let dir = TempDir::new(if pipelined {
            "md-pipeline"
        } else {
            "md-thread"
        });
        let gateway = Gateway::start_with(&dir, ServerConfig::default(), pipelined);
        let mut seller = gateway.login(1);
        seller.send(&limit(1, Side::Sell, 101, 5)).unwrap();
        report(&mut seller);
        report(&mut seller);
        let mut watcher = gateway.login(3);
        watcher.send(&Inbound::Subscribe).unwrap();
        assert_eq!(
            watcher.receive().unwrap(),
            Outbound::BookSnapshot { seq: 1, levels: 1 }
        );
        let level = |seq, side, price, qty, orders| {
            Outbound::LevelUpdate(LevelUpdate {
                seq,
                side,
                price,
                qty,
                orders,
            })
        };
        assert_eq!(watcher.receive().unwrap(), level(1, Side::Sell, 101, 5, 1));
        let mut buyer = gateway.login(2);
        buyer.send(&limit(2, Side::Buy, 101, 2)).unwrap();
        assert_eq!(
            watcher.receive().unwrap(),
            Outbound::TradeTick(TradeTick {
                seq: 2,
                trade_id: 1,
                side: Side::Buy,
                price: 101,
                qty: 2
            })
        );
        assert_eq!(watcher.receive().unwrap(), level(2, Side::Sell, 101, 3, 1));
        gateway.stop();
    }
}
