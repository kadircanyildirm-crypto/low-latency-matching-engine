//! The mirror against a real exchange over real sockets: after each of the venue's books,
//! the exchange's book holds the venue's orders at the followed prices, level by level; a
//! venue's trade sent again trades against them at the venue's price; and the mirror's
//! account of its own orders stays in line with the exchange's reports.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use engine::{Discard, Engine, EngineConfig};
use gateway::client::Client;
use gateway::{Account, Exchange, Server, ServerConfig, Timing};
use mirror::bitstamp::{Book, Resting};
use mirror::book::{self, Mirror};
use orderbook::{BookConfig, Side};
use protocol::{Inbound, Outbound};

/// A directory removed when the test ends.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path = std::env::temp_dir().join(format!("mirror-{name}-{}", std::process::id()));
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

/// A running gateway, with accounts 1 to 3.
struct Gateway {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Gateway {
    fn start(dir: &TempDir) -> Gateway {
        let path = dir.0.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let (tx, rx) = mpsc::channel();
        let thread = thread::spawn(move || {
            let book = BookConfig {
                max_owners: 8,
                ..BookConfig::new(1, 10_000, 1_024)
            };
            let engine = Engine::open(&path, EngineConfig::new(book), &mut Discard)
                .unwrap()
                .0;
            let accounts: Vec<Account> = (1..=3).map(account).collect();
            let exchange = Exchange::new(
                engine.book(),
                engine.last_seq(),
                &accounts,
                Timing::default(),
            )
            .unwrap();
            let addr = "127.0.0.1:0".parse().unwrap();
            let mut server = Server::bind(exchange, engine, addr, ServerConfig::default()).unwrap();
            tx.send(server.local_addr().unwrap()).unwrap();
            server.run(&stopping).unwrap();
        });
        let addr = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        Gateway {
            addr,
            stop,
            thread: Some(thread),
        }
    }

    fn login(&self, account: u32) -> Client {
        let (mut client, _) = Client::login(self.addr, account, 100 + u64::from(account)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_millis(20)))
            .unwrap();
        client
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

/// A side's levels: quantity and number of orders by price.
type Levels = BTreeMap<i64, (u64, u32)>;

/// The levels of the venue's orders that the mirror follows: `levels` prices of each side.
fn expected(book: &Book, levels: usize) -> (Levels, Levels) {
    let side = |orders: &[Resting]| {
        let mut out = Levels::new();
        for order in orders {
            if out.len() == levels && !out.contains_key(&order.price) {
                break;
            }
            let level = out.entry(order.price).or_default();
            level.0 += order.lots;
            level.1 += 1;
        }
        out
    };
    (side(&book.bids), side(&book.asks))
}

/// A watcher's view of the exchange's book, from its market data.
#[derive(Default)]
struct View {
    bids: Levels,
    asks: Levels,
    trades: Vec<(i64, u64)>,
}

impl View {
    fn read(&mut self, watcher: &mut Client) {
        while let Some(message) = watcher.try_receive().unwrap() {
            match message {
                Outbound::LevelUpdate(level) => {
                    let side = match level.side {
                        Side::Buy => &mut self.bids,
                        Side::Sell => &mut self.asks,
                    };
                    if level.orders == 0 {
                        side.remove(&level.price);
                    } else {
                        side.insert(level.price, (level.qty, level.orders));
                    }
                }
                Outbound::TradeTick(trade) => self.trades.push((trade.price, trade.qty)),
                _ => {}
            }
        }
    }
}

/// Follows `venue` as the mirror's binary does, round after round, until everything is
/// answered, nothing is left to send, and the watcher sees `want`.
fn follow(
    mirror: &mut Mirror,
    venue: &Book,
    client: &mut Client,
    watcher: &mut Client,
    view: &mut View,
    want: &(Levels, Levels),
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        while let Some(message) = client.try_receive().unwrap() {
            match message {
                Outbound::Report(report) => mirror.on_report(&report),
                Outbound::Reject { client_ref, .. } => mirror.on_refused(client_ref),
                _ => {}
            }
        }
        let messages = mirror.follow(venue);
        for message in &messages {
            client.queue(message);
        }
        client.flush().unwrap();
        view.read(watcher);
        if messages.is_empty()
            && mirror.is_settled()
            && (&view.bids, &view.asks) == (&want.0, &want.1)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "bids {:?} asks {:?}, want {want:?}, settled {}, {mirror:?}",
            view.bids,
            view.asks,
            mirror.is_settled()
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn resting(id: u64, price: i64, lots: u64) -> Resting {
    Resting { id, price, lots }
}

#[test]
fn the_exchange_holds_the_venues_orders() {
    let dir = TempDir::new("follow");
    let gateway = Gateway::start(&dir);
    let mut client = gateway.login(1);
    let mut tape = gateway.login(2);
    let mut watcher = gateway.login(3);
    watcher.send(&Inbound::Subscribe).unwrap();
    let mut view = View::default();
    let mut mirror = Mirror::new(3);

    let first = Book {
        bids: vec![
            resting(11, 1_000, 5),
            resting(12, 1_000, 3),
            resting(13, 999, 7),
            resting(14, 997, 2),
            // A fourth price: not followed.
            resting(15, 996, 9),
        ],
        asks: vec![
            resting(21, 1_002, 4),
            resting(22, 1_003, 6),
            resting(23, 1_003, 1),
        ],
    };
    let want = expected(&first, 3);
    assert_eq!(want.0.len(), 3);
    follow(
        &mut mirror,
        &first,
        &mut client,
        &mut watcher,
        &mut view,
        &want,
    );

    // The venue moves: 12 traded down to 1, 13 went, a new best bid at 1,001, the best
    // offer went, and the price that was fourth is now third.
    let second = Book {
        bids: vec![
            resting(16, 1_001, 2),
            resting(11, 1_000, 5),
            resting(12, 1_000, 1),
            resting(14, 997, 2),
            resting(15, 996, 9),
        ],
        asks: vec![resting(22, 1_003, 6), resting(23, 1_003, 1)],
    };
    let want = expected(&second, 3);
    follow(
        &mut mirror,
        &second,
        &mut client,
        &mut watcher,
        &mut view,
        &want,
    );

    // A trade on the venue: a buyer takes 2 at 1,003. Sent again, it trades at 1,003 with
    // the first order there, and leaves the book as the venue's next book has it.
    tape.send(&book::retrade(1, Side::Buy, 1_003, 2)).unwrap();
    let third = Book {
        asks: vec![resting(22, 1_003, 4), resting(23, 1_003, 1)],
        ..second.clone()
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while view.trades.is_empty() {
        assert!(Instant::now() < deadline, "no trade");
        view.read(&mut watcher);
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(view.trades, [(1_003, 2)]);
    let want = expected(&third, 3);
    follow(
        &mut mirror,
        &third,
        &mut client,
        &mut watcher,
        &mut view,
        &want,
    );
    // The fill already made the mirror's order what the venue's is: nothing to send.
    assert!(mirror.follow(&third).is_empty());

    // Someone takes all of the best bid, which the venue still has: the mirror puts it
    // back.
    tape.send(&book::retrade(2, Side::Sell, 1_001, 2)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while view.trades.len() < 2 {
        assert!(Instant::now() < deadline, "no second trade");
        view.read(&mut watcher);
        thread::sleep(Duration::from_millis(5));
    }
    follow(
        &mut mirror,
        &third,
        &mut client,
        &mut watcher,
        &mut view,
        &want,
    );
    assert_eq!(view.trades, [(1_003, 2), (1_001, 2)]);
}
