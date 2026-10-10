//! The web gateway over real sockets: the page over HTTP, and a browser's session over
//! WebSocket that registers, logs in, subscribes and trades against a binary client.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use engine::EngineConfig;
use engine::storage::FsStorage;
use gateway::client::Client;
use gateway::web::json::Started;
use gateway::web::{Guests, ws};
use gateway::{Account, Funds, Server, ServerConfig, Timing, recovery};
use orderbook::{BookConfig, Side, TimeInForce};
use protocol::{Inbound, NewOrder, OrderKind, Outbound};
use serde_json::{Value, json};

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        let path = std::env::temp_dir().join(format!("gateway-web-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A gateway with a binary listener and a web one.
struct Gateway {
    binary: SocketAddr,
    web: SocketAddr,
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
                max_owners: 16,
                ..BookConfig::new(1, 1_000, 1_024)
            };
            let bot = Account {
                id: 1,
                token: 101,
                max_open_orders: 100,
                messages_per_second: 10_000,
                funds: None,
            };
            // The guests of earlier runs, as the gateway binary loads them.
            let mut accounts = vec![bot];
            let guests_file = path.join("guests.txt");
            if let Ok(text) = std::fs::read_to_string(&guests_file) {
                accounts.extend(gateway::accounts::parse(&text, book.max_owners).unwrap());
            }
            let data = path.join("data");
            let (engine, exchange, recovered) = recovery::open(
                FsStorage,
                &data,
                EngineConfig::new(book),
                &accounts,
                Timing::default(),
            )
            .unwrap();
            let addr = "127.0.0.1:0".parse().unwrap();
            let started = Started {
                recovered: engine.last_seq(),
                orders: engine.book().order_count() as u64,
                digest: engine.book().digest(),
                ..Started::default()
            };
            let mut server = Server::bind(exchange, engine, addr, ServerConfig::default()).unwrap();
            server.set_started(started);
            server.checkpoint_to(data, 1_000, recovered.checkpoint.unwrap_or(0));
            let guests = Guests {
                file: Some(guests_file),
                ids: 10..12,
                max_open_orders: 5,
                messages_per_second: 100,
                funds: Some(Funds {
                    cash: 100_000,
                    position: 100,
                }),
            };
            server.serve_web(addr, Some(guests)).unwrap();
            let addrs = (
                server.local_addr().unwrap(),
                server.web_addr().unwrap().unwrap(),
            );
            tx.send(addrs).unwrap();
            server.run(&stopping).unwrap();
        });
        let (binary, web) = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        Gateway {
            binary,
            web,
            stop,
            thread: Some(thread),
        }
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

/// Sends `request` and reads until the server closes the connection.
fn http(addr: SocketAddr, request: &[u8]) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(request).unwrap();
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    String::from_utf8_lossy(&response).into_owned()
}

/// A browser's WebSocket.
struct Browser {
    stream: TcpStream,
    input: Vec<u8>,
}

impl Browser {
    fn connect(addr: SocketAddr) -> Browser {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .write_all(
                b"GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n\
                  Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\
                  Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
            )
            .unwrap();
        let mut browser = Browser {
            stream,
            input: Vec::new(),
        };
        // The handshake's answer, up to the blank line.
        while !browser.input.windows(4).any(|w| w == b"\r\n\r\n") {
            browser.fill();
        }
        let end = browser
            .input
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .unwrap()
            + 4;
        let answer = String::from_utf8(browser.input.drain(..end).collect()).unwrap();
        assert!(answer.starts_with("HTTP/1.1 101"), "{answer}");
        assert!(answer.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        browser
    }

    fn fill(&mut self) {
        let mut chunk = [0; 4_096];
        let read = self.stream.read(&mut chunk).unwrap();
        assert!(read > 0, "the server closed the connection");
        self.input.extend_from_slice(&chunk[..read]);
    }

    fn send(&mut self, message: Value) {
        let mut frame = Vec::new();
        ws::encode_client(
            true,
            message.to_string().as_bytes(),
            [7, 1, 9, 3],
            &mut frame,
        );
        self.stream.write_all(&frame).unwrap();
    }

    /// The next message: a server's frames are unmasked, with the length after the opcode.
    fn receive(&mut self) -> Value {
        loop {
            if self.input.len() >= 2 {
                let (opcode, short) = (self.input[0] & 0x0F, usize::from(self.input[1] & 0x7F));
                let (len, at) = match short {
                    126 if self.input.len() >= 4 => (
                        usize::from(u16::from_be_bytes([self.input[2], self.input[3]])),
                        4,
                    ),
                    126 => (usize::MAX, 4),
                    127 => panic!("no message is that long"),
                    len => (len, 2),
                };
                if len != usize::MAX && self.input.len() >= at + len {
                    let payload: Vec<u8> = self.input.drain(..at + len).skip(at).collect();
                    match opcode {
                        1 => return serde_json::from_slice(&payload).unwrap(),
                        8 => return json!({"type": "closed"}),
                        _ => continue,
                    }
                }
            }
            self.fill();
        }
    }

    /// The next message of `kind`, skipping the others.
    fn expect(&mut self, kind: &str) -> Value {
        loop {
            let message = self.receive();
            if message["type"] == kind {
                return message;
            }
        }
    }
}

/// The wall clock, in seconds since the Unix epoch.
fn unix_seconds() -> u64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
}

#[test]
fn the_page_is_served_and_nothing_else() {
    let dir = TempDir::new("page");
    let gateway = Gateway::start(&dir);
    let page = http(gateway.web, b"GET / HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(page.starts_with("HTTP/1.1 200 OK\r\n"), "{page}");
    assert!(page.contains("Content-Type: text/html"));
    assert!(page.contains("<!doctype html>"));
    let script = http(gateway.web, b"GET /app.js HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(script.contains("text/javascript"));
    assert!(page.contains(r#"<meta property="og:image" content="/og.png">"#));
    let preview = http(gateway.web, b"GET /og.png HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(preview.contains("Content-Type: image/png"));
    let icon = http(gateway.web, b"GET /favicon.svg HTTP/1.1\r\nHost: x\r\n\r\n");
    assert!(
        icon.contains("image/svg+xml") && icon.contains("<svg"),
        "{icon}"
    );
    assert!(http(gateway.web, b"GET /secret HTTP/1.1\r\n\r\n").starts_with("HTTP/1.1 404"));
    assert!(http(gateway.web, b"POST / HTTP/1.1\r\n\r\n").starts_with("HTTP/1.1 400"));
    // The binary listener does not speak HTTP: it logs the connection out.
    let mut stream = TcpStream::connect(gateway.binary).unwrap();
    stream.write_all(b"GET / HTTP/1.1\r\n\r\n").unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut reply = Vec::new();
    let _ = stream.read_to_end(&mut reply);
    assert!(!reply.starts_with(b"HTTP"));
}

#[test]
fn a_browser_registers_trades_and_watches_the_market() {
    let dir = TempDir::new("trade");
    let gateway = Gateway::start(&dir);
    let mut browser = Browser::connect(gateway.web);
    browser.send(json!({"type": "register"}));
    let registered = browser.expect("registered");
    assert_eq!(registered["account"], 10);
    let token = registered["token"].as_str().unwrap().to_owned();
    assert_eq!(token.len(), 16);
    // Saved before it was handed out.
    let saved = std::fs::read_to_string(dir.0.join("guests.txt")).unwrap();
    assert!(saved.starts_with("10 "), "{saved}");
    // One account per connection.
    browser.send(json!({"type": "register"}));
    assert_eq!(
        browser.expect("error")["message"],
        "one account per connection"
    );

    browser.send(json!({"type": "login", "account": 10, "token": token}));
    assert_eq!(browser.expect("login_accepted")["account"], 10);
    browser.send(json!({"type": "subscribe"}));
    assert_eq!(browser.expect("book")["levels"], 0);
    // Nothing has traded yet.
    let history = browser.expect("history");
    assert!(browser.expect("stats_history")["stats"].is_array());
    // A new exchange: nothing recovered, an empty book.
    let started = browser.expect("started");
    assert_eq!(
        (started["recovered"].clone(), started["orders"].clone()),
        (json!(0), json!(0))
    );
    assert_eq!(started["digest"].as_str().unwrap().len(), 16, "{started}");
    assert_eq!(
        (history["interval"].clone(), history["candles"].clone()),
        (json!(5), json!([]))
    );
    browser.send(json!({"type": "order", "ref": 1, "side": "sell", "qty": 3, "price": 105}));
    let accepted = browser.expect("report");
    assert_eq!(
        (accepted["kind"].clone(), accepted["ref"].clone()),
        (json!("accepted"), json!(1))
    );
    let id = accepted["id"].as_u64().unwrap();
    assert_eq!(browser.expect("report")["kind"], "rested");
    let level = browser.expect("level");
    assert_eq!(
        (level["side"].clone(), level["price"].clone()),
        (json!("sell"), json!(105))
    );

    // A bot over the binary protocol takes two.
    let (mut bot, _) = Client::login(gateway.binary, 1, 101).unwrap();
    bot.send(&Inbound::NewOrder(NewOrder {
        client_ref: 7,
        side: Side::Buy,
        qty: 2,
        kind: OrderKind::Limit {
            price: 105,
            tif: TimeInForce::Ioc,
            display: None,
        },
    }))
    .unwrap();
    let fill = browser.expect("report");
    assert_eq!(fill["kind"], "fill");
    assert_eq!(
        (fill["id"].as_u64(), fill["leaves"].as_u64()),
        (Some(id), Some(1))
    );
    let trade = browser.expect("trade");
    assert_eq!(
        (trade["qty"].clone(), trade["side"].clone()),
        (json!(2), json!("buy"))
    );
    assert_eq!(browser.expect("level")["qty"], 1);
    assert!(matches!(bot.receive().unwrap(), Outbound::Report(_)));
    // A chart opened now starts from the trade, at the time it happened.
    browser.send(json!({"type": "subscribe"}));
    let candles = browser.expect("history")["candles"].clone();
    let candle = &candles[0];
    assert_eq!(candles.as_array().unwrap().len(), 1, "{candles}");
    assert_eq!(
        [
            &candle["o"],
            &candle["h"],
            &candle["l"],
            &candle["c"],
            &candle["v"]
        ],
        [
            &json!(105),
            &json!(105),
            &json!(105),
            &json!(105),
            &json!(2)
        ]
    );
    let opened = candle["t"].as_u64().unwrap();
    assert!(unix_seconds().abs_diff(opened) <= 10, "{opened}");
    assert_eq!(opened % 5, 0);
    // What is left of the order shows in its queue, under its id.
    let queues = browser.expect("queues");
    assert_eq!(queues["sell"], json!([[105, [id, 1]]]), "{queues}");
    assert_eq!(queues["buy"], json!([]));
    // The engine log tells the order and the bot's trade against it, in sequence.
    let mut logged = Vec::new();
    while logged.len() < 2 {
        let log = browser.expect("log");
        logged.extend(log["entries"].as_array().unwrap().iter().cloned());
    }
    assert_eq!(
        logged[0],
        json!({"seq": id, "paper": true, "cmd": "limit", "owner": 10, "side": "sell",
            "price": 105, "qty": 3, "tif": "gtc", "rested": 3})
    );
    assert_eq!(
        logged[1],
        json!({"seq": id + 1, "paper": false, "cmd": "limit", "owner": 1, "side": "buy",
            "price": 105, "qty": 2, "tif": "ioc", "trades": 1, "traded": 2})
    );
    // The account sold 2 at 105, its only trade: it leads, with nothing made yet.
    let leaders = browser.expect("leaders");
    assert_eq!(
        leaders,
        json!({"type": "leaders", "total": 1,
            "leaders": [{"account": 10, "value": 100_000 + 100 * 105, "profit": 0}]})
    );
    assert_eq!(
        browser.expect("rank"),
        json!({"type": "rank", "rank": 1, "of": 1})
    );

    // The browser cancels the rest, and leaves.
    browser.send(json!({"type": "cancel", "id": id}));
    assert_eq!(browser.expect("report")["kind"], "cancelled");
    browser.send(json!({"type": "logout"}));
    assert_eq!(browser.expect("logout")["reason"], "requested");
    assert_eq!(browser.expect("closed")["type"], "closed");

    // A browser that goes away keeps its orders, and is told them when it comes back.
    let mut again = Browser::connect(gateway.web);
    again.send(json!({"type": "login", "account": 10, "token": token}));
    again.send(json!({"type": "order", "ref": 9, "side": "sell", "qty": 1, "price": 150}));
    let kept = again.expect("report");
    assert_eq!(kept["kind"], "accepted");
    drop(again);
    std::thread::sleep(Duration::from_millis(100));
    let mut back = Browser::connect(gateway.web);
    back.send(json!({"type": "login", "account": 10, "token": token}));
    let told = back.expect("report");
    assert_eq!(
        (
            told["kind"].clone(),
            told["id"].clone(),
            told["ref"].clone()
        ),
        (json!("rested"), kept["id"].clone(), json!(9))
    );
    drop(back);

    // A browser that subscribes before logging in is logged out, with no chart.
    let mut early = Browser::connect(gateway.web);
    early.send(json!({"type": "subscribe"}));
    loop {
        let message = early.receive();
        assert_ne!(message["type"], "history");
        if message["type"] == "closed" {
            break;
        }
    }

    // Garbage ends a session with an error first.
    let mut other = Browser::connect(gateway.web);
    other.send(json!({"type": "nonsense"}));
    assert_eq!(other.expect("error")["type"], "error");
    // Every browser is told how the exchange is doing, once a second.
    let stats = Browser::connect(gateway.web).expect("stats");
    assert!(stats["sessions"].as_u64().unwrap() >= 1, "{stats}");
    assert!(stats["turn_max_ns"].as_u64().unwrap() >= stats["turn_p50_ns"].as_u64().unwrap());
    let time = stats["time"].as_u64().unwrap() / 1_000;
    assert!(unix_seconds().abs_diff(time) <= 10, "{stats}");
    let buckets = stats["turn_buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 18, "{stats}");

    // A second guest gets the next id, and the third finds none left.
    let mut second = Browser::connect(gateway.web);
    second.send(json!({"type": "register"}));
    assert_eq!(second.expect("registered")["account"], 11);
    let mut third = Browser::connect(gateway.web);
    third.send(json!({"type": "register"}));
    assert_eq!(third.expect("error")["message"], "no accounts are left");
}

/// A guest's money is where it was after the gateway stops and starts again, and so are its
/// orders, with their references.
#[test]
fn a_guests_money_survives_a_restart() {
    let dir = TempDir::new("restart");
    let gateway = Gateway::start(&dir);
    let mut browser = Browser::connect(gateway.web);
    browser.send(json!({"type": "register"}));
    let registered = browser.expect("registered");
    let token = registered["token"].as_str().unwrap().to_owned();
    browser.send(json!({"type": "login", "account": 10, "token": token}));
    assert_eq!(browser.expect("balance")["cash"], 100_000);
    browser.send(json!({"type": "order", "ref": 1, "side": "sell", "qty": 30, "price": 120}));
    browser.send(json!({"type": "order", "ref": 2, "side": "buy", "qty": 10, "price": 90}));
    let (mut bot, _) = Client::login(gateway.binary, 1, 101).unwrap();
    bot.send(&Inbound::NewOrder(NewOrder {
        client_ref: 7,
        side: Side::Buy,
        qty: 12,
        kind: OrderKind::Limit {
            price: 120,
            tif: TimeInForce::Ioc,
            display: None,
        },
    }))
    .unwrap();
    // Twelve sold at 120; eighteen still offered, ten bid for at 90.
    let after = loop {
        let balance = browser.expect("balance");
        if balance["position"] == 88 {
            break balance;
        }
    };
    assert_eq!(after["cash"], 100_000 + 12 * 120);
    assert_eq!(
        (after["cash_held"].clone(), after["position_held"].clone()),
        (json!(900), json!(18))
    );
    // The gateway stops with the browser still there: its orders stay. Had the browser
    // left first, they would have been cancelled with its connection.
    drop(gateway);
    assert_eq!(browser.expect("logout")["reason"], "shutdown");
    drop((browser, bot));

    let gateway = Gateway::start(&dir);
    let mut browser = Browser::connect(gateway.web);
    browser.send(json!({"type": "login", "account": 10, "token": token}));
    assert_eq!(browser.expect("login_accepted")["account"], 10);
    let balance = browser.expect("balance");
    assert_eq!(balance["cash"], 100_000 + 12 * 120, "{balance}");
    assert_eq!(balance["position"], 88);
    // Its orders came back with their references, and it is told them on login.
    let first = browser.expect("report");
    assert_eq!(
        (
            first["kind"].clone(),
            first["id"].clone(),
            first["ref"].clone()
        ),
        (json!("rested"), json!(1), json!(1))
    );
    assert_eq!(
        (first["price"].clone(), first["qty"].clone()),
        (json!(120), json!(18))
    );
    let second = browser.expect("report");
    assert_eq!(
        (
            second["id"].clone(),
            second["ref"].clone(),
            second["qty"].clone()
        ),
        (json!(2), json!(2), json!(10))
    );
    browser.send(json!({"type": "cancel", "id": 2}));
    let cancelled = browser.expect("report");
    assert_eq!(
        (cancelled["kind"].clone(), cancelled["ref"].clone()),
        (json!("cancelled"), json!(2))
    );
}
