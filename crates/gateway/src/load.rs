//! A load generator: clients that trade against each other through a gateway, each keeping
//! a fixed number of new orders in flight, and the time from sending each order to the
//! report of its acceptance or refusal.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::thread;
use std::time::{Duration, Instant};

use orderbook::workload::SplitMix64;
use orderbook::{Side, TimeInForce};
use protocol::{Inbound, LogoutReason, NewOrder, OrderKind, Outbound, ReportKind};

use crate::accounts::Account;
use crate::client::Client;

/// What the load looks like.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LoadConfig {
    /// How long the clients send new orders.
    pub duration: Duration,
    /// New orders each client keeps in flight.
    pub window: usize,
    /// Orders are priced up to this many ticks either side of `mid`, so they cross often.
    pub mid: i64,
    /// See `mid`.
    pub spread: i64,
    /// Resting orders a client keeps before it cancels its oldest.
    pub max_resting: usize,
    /// Seeds each client's choices, together with its account id.
    pub seed: u64,
}

impl Default for LoadConfig {
    /// Five seconds, 16 orders in flight, priced 50,000 ± 20, at most 100 resting.
    fn default() -> Self {
        LoadConfig {
            duration: Duration::from_secs(5),
            window: 16,
            mid: 50_000,
            spread: 20,
            max_resting: 100,
            seed: 1,
        }
    }
}

/// What the clients saw.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LoadReport {
    /// New orders sent.
    pub orders: u64,
    /// New orders accepted.
    pub accepted: u64,
    /// New orders the book refused.
    pub rejected: u64,
    /// New orders the gateway refused: throttled or over the open-order limit.
    pub refused: u64,
    /// Cancels sent.
    pub cancels: u64,
    /// Fills received, counting both sides of a trade between two clients.
    pub fills: u64,
    /// Nanoseconds from sending each new order to its acceptance or refusal, sorted.
    pub latencies: Vec<u64>,
    /// How long the run took, until every client had its answers and had logged out.
    pub elapsed: Duration,
}

impl LoadReport {
    /// The latency at or below which a fraction `q` of the orders were answered.
    pub fn percentile(&self, q: f64) -> Option<u64> {
        if self.latencies.is_empty() {
            return None;
        }
        let rank = (q * (self.latencies.len() - 1) as f64).round() as usize;
        Some(self.latencies[rank.min(self.latencies.len() - 1)])
    }

    fn merge(&mut self, other: LoadReport) {
        self.orders += other.orders;
        self.accepted += other.accepted;
        self.rejected += other.rejected;
        self.refused += other.refused;
        self.cancels += other.cancels;
        self.fills += other.fills;
        self.latencies.extend(other.latencies);
    }
}

/// Runs one client per account against the gateway at `addr`.
pub fn run(addr: SocketAddr, accounts: &[Account], config: LoadConfig) -> io::Result<LoadReport> {
    let started = Instant::now();
    let deadline = started + config.duration;
    let clients: Vec<_> = accounts
        .iter()
        .map(|&account| thread::spawn(move || client(addr, account, config, deadline)))
        .collect();
    let mut report = LoadReport::default();
    for client in clients {
        report.merge(client.join().expect("a client thread")?);
    }
    report.latencies.sort_unstable();
    report.elapsed = started.elapsed();
    Ok(report)
}

fn client(
    addr: SocketAddr,
    account: Account,
    config: LoadConfig,
    deadline: Instant,
) -> io::Result<LoadReport> {
    let (mut client, _) = Client::login(addr, account.id, account.token)?;
    client.set_read_timeout(Some(Duration::from_secs(30)))?;
    let mut rng = SplitMix64::new(config.seed ^ u64::from(account.id).rotate_left(32));
    let mut report = LoadReport::default();
    let mut in_flight: HashMap<u64, Instant> = HashMap::new();
    let mut resting: VecDeque<u64> = VecDeque::new();
    let mut next_ref = 1;
    loop {
        let sending = Instant::now() < deadline;
        while sending && in_flight.len() < config.window {
            client.queue(&order(&mut rng, next_ref, &config));
            in_flight.insert(next_ref, Instant::now());
            next_ref += 1;
            report.orders += 1;
        }
        while resting.len() > config.max_resting {
            let order_id = resting.pop_front().expect("a resting order");
            client.queue(&Inbound::Cancel { order_id });
            report.cancels += 1;
        }
        client.flush()?;
        if !sending && in_flight.is_empty() {
            break;
        }
        let mut message = Some(client.receive()?);
        while let Some(received) = message {
            let now = Instant::now();
            // Whether `client_ref` was waiting for its answer; records how long it waited.
            let mut answered = |client_ref: u64, report: &mut LoadReport| {
                let Some(sent) = in_flight.remove(&client_ref) else {
                    return false;
                };
                let latency = now.duration_since(sent).as_nanos();
                report
                    .latencies
                    .push(u64::try_from(latency).unwrap_or(u64::MAX));
                true
            };
            match received {
                Outbound::Reject { client_ref, .. } => {
                    answered(client_ref, &mut report);
                    report.refused += 1;
                }
                Outbound::Report(r) => match r.kind {
                    ReportKind::Accepted => {
                        answered(r.client_ref, &mut report);
                        report.accepted += 1;
                    }
                    // A refused cancel carries no client ref, a refused new order its own.
                    ReportKind::Rejected(_) => {
                        if answered(r.client_ref, &mut report) {
                            report.rejected += 1;
                        }
                    }
                    ReportKind::Rested { .. } => resting.push_back(r.order_id),
                    ReportKind::Fill { .. } => report.fills += 1,
                    _ => {}
                },
                Outbound::Logout { reason } => {
                    return Err(io::Error::other(format!("logged out: {reason:?}")));
                }
                _ => {}
            }
            message = client.try_receive()?;
        }
    }
    // Leaves nothing behind.
    client.send(&Inbound::MassCancel)?;
    client.send(&Inbound::Logout)?;
    loop {
        match client.receive() {
            Ok(Outbound::Logout {
                reason: LogoutReason::Requested,
            }) => break,
            Ok(_) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(report)
}

/// A random new order: mostly limit orders around the mid, some market and IOC orders.
fn order(rng: &mut SplitMix64, client_ref: u64, config: &LoadConfig) -> Inbound {
    let side = if rng.below(2) == 0 {
        Side::Buy
    } else {
        Side::Sell
    };
    let price = config.mid - config.spread + rng.below(2 * config.spread as u64 + 1) as i64;
    let qty = 1 + rng.below(10);
    let kind = match rng.below(20) {
        0 => OrderKind::Market,
        1..=3 => OrderKind::Limit {
            price,
            tif: TimeInForce::Ioc,
            display: None,
        },
        _ => OrderKind::Limit {
            price,
            tif: TimeInForce::Gtc,
            display: None,
        },
    };
    Inbound::NewOrder(NewOrder {
        client_ref,
        side,
        qty,
        kind,
    })
}
