//! Shared pieces of the fuzz targets: structured inputs that `arbitrary` builds from the
//! fuzzer's bytes, their mapping onto valid book configurations and commands, and the
//! property tests' reference book.
//!
//! The mapping biases inputs the way `crates/orderbook/tests/common/strategies.rs` does,
//! toward the places bugs live: few order ids (duplicates and unknown ids), few owners
//! (self-trades, and owners outside the owner table), prices at band edges, just outside
//! the band and at bitset word and summary-word boundaries, and quantities at zero, at
//! `max_order_qty`, just above it and at `u64::MAX`. Unlike the property tests, the fuzzer
//! also chooses where the band lies, out to the ends of the `i64` range, and every other
//! configuration value, up to `u32::MAX` ticks of protection and of band. Phase changes are
//! commands like any other, biased toward continuous trading and calls, so books cross in
//! calls and uncross when they end.

#![forbid(unsafe_code)]

use arbitrary::Arbitrary;
use orderbook::{
    BookConfig, Command, OrderBook, OrderId, OwnerId, Phase, Price, Qty, QueuedOrder,
    SelfTradePolicy, Side, TimeInForce,
};

/// The deliberately naive reference book of the property tests. It is compiled from the
/// test tree rather than copied, so the fuzzer and the tests share one oracle.
#[path = "../../crates/orderbook/tests/common/reference.rs"]
pub mod reference;

use reference::ReferenceBook;

/// Every order on the book: `[bids, asks]`, each best price first, each level's queue in
/// time priority. The reference book's `snapshot()` returns the same shape.
pub type Snapshot = [Vec<(Price, Vec<QueuedOrder>)>; 2];

/// The engine's book in the reference book's [`Snapshot`] shape.
pub fn snapshot(book: &OrderBook) -> Snapshot {
    let side = |side: Side| {
        book.depth(side)
            .map(|level| (level.price, book.queue(side, level.price).collect()))
            .collect()
    };
    [side(Side::Buy), side(Side::Sell)]
}

/// Panics unless the engine holds exactly the reference book's orders, in the same queue
/// order, its pending stops in the same trigger order, and the same trade count, reference
/// price and phase, or if the engine's internal invariants are broken. `step` and
/// `command` only label the failure.
pub fn assert_matches(
    engine: &OrderBook,
    reference: &ReferenceBook,
    step: usize,
    command: &Command,
) {
    assert_eq!(
        snapshot(engine),
        reference.snapshot(),
        "books differ at step {step}: {command:?}"
    );
    for side in [Side::Buy, Side::Sell] {
        let stops: Vec<_> = engine.stops(side).collect();
        assert_eq!(
            stops,
            reference.stops(side),
            "{side:?} stops differ at step {step}: {command:?}"
        );
    }
    assert_eq!(engine.trade_count(), reference.trade_count());
    assert_eq!(engine.reference_price(), reference.reference_price());
    assert_eq!(engine.phase(), reference.phase());
    if let Err(violation) = engine.validate() {
        panic!("invariant broken at step {step} ({command:?}): {violation}");
    }
}

/// Most commands one input runs. Inputs that decode to more are cut, so a run with a large
/// `-max_len` still finishes quickly.
pub const MAX_COMMANDS: usize = 512;

/// Room kept between the price band and the ends of the `i64` range when the reference book
/// takes part. The reference adds tick counts of up to `u32::MAX` to prices in plain `i64`
/// arithmetic, which would overflow next to the ends of the range. The engine works in
/// level indices and needs no such room.
const REFERENCE_HEADROOM: i64 = 1 << 33;

/// Which prices a fuzzed band may cover.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Prices {
    /// Up to [`REFERENCE_HEADROOM`] from the ends of `i64`, so the reference book can take
    /// part.
    ForReference,
    /// Anywhere, `i64::MIN` and `i64::MAX` included.
    Unrestricted,
}

/// Whether the reference book can follow a book with this configuration without
/// overflowing.
pub fn reference_can_follow(config: &BookConfig) -> bool {
    config.min_price >= i64::MIN + REFERENCE_HEADROOM
        && config.max_price <= i64::MAX - REFERENCE_HEADROOM
}

/// A book configuration. Every value maps onto a configuration that `OrderBook::new`
/// accepts.
#[derive(Arbitrary, Clone, Copy, Debug)]
pub struct ConfigInput {
    band: BandInput,
    /// `1 + max_orders % 64`.
    max_orders: u8,
    max_owners: OwnersInput,
    max_order_qty: QtyLimitInput,
    max_iceberg_tranches: TranchesInput,
    price_protection: Option<TicksInput>,
    price_band: Option<TicksInput>,
    reference_price: Option<LevelInput>,
    cancel_incoming: bool,
    auction_on_band: bool,
}

/// Where the price band lies and how many levels it has.
#[derive(Arbitrary, Clone, Copy, Debug)]
enum BandInput {
    /// 100..=140: queues build up and most orders cross.
    Dense,
    /// 0..=9_999: levels spread over several bitset words and summary words.
    Sparse,
    /// -5_000..=5_000: negative prices are legal.
    Signed,
    /// Anywhere.
    Custom { base: BaseInput, width: WidthInput },
}

/// The lowest price of a custom band, clamped so the whole band fits.
#[derive(Arbitrary, Clone, Copy, Debug)]
enum BaseInput {
    Zero,
    Small(i16),
    Any(i64),
    /// As low as [`Prices`] allows.
    Lowest,
    /// As high as [`Prices`] allows.
    Highest,
}

/// Number of levels of a custom band.
#[derive(Arbitrary, Clone, Copy, Debug)]
enum WidthInput {
    /// 1 to 256 levels: up to four bitset words.
    Narrow(u8),
    /// 1 to 12_289 levels: up to three full summary words and one level more.
    Wide(u16),
}

#[derive(Arbitrary, Clone, Copy, Debug)]
enum OwnersInput {
    /// 1 to 5 owners. Commands use owners 0 to 5, so some fall outside the table.
    Few(u8),
    /// `BookConfig::DEFAULT_MAX_OWNERS`.
    Default,
}

#[derive(Arbitrary, Clone, Copy, Debug)]
enum QtyLimitInput {
    /// 1 to 255 lots, so `max_order_qty + 1` is easy to hit.
    Small(u8),
    /// As large as the capacity allows, so level totals approach `u64::MAX`.
    Largest,
    /// Anything from 1 to the largest the capacity allows.
    Any(u64),
}

/// The most tranches an iceberg may have: at most 255. Each tranche is a separate trade,
/// so with, say, `u32::MAX` tranches one command could sweep an iceberg of a billion lots
/// one lot at a time: legal, but its billions of events only exhaust the fuzzer's memory.
#[derive(Arbitrary, Clone, Copy, Debug)]
enum TranchesInput {
    /// No icebergs at all.
    None,
    Few(u8),
    /// `BookConfig::DEFAULT_MAX_ICEBERG_TRANCHES`.
    Default,
}

/// A number of ticks for price protection or the price band.
#[derive(Arbitrary, Clone, Copy, Debug)]
enum TicksInput {
    /// 0 to 12 ticks.
    Small(u8),
    Any(u32),
}

/// A level of the band.
#[derive(Arbitrary, Clone, Copy, Debug)]
enum LevelInput {
    Lowest,
    Middle,
    Highest,
    At(u16),
}

impl ConfigInput {
    /// The configuration this input stands for; always valid.
    pub fn config(&self, prices: Prices) -> BookConfig {
        let (min_price, max_price) = self.band.bounds(prices);
        let max_orders = 1 + u32::from(self.max_orders % 64);
        let largest_qty = u64::MAX / u64::from(max_orders);
        let ticks = |input: TicksInput| match input {
            TicksInput::Small(ticks) => u32::from(ticks % 13),
            TicksInput::Any(ticks) => ticks,
        };
        BookConfig {
            min_price,
            max_price,
            max_orders,
            max_owners: match self.max_owners {
                OwnersInput::Few(owners) => 1 + u32::from(owners % 5),
                OwnersInput::Default => BookConfig::DEFAULT_MAX_OWNERS,
            },
            max_order_qty: match self.max_order_qty {
                QtyLimitInput::Small(qty) => u64::from(qty).max(1),
                QtyLimitInput::Largest => largest_qty,
                QtyLimitInput::Any(qty) => qty.clamp(1, largest_qty),
            },
            max_iceberg_tranches: match self.max_iceberg_tranches {
                TranchesInput::None => 0,
                TranchesInput::Few(tranches) => u32::from(tranches),
                TranchesInput::Default => BookConfig::DEFAULT_MAX_ICEBERG_TRANCHES,
            },
            price_protection: self.price_protection.map(ticks),
            price_band: self.price_band.map(ticks),
            reference_price: self.reference_price.map(|level| match level {
                LevelInput::Lowest => min_price,
                LevelInput::Middle => min_price + (max_price - min_price) / 2,
                LevelInput::Highest => max_price,
                LevelInput::At(level) => min_price + i64::from(level) % (max_price - min_price + 1),
            }),
            self_trade: if self.cancel_incoming {
                SelfTradePolicy::CancelIncoming
            } else {
                SelfTradePolicy::CancelResting
            },
            auction_on_band: self.auction_on_band,
        }
    }
}

impl BandInput {
    fn bounds(self, prices: Prices) -> (Price, Price) {
        let (base, width) = match self {
            BandInput::Dense => return (100, 140),
            BandInput::Sparse => return (0, 9_999),
            BandInput::Signed => return (-5_000, 5_000),
            BandInput::Custom { base, width } => (base, width),
        };
        let span = match width {
            WidthInput::Narrow(span) => i64::from(span),
            WidthInput::Wide(span) => i64::from(span) % 12_289,
        };
        let headroom = match prices {
            Prices::ForReference => REFERENCE_HEADROOM,
            Prices::Unrestricted => 0,
        };
        let (lowest, highest) = (i64::MIN + headroom, i64::MAX - headroom - span);
        let min_price = match base {
            BaseInput::Zero => 0,
            BaseInput::Small(price) => i64::from(price),
            BaseInput::Any(price) => price,
            BaseInput::Lowest => lowest,
            BaseInput::Highest => highest,
        }
        .clamp(lowest, highest);
        (min_price, min_price + span)
    }
}

/// A command. Every field is mapped onto the configuration's band and limits with the
/// biases described in the module documentation.
#[derive(Arbitrary, Clone, Copy, Debug)]
pub enum CommandInput {
    Limit {
        id: IdInput,
        owner: OwnerInput,
        buy: bool,
        price: PriceInput,
        qty: QtyInput,
        tif: TifInput,
        display: DisplayInput,
    },
    Market {
        id: IdInput,
        owner: OwnerInput,
        buy: bool,
        qty: QtyInput,
    },
    Cancel {
        id: IdInput,
        owner: OwnerInput,
    },
    Modify {
        id: IdInput,
        owner: OwnerInput,
        price: PriceInput,
        qty: QtyInput,
    },
    CancelAll {
        owner: OwnerInput,
    },
    Stop {
        id: IdInput,
        owner: OwnerInput,
        buy: bool,
        trigger: PriceInput,
        limit: Option<PriceInput>,
        qty: QtyInput,
    },
    SetPhase(PhaseInput),
}

impl CommandInput {
    /// The command this input stands for in a book with `config`.
    pub fn command(&self, config: &BookConfig) -> Command {
        match *self {
            CommandInput::Limit {
                id,
                owner,
                buy,
                price,
                qty,
                tif,
                display,
            } => Command::Limit {
                id: id.id(),
                owner: owner.owner(),
                side: side(buy),
                price: price.price(config),
                qty: qty.qty(config),
                tif: tif.tif(),
                display: display.display(),
            },
            CommandInput::Market {
                id,
                owner,
                buy,
                qty,
            } => Command::Market {
                id: id.id(),
                owner: owner.owner(),
                side: side(buy),
                qty: qty.qty(config),
            },
            CommandInput::Cancel { id, owner } => Command::Cancel {
                id: id.id(),
                owner: owner.owner(),
            },
            CommandInput::Modify {
                id,
                owner,
                price,
                qty,
            } => Command::Modify {
                id: id.id(),
                owner: owner.owner(),
                price: price.price(config),
                qty: qty.qty(config),
            },
            CommandInput::CancelAll { owner } => Command::CancelAll {
                owner: owner.owner(),
            },
            CommandInput::Stop {
                id,
                owner,
                buy,
                trigger,
                limit,
                qty,
            } => Command::Stop {
                id: id.id(),
                owner: owner.owner(),
                side: side(buy),
                trigger: trigger.price(config),
                limit: limit.map(|limit| limit.price(config)),
                qty: qty.qty(config),
            },
            CommandInput::SetPhase(phase) => Command::SetPhase {
                phase: phase.phase(),
            },
        }
    }
}

/// `Side::Buy` for `true`.
pub fn side(buy: bool) -> Side {
    if buy { Side::Buy } else { Side::Sell }
}

/// An order id from a pool of 48, so ids collide and refer to orders that are gone, plus
/// now and then `u64::MAX`.
#[derive(Arbitrary, Clone, Copy, Debug)]
pub struct IdInput(u8);

impl IdInput {
    pub fn id(self) -> OrderId {
        match self.0 {
            u8::MAX => OrderId::MAX,
            id => OrderId::from(id % 48),
        }
    }
}

/// An owner from 0 to 5, so owners trade with themselves and some fall outside a small
/// owner table, plus now and then `u32::MAX`.
#[derive(Arbitrary, Clone, Copy, Debug)]
pub struct OwnerInput(u8);

impl OwnerInput {
    pub fn owner(self) -> OwnerId {
        match self.0 {
            u8::MAX => OwnerId::MAX,
            owner => OwnerId::from(owner % 6),
        }
    }
}

/// A price: mostly inside the band, often next to a band edge or a bitset word or summary
/// word boundary, sometimes just outside the band, and now and then at the ends of `i64`.
#[derive(Arbitrary, Clone, Copy, Debug)]
pub struct PriceInput {
    kind: u8,
    value: u16,
}

impl PriceInput {
    pub fn price(self, config: &BookConfig) -> Price {
        let (min, max) = (config.min_price, config.max_price);
        let span = max - min;
        let value = i64::from(self.value);
        match self.kind % 16 {
            0..=10 => min + value % (span + 1),
            11..=13 => {
                let anchors = [
                    0,
                    63,
                    64,
                    127,
                    128,
                    4_095,
                    4_096,
                    8_191,
                    8_192,
                    span / 2,
                    span,
                ];
                let anchor = anchors[self.value as usize % anchors.len()];
                let delta = (value >> 8) % 7 - 3;
                min + (anchor + delta).clamp(0, span)
            }
            14 if value % 2 == 0 => min.saturating_sub(1 + (value >> 1) % 3),
            14 => max.saturating_add(1 + (value >> 1) % 3),
            _ if value % 2 == 0 => Price::MIN,
            _ => Price::MAX,
        }
    }
}

/// A quantity: mostly small, sometimes at or just below `max_order_qty`, and now and then
/// zero, just above the limit, `u64::MAX` or anything at all.
#[derive(Arbitrary, Clone, Copy, Debug)]
pub struct QtyInput {
    kind: u8,
    value: u64,
}

impl QtyInput {
    pub fn qty(self, config: &BookConfig) -> Qty {
        let max = config.max_order_qty;
        match self.kind % 32 {
            0 => 0,
            1..=24 => 1 + self.value % max.min(30),
            25..=27 => max.saturating_sub(self.value % 4).max(1),
            28 => max.saturating_add(1),
            29 => Qty::MAX,
            _ => self.value,
        }
    }
}

/// A trading phase: mostly continuous trading or a call, sometimes a halt or the close.
#[derive(Arbitrary, Clone, Copy, Debug)]
pub struct PhaseInput(u8);

impl PhaseInput {
    pub fn phase(self) -> Phase {
        match self.0 % 8 {
            0..=2 => Phase::Continuous,
            3..=5 => Phase::Auction,
            6 => Phase::Halted,
            _ => Phase::Closed,
        }
    }
}

/// A time in force: mostly GTC.
#[derive(Arbitrary, Clone, Copy, Debug)]
pub struct TifInput(u8);

impl TifInput {
    pub fn tif(self) -> TimeInForce {
        match self.0 % 8 {
            0..=4 => TimeInForce::Gtc,
            5 => TimeInForce::Ioc,
            6 => TimeInForce::Fok,
            _ => TimeInForce::PostOnly,
        }
    }
}

/// An iceberg display: mostly none; else small, so tranches run out and replenish often;
/// now and then zero or anything at all.
#[derive(Arbitrary, Clone, Copy, Debug)]
pub struct DisplayInput {
    kind: u8,
    value: u64,
}

impl DisplayInput {
    pub fn display(self) -> Option<Qty> {
        match self.kind % 16 {
            0..=9 => None,
            10..=13 => Some(1 + self.value % 4),
            14 => Some(0),
            _ => Some(self.value),
        }
    }
}
