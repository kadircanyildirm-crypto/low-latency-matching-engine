//! Binary encodings of commands and book snapshots: fixed-width little-endian fields, and
//! strict.
//!
//! Decoding accepts exactly what encoding produces. Every byte has one meaning: unused
//! fields, padding and absent optional values must be zero, and flags and enum codes must
//! be ones the encoder writes. A decoder that tolerated variants would let two different
//! byte strings stand for the same command, and a damaged record could then pass for a
//! valid one. Decoding re-encodes what it read and compares, so the rule cannot drift
//! from the encoder.

use std::fmt;

use orderbook::{
    BookConfig, BookSnapshot, Command, ConfigError, Phase, SelfTradePolicy, Side, SnapshotOrder,
    StopOrder, TimeInForce,
};

/// Size of an encoded command.
pub const COMMAND_SIZE: usize = 40;

/// Why bytes are not a valid encoding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The command kind byte is not one of the encoder's.
    UnknownKind(u8),
    /// A field holds a value no encoding uses: an unknown side, time in force, phase or
    /// self-trade policy, or a flag byte other than 0 and 1.
    InvalidField(&'static str),
    /// Every field is valid, but an unused field, padding byte or absent optional value
    /// is not zero, so these are not the bytes the encoder writes.
    NonCanonical,
    /// The input ends inside a value.
    Truncated,
    /// Bytes follow the end of the encoded value.
    TrailingBytes,
    /// A snapshot's configuration would not build a book.
    InvalidConfig(ConfigError),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownKind(kind) => write!(f, "unknown command kind {kind}"),
            Self::InvalidField(field) => write!(f, "invalid {field}"),
            Self::NonCanonical => f.write_str("not a canonical encoding"),
            Self::Truncated => f.write_str("truncated"),
            Self::TrailingBytes => f.write_str("trailing bytes"),
            Self::InvalidConfig(error) => write!(f, "invalid configuration: {error}"),
        }
    }
}

impl std::error::Error for DecodeError {}

// Command layout, all little-endian:
//
//   0      kind    1 limit, 2 market, 3 cancel, 4 modify, 5 stop, 6 cancel all, 7 set phase
//   1      side    0 buy, 1 sell
//   2      code    time in force (limit) or phase (set phase)
//   3      option  1 if the optional field is present: display (limit) or limit (stop)
//   4..8   owner   u32
//   8..16  id      u64
//   16..24 price   i64: limit price, new price (modify) or trigger (stop)
//   24..32 qty     u64
//   32..40 option  u64 display or i64 stop limit
//
// Fields a kind does not use are zero.

const LIMIT: u8 = 1;
const MARKET: u8 = 2;
const CANCEL: u8 = 3;
const MODIFY: u8 = 4;
const STOP: u8 = 5;
const CANCEL_ALL: u8 = 6;
const SET_PHASE: u8 = 7;

/// Writes `command` into `out`.
pub fn encode_command(command: &Command, out: &mut [u8; COMMAND_SIZE]) {
    *out = [0; COMMAND_SIZE];
    let mut put = |at: usize, bytes: &[u8]| out[at..at + bytes.len()].copy_from_slice(bytes);
    match *command {
        Command::Limit {
            id,
            owner,
            side,
            price,
            qty,
            tif,
            display,
        } => {
            put(
                0,
                &[
                    LIMIT,
                    side_code(side),
                    tif_code(tif),
                    u8::from(display.is_some()),
                ],
            );
            put(4, &owner.to_le_bytes());
            put(8, &id.to_le_bytes());
            put(16, &price.to_le_bytes());
            put(24, &qty.to_le_bytes());
            put(32, &display.unwrap_or(0).to_le_bytes());
        }
        Command::Market {
            id,
            owner,
            side,
            qty,
        } => {
            put(0, &[MARKET, side_code(side)]);
            put(4, &owner.to_le_bytes());
            put(8, &id.to_le_bytes());
            put(24, &qty.to_le_bytes());
        }
        Command::Cancel { id, owner } => {
            put(0, &[CANCEL]);
            put(4, &owner.to_le_bytes());
            put(8, &id.to_le_bytes());
        }
        Command::Modify {
            id,
            owner,
            price,
            qty,
        } => {
            put(0, &[MODIFY]);
            put(4, &owner.to_le_bytes());
            put(8, &id.to_le_bytes());
            put(16, &price.to_le_bytes());
            put(24, &qty.to_le_bytes());
        }
        Command::Stop {
            id,
            owner,
            side,
            trigger,
            limit,
            qty,
        } => {
            put(0, &[STOP, side_code(side), 0, u8::from(limit.is_some())]);
            put(4, &owner.to_le_bytes());
            put(8, &id.to_le_bytes());
            put(16, &trigger.to_le_bytes());
            put(24, &qty.to_le_bytes());
            put(32, &limit.unwrap_or(0).to_le_bytes());
        }
        Command::CancelAll { owner } => {
            put(0, &[CANCEL_ALL]);
            put(4, &owner.to_le_bytes());
        }
        Command::SetPhase { phase } => put(0, &[SET_PHASE, 0, phase_code(phase)]),
    }
}

/// The command `bytes` encode.
pub fn decode_command(bytes: &[u8; COMMAND_SIZE]) -> Result<Command, DecodeError> {
    let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    let i64_at = |at: usize| i64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    let side = || side_of(bytes[1]);
    let present = || flag_of(bytes[3], "option flag");
    let (owner, id) = (u32_at(4), u64_at(8));
    let command = match bytes[0] {
        LIMIT => Command::Limit {
            id,
            owner,
            side: side()?,
            price: i64_at(16),
            qty: u64_at(24),
            tif: tif_of(bytes[2])?,
            display: present()?.then(|| u64_at(32)),
        },
        MARKET => Command::Market {
            id,
            owner,
            side: side()?,
            qty: u64_at(24),
        },
        CANCEL => Command::Cancel { id, owner },
        MODIFY => Command::Modify {
            id,
            owner,
            price: i64_at(16),
            qty: u64_at(24),
        },
        STOP => Command::Stop {
            id,
            owner,
            side: side()?,
            trigger: i64_at(16),
            limit: present()?.then(|| i64_at(32)),
            qty: u64_at(24),
        },
        CANCEL_ALL => Command::CancelAll { owner },
        SET_PHASE => Command::SetPhase {
            phase: phase_of(bytes[2])?,
        },
        kind => return Err(DecodeError::UnknownKind(kind)),
    };
    let mut canonical = [0; COMMAND_SIZE];
    encode_command(&command, &mut canonical);
    if canonical != *bytes {
        return Err(DecodeError::NonCanonical);
    }
    Ok(command)
}

// Snapshot layout, all little-endian, an optional value as a 0/1 byte and then the value
// (zero when absent), a bool as one byte:
//
//   config      min_price i64, max_price i64, max_orders u32, max_owners u32,
//               max_order_qty u64, max_iceberg_tranches u32, price_protection Option<u32>,
//               price_band Option<u32>, reference_price Option<i64>, auction_on_band bool,
//               self_trade u8 (0 cancel resting, 1 cancel incoming)
//   state       trade_count u64, reference_price Option<i64>, phase u8
//   orders      count u32, then each: id u64, owner u32, side u8, price i64, leaves u64,
//               filled u64, post_only bool, display Option<u64>, visible u64
//   stops       count u32, then each: id u64, owner u32, side u8, trigger i64,
//               limit Option<i64>, qty u64

/// Encoded size of one resting order.
const ORDER_SIZE: usize = 8 + 4 + 1 + 8 + 8 + 8 + 1 + 9 + 8;
/// Encoded size of one pending stop.
const STOP_SIZE: usize = 8 + 4 + 1 + 8 + 9 + 8;

/// Appends the encoding of `snapshot` to `out`.
///
/// # Panics
///
/// If the snapshot holds more than `u32::MAX` orders or stops, which no book can.
pub fn encode_snapshot(snapshot: &BookSnapshot, out: &mut Vec<u8>) {
    let config = &snapshot.config;
    let mut w = Writer(out);
    w.i64(config.min_price);
    w.i64(config.max_price);
    w.u32(config.max_orders);
    w.u32(config.max_owners);
    w.u64(config.max_order_qty);
    w.u32(config.max_iceberg_tranches);
    w.option_u32(config.price_protection);
    w.option_u32(config.price_band);
    w.option_i64(config.reference_price);
    w.bool(config.auction_on_band);
    w.u8(match config.self_trade {
        SelfTradePolicy::CancelResting => 0,
        SelfTradePolicy::CancelIncoming => 1,
    });
    w.u64(snapshot.trade_count);
    w.option_i64(snapshot.reference_price);
    w.u8(phase_code(snapshot.phase));
    w.u32(u32::try_from(snapshot.orders.len()).expect("fewer than 2^32 orders"));
    for order in &snapshot.orders {
        w.u64(order.id);
        w.u32(order.owner);
        w.u8(side_code(order.side));
        w.i64(order.price);
        w.u64(order.leaves);
        w.u64(order.filled);
        w.bool(order.post_only);
        w.option_u64(order.display);
        w.u64(order.visible);
    }
    w.u32(u32::try_from(snapshot.stops.len()).expect("fewer than 2^32 stops"));
    for stop in &snapshot.stops {
        w.u64(stop.id);
        w.u32(stop.owner);
        w.u8(side_code(stop.side));
        w.i64(stop.trigger);
        w.option_i64(stop.limit);
        w.u64(stop.qty);
    }
}

/// The snapshot `bytes` encode. Its configuration is one [`orderbook::OrderBook::new`]
/// accepts; whether its orders could rest in that book is for
/// [`orderbook::OrderBook::restore`] to decide.
pub fn decode_snapshot(bytes: &[u8]) -> Result<BookSnapshot, DecodeError> {
    let mut r = Reader(bytes);
    let config = BookConfig {
        min_price: r.i64()?,
        max_price: r.i64()?,
        max_orders: r.u32()?,
        max_owners: r.u32()?,
        max_order_qty: r.u64()?,
        max_iceberg_tranches: r.u32()?,
        price_protection: r.option_u32()?,
        price_band: r.option_u32()?,
        reference_price: r.option_i64()?,
        auction_on_band: r.bool("auction_on_band")?,
        self_trade: match r.u8()? {
            0 => SelfTradePolicy::CancelResting,
            1 => SelfTradePolicy::CancelIncoming,
            _ => return Err(DecodeError::InvalidField("self-trade policy")),
        },
    };
    config.check().map_err(DecodeError::InvalidConfig)?;
    let trade_count = r.u64()?;
    let reference_price = r.option_i64()?;
    let phase = phase_of(r.u8()?)?;
    // Counts are checked against the bytes left before anything is allocated, so a damaged
    // count cannot ask for gigabytes.
    let count = r.count(ORDER_SIZE)?;
    let mut orders = Vec::with_capacity(count);
    for _ in 0..count {
        orders.push(SnapshotOrder {
            id: r.u64()?,
            owner: r.u32()?,
            side: side_of(r.u8()?)?,
            price: r.i64()?,
            leaves: r.u64()?,
            filled: r.u64()?,
            post_only: r.bool("post_only")?,
            display: r.option_u64()?,
            visible: r.u64()?,
        });
    }
    let count = r.count(STOP_SIZE)?;
    let mut stops = Vec::with_capacity(count);
    for _ in 0..count {
        stops.push(StopOrder {
            id: r.u64()?,
            owner: r.u32()?,
            side: side_of(r.u8()?)?,
            trigger: r.i64()?,
            limit: r.option_i64()?,
            qty: r.u64()?,
        });
    }
    if !r.0.is_empty() {
        return Err(DecodeError::TrailingBytes);
    }
    let snapshot = BookSnapshot {
        config,
        trade_count,
        reference_price,
        phase,
        orders,
        stops,
    };
    let mut canonical = Vec::with_capacity(bytes.len());
    encode_snapshot(&snapshot, &mut canonical);
    if canonical != bytes {
        return Err(DecodeError::NonCanonical);
    }
    Ok(snapshot)
}

fn side_code(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn side_of(code: u8) -> Result<Side, DecodeError> {
    match code {
        0 => Ok(Side::Buy),
        1 => Ok(Side::Sell),
        _ => Err(DecodeError::InvalidField("side")),
    }
}

fn tif_code(tif: TimeInForce) -> u8 {
    match tif {
        TimeInForce::Gtc => 0,
        TimeInForce::Ioc => 1,
        TimeInForce::Fok => 2,
        TimeInForce::PostOnly => 3,
    }
}

fn tif_of(code: u8) -> Result<TimeInForce, DecodeError> {
    match code {
        0 => Ok(TimeInForce::Gtc),
        1 => Ok(TimeInForce::Ioc),
        2 => Ok(TimeInForce::Fok),
        3 => Ok(TimeInForce::PostOnly),
        _ => Err(DecodeError::InvalidField("time in force")),
    }
}

fn phase_code(phase: Phase) -> u8 {
    match phase {
        Phase::Continuous => 0,
        Phase::Auction => 1,
        Phase::Halted => 2,
        Phase::Closed => 3,
    }
}

fn phase_of(code: u8) -> Result<Phase, DecodeError> {
    match code {
        0 => Ok(Phase::Continuous),
        1 => Ok(Phase::Auction),
        2 => Ok(Phase::Halted),
        3 => Ok(Phase::Closed),
        _ => Err(DecodeError::InvalidField("phase")),
    }
}

fn flag_of(code: u8, field: &'static str) -> Result<bool, DecodeError> {
    match code {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DecodeError::InvalidField(field)),
    }
}

struct Writer<'a>(&'a mut Vec<u8>);

impl Writer<'_> {
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }
    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn i64(&mut self, value: i64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn option_u32(&mut self, value: Option<u32>) {
        self.bool(value.is_some());
        self.u32(value.unwrap_or(0));
    }
    fn option_u64(&mut self, value: Option<u64>) {
        self.bool(value.is_some());
        self.u64(value.unwrap_or(0));
    }
    fn option_i64(&mut self, value: Option<i64>) {
        self.bool(value.is_some());
        self.i64(value.unwrap_or(0));
    }
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let (head, rest) = self.0.split_at_checked(N).ok_or(DecodeError::Truncated)?;
        self.0 = rest;
        Ok(head.try_into().unwrap())
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take::<1>()?[0])
    }
    fn bool(&mut self, field: &'static str) -> Result<bool, DecodeError> {
        flag_of(self.u8()?, field)
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take()?))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.take()?))
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.take()?))
    }
    fn option_u32(&mut self) -> Result<Option<u32>, DecodeError> {
        let present = self.bool("option flag")?;
        let value = self.u32()?;
        Ok(present.then_some(value))
    }
    fn option_u64(&mut self) -> Result<Option<u64>, DecodeError> {
        let present = self.bool("option flag")?;
        let value = self.u64()?;
        Ok(present.then_some(value))
    }
    fn option_i64(&mut self) -> Result<Option<i64>, DecodeError> {
        let present = self.bool("option flag")?;
        let value = self.i64()?;
        Ok(present.then_some(value))
    }
    /// A count of entries of `size` bytes each, if that many bytes are left.
    fn count(&mut self, size: usize) -> Result<usize, DecodeError> {
        let count = self.u32()? as usize;
        if count > self.0.len() / size {
            return Err(DecodeError::Truncated);
        }
        Ok(count)
    }
}
