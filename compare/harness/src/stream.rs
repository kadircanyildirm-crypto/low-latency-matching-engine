//! The engine-neutral command stream file.
//!
//! A stream is a 128-byte header followed by fixed-size 24-byte records, all little-endian.
//! The layout is deliberately trivial so that a Java or C++ adapter can read it in a few
//! lines, and it is documented here byte for byte.
//!
//! Header:
//!
//! | Offset | Type | Field |
//! |---:|---|---|
//! | 0 | `[u8; 8]` | magic `LOBCMDS1` |
//! | 8 | `u32` | format version, 1 |
//! | 12 | `u32` | record length, 24 |
//! | 16 | `u64` | number of records |
//! | 24 | `u64` | records in the warm-up prefix |
//! | 32 | `i64` | lowest price of the recording book's band; every price lies inside |
//! | 40 | `i64` | highest price of the recording book's band |
//! | 48 | `u64` | most orders resting at once (a capacity hint) |
//! | 56 | `u64` | largest order id |
//! | 64 | `u64` | expected trades over the whole stream |
//! | 72 | `u64` | expected traded quantity over the whole stream |
//! | 80 | `u64` | expected resting orders at the end |
//! | 88 | `u64` | expected resting quantity at the end |
//! | 96 | `i64` | expected best bid at the end, `i64::MIN` if none |
//! | 104 | `i64` | expected best ask at the end, `i64::MAX` if none |
//! | 112 | `u64` | generator seed, for reference |
//! | 120 | `u64` | reserved, 0 |
//!
//! Record:
//!
//! | Offset | Type | Field |
//! |---:|---|---|
//! | 0 | `u8` | kind: 0 limit (GTC), 1 market, 2 cancel, 3 move |
//! | 1 | `u8` | side: 0 buy, 1 sell; for cancel and move, the side of the target order |
//! | 2 | `u16` | reserved, 0 |
//! | 4 | `u32` | limit, market: order quantity; move: the order's total quantity (filled + open) |
//! | 8 | `u64` | order id |
//! | 16 | `i64` | limit: limit price; move: new price; otherwise 0 |
//!
//! A move is a cancel/replace to a new price that keeps the order's open quantity: the order
//! loses time priority, goes to the back of the new level, and trades first if the new
//! price crosses. Its quantity field carries the order's total quantity for engines whose
//! modify takes a total quantity (FIX style, like ours); engines whose move keeps the open
//! quantity by itself ignore it.
//!
//! Every new order belongs to owner 0 if it buys and owner 1 if it sells, so two orders of
//! one owner can never meet and self-trade prevention never fires. Cancels and moves carry
//! the side of their order, from which the owner follows.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

/// File magic.
pub const MAGIC: [u8; 8] = *b"LOBCMDS1";
/// Format version.
pub const VERSION: u32 = 1;
/// Header length in bytes.
pub const HEADER_LEN: usize = 128;
/// Record length in bytes.
pub const RECORD_LEN: usize = 24;

/// What a record asks the engine to do.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// New good-till-cancelled limit order.
    Limit = 0,
    /// New market order; whatever cannot fill is cancelled.
    Market = 1,
    /// Cancel a resting order.
    Cancel = 2,
    /// Move a resting order to a new price, keeping its open quantity.
    Move = 3,
}

impl Kind {
    /// The kind with this code.
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Kind::Limit),
            1 => Some(Kind::Market),
            2 => Some(Kind::Cancel),
            3 => Some(Kind::Move),
            _ => None,
        }
    }
}

/// Buy side code.
pub const BUY: u8 = 0;
/// Sell side code.
pub const SELL: u8 = 1;

/// One command. `repr(C)` with the same layout as the file, so the C++ adapter can read a
/// slice of records directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Record {
    /// [`Kind`] code.
    pub kind: u8,
    /// [`BUY`] or [`SELL`].
    pub side: u8,
    /// Reserved, zero.
    pub reserved: u16,
    /// Order quantity (limit, market) or total quantity (move).
    pub qty: u32,
    /// Order id.
    pub id: u64,
    /// Limit price (limit) or new price (move).
    pub price: i64,
}

const _: () = assert!(std::mem::size_of::<Record>() == RECORD_LEN);

impl Record {
    /// The record's kind.
    ///
    /// # Panics
    ///
    /// On an unknown code; streams are validated when they are read.
    #[inline]
    pub fn kind(&self) -> Kind {
        match Kind::from_code(self.kind) {
            Some(kind) => kind,
            None => panic!("unknown record kind {}", self.kind),
        }
    }

    /// Whether the order buys.
    #[inline]
    pub fn is_buy(&self) -> bool {
        self.side == BUY
    }

    fn encode(&self) -> [u8; RECORD_LEN] {
        let mut out = [0u8; RECORD_LEN];
        out[0] = self.kind;
        out[1] = self.side;
        out[2..4].copy_from_slice(&self.reserved.to_le_bytes());
        out[4..8].copy_from_slice(&self.qty.to_le_bytes());
        out[8..16].copy_from_slice(&self.id.to_le_bytes());
        out[16..24].copy_from_slice(&self.price.to_le_bytes());
        out
    }

    fn decode(b: &[u8]) -> Self {
        Self {
            kind: b[0],
            side: b[1],
            reserved: u16::from_le_bytes([b[2], b[3]]),
            qty: u32::from_le_bytes(b[4..8].try_into().expect("4 bytes")),
            id: u64::from_le_bytes(b[8..16].try_into().expect("8 bytes")),
            price: i64::from_le_bytes(b[16..24].try_into().expect("8 bytes")),
        }
    }
}

/// The state every engine must reach: the same trades and the same final book.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    /// Executions, one per resting order a taker trades with.
    pub trades: u64,
    /// Total traded quantity.
    pub traded_qty: u64,
    /// Resting orders at the end.
    pub resting_orders: u64,
    /// Open quantity resting at the end.
    pub resting_qty: u64,
    /// Best bid at the end, `i64::MIN` if the bid side is empty.
    pub best_bid: i64,
    /// Best ask at the end, `i64::MAX` if the ask side is empty.
    pub best_ask: i64,
}

impl Summary {
    /// Best bid value of an empty bid side.
    pub const NO_BID: i64 = i64::MIN;
    /// Best ask value of an empty ask side.
    pub const NO_ASK: i64 = i64::MAX;
}

/// Stream metadata and the outcome recorded from our engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    /// Number of records.
    pub count: u64,
    /// Records in the warm-up prefix; the rest is measured.
    pub warmup: u64,
    /// Lowest price of the band the stream was recorded on; every price lies inside it.
    pub min_price: i64,
    /// Highest price of that band.
    pub max_price: i64,
    /// Most orders resting at once.
    pub max_live: u64,
    /// Largest order id.
    pub max_id: u64,
    /// What our engine produced over the whole stream.
    pub expect: Summary,
    /// Generator seed.
    pub seed: u64,
}

impl Header {
    fn encode(&self) -> [u8; HEADER_LEN] {
        let mut out = [0u8; HEADER_LEN];
        out[0..8].copy_from_slice(&MAGIC);
        out[8..12].copy_from_slice(&VERSION.to_le_bytes());
        out[12..16].copy_from_slice(&(RECORD_LEN as u32).to_le_bytes());
        let fields: [[u8; 8]; 14] = [
            self.count.to_le_bytes(),
            self.warmup.to_le_bytes(),
            self.min_price.to_le_bytes(),
            self.max_price.to_le_bytes(),
            self.max_live.to_le_bytes(),
            self.max_id.to_le_bytes(),
            self.expect.trades.to_le_bytes(),
            self.expect.traded_qty.to_le_bytes(),
            self.expect.resting_orders.to_le_bytes(),
            self.expect.resting_qty.to_le_bytes(),
            self.expect.best_bid.to_le_bytes(),
            self.expect.best_ask.to_le_bytes(),
            self.seed.to_le_bytes(),
            0u64.to_le_bytes(),
        ];
        for (i, field) in fields.iter().enumerate() {
            out[16 + 8 * i..24 + 8 * i].copy_from_slice(field);
        }
        out
    }

    fn decode(b: &[u8; HEADER_LEN]) -> io::Result<Self> {
        if b[0..8] != MAGIC {
            return Err(invalid("not a command stream (bad magic)"));
        }
        let version = u32::from_le_bytes(b[8..12].try_into().expect("4 bytes"));
        let record_len = u32::from_le_bytes(b[12..16].try_into().expect("4 bytes"));
        if version != VERSION || record_len as usize != RECORD_LEN {
            return Err(invalid("unsupported stream version"));
        }
        let u = |i: usize| u64::from_le_bytes(b[16 + 8 * i..24 + 8 * i].try_into().expect("8"));
        let s = |i: usize| i64::from_le_bytes(b[16 + 8 * i..24 + 8 * i].try_into().expect("8"));
        Ok(Self {
            count: u(0),
            warmup: u(1),
            min_price: s(2),
            max_price: s(3),
            max_live: u(4),
            max_id: u(5),
            expect: Summary {
                trades: u(6),
                traded_qty: u(7),
                resting_orders: u(8),
                resting_qty: u(9),
                best_bid: s(10),
                best_ask: s(11),
            },
            seed: u(12),
        })
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// Writes a stream to `path`, through a temporary file so a reader never sees half of one.
pub fn write(path: &Path, header: &Header, records: &[Record]) -> io::Result<()> {
    assert_eq!(header.count, records.len() as u64, "header count");
    let tmp = path.with_extension("tmp");
    {
        let mut out = BufWriter::with_capacity(1 << 20, File::create(&tmp)?);
        out.write_all(&header.encode())?;
        for record in records {
            out.write_all(&record.encode())?;
        }
        out.flush()?;
    }
    std::fs::rename(&tmp, path)
}

/// Reads and validates a stream.
pub fn read(path: &Path) -> io::Result<(Header, Vec<Record>)> {
    let mut input = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut head = [0u8; HEADER_LEN];
    input.read_exact(&mut head)?;
    let header = Header::decode(&head)?;
    let count = usize::try_from(header.count).map_err(|_| invalid("too many records"))?;
    if header.warmup > header.count {
        return Err(invalid("warm-up longer than the stream"));
    }
    let mut records = Vec::with_capacity(count);
    let mut chunk = vec![0u8; RECORD_LEN * 4096];
    while records.len() < count {
        let n = (count - records.len()).min(4096);
        let bytes = &mut chunk[..n * RECORD_LEN];
        input.read_exact(bytes)?;
        for b in bytes.chunks_exact(RECORD_LEN) {
            let record = Record::decode(b);
            if Kind::from_code(record.kind).is_none() || record.side > SELL {
                return Err(invalid("bad record"));
            }
            records.push(record);
        }
    }
    if input.read(&mut [0u8; 1])? != 0 {
        return Err(invalid("trailing bytes after the last record"));
    }
    Ok((header, records))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let header = Header {
            count: 2,
            warmup: 1,
            min_price: -5,
            max_price: 200_000,
            max_live: 10_000,
            max_id: 7,
            expect: Summary {
                trades: 3,
                traded_qty: 40,
                resting_orders: 1,
                resting_qty: 9,
                best_bid: Summary::NO_BID,
                best_ask: 101,
            },
            seed: 0x5EED,
        };
        let records = [
            Record {
                kind: Kind::Limit as u8,
                side: SELL,
                reserved: 0,
                qty: 9,
                id: 7,
                price: 101,
            },
            Record {
                kind: Kind::Move as u8,
                side: BUY,
                reserved: 0,
                qty: 12,
                id: u64::MAX,
                price: -5,
            },
        ];
        let dir = std::env::temp_dir().join(format!("lobcmds-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("round_trip.bin");
        write(&path, &header, &records).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), HEADER_LEN + 2 * RECORD_LEN);
        assert_eq!(&bytes[0..8], b"LOBCMDS1");
        // A few fields at their documented offsets.
        assert_eq!(bytes[HEADER_LEN], 0);
        assert_eq!(bytes[HEADER_LEN + 1], 1);
        assert_eq!(u32::from_le_bytes(bytes[132..136].try_into().unwrap()), 9);
        assert_eq!(i64::from_le_bytes(bytes[104..112].try_into().unwrap()), 101);
        let (h, r) = read(&path).unwrap();
        assert_eq!(h, header);
        assert_eq!(r, records);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
