//! The write-ahead journal: every command, in sequence, in fixed-size checksummed records.
//!
//! The journal is a series of segment files, `journal-<first seq>.log`, each preallocated
//! with zeros to hold `capacity` records after a 64-byte header. The record for sequence
//! number `seq` lives at slot `seq - first_seq` of its segment, so finding where replay
//! starts is arithmetic, and an all-zero slot is free: the end of the log.
//!
//! Every record carries the highest sequence number that had been synced when it was
//! written. Recovery uses that to tell a crash apart from damage. A record that fails its
//! checksum, followed only by records written before anything at or after it was synced,
//! lies in the unsynced tail: a power failure may tear or drop such writes in any order, and
//! nothing there was promised to anyone. Recovery cuts the log there and zeroes what
//! follows, so stale records can never be mistaken for new ones later. But if a later record
//! says the bad one had already been synced, data that was durable is gone, and recovery
//! stops with an error instead of quietly losing acknowledged commands.
//!
//! Records vouch only for what was synced before they were written. Damage to the records
//! of the last synced batch, when nothing was written after it, therefore looks exactly like
//! a torn write, and recovery cuts them like one.

use std::io;
use std::path::{Path, PathBuf};

use orderbook::Command;

use crate::codec::{COMMAND_SIZE, decode_command, encode_command};
use crate::storage::{Storage, StorageFile};
use crate::{Error, Seq};

/// Size of a journal record.
pub const RECORD_SIZE: usize = 64;

/// Size of a segment's header.
const HEADER_SIZE: u64 = 64;

const MAGIC: [u8; 8] = *b"LMEJRNL\0";
const VERSION: u32 = 1;

/// The only record kind so far.
const KIND_COMMAND: u8 = 1;

// Segment header, little-endian:
//
//   0..8    magic "LMEJRNL\0"
//   8..12   version
//   12..16  record size
//   16..24  first sequence number
//   24..32  fingerprint of the book configuration
//   32..36  capacity in records
//   36..60  zero
//   60..64  CRC-32 of bytes 0..60
//
// Record:
//
//   0..4    CRC-32 of bytes 4..64
//   4       kind: 1 = command
//   5       payload length: 40
//   6..8    zero
//   8..16   sequence number
//   16..24  highest sequence number synced when the record was written
//   24..64  the command (`codec::encode_command`)

/// What a slot holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Slot {
    /// All zeros: never written, or cleared by recovery.
    Empty,
    /// A record whose checksum and fields are valid.
    Record {
        seq: Seq,
        durable: Seq,
        command: Command,
    },
    /// Anything else: torn, damaged or stale.
    Invalid,
}

fn encode_record(seq: Seq, durable: Seq, command: &Command, out: &mut [u8]) {
    let out: &mut [u8; RECORD_SIZE] = out.try_into().expect("one record");
    *out = [0; RECORD_SIZE];
    out[4] = KIND_COMMAND;
    out[5] = COMMAND_SIZE as u8;
    out[8..16].copy_from_slice(&seq.to_le_bytes());
    out[16..24].copy_from_slice(&durable.to_le_bytes());
    let payload: &mut [u8; COMMAND_SIZE] = (&mut out[24..]).try_into().unwrap();
    encode_command(command, payload);
    let crc = crc32fast::hash(&out[4..]);
    out[..4].copy_from_slice(&crc.to_le_bytes());
}

fn decode_record(bytes: &[u8]) -> Slot {
    let bytes: &[u8; RECORD_SIZE] = bytes.try_into().expect("one record");
    if bytes.iter().all(|&b| b == 0) {
        return Slot::Empty;
    }
    let crc = u32::from_le_bytes(bytes[..4].try_into().unwrap());
    if crc != crc32fast::hash(&bytes[4..])
        || bytes[4] != KIND_COMMAND
        || bytes[5] != COMMAND_SIZE as u8
        || bytes[6..8] != [0, 0]
    {
        return Slot::Invalid;
    }
    let payload: &[u8; COMMAND_SIZE] = bytes[24..].try_into().unwrap();
    match decode_command(payload) {
        Ok(command) => Slot::Record {
            seq: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            durable: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            command,
        },
        Err(_) => Slot::Invalid,
    }
}

/// A segment file's header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Header {
    first_seq: Seq,
    fingerprint: u64,
    capacity: u32,
}

impl Header {
    fn encode(&self) -> [u8; HEADER_SIZE as usize] {
        let mut out = [0; HEADER_SIZE as usize];
        out[..8].copy_from_slice(&MAGIC);
        out[8..12].copy_from_slice(&VERSION.to_le_bytes());
        out[12..16].copy_from_slice(&(RECORD_SIZE as u32).to_le_bytes());
        out[16..24].copy_from_slice(&self.first_seq.to_le_bytes());
        out[24..32].copy_from_slice(&self.fingerprint.to_le_bytes());
        out[32..36].copy_from_slice(&self.capacity.to_le_bytes());
        let crc = crc32fast::hash(&out[..60]);
        out[60..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8; HEADER_SIZE as usize]) -> Option<Header> {
        let crc = u32::from_le_bytes(bytes[60..].try_into().unwrap());
        let header = Header {
            first_seq: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            fingerprint: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            capacity: u32::from_le_bytes(bytes[32..36].try_into().unwrap()),
        };
        (crc == crc32fast::hash(&bytes[..60]) && header.encode() == *bytes).then_some(header)
    }

    fn file_len(&self) -> u64 {
        HEADER_SIZE + u64::from(self.capacity) * RECORD_SIZE as u64
    }

    fn offset(&self, slot: u32) -> u64 {
        HEADER_SIZE + u64::from(slot) * RECORD_SIZE as u64
    }
}

/// The name of the segment whose first sequence number is `first_seq`.
fn segment_name(first_seq: Seq) -> String {
    format!("journal-{first_seq:020}.log")
}

/// The first sequence number of the segment called `name`, if it is one.
fn segment_seq(name: &str) -> Option<Seq> {
    let digits = name.strip_prefix("journal-")?.strip_suffix(".log")?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

/// What opening the journal found and did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JournalReport {
    /// Commands replayed, after the starting point.
    pub replayed: u64,
    /// The sequence number of the last command in the journal.
    pub last_seq: Seq,
    /// Records past the end of the log that recovery cleared: torn or out-of-order writes
    /// from the unsynced tail of a crash.
    pub cleared_records: u64,
    /// Segment files recovery deleted: ones created in the unsynced tail of a crash, or ones
    /// a snapshot made obsolete because the journal ended before it.
    pub removed_segments: u64,
}

/// The journal, open for appending.
pub struct Journal<S: Storage> {
    storage: S,
    dir: PathBuf,
    fingerprint: u64,
    /// Records per new segment.
    capacity: u32,
    /// Segments on disk, oldest first: first sequence number and capacity.
    segments: Vec<Header>,
    /// The segment being appended to.
    file: S::File,
    /// Its next free slot.
    slot: u32,
    last_seq: Seq,
    durable: Seq,
    /// Encoded records waiting for one write call.
    buf: Vec<u8>,
}

/// Records encoded per write call at most.
const BATCH_RECORDS: usize = 256;

/// Records read per read call during recovery.
const READ_RECORDS: usize = 16_384;

impl<S: Storage> Journal<S> {
    /// Opens the journal in `dir`, creating it if there is none, and replays every command
    /// after `after` through `apply`, in order. Recovery cuts a torn tail as described in
    /// the module documentation. New segments hold `capacity` records.
    pub(crate) fn open(
        mut storage: S,
        dir: &Path,
        fingerprint: u64,
        capacity: u32,
        after: Seq,
        mut apply: impl FnMut(Seq, Command) -> Result<(), Error>,
    ) -> Result<(Journal<S>, JournalReport), Error> {
        assert!(capacity > 0, "segments need room for a record");
        storage.create_dir_all(dir)?;
        let mut names: Vec<(Seq, String)> = storage
            .list(dir)?
            .into_iter()
            .filter_map(|name| Some((segment_seq(&name)?, name)))
            .collect();
        names.sort();
        let mut report = JournalReport::default();

        // Read every header. Only the newest segment may be damaged: it can have been
        // created in the unsynced tail, before its header reached the disk.
        let mut segments = Vec::with_capacity(names.len());
        for (index, (seq, name)) in names.iter().enumerate() {
            let path = dir.join(name);
            let mut file = storage.open(&path)?;
            let mut bytes = [0; HEADER_SIZE as usize];
            let header = file
                .read_at(0, &mut bytes)
                .ok()
                .and_then(|()| Header::decode(&bytes));
            match header {
                Some(header) if header.first_seq == *seq => {
                    if header.fingerprint != fingerprint {
                        return Err(Error::ConfigMismatch { file: path });
                    }
                    segments.push(header);
                }
                // A segment's header is synced before any record is written into it, so a
                // torn header with no records behind it is a crash during creation. With
                // records behind it, the header was synced and has since been damaged.
                _ if index + 1 == names.len() && !holds_records(&mut file)? => {
                    drop(file);
                    storage.remove(&path)?;
                    storage.sync_dir(dir)?;
                    report.removed_segments += 1;
                }
                _ => {
                    return Err(Error::Corrupt {
                        file: path,
                        detail: "invalid segment header".into(),
                    });
                }
            }
        }

        // Segments are contiguous: each starts where the one before ends.
        for pair in segments.windows(2) {
            if pair[0].first_seq + u64::from(pair[0].capacity) != pair[1].first_seq {
                return Err(Error::Corrupt {
                    file: dir.join(segment_name(pair[1].first_seq)),
                    detail: "segment does not follow the one before".into(),
                });
            }
        }

        // Replay starts with the record after `after`, in the segment that covers it.
        if segments.first().is_some_and(|s| s.first_seq > after + 1) {
            return Err(Error::MissingJournal { from: after + 1 });
        }
        let covering = segments.iter().position(|s| covers(s, after + 1));
        if covering.is_none()
            && segments
                .last()
                .is_some_and(|last| last.first_seq + u64::from(last.capacity) != after + 1)
        {
            // The journal ends before the snapshot it is opened with, which only damage to
            // synced records can cause, since the journal is synced before every snapshot.
            // The snapshot covers all of it, and a new segment could not follow it.
            for segment in segments.drain(..) {
                storage.remove(&dir.join(segment_name(segment.first_seq)))?;
                report.removed_segments += 1;
            }
            storage.sync_dir(dir)?;
        }

        let (file, slot) = match covering {
            Some(i) => {
                let first_seq = segments[i].first_seq;
                let file = storage.open(&dir.join(segment_name(first_seq)))?;
                (file, (after + 1 - first_seq) as u32)
            }
            None => (storage.create(&dir.join(segment_name(after + 1)))?, 0),
        };
        let mut journal = Journal {
            file,
            storage,
            dir: dir.to_owned(),
            fingerprint,
            capacity,
            segments,
            slot,
            last_seq: after,
            durable: after,
            buf: Vec::with_capacity(BATCH_RECORDS * RECORD_SIZE),
        };
        match covering {
            Some(start) => journal.replay(start, &mut apply, &mut report)?,
            None => journal.initialise_segment(after + 1)?,
        }
        report.last_seq = journal.last_seq;
        Ok((journal, report))
    }

    /// Replays from the current position to the end of the log, then cuts the log there.
    fn replay(
        &mut self,
        start: usize,
        apply: &mut impl FnMut(Seq, Command) -> Result<(), Error>,
        report: &mut JournalReport,
    ) -> Result<(), Error> {
        let mut chunk = vec![0; READ_RECORDS * RECORD_SIZE];
        let mut index = start;
        loop {
            let segment = self.segments[index];
            let filled = read_slots(&mut self.file, &segment, self.slot, &mut chunk)?;
            let mut end = None;
            for (i, bytes) in chunk[..filled * RECORD_SIZE]
                .chunks_exact(RECORD_SIZE)
                .enumerate()
            {
                match decode_record(bytes) {
                    Slot::Record { seq, command, .. } if seq == self.last_seq + 1 => {
                        apply(seq, command)?;
                        self.last_seq = seq;
                        report.replayed += 1;
                    }
                    _ => {
                        end = Some(self.slot + i as u32);
                        break;
                    }
                }
            }
            match end {
                Some(slot) => {
                    self.slot = slot;
                    break;
                }
                None => self.slot += filled as u32,
            }
            if self.slot < segment.capacity {
                continue;
            }
            // This segment is full; go on in the next, if there is one.
            match self.segments.get(index + 1) {
                Some(next) => {
                    index += 1;
                    self.file = self
                        .storage
                        .open(&self.dir.join(segment_name(next.first_seq)))?;
                    self.slot = 0;
                }
                None => break,
            }
        }
        self.cut(index, report)?;
        // What survived the crash may only be in the OS cache (a killed process loses
        // nothing that was written). Records appended from now on will claim it durable, so
        // it has to be.
        self.file.sync()?;
        self.durable = self.last_seq;
        Ok(())
    }

    /// Clears everything past the end of the log, after checking that none of it was ever
    /// durable, and drops the segments after the current one.
    fn cut(&mut self, index: usize, report: &mut JournalReport) -> Result<(), Error> {
        let mut chunk = vec![0; READ_RECORDS * RECORD_SIZE];
        let next_seq = self.last_seq + 1;
        let segment = self.segments[index];
        let path = self.dir.join(segment_name(segment.first_seq));
        // The current segment's tail.
        let mut slot = self.slot;
        let mut stale = Vec::new();
        while slot < segment.capacity {
            let filled = read_slots(&mut self.file, &segment, slot, &mut chunk)?;
            for (i, bytes) in chunk[..filled * RECORD_SIZE]
                .chunks_exact(RECORD_SIZE)
                .enumerate()
            {
                match decode_record(bytes) {
                    Slot::Empty => {}
                    Slot::Record { durable, .. } if durable >= next_seq => {
                        return Err(lost_durable(&path, next_seq));
                    }
                    _ => stale.push(slot + i as u32),
                }
            }
            slot += filled as u32;
        }
        // Later segments only hold records written after the cut.
        let later: Vec<Header> = self.segments.drain(index + 1..).collect();
        for header in &later {
            let path = self.dir.join(segment_name(header.first_seq));
            let mut file = self.storage.open(&path)?;
            let mut slot = 0;
            while slot < header.capacity {
                let filled = read_slots(&mut file, header, slot, &mut chunk)?;
                for bytes in chunk[..filled * RECORD_SIZE].chunks_exact(RECORD_SIZE) {
                    if let Slot::Record { durable, .. } = decode_record(bytes) {
                        if durable >= next_seq {
                            return Err(lost_durable(&path, next_seq));
                        }
                    }
                }
                slot += filled as u32;
            }
        }
        if !stale.is_empty() {
            let zero = [0; RECORD_SIZE];
            for &slot in &stale {
                self.file.write_at(segment.offset(slot), &zero)?;
            }
            self.file.sync()?;
            report.cleared_records += stale.len() as u64;
        }
        if !later.is_empty() {
            for header in &later {
                self.storage
                    .remove(&self.dir.join(segment_name(header.first_seq)))?;
            }
            self.storage.sync_dir(&self.dir)?;
            report.removed_segments += later.len() as u64;
        }
        Ok(())
    }

    /// Creates the file for the segment starting at `first_seq`, which `self.file` already
    /// is, and makes its header and size durable before any record is written into it.
    fn initialise_segment(&mut self, first_seq: Seq) -> Result<(), Error> {
        let header = Header {
            first_seq,
            fingerprint: self.fingerprint,
            capacity: self.capacity,
        };
        self.file.set_len(header.file_len())?;
        self.file.write_at(0, &header.encode())?;
        self.file.sync()?;
        self.storage.sync_dir(&self.dir)?;
        self.segments.push(header);
        self.slot = 0;
        Ok(())
    }

    /// Starts the next segment, after making the current one durable: a segment exists
    /// only once every record before it is on disk.
    fn roll(&mut self) -> Result<(), Error> {
        self.sync()?;
        let first_seq = self.last_seq + 1;
        self.file = self
            .storage
            .create(&self.dir.join(segment_name(first_seq)))?;
        self.initialise_segment(first_seq)
    }

    fn current(&self) -> &Header {
        self.segments.last().expect("a current segment")
    }

    /// Appends `commands` under the next sequence numbers and returns the last one. Not
    /// durable until [`sync`](Self::sync).
    pub(crate) fn append(&mut self, commands: &[Command]) -> Result<Seq, Error> {
        let mut rest = commands;
        while !rest.is_empty() {
            if self.slot == self.current().capacity {
                self.roll()?;
            }
            let room = (self.current().capacity - self.slot) as usize;
            let n = rest.len().min(room).min(BATCH_RECORDS);
            self.buf.resize(n * RECORD_SIZE, 0);
            for (i, command) in rest[..n].iter().enumerate() {
                let seq = self.last_seq + 1 + i as u64;
                encode_record(
                    seq,
                    self.durable,
                    command,
                    &mut self.buf[i * RECORD_SIZE..][..RECORD_SIZE],
                );
            }
            let offset = self.current().offset(self.slot);
            self.file.write_at(offset, &self.buf)?;
            self.slot += n as u32;
            self.last_seq += n as u64;
            rest = &rest[n..];
        }
        Ok(self.last_seq)
    }

    /// Makes every appended record durable.
    pub(crate) fn sync(&mut self) -> Result<(), Error> {
        if self.durable < self.last_seq {
            self.file.sync()?;
            self.durable = self.last_seq;
        }
        Ok(())
    }

    /// The sequence number of the last appended command.
    pub(crate) fn last_seq(&self) -> Seq {
        self.last_seq
    }

    /// The highest sequence number known to be durable.
    pub(crate) fn durable(&self) -> Seq {
        self.durable
    }

    /// Deletes the segments that hold nothing after `seq`, except the current one.
    pub(crate) fn remove_through(&mut self, seq: Seq) -> Result<u64, Error> {
        let keep_from = self
            .segments
            .iter()
            .position(|s| s.first_seq + u64::from(s.capacity) > seq + 1)
            .unwrap_or(self.segments.len() - 1)
            .min(self.segments.len() - 1);
        if keep_from == 0 {
            return Ok(0);
        }
        for header in self.segments.drain(..keep_from) {
            self.storage
                .remove(&self.dir.join(segment_name(header.first_seq)))?;
        }
        self.storage.sync_dir(&self.dir)?;
        Ok(keep_from as u64)
    }

    /// The storage, for the snapshot store in the same directory.
    pub(crate) fn storage(&mut self) -> &mut S {
        &mut self.storage
    }
}

/// Whether a segment file, whose header cannot be trusted, holds any valid record.
fn holds_records<F: StorageFile>(file: &mut F) -> Result<bool, Error> {
    let len = file.size()?;
    let mut chunk = vec![0; READ_RECORDS * RECORD_SIZE];
    let mut offset = HEADER_SIZE;
    while offset < len {
        let n = ((len - offset) as usize).min(chunk.len()) / RECORD_SIZE * RECORD_SIZE;
        if n == 0 {
            break;
        }
        file.read_at(offset, &mut chunk[..n])?;
        if chunk[..n]
            .chunks_exact(RECORD_SIZE)
            .any(|bytes| matches!(decode_record(bytes), Slot::Record { .. }))
        {
            return Ok(true);
        }
        offset += n as u64;
    }
    Ok(false)
}

/// Whether `segment` has a slot for `seq`.
fn covers(segment: &Header, seq: Seq) -> bool {
    segment.first_seq <= seq && seq < segment.first_seq + u64::from(segment.capacity)
}

/// Reads slots from `first` on into `chunk`, as many as fit and the segment holds, and
/// returns how many: at least one while `first` is below the capacity. Slots past the end of a file that was not fully preallocated read as
/// empty.
fn read_slots<F: StorageFile>(
    file: &mut F,
    segment: &Header,
    first: u32,
    chunk: &mut [u8],
) -> io::Result<usize> {
    let count = (chunk.len() / RECORD_SIZE).min((segment.capacity - first) as usize);
    let bytes = &mut chunk[..count * RECORD_SIZE];
    let offset = segment.offset(first);
    let len = file.size()?;
    let available = len.saturating_sub(offset).min(bytes.len() as u64) as usize;
    bytes[available..].fill(0);
    if available > 0 {
        file.read_at(offset, &mut bytes[..available])?;
    }
    Ok(count)
}

fn lost_durable(file: &Path, seq: Seq) -> Error {
    Error::Corrupt {
        file: file.to_owned(),
        detail: format!("the record for {seq} is damaged, but a later record shows it was durable"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip_and_reject_damage() {
        let command = Command::Cancel { id: 7, owner: 3 };
        let mut bytes = [0; RECORD_SIZE];
        encode_record(42, 40, &command, &mut bytes);
        assert_eq!(
            decode_record(&bytes),
            Slot::Record {
                seq: 42,
                durable: 40,
                command
            }
        );
        for bit in 0..RECORD_SIZE * 8 {
            let mut damaged = bytes;
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert_eq!(decode_record(&damaged), Slot::Invalid, "bit {bit}");
        }
        assert_eq!(decode_record(&[0; RECORD_SIZE]), Slot::Empty);
    }

    /// A record whose checksum holds but whose command is not a canonical encoding.
    #[test]
    fn records_need_a_canonical_command() {
        let mut bytes = [0; RECORD_SIZE];
        encode_record(1, 0, &Command::CancelAll { owner: 1 }, &mut bytes);
        bytes[24 + 8] = 1; // the id field, unused by a mass cancel
        let crc = crc32fast::hash(&bytes[4..]);
        bytes[..4].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode_record(&bytes), Slot::Invalid);
    }

    #[test]
    fn headers_round_trip_and_reject_damage() {
        let header = Header {
            first_seq: 1_001,
            fingerprint: 0xDEAD_BEEF,
            capacity: 64,
        };
        let bytes = header.encode();
        assert_eq!(Header::decode(&bytes), Some(header));
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes;
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert_eq!(Header::decode(&damaged), None, "bit {bit}");
        }
    }

    #[test]
    fn segment_names_round_trip() {
        for seq in [1, 42, u64::MAX] {
            assert_eq!(segment_seq(&segment_name(seq)), Some(seq));
        }
        for name in [
            "journal-1.log",
            "journal-0000000000000000000x.log",
            "snapshot-00000000000000000001.snap",
        ] {
            assert_eq!(segment_seq(name), None, "{name}");
        }
    }
}
