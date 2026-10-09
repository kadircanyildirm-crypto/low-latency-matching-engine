//! The write-ahead journal: every command, in sequence, in fixed-size checksummed records.
//!
//! The journal is a series of segment files, `journal-<first seq>.log`, each written full of
//! zeros to hold `capacity` records after a 64-byte header. The record for sequence number
//! `seq` lives at slot `seq - first_seq` of its segment, so finding where replay starts is
//! arithmetic, and an all-zero slot is free: the end of the log.
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
//!
//! Recovery changes nothing on disk until it has read and checked everything it relies on,
//! and it deletes only what it can prove never held a durable record.

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
//   8..12   format version
//   12..16  record size
//   16..24  first sequence number
//   24..32  fingerprint of the book configuration
//   32..36  capacity in records
//   36..40  version of the matching rules the commands were written under
//   40..60  zero
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
    rules: u32,
}

/// Why a header could not be used.
#[derive(Clone, Debug, PartialEq, Eq)]
enum HeaderError {
    /// Shorter than a header, or its checksum fails: a write that did not finish, or damage.
    Torn,
    /// Its checksum holds, but it is not a header this version writes: another format
    /// version or record size, or reserved bytes in use.
    Unsupported(String),
    /// Its checksum holds, but it is not a journal segment at all.
    Foreign,
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
        out[36..40].copy_from_slice(&self.rules.to_le_bytes());
        let crc = crc32fast::hash(&out[..60]);
        out[60..].copy_from_slice(&crc.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8; HEADER_SIZE as usize]) -> Result<Header, HeaderError> {
        let crc = u32::from_le_bytes(bytes[60..].try_into().unwrap());
        if crc != crc32fast::hash(&bytes[..60]) {
            return Err(HeaderError::Torn);
        }
        if bytes[..8] != MAGIC {
            return Err(HeaderError::Foreign);
        }
        let field = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let (version, record_size) = (field(8), field(12));
        if version != VERSION || record_size != RECORD_SIZE as u32 {
            return Err(HeaderError::Unsupported(format!(
                "format version {version} with {record_size}-byte records"
            )));
        }
        if bytes[40..60].iter().any(|&b| b != 0) {
            return Err(HeaderError::Unsupported(
                "reserved header bytes in use".into(),
            ));
        }
        Ok(Header {
            first_seq: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            fingerprint: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            capacity: field(32),
            rules: field(36),
        })
    }

    fn offset(&self, slot: u32) -> u64 {
        HEADER_SIZE + u64::from(slot) * RECORD_SIZE as u64
    }

    /// Whether this segment has a slot for `seq`.
    fn covers(&self, seq: Seq) -> bool {
        self.first_seq <= seq && seq - self.first_seq < u64::from(self.capacity)
    }
}

/// Reads and checks the header of the segment file `path`.
fn read_header<F: StorageFile>(file: &mut F) -> Result<Result<Header, HeaderError>, Error> {
    let mut bytes = [0; HEADER_SIZE as usize];
    match file.read_at(0, &mut bytes) {
        Ok(()) => Ok(Header::decode(&bytes)),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(Err(HeaderError::Torn)),
        Err(error) => Err(error.into()),
    }
}

/// The name of the segment whose first sequence number is `first_seq`.
fn segment_name(first_seq: Seq) -> String {
    format!("journal-{first_seq:020}.log")
}

/// The name of the segment starting at `first_seq` while it is being prepared.
fn prepared_name(first_seq: Seq) -> String {
    format!("journal-{first_seq:020}.next")
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
    /// Segment files recovery deleted: ones created in the unsynced tail of a crash, which
    /// held no record.
    pub removed_segments: u64,
    /// Records at the end of the log that no later record vouches for, written again and
    /// synced before recovery called them durable. After a failed sync their bytes may be
    /// only in the OS cache, which would otherwise never write them back.
    pub rewritten_records: u64,
}

/// The journal, open for appending.
pub struct Journal<S: Storage> {
    storage: S,
    dir: PathBuf,
    fingerprint: u64,
    rules: u32,
    /// Records per new segment.
    capacity: u32,
    /// Segments on disk, oldest first. Those wholly before the point recovery started from
    /// were not read; their capacities come from the names of the segments after them.
    segments: Vec<Header>,
    /// The segment being appended to.
    file: S::File,
    /// Its next free slot.
    slot: u32,
    last_seq: Seq,
    durable: Seq,
    /// Encoded records waiting for one write call.
    buf: Vec<u8>,
    /// The next segment, filled with zeros a piece at a time while this one fills, under a
    /// temporary name; rolling over then only writes its header and renames it.
    next: Option<Prepared<S::File>>,
    /// Bytes of zeros the next segment is owed, from records appended since the last piece.
    owed: u64,
    /// Why preparing the next segment failed, until someone asks.
    prepare_failure: Option<Error>,
    /// Whether preparing is off until the next roll, after a failure.
    paused: bool,
}

/// A segment file being filled with zeros under the name `journal-<first seq>.next`.
struct Prepared<F> {
    file: F,
    header: Header,
    /// Bytes after the header already zero.
    filled: u64,
}

/// Bytes of zeros written per piece.
const PIECE: usize = 1 << 16;
static ZEROS: [u8; PIECE] = [0; PIECE];

/// Records encoded per write call at most.
const BATCH_RECORDS: usize = 256;

/// Records read per read call during recovery.
const READ_RECORDS: usize = 16_384;

/// What the journal's files are expected to be.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Expect {
    /// Fingerprint of the book configuration.
    pub fingerprint: u64,
    /// Version of the matching rules.
    pub rules: u32,
    /// Records per new segment.
    pub capacity: u32,
}

impl<S: Storage> Journal<S> {
    /// Opens the journal in `dir`, creating it if there is none, and replays every command
    /// after `after` through `apply`, in order. Recovery cuts a torn tail as described in
    /// the module documentation.
    pub(crate) fn open(
        mut storage: S,
        dir: &Path,
        expect: Expect,
        after: Seq,
        mut apply: impl FnMut(Seq, Command) -> Result<(), Error>,
    ) -> Result<(Journal<S>, JournalReport), Error> {
        assert!(expect.capacity > 0, "segments need room for a record");
        let mut report = JournalReport::default();
        let names = segment_names(&mut storage, dir)?;
        let clean = read_clean(&mut storage, dir)?;

        // Read the headers. Those of segments wholly before `after + 1` only serve older
        // snapshots: damage to them, or a read error, must not stop recovery from a newer
        // one, so their extent is then taken from the names. Nothing is changed yet.
        let mut segments = Vec::with_capacity(names.len());
        let mut torn_newest = None;
        for (index, &seq) in names.iter().enumerate() {
            let path = dir.join(segment_name(seq));
            let next = names.get(index + 1).copied();
            let opened = storage
                .open(&path)
                .map_err(Error::from)
                .and_then(|mut file| Ok((read_header(&mut file)?, file)));
            if let Some(next) = next.filter(|&next| next <= after + 1) {
                segments.push(match opened {
                    Ok((Ok(header), _))
                        if header.first_seq == seq && header.fingerprint == expect.fingerprint =>
                    {
                        header
                    }
                    _ => Header {
                        first_seq: seq,
                        fingerprint: expect.fingerprint,
                        capacity: u32::try_from(next - seq).unwrap_or(u32::MAX),
                        rules: expect.rules,
                    },
                });
                continue;
            }
            let (header, mut file) = opened?;
            match header {
                Ok(header) if header.first_seq != seq => {
                    return Err(Error::Corrupt {
                        file: path,
                        detail: format!("its header says it starts at {}", header.first_seq),
                    });
                }
                Ok(header) if header.fingerprint != expect.fingerprint => {
                    return Err(Error::ConfigMismatch { file: path });
                }
                Ok(header) => segments.push(header),
                // A segment gets its final name only once its header is synced, so a torn
                // header is damage. With no valid record behind it, nothing is lost by
                // removing the file.
                Err(HeaderError::Torn) if next.is_none() && !holds_records(&mut file, 0)? => {
                    torn_newest = Some(path);
                }
                Err(HeaderError::Torn) => {
                    return Err(Error::Corrupt {
                        file: path,
                        detail: "damaged segment header".into(),
                    });
                }
                Err(HeaderError::Unsupported(detail)) => {
                    return Err(Error::Unsupported { file: path, detail });
                }
                Err(HeaderError::Foreign) => {
                    return Err(Error::Corrupt {
                        file: path,
                        detail: "not a journal segment".into(),
                    });
                }
            }
        }

        // The segments replay needs are contiguous: each starts where the one before ends.
        // A gap among older ones only costs an older snapshot its journal.
        let first_needed = names
            .iter()
            .position(|&seq| seq > after + 1)
            .map_or(segments.len().saturating_sub(1), |i| i.saturating_sub(1));
        for pair in segments[first_needed.min(segments.len())..].windows(2) {
            if pair[0].first_seq + u64::from(pair[0].capacity) != pair[1].first_seq {
                return Err(Error::Corrupt {
                    file: dir.join(segment_name(pair[1].first_seq)),
                    detail: "segment does not follow the one before".into(),
                });
            }
        }

        // Replay starts with the record after `after`, in the segment that covers it, or in
        // a new segment if the journal ends exactly at `after`. A journal that ends before
        // `after` disagrees with the snapshot: the journal is synced before every snapshot,
        // so only damage can do that, and recovery refuses.
        if segments.first().is_some_and(|s| s.first_seq > after + 1) {
            return Err(Error::MissingJournal { from: after + 1 });
        }
        let mut covering = segments.iter().position(|s| s.covers(after + 1));
        if covering.is_none()
            && segments
                .last()
                .is_some_and(|last| last.first_seq + u64::from(last.capacity) != after + 1)
        {
            return Err(Error::Corrupt {
                file: dir.join(segment_name(segments.last().unwrap().first_seq)),
                detail: format!("the journal ends before the snapshot at {after}"),
            });
        }

        // A segment written under other matching rules can only be kept if nothing in it
        // needs replaying: the upgrade path, where the old version took a snapshot before it
        // stopped. The journal is then cut at the snapshot and continues under these rules.
        let mut upgrade = None;
        if let Some(i) = covering.filter(|&i| segments[i].rules != expect.rules) {
            let path = dir.join(segment_name(segments[i].first_seq));
            for later in &segments[i..] {
                let path = dir.join(segment_name(later.first_seq));
                if holds_records(&mut storage.open(&path)?, after)? {
                    return Err(Error::RulesMismatch {
                        file: path,
                        found: later.rules,
                    });
                }
            }
            upgrade = Some((i, path));
        }

        // Everything checks out: from here on, recovery repairs. A segment that was still
        // being prepared never held a record, and the clean-shutdown marker is about to be
        // out of date.
        if let Some(path) = &torn_newest {
            storage.remove(path)?;
            report.removed_segments += 1;
        }
        for name in storage.list(dir)? {
            if name.starts_with("journal-") && name.ends_with(".next") {
                storage.remove(&dir.join(name))?;
            }
        }
        if clean.is_some() {
            storage.remove(&dir.join(CLEAN))?;
        }
        if let Some((i, path)) = upgrade {
            for later in segments.drain(i + 1..) {
                storage.remove(&dir.join(segment_name(later.first_seq)))?;
                report.removed_segments += 1;
            }
            let segment = &mut segments[i];
            if segment.first_seq == after + 1 {
                storage.remove(&path)?;
                segments.pop();
                report.removed_segments += 1;
            } else {
                // End the old segment at the snapshot.
                segment.capacity = (after + 1 - segment.first_seq) as u32;
                let mut file = storage.open(&path)?;
                file.write_at(0, &segment.encode())?;
                file.sync()?;
            }
            covering = None;
        }

        let (file, slot) = match covering {
            Some(i) => {
                let first_seq = segments[i].first_seq;
                let file = storage.open(&dir.join(segment_name(first_seq)))?;
                (file, (after + 1 - first_seq) as u32)
            }
            None => {
                let header = Header {
                    first_seq: after + 1,
                    fingerprint: expect.fingerprint,
                    capacity: expect.capacity,
                    rules: expect.rules,
                };
                let prepared = prepare(&mut storage, dir, header)?;
                let file = finish(&mut storage, dir, prepared)?;
                segments.push(header);
                (file, 0)
            }
        };
        let mut journal = Journal {
            next: None,
            owed: 0,
            prepare_failure: None,
            paused: false,
            file,
            storage,
            dir: dir.to_owned(),
            fingerprint: expect.fingerprint,
            rules: expect.rules,
            capacity: expect.capacity,
            segments,
            slot,
            last_seq: after,
            durable: after,
            buf: Vec::with_capacity(BATCH_RECORDS * RECORD_SIZE),
        };
        if let Some(start) = covering {
            journal.replay(start, clean, &mut apply, &mut report)?;
        }
        // The directory may hold entries that only the OS cache knows of: a segment a killed
        // process created, or the removals above and those of leftover temporary snapshots.
        // New records will be written into, and vouch for, what the directory lists now, so
        // it has to be durable.
        journal.storage.sync_dir(dir)?;
        report.last_seq = journal.last_seq;
        Ok((journal, report))
    }

    /// Replays from the current position to the end of the log, then cuts the log there and
    /// makes what it kept durable. `clean` is the sequence number a clean shutdown recorded
    /// as durable, if there was one.
    fn replay(
        &mut self,
        start: usize,
        clean: Option<Seq>,
        apply: &mut impl FnMut(Seq, Command) -> Result<(), Error>,
        report: &mut JournalReport,
    ) -> Result<(), Error> {
        let mut chunk = vec![0; READ_RECORDS * RECORD_SIZE];
        let mut index = start;
        // The highest sequence number known to have been synced: the starting point, which
        // the journal was synced to before the snapshot, a clean shutdown, and what the
        // records themselves say.
        let mut vouched = self.last_seq.max(clean.unwrap_or(0));
        loop {
            let segment = self.segments[index];
            let filled = read_slots(&mut self.file, &segment, self.slot, &mut chunk)?;
            let mut end = None;
            for (i, bytes) in chunk[..filled * RECORD_SIZE]
                .chunks_exact(RECORD_SIZE)
                .enumerate()
            {
                match decode_record(bytes) {
                    Slot::Record {
                        seq,
                        durable,
                        command,
                    } if seq == self.last_seq + 1 => {
                        apply(seq, command)?;
                        self.last_seq = seq;
                        vouched = vouched.max(durable);
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
            // This segment is full; go on in the next, if there is one. A next segment of
            // other rules may only be empty: what it holds cannot be replayed under these.
            match self.segments.get(index + 1).copied() {
                Some(next) => {
                    let path = self.dir.join(segment_name(next.first_seq));
                    let mut file = self.storage.open(&path)?;
                    if next.rules != self.rules {
                        if holds_records(&mut file, self.last_seq)? {
                            return Err(Error::RulesMismatch {
                                file: path,
                                found: next.rules,
                            });
                        }
                        break;
                    }
                    index += 1;
                    self.file = file;
                    self.slot = 0;
                }
                None => break,
            }
        }
        if clean.is_some_and(|clean| clean > self.last_seq) {
            return Err(Error::Corrupt {
                file: self.dir.join(CLEAN),
                detail: format!(
                    "a clean shutdown had {} durable, but the journal ends at {}",
                    clean.unwrap(),
                    self.last_seq
                ),
            });
        }
        self.cut(index, report)?;
        self.rewrite_unvouched(vouched, report)?;
        // What survived may only be in the OS cache: a killed process loses nothing it
        // wrote. Records appended from now on will claim it durable, so it has to be.
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
        // Replay syncs the file, and the directory, right after the cut.
        for &slot in &stale {
            self.file
                .write_at(segment.offset(slot), &[0; RECORD_SIZE])?;
        }
        report.cleared_records += stale.len() as u64;
        for header in &later {
            self.storage
                .remove(&self.dir.join(segment_name(header.first_seq)))?;
        }
        report.removed_segments += later.len() as u64;
        Ok(())
    }

    /// Writes the records after `vouched` again, so that the sync after it really puts them
    /// on disk. After a failed sync, Linux marks the pages clean although they never reached
    /// the disk; a process that reopens the journal without a reboot reads them from the
    /// cache, and a plain sync would do nothing for them. Each segment other than the
    /// current one is synced once; replay syncs the current one.
    fn rewrite_unvouched(&mut self, vouched: Seq, report: &mut JournalReport) -> Result<(), Error> {
        let mut chunk = vec![0; READ_RECORDS * RECORD_SIZE];
        let current = *self.current();
        for segment in self.segments.clone() {
            let end = segment.first_seq + u64::from(segment.capacity) - 1;
            let (from, to) = ((vouched + 1).max(segment.first_seq), self.last_seq.min(end));
            if from > to {
                continue;
            }
            let mut other = None;
            if segment != current {
                other = Some(
                    self.storage
                        .open(&self.dir.join(segment_name(segment.first_seq)))?,
                );
            }
            let mut seq = from;
            while seq <= to {
                let count = (to - seq + 1).min(READ_RECORDS as u64) as usize;
                let bytes = &mut chunk[..count * RECORD_SIZE];
                let offset = segment.offset((seq - segment.first_seq) as u32);
                let file = other.as_mut().unwrap_or(&mut self.file);
                file.read_at(offset, bytes)?;
                file.write_at(offset, bytes)?;
                seq += count as u64;
                report.rewritten_records += count as u64;
            }
            if let Some(mut file) = other {
                file.sync()?;
            }
        }
        Ok(())
    }

    /// Writes zeros into the next segment for the records appended since the last piece,
    /// twice as fast as records arrive, so it is ready long before it is needed.
    fn prefill(&mut self, records: usize) -> Result<(), Error> {
        if self.next.is_none() {
            let header = Header {
                first_seq: self.current().first_seq + u64::from(self.current().capacity),
                fingerprint: self.fingerprint,
                capacity: self.capacity,
                rules: self.rules,
            };
            self.next = Some(prepare(&mut self.storage, &self.dir, header)?);
        }
        let next = self.next.as_mut().expect("a prepared segment");
        let body = u64::from(next.header.capacity) * RECORD_SIZE as u64;
        self.owed += 2 * (records * RECORD_SIZE) as u64;
        while next.filled < body && (self.owed >= PIECE as u64 || self.owed >= body - next.filled) {
            let n = (body - next.filled).min(PIECE as u64);
            next.file
                .write_at(HEADER_SIZE + next.filled, &ZEROS[..n as usize])?;
            next.filled += n;
            self.owed = self.owed.saturating_sub(n);
        }
        Ok(())
    }

    /// Starts the next segment, after making the current one durable: a segment exists
    /// only once every record before it is on disk.
    fn roll(&mut self) -> Result<(), Error> {
        self.sync()?;
        let first_seq = self.last_seq + 1;
        // A roll happens when the current segment is full, which is where the prepared one
        // starts.
        let prepared = match self.next.take() {
            Some(prepared) => {
                debug_assert_eq!(prepared.header.first_seq, first_seq);
                prepared
            }
            None => {
                let header = Header {
                    first_seq,
                    fingerprint: self.fingerprint,
                    capacity: self.capacity,
                    rules: self.rules,
                };
                prepare(&mut self.storage, &self.dir, header)?
            }
        };
        let header = prepared.header;
        self.file = finish(&mut self.storage, &self.dir, prepared)?;
        self.segments.push(header);
        self.slot = 0;
        self.owed = 0;
        self.paused = false;
        Ok(())
    }

    /// Syncs the journal and records that everything in it is durable, so the next
    /// recovery need not write anything again.
    pub(crate) fn close(&mut self) -> Result<(), Error> {
        self.sync()?;
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(&CLEAN_MAGIC);
        bytes[8..16].copy_from_slice(&self.last_seq.to_le_bytes());
        let crc = crc32fast::hash(&bytes[..16]);
        bytes[16..20].copy_from_slice(&crc.to_le_bytes());
        let mut file = self.storage.create(&self.dir.join(CLEAN))?;
        file.write_at(0, &bytes)?;
        file.sync()?;
        self.storage.sync_dir(&self.dir)?;
        Ok(())
    }

    /// Why preparing the next segment failed, if it did since the last call. Appending goes
    /// on; the segment is prepared again when it is needed.
    pub(crate) fn take_prepare_failure(&mut self) -> Option<Error> {
        self.prepare_failure.take()
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
            // The records are written; a failure to prepare the next segment, such as a
            // full disk, is not theirs. Preparing stops until the roll, which tries again.
            if !self.paused {
                if let Err(error) = self.prefill(n) {
                    self.next = None;
                    self.paused = true;
                    self.prepare_failure = Some(error);
                }
            }
        }
        Ok(self.last_seq)
    }

    /// Makes every appended record durable.
    pub(crate) fn sync(&mut self) -> Result<(), Error> {
        if self.durable != self.last_seq {
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
            .unwrap_or(self.segments.len() - 1);
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

/// Whether a segment file, whose header may not be trusted, holds a valid record of a
/// sequence number after `after`.
fn holds_records<F: StorageFile>(file: &mut F, after: Seq) -> Result<bool, Error> {
    let len = file.size()?;
    let mut chunk = vec![0; READ_RECORDS * RECORD_SIZE];
    let mut offset = HEADER_SIZE;
    loop {
        let records = (len.saturating_sub(offset) as usize).min(chunk.len()) / RECORD_SIZE;
        if records == 0 {
            return Ok(false);
        }
        let n = records * RECORD_SIZE;
        file.read_at(offset, &mut chunk[..n])?;
        if chunk[..n]
            .chunks_exact(RECORD_SIZE)
            .any(|bytes| matches!(decode_record(bytes), Slot::Record { seq, .. } if seq > after))
        {
            return Ok(true);
        }
        offset += n as u64;
    }
}

/// The first sequence numbers of the segments in `dir`, in order.
fn segment_names<S: Storage>(storage: &mut S, dir: &Path) -> Result<Vec<Seq>, Error> {
    let mut names: Vec<Seq> = storage
        .list(dir)?
        .iter()
        .filter_map(|name| segment_seq(name))
        .collect();
    names.sort_unstable();
    Ok(names)
}

/// The file a clean shutdown leaves: the sequence number up to which everything was synced.
const CLEAN: &str = "journal.clean";
const CLEAN_MAGIC: [u8; 8] = *b"LMECLEAN";

/// The sequence number a clean shutdown recorded as durable, if the marker is there and
/// intact.
fn read_clean<S: Storage>(storage: &mut S, dir: &Path) -> Result<Option<Seq>, Error> {
    if !storage.list(dir)?.iter().any(|name| name == CLEAN) {
        return Ok(None);
    }
    let mut bytes = [0; 24];
    let read = storage.open(&dir.join(CLEAN))?.read_at(0, &mut bytes);
    let crc = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    Ok(
        (read.is_ok() && bytes[..8] == CLEAN_MAGIC && crc == crc32fast::hash(&bytes[..16]))
            .then(|| u64::from_le_bytes(bytes[8..16].try_into().unwrap())),
    )
}

/// Creates the file for the segment `header` describes, under its temporary name.
fn prepare<S: Storage>(
    storage: &mut S,
    dir: &Path,
    header: Header,
) -> Result<Prepared<S::File>, Error> {
    Ok(Prepared {
        file: storage.create(&dir.join(prepared_name(header.first_seq)))?,
        header,
        filled: 0,
    })
}

/// Makes a prepared segment ready for records: finishes filling it with zeros, writes its
/// header, makes both durable, gives it its final name and makes that durable. Writing the
/// zeros, rather than only setting the length, makes the file system allocate every block:
/// appends then overwrite allocated blocks in place, and a data sync has no block allocation
/// to record. It also replaces whatever an earlier file of the same name left there.
fn finish<S: Storage>(
    storage: &mut S,
    dir: &Path,
    mut prepared: Prepared<S::File>,
) -> Result<S::File, Error> {
    let header = prepared.header;
    let body = u64::from(header.capacity) * RECORD_SIZE as u64;
    while prepared.filled < body {
        let n = (body - prepared.filled).min(PIECE as u64);
        prepared
            .file
            .write_at(HEADER_SIZE + prepared.filled, &ZEROS[..n as usize])?;
        prepared.filled += n;
    }
    prepared.file.write_at(0, &header.encode())?;
    prepared.file.sync()?;
    storage.rename(
        &dir.join(prepared_name(header.first_seq)),
        &dir.join(segment_name(header.first_seq)),
    )?;
    storage.sync_dir(dir)?;
    Ok(prepared.file)
}

/// Reads the commands `from..=to` from the journal in `dir` and passes them to `apply`,
/// without changing anything. Returns why not, if the journal does not hold all of them
/// intact under the expected configuration and rules.
pub(crate) fn read_range<S: Storage>(
    storage: &mut S,
    dir: &Path,
    expect: Expect,
    from: Seq,
    to: Seq,
    mut apply: impl FnMut(Seq, Command),
) -> Result<Result<(), String>, Error> {
    let names = segment_names(storage, dir)?;
    let mut chunk = vec![0; READ_RECORDS * RECORD_SIZE];
    let mut next = from;
    while next <= to {
        let Some(&first_seq) = names.iter().rfind(|&&seq| seq <= next) else {
            return Ok(Err(format!("no segment holds {next}")));
        };
        let mut file = match storage.open(&dir.join(segment_name(first_seq))) {
            Ok(file) => file,
            Err(error) => return Ok(Err(error.to_string())),
        };
        let header = match read_header(&mut file) {
            Ok(Ok(header))
                if header.first_seq == first_seq
                    && header.fingerprint == expect.fingerprint
                    && header.rules == expect.rules
                    && header.covers(next) =>
            {
                header
            }
            Ok(Ok(_)) => return Ok(Err(format!("the segment for {next} does not fit"))),
            Ok(Err(_)) => return Ok(Err(format!("the segment for {next} is damaged"))),
            Err(error) => return Ok(Err(error.to_string())),
        };
        let mut slot = (next - header.first_seq) as u32;
        while next <= to && slot < header.capacity {
            let filled = match read_slots(&mut file, &header, slot, &mut chunk) {
                Ok(filled) => filled,
                Err(error) => return Ok(Err(error.to_string())),
            };
            for bytes in chunk[..filled * RECORD_SIZE].chunks_exact(RECORD_SIZE) {
                if next > to {
                    break;
                }
                match decode_record(bytes) {
                    Slot::Record { seq, command, .. } if seq == next => apply(seq, command),
                    _ => return Ok(Err(format!("the record for {next} is missing"))),
                }
                next += 1;
            }
            slot += filled as u32;
        }
    }
    Ok(Ok(()))
}

/// Reads slots from `first` on into `chunk`, as many as fit and the segment holds, and
/// returns how many: at least one while `first` is below the capacity. Slots past the end
/// of a file that was not fully written read as empty.
fn read_slots<F: StorageFile>(
    file: &mut F,
    segment: &Header,
    first: u32,
    chunk: &mut [u8],
) -> io::Result<usize> {
    debug_assert!(first < segment.capacity, "reading past the segment");
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

    /// Records whose checksum holds but whose kind, length, padding or command is not what
    /// the encoder writes.
    #[test]
    fn records_need_every_field_valid() {
        let mut valid = [0; RECORD_SIZE];
        encode_record(1, 0, &Command::CancelAll { owner: 1 }, &mut valid);
        // Kind, payload length, padding, and the id field a mass cancel leaves unused.
        for (at, value) in [(4, 2), (5, 39), (6, 1), (7, 1), (24 + 8, 1)] {
            let mut bytes = valid;
            bytes[at] = value;
            let crc = crc32fast::hash(&bytes[4..]);
            bytes[..4].copy_from_slice(&crc.to_le_bytes());
            assert_eq!(decode_record(&bytes), Slot::Invalid, "byte {at}");
        }
    }

    fn header() -> Header {
        Header {
            first_seq: 1_001,
            fingerprint: 0xDEAD_BEEF,
            capacity: 64,
            rules: 7,
        }
    }

    /// Headers whose checksum holds are told apart by what is wrong with them: another
    /// format, reserved bytes in use, or not a journal at all.
    #[test]
    fn headers_with_valid_checksums_are_classified() {
        let with = |at: usize, value: u8| {
            let mut bytes = header().encode();
            bytes[at] ^= value;
            let crc = crc32fast::hash(&bytes[..60]);
            bytes[60..].copy_from_slice(&crc.to_le_bytes());
            Header::decode(&bytes)
        };
        assert_eq!(with(0, 1), Err(HeaderError::Foreign));
        for at in [8, 12] {
            assert!(
                matches!(with(at, 1), Err(HeaderError::Unsupported(_))),
                "byte {at}"
            );
        }
        for at in [40, 59] {
            assert!(
                matches!(with(at, 1), Err(HeaderError::Unsupported(_))),
                "byte {at}"
            );
        }
    }

    #[test]
    fn headers_round_trip_and_reject_damage() {
        let bytes = header().encode();
        assert_eq!(Header::decode(&bytes), Ok(header()));
        for bit in 0..bytes.len() * 8 {
            let mut damaged = bytes;
            damaged[bit / 8] ^= 1 << (bit % 8);
            assert_eq!(
                Header::decode(&damaged),
                Err(HeaderError::Torn),
                "bit {bit}"
            );
        }
    }

    #[test]
    fn coverage_ends_at_the_capacity() {
        let header = header();
        assert!(!header.covers(1_000));
        assert!(header.covers(1_001));
        assert!(header.covers(1_064));
        assert!(!header.covers(1_065));
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
