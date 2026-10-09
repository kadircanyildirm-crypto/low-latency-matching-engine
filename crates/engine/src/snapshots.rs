//! Snapshot files: the book's state after a given command, so recovery can start there
//! instead of replaying the whole journal.
//!
//! A snapshot is written to `snapshot-<seq>.tmp`, synced, and only then renamed to
//! `snapshot-<seq>.snap`, so a file with the final name is always complete. Its header
//! records the sequence number, the book's digest, and checksums of itself and of the
//! encoded state; loading checks all three, and that the restored book has that digest.

use std::path::{Path, PathBuf};

use orderbook::{BookConfig, OrderBook};

use crate::codec::{decode_snapshot, encode_snapshot};
use crate::storage::{Storage, StorageFile};
use crate::{Error, Seq};

const MAGIC: [u8; 8] = *b"LMESNAP\0";
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 64;

// Header, little-endian:
//
//   0..8    magic "LMESNAP\0"
//   8..12   version
//   12..16  zero
//   16..24  sequence number of the last command applied
//   24..32  the book's digest
//   32..40  length of the encoded state that follows
//   40..44  CRC-32 of the encoded state
//   44..60  zero
//   60..64  CRC-32 of bytes 0..60

/// The name of the snapshot taken after command `seq`.
fn snapshot_name(seq: Seq) -> String {
    format!("snapshot-{seq:020}.snap")
}

/// The sequence number of the snapshot called `name`, if it is one.
fn snapshot_seq(name: &str) -> Option<Seq> {
    let digits = name.strip_prefix("snapshot-")?.strip_suffix(".snap")?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

/// The sequence numbers of the snapshots in `dir`, oldest first.
pub(crate) fn list<S: Storage>(storage: &mut S, dir: &Path) -> Result<Vec<Seq>, Error> {
    let mut seqs: Vec<Seq> = storage
        .list(dir)?
        .iter()
        .filter_map(|name| snapshot_seq(name))
        .collect();
    seqs.sort_unstable();
    Ok(seqs)
}

/// Deletes the leftovers of snapshots that were being written when the process stopped.
pub(crate) fn remove_partial<S: Storage>(storage: &mut S, dir: &Path) -> Result<(), Error> {
    let partial: Vec<String> = storage
        .list(dir)?
        .into_iter()
        .filter(|name| name.starts_with("snapshot-") && name.ends_with(".tmp"))
        .collect();
    for name in &partial {
        storage.remove(&dir.join(name))?;
    }
    storage.sync_dir(dir)?;
    Ok(())
}

/// Writes the snapshot of `book` after command `seq`, durably. `buf` is scratch space.
pub(crate) fn write<S: Storage>(
    storage: &mut S,
    dir: &Path,
    seq: Seq,
    book: &OrderBook,
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    buf.clear();
    buf.resize(HEADER_SIZE, 0);
    encode_snapshot(&book.snapshot(), buf);
    let body = &buf[HEADER_SIZE..];
    let mut header = [0; HEADER_SIZE];
    header[..8].copy_from_slice(&MAGIC);
    header[8..12].copy_from_slice(&VERSION.to_le_bytes());
    header[16..24].copy_from_slice(&seq.to_le_bytes());
    header[24..32].copy_from_slice(&book.digest().to_le_bytes());
    header[32..40].copy_from_slice(&(body.len() as u64).to_le_bytes());
    header[40..44].copy_from_slice(&crc32fast::hash(body).to_le_bytes());
    let crc = crc32fast::hash(&header[..60]);
    header[60..].copy_from_slice(&crc.to_le_bytes());
    buf[..HEADER_SIZE].copy_from_slice(&header);

    let name = snapshot_name(seq);
    let partial = dir.join(name.replace(".snap", ".tmp"));
    let mut file = storage.create(&partial)?;
    file.write_at(0, buf)?;
    file.sync()?;
    drop(file);
    storage.rename(&partial, &dir.join(name))?;
    storage.sync_dir(dir)?;
    Ok(())
}

/// Loads the snapshot taken after command `seq`, for a book configured as `config`.
pub(crate) fn read<S: Storage>(
    storage: &mut S,
    dir: &Path,
    seq: Seq,
    config: &BookConfig,
) -> Result<OrderBook, Error> {
    let path = dir.join(snapshot_name(seq));
    let corrupt = |detail: &str| Error::Corrupt {
        file: path.clone(),
        detail: detail.to_owned(),
    };
    let mut file = storage.open(&path)?;
    let mut header = [0; HEADER_SIZE];
    file.read_at(0, &mut header)
        .map_err(|_| corrupt("shorter than its header"))?;
    let field = |at: usize| u64::from_le_bytes(header[at..at + 8].try_into().unwrap());
    let header_crc = u32::from_le_bytes(header[60..].try_into().unwrap());
    if header[..8] != MAGIC
        || header[8..12] != VERSION.to_le_bytes()
        || header[12..16] != [0; 4]
        || header[44..60] != [0; 16]
        || header_crc != crc32fast::hash(&header[..60])
    {
        return Err(corrupt("invalid header"));
    }
    if field(16) != seq {
        return Err(corrupt("sequence number differs from the file name"));
    }
    let len = field(32);
    if file.size()? != HEADER_SIZE as u64 + len {
        return Err(corrupt("length differs from the header"));
    }
    let mut body = vec![0; len as usize];
    file.read_at(HEADER_SIZE as u64, &mut body)?;
    if u32::from_le_bytes(header[40..44].try_into().unwrap()) != crc32fast::hash(&body) {
        return Err(corrupt("checksum mismatch"));
    }
    let snapshot = decode_snapshot(&body).map_err(|error| corrupt(&error.to_string()))?;
    if snapshot.config != *config {
        return Err(Error::ConfigMismatch { file: path });
    }
    let book = OrderBook::restore(&snapshot).map_err(|error| corrupt(&error.to_string()))?;
    if book.digest() != field(24) {
        return Err(corrupt(
            "the restored book's digest differs from the header",
        ));
    }
    Ok(book)
}

/// Deletes the snapshot taken after command `seq`.
pub(crate) fn remove<S: Storage>(storage: &mut S, dir: &Path, seq: Seq) -> Result<(), Error> {
    storage.remove(&dir.join(snapshot_name(seq)))?;
    Ok(())
}

/// The path of the snapshot taken after command `seq`.
pub fn path(dir: &Path, seq: Seq) -> PathBuf {
    dir.join(snapshot_name(seq))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_names_round_trip() {
        for seq in [0, 42, u64::MAX] {
            assert_eq!(snapshot_seq(&snapshot_name(seq)), Some(seq));
        }
        for name in [
            "snapshot-1.snap",
            "snapshot-00000000000000000001.tmp",
            "journal-00000000000000000001.log",
        ] {
            assert_eq!(snapshot_seq(name), None, "{name}");
        }
    }
}
