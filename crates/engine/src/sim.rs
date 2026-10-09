//! An in-memory disk that can lose power, for crash tests.
//!
//! [`SimStorage`] records which writes and directory changes have been made durable by a
//! sync. Its [`crash`](SimStorage::crash) returns the disk as a power failure could leave
//! it: every synced byte, plus any part of what was written since, down to a torn single
//! write.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use orderbook::workload::SplitMix64;

use crate::storage::{Storage, StorageFile};

/// How a simulated power failure treats the writes that were not yet synced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashModel {
    /// Writes reach the disk in the order they were made: some prefix of them survives,
    /// and the next one may be torn at any byte.
    InOrder,
    /// The OS writes cached data back in any order: each write independently survives,
    /// is lost, or is torn.
    AnyOrder,
}

/// An in-memory disk that remembers what was synced, for crash tests.
///
/// Clones share the disk. Files stay usable after [`crash`](Self::crash), which returns a
/// separate disk with the post-crash contents.
#[derive(Clone, Debug, Default)]
pub struct SimStorage {
    disk: Rc<RefCell<Disk>>,
}

#[derive(Debug, Default)]
struct Disk {
    /// The directory as the program sees it.
    names: BTreeMap<PathBuf, Rc<RefCell<Inode>>>,
    /// The directory as of the last directory sync.
    durable_names: BTreeMap<PathBuf, Rc<RefCell<Inode>>>,
    /// Directory changes since then, in order.
    pending: Vec<NameChange>,
    /// Whether writes and syncs fail, as on a full or failing disk.
    failing: bool,
}

#[derive(Debug)]
enum NameChange {
    Create(PathBuf, Rc<RefCell<Inode>>),
    Rename(PathBuf, PathBuf),
    Remove(PathBuf),
}

#[derive(Debug, Default)]
struct Inode {
    /// The contents as the program sees them.
    current: Vec<u8>,
    /// The contents as of the last sync.
    durable: Vec<u8>,
    /// Writes since then, in order.
    unsynced: Vec<Change>,
}

#[derive(Clone, Debug)]
enum Change {
    Write(u64, Vec<u8>),
    SetLen(u64),
}

impl Change {
    fn apply(&self, contents: &mut Vec<u8>) {
        match self {
            Change::Write(offset, bytes) => {
                let end = *offset as usize + bytes.len();
                if contents.len() < end {
                    contents.resize(end, 0);
                }
                contents[*offset as usize..end].copy_from_slice(bytes);
            }
            Change::SetLen(len) => contents.resize(*len as usize, 0),
        }
    }

    /// A random part of a write, as a torn write leaves it; a length change is atomic.
    fn torn(&self, rng: &mut SplitMix64) -> Option<Change> {
        match self {
            Change::Write(offset, bytes) if bytes.len() > 1 => {
                let start = rng.below(bytes.len() as u64) as usize;
                let end = start + 1 + rng.below((bytes.len() - start) as u64) as usize;
                Some(Change::Write(
                    offset + start as u64,
                    bytes[start..end].to_vec(),
                ))
            }
            _ => None,
        }
    }
}

/// A file of [`SimStorage`].
#[derive(Debug)]
pub struct SimFile {
    inode: Rc<RefCell<Inode>>,
    disk: Rc<RefCell<Disk>>,
}

impl SimFile {
    fn check(&self) -> io::Result<()> {
        if self.disk.borrow().failing {
            return Err(io::Error::other("injected failure"));
        }
        Ok(())
    }
}

impl SimStorage {
    /// An empty disk.
    pub fn new() -> Self {
        Self::default()
    }

    /// The disk a power failure would leave behind now: all synced data and directory
    /// entries, plus a random selection of the unsynced writes and directory changes,
    /// chosen by `model`. The unsynced directory changes survive as a random prefix, in
    /// order, as journaling file systems apply them.
    pub fn crash(&self, rng: &mut SplitMix64, model: CrashModel) -> SimStorage {
        let disk = self.disk.borrow();
        let mut after: BTreeMap<PathBuf, Rc<RefCell<Inode>>> = BTreeMap::new();
        let mut contents: BTreeMap<*const RefCell<Inode>, Rc<RefCell<Inode>>> = BTreeMap::new();
        let mut survivor = |inode: &Rc<RefCell<Inode>>, rng: &mut SplitMix64| {
            contents
                .entry(Rc::as_ptr(inode))
                .or_insert_with(|| {
                    Rc::new(RefCell::new(Inode::survivor(&inode.borrow(), rng, model)))
                })
                .clone()
        };
        for (name, inode) in &disk.durable_names {
            after.insert(name.clone(), survivor(inode, rng));
        }
        let applied = rng.below(disk.pending.len() as u64 + 1) as usize;
        for change in &disk.pending[..applied] {
            match change {
                NameChange::Create(name, inode) => {
                    after.insert(name.clone(), survivor(inode, rng));
                }
                NameChange::Rename(from, to) => {
                    if let Some(inode) = after.remove(from) {
                        after.insert(to.clone(), inode);
                    }
                }
                NameChange::Remove(name) => {
                    after.remove(name);
                }
            }
        }
        SimStorage {
            disk: Rc::new(RefCell::new(Disk {
                names: after.clone(),
                durable_names: after,
                pending: Vec::new(),
                failing: false,
            })),
        }
    }

    /// Flips one bit of the file at `path`, in what it holds and in what is durable, as
    /// decay or a misdirected write would. `bit` is taken modulo the file's size in bits;
    /// an empty or missing file is left alone.
    pub fn flip_bit(&self, path: &Path, bit: u64) {
        let disk = self.disk.borrow();
        let Some(inode) = disk.names.get(path) else {
            return;
        };
        let mut inode = inode.borrow_mut();
        let bits = inode.current.len() as u64 * 8;
        if bits == 0 {
            return;
        }
        let bit = bit % bits;
        let (byte, mask) = ((bit / 8) as usize, 1u8 << (bit % 8));
        inode.current[byte] ^= mask;
        if let Some(durable) = inode.durable.get_mut(byte) {
            *durable ^= mask;
        }
        for change in &mut inode.unsynced {
            if let Change::Write(offset, bytes) = change {
                if let Some(at) = (byte as u64).checked_sub(*offset) {
                    if let Some(written) = bytes.get_mut(at as usize) {
                        *written ^= mask;
                    }
                }
            }
        }
    }

    /// From now on, every write, length change and sync fails if `failing`.
    pub fn set_failing(&self, failing: bool) {
        self.disk.borrow_mut().failing = failing;
    }

    /// The names of every file, with their sizes, in path order.
    pub fn files(&self) -> Vec<(PathBuf, u64)> {
        let disk = self.disk.borrow();
        disk.names
            .iter()
            .map(|(name, inode)| (name.clone(), inode.borrow().current.len() as u64))
            .collect()
    }
}

impl Inode {
    fn survivor(inode: &Inode, rng: &mut SplitMix64, model: CrashModel) -> Inode {
        let mut contents = inode.durable.clone();
        match model {
            CrashModel::InOrder => {
                let kept = rng.below(inode.unsynced.len() as u64 + 1) as usize;
                for change in &inode.unsynced[..kept] {
                    change.apply(&mut contents);
                }
                if let Some(torn) = inode.unsynced.get(kept).and_then(|c| c.torn(rng)) {
                    if rng.below(2) == 0 {
                        torn.apply(&mut contents);
                    }
                }
            }
            CrashModel::AnyOrder => {
                for change in &inode.unsynced {
                    match rng.below(3) {
                        0 => change.apply(&mut contents),
                        1 => {
                            if let Some(torn) = change.torn(rng) {
                                torn.apply(&mut contents);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        Inode {
            current: contents.clone(),
            durable: contents,
            unsynced: Vec::new(),
        }
    }
}

impl Storage for SimStorage {
    type File = SimFile;

    fn create_dir_all(&mut self, _dir: &Path) -> io::Result<()> {
        Ok(())
    }

    fn list(&mut self, dir: &Path) -> io::Result<Vec<String>> {
        let disk = self.disk.borrow();
        Ok(disk
            .names
            .keys()
            .filter(|name| name.parent() == Some(dir))
            .filter_map(|name| name.file_name()?.to_str().map(str::to_owned))
            .collect())
    }

    fn create(&mut self, path: &Path) -> io::Result<SimFile> {
        let inode = Rc::new(RefCell::new(Inode::default()));
        let mut disk = self.disk.borrow_mut();
        disk.names.insert(path.to_owned(), inode.clone());
        disk.pending
            .push(NameChange::Create(path.to_owned(), inode.clone()));
        Ok(SimFile {
            inode,
            disk: self.disk.clone(),
        })
    }

    fn open(&mut self, path: &Path) -> io::Result<SimFile> {
        let disk = self.disk.borrow();
        let inode = disk.names.get(path).ok_or_else(not_found)?.clone();
        Ok(SimFile {
            inode,
            disk: self.disk.clone(),
        })
    }

    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        let mut disk = self.disk.borrow_mut();
        let inode = disk.names.remove(from).ok_or_else(not_found)?;
        disk.names.insert(to.to_owned(), inode);
        disk.pending
            .push(NameChange::Rename(from.to_owned(), to.to_owned()));
        Ok(())
    }

    fn remove(&mut self, path: &Path) -> io::Result<()> {
        let mut disk = self.disk.borrow_mut();
        disk.names.remove(path).ok_or_else(not_found)?;
        disk.pending.push(NameChange::Remove(path.to_owned()));
        Ok(())
    }

    fn sync_dir(&mut self, dir: &Path) -> io::Result<()> {
        let mut disk = self.disk.borrow_mut();
        let disk = &mut *disk;
        let mut kept = Vec::new();
        for change in disk.pending.drain(..) {
            let in_dir = |path: &Path| path.parent() == Some(dir);
            match &change {
                NameChange::Create(name, inode) if in_dir(name) => {
                    disk.durable_names.insert(name.clone(), inode.clone());
                }
                NameChange::Rename(from, to) if in_dir(from) && in_dir(to) => {
                    if let Some(inode) = disk.durable_names.remove(from) {
                        disk.durable_names.insert(to.clone(), inode);
                    }
                }
                NameChange::Remove(name) if in_dir(name) => {
                    disk.durable_names.remove(name);
                }
                _ => kept.push(change),
            }
        }
        disk.pending = kept;
        Ok(())
    }
}

impl StorageFile for SimFile {
    fn size(&mut self) -> io::Result<u64> {
        Ok(self.inode.borrow().current.len() as u64)
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.check()?;
        let mut inode = self.inode.borrow_mut();
        let change = Change::SetLen(len);
        change.apply(&mut inode.current);
        inode.unsynced.push(change);
        Ok(())
    }

    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let inode = self.inode.borrow();
        let bytes = usize::try_from(offset)
            .ok()
            .and_then(|start| inode.current.get(start..start.checked_add(buf.len())?))
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        buf.copy_from_slice(bytes);
        Ok(())
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.check()?;
        let mut inode = self.inode.borrow_mut();
        let change = Change::Write(offset, buf.to_vec());
        change.apply(&mut inode.current);
        inode.unsynced.push(change);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.check()?;
        let mut inode = self.inode.borrow_mut();
        let inode = &mut *inode;
        for change in inode.unsynced.drain(..) {
            change.apply(&mut inode.durable);
        }
        Ok(())
    }
}

fn not_found() -> io::Error {
    io::Error::from(io::ErrorKind::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contents(storage: &mut SimStorage, path: &str) -> Option<Vec<u8>> {
        let mut file = storage.open(Path::new(path)).ok()?;
        let mut bytes = vec![0; file.size().unwrap() as usize];
        file.read_at(0, &mut bytes).unwrap();
        Some(bytes)
    }

    /// Synced data and directory entries always survive; unsynced ones survive as the
    /// model allows; a torn write leaves part of one write.
    #[test]
    fn a_crash_keeps_what_was_synced() {
        let dir = Path::new("d");
        let mut seen_torn = false;
        let mut seen_reordered = false;
        for seed in 0..500 {
            let mut disk = SimStorage::new();
            let mut file = disk.create(&dir.join("f")).unwrap();
            file.write_at(0, b"synced").unwrap();
            file.sync().unwrap();
            disk.sync_dir(dir).unwrap();
            file.write_at(6, b"-one").unwrap();
            file.write_at(10, b"-two").unwrap();
            let model = if seed % 2 == 0 {
                CrashModel::InOrder
            } else {
                CrashModel::AnyOrder
            };
            let mut after = disk.crash(&mut SplitMix64::new(seed), model);
            let bytes = contents(&mut after, "d/f").expect("a synced file survives");
            assert!(bytes.starts_with(b"synced"), "{bytes:?}");
            let tail = &bytes[6..];
            let one = tail.get(..4) == Some(b"-one");
            let two = tail.get(4..8) == Some(b"-two");
            if model == CrashModel::InOrder && two {
                assert!(
                    one,
                    "in order, the second write survives only after the first"
                );
            }
            seen_reordered |= two && !one;
            seen_torn |= !tail.is_empty() && !one && !two;
        }
        assert!(seen_torn && seen_reordered);
    }

    /// Unsynced creations, renames and removals survive as an ordered prefix.
    #[test]
    fn directory_changes_survive_in_order() {
        let dir = Path::new("d");
        let mut outcomes = std::collections::BTreeSet::new();
        for seed in 0..200 {
            let mut disk = SimStorage::new();
            disk.create(&dir.join("a")).unwrap().sync().unwrap();
            disk.sync_dir(dir).unwrap();
            disk.create(&dir.join("b")).unwrap().sync().unwrap();
            disk.rename(&dir.join("a"), &dir.join("c")).unwrap();
            disk.remove(&dir.join("b")).unwrap();
            // A change in another directory is not made durable by syncing this one.
            disk.create(Path::new("e/x")).unwrap();
            disk.sync_dir(Path::new("other")).unwrap();
            let mut after = disk.crash(&mut SplitMix64::new(seed), CrashModel::InOrder);
            let names = after.list(dir).unwrap().join(",");
            assert!(
                ["a", "a,b", "b,c", "c"].contains(&names.as_str()),
                "{names}"
            );
            outcomes.insert(names);
        }
        assert_eq!(outcomes.len(), 4);
    }

    #[test]
    fn flipped_bits_reach_every_copy_and_failures_are_injected() {
        let path = Path::new("d/f");
        let mut disk = SimStorage::new();
        let mut file = disk.create(path).unwrap();
        file.write_at(0, &[0; 4]).unwrap();
        file.sync().unwrap();
        disk.sync_dir(Path::new("d")).unwrap();
        file.write_at(2, &[0; 2]).unwrap();
        disk.flip_bit(path, 3 * 8 + 1);
        disk.flip_bit(Path::new("d/missing"), 0);
        disk.create(Path::new("d/empty")).unwrap();
        disk.flip_bit(Path::new("d/empty"), 0);
        // A crash that keeps the unsynced write keeps its flipped bit too.
        for seed in 0..20 {
            let mut after = disk.crash(&mut SplitMix64::new(seed), CrashModel::InOrder);
            assert_eq!(contents(&mut after, "d/f").unwrap()[3], 2);
        }
        assert_eq!(disk.files().len(), 2);
        disk.set_failing(true);
        assert!(file.write_at(0, &[1]).is_err());
        assert!(file.set_len(0).is_err());
        assert!(file.sync().is_err());
        assert!(disk.open(Path::new("d/missing")).is_err());
        assert!(disk.rename(Path::new("d/missing"), path).is_err());
        assert!(disk.remove(Path::new("d/missing")).is_err());
        let mut short = [0; 8];
        assert!(file.read_at(0, &mut short).is_err());
    }
}
