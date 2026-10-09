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
    /// Whether every change fails, as on a full or failing disk. A failed file sync also
    /// forgets the file's unsynced writes, as Linux marks the pages clean after a failed
    /// write-back: they stay readable but never become durable.
    failing: bool,
    /// Whether file syncs fail, while everything else works: the write-back fails.
    failing_syncs: bool,
    /// Whether reads fail.
    failing_reads: bool,
    /// Changes to files whose name contains this fail, as if their part of the disk were
    /// full or broken.
    failing_names: Option<String>,
    /// Changes the process may still make before it dies, if limited.
    budget: Option<u64>,
    /// Whether the process has died: every change fails until a crash or a revival.
    dead: bool,
    /// Changes made so far: writes, length changes, syncs, creations, renames, removals.
    changes: u64,
    /// Directories someone holds the lock of. A crash releases them all.
    locked: std::collections::BTreeSet<PathBuf>,
}

impl Disk {
    /// [`change`](Self::change) to the file at `path`.
    fn change_to(&mut self, path: &Path) -> io::Result<()> {
        self.check_name(path)?;
        self.change()
    }

    /// Fails if changes to the file at `path` are made to fail.
    fn check_name(&self, path: &Path) -> io::Result<()> {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if self
            .failing_names
            .as_deref()
            .is_some_and(|part| name.contains(part))
        {
            return Err(io::Error::other("injected failure"));
        }
        Ok(())
    }

    /// Accounts for one change, or fails it: the process is dead, or dies now, or the disk
    /// is failing.
    fn change(&mut self) -> io::Result<()> {
        if self.dead {
            return Err(io::Error::other("the process is dead"));
        }
        if let Some(budget) = self.budget.as_mut() {
            if *budget == 0 {
                self.dead = true;
                return Err(io::Error::other("the process died"));
            }
            *budget -= 1;
        }
        if self.failing {
            return Err(io::Error::other("injected failure"));
        }
        self.changes += 1;
        Ok(())
    }
}

/// The lock on a directory of a [`SimStorage`], released when dropped.
#[derive(Debug)]
pub struct SimLock {
    disk: Rc<RefCell<Disk>>,
    dir: PathBuf,
}

impl Drop for SimLock {
    fn drop(&mut self) {
        self.disk.borrow_mut().locked.remove(&self.dir);
    }
}

#[derive(Debug)]
enum NameChange {
    Create(PathBuf, Rc<RefCell<Inode>>),
    Rename(PathBuf, PathBuf),
    Remove(PathBuf),
}

#[derive(Debug, Default)]
struct Inode {
    /// The file's name, which renames change.
    name: PathBuf,
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

    /// The parts of a write a torn write leaves, sector by sector: a prefix of its sectors
    /// in order, or any of them. A length change is atomic.
    fn torn(&self, rng: &mut SplitMix64, in_order: bool) -> Vec<Change> {
        let Change::Write(offset, bytes) = self else {
            return Vec::new();
        };
        // The write's pieces, cut at sector boundaries of the file.
        let mut sectors = Vec::new();
        let mut at = 0;
        while at < bytes.len() {
            let to_boundary = SECTOR - ((offset + at as u64) % SECTOR as u64) as usize;
            let end = (at + to_boundary).min(bytes.len());
            sectors.push(Change::Write(offset + at as u64, bytes[at..end].to_vec()));
            at = end;
        }
        if in_order {
            let kept = rng.below(sectors.len() as u64) as usize;
            sectors.truncate(kept);
            sectors
        } else {
            sectors.into_iter().filter(|_| rng.below(2) == 0).collect()
        }
    }
}

/// The unit a disk writes atomically: a write that is torn keeps or loses whole sectors.
const SECTOR: usize = 512;

/// A file of [`SimStorage`].
#[derive(Debug)]
pub struct SimFile {
    inode: Rc<RefCell<Inode>>,
    disk: Rc<RefCell<Disk>>,
}

impl SimFile {
    fn change(&self) -> io::Result<()> {
        let name = self.inode.borrow().name.clone();
        self.disk.borrow_mut().change_to(&name)
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
        // A rename that did not survive leaves the file under its old name.
        for (name, inode) in &after {
            inode.borrow_mut().name = name.clone();
        }
        SimStorage {
            disk: Rc::new(RefCell::new(Disk {
                names: after.clone(),
                durable_names: after,
                pending: Vec::new(),
                ..Disk::default()
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

    /// From now on, every change fails if `failing`: writes, length changes, syncs,
    /// creations, renames, removals and directory syncs. A failed file sync forgets the
    /// file's unsynced writes, as Linux does: they stay readable but never become durable.
    pub fn set_failing(&self, failing: bool) {
        self.disk.borrow_mut().failing = failing;
    }

    /// From now on, every file sync fails if `failing`, and forgets the file's unsynced
    /// writes as a failed write-back on Linux does; writes still work.
    pub fn set_failing_syncs(&self, failing: bool) {
        self.disk.borrow_mut().failing_syncs = failing;
    }

    /// From now on, every read fails if `failing`.
    pub fn set_failing_reads(&self, failing: bool) {
        self.disk.borrow_mut().failing_reads = failing;
    }

    /// From now on, every change to a file whose name contains `part` fails, or none if
    /// `None`: creating, writing, syncing, renaming or removing it.
    pub fn set_failing_names(&self, part: Option<&str>) {
        self.disk.borrow_mut().failing_names = part.map(str::to_owned);
    }

    /// Lets the process make `changes` more changes, then dies: the next change fails, and
    /// so does every one after it, until [`crash`](Self::crash) or [`revive`](Self::revive).
    pub fn die_after(&self, changes: u64) {
        self.disk.borrow_mut().budget = Some(changes);
    }

    /// Whether the process has died.
    pub fn is_dead(&self) -> bool {
        self.disk.borrow().dead
    }

    /// Changes made so far.
    pub fn changes(&self) -> u64 {
        self.disk.borrow().changes
    }

    /// Starts a new process on the same disk, as after a kill: whatever the dead one wrote
    /// stays, synced or not.
    pub fn revive(&self) {
        let mut disk = self.disk.borrow_mut();
        disk.dead = false;
        disk.budget = None;
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
                if let Some(change) = inode.unsynced.get(kept) {
                    for part in change.torn(rng, true) {
                        part.apply(&mut contents);
                    }
                }
            }
            CrashModel::AnyOrder => {
                for change in &inode.unsynced {
                    match rng.below(3) {
                        0 => change.apply(&mut contents),
                        1 => change
                            .torn(rng, false)
                            .iter()
                            .for_each(|part| part.apply(&mut contents)),
                        _ => {}
                    }
                }
            }
        }
        Inode {
            name: inode.name.clone(),
            current: contents.clone(),
            durable: contents,
            unsynced: Vec::new(),
        }
    }
}

impl Storage for SimStorage {
    type File = SimFile;
    type Lock = SimLock;

    fn lock(&mut self, dir: &Path) -> io::Result<Option<SimLock>> {
        let mut disk = self.disk.borrow_mut();
        if !disk.locked.insert(dir.to_owned()) {
            return Ok(None);
        }
        Ok(Some(SimLock {
            disk: self.disk.clone(),
            dir: dir.to_owned(),
        }))
    }

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
        let inode = Rc::new(RefCell::new(Inode {
            name: path.to_owned(),
            ..Inode::default()
        }));
        let mut disk = self.disk.borrow_mut();
        disk.change_to(path)?;
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
        disk.check_name(from)?;
        disk.change_to(to)?;
        let inode = disk.names.remove(from).ok_or_else(not_found)?;
        inode.borrow_mut().name = to.to_owned();
        disk.names.insert(to.to_owned(), inode);
        disk.pending
            .push(NameChange::Rename(from.to_owned(), to.to_owned()));
        Ok(())
    }

    fn remove(&mut self, path: &Path) -> io::Result<()> {
        let mut disk = self.disk.borrow_mut();
        disk.change_to(path)?;
        disk.names.remove(path).ok_or_else(not_found)?;
        disk.pending.push(NameChange::Remove(path.to_owned()));
        Ok(())
    }

    fn sync_dir(&mut self, dir: &Path) -> io::Result<()> {
        let mut disk = self.disk.borrow_mut();
        disk.change()?;
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
        self.change()?;
        let mut inode = self.inode.borrow_mut();
        let change = Change::SetLen(len);
        change.apply(&mut inode.current);
        inode.unsynced.push(change);
        Ok(())
    }

    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        if self.disk.borrow().failing_reads {
            return Err(io::Error::other("injected read failure"));
        }
        let inode = self.inode.borrow();
        let bytes = usize::try_from(offset)
            .ok()
            .and_then(|start| inode.current.get(start..start.checked_add(buf.len())?))
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        buf.copy_from_slice(bytes);
        Ok(())
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.change()?;
        let mut inode = self.inode.borrow_mut();
        let change = Change::Write(offset, buf.to_vec());
        change.apply(&mut inode.current);
        inode.unsynced.push(change);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        let result = if self.disk.borrow().failing_syncs {
            Err(io::Error::other("injected sync failure"))
        } else {
            self.change()
        };
        if let Err(error) = result {
            let disk = self.disk.borrow();
            if disk.failing || disk.failing_syncs {
                // The write-back failed: the pages are clean now, but not on the disk.
                self.inode.borrow_mut().unsynced.clear();
            }
            return Err(error);
        }
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

    /// Synced data and directory entries always survive; unsynced writes survive as the
    /// model allows; a torn write keeps whole sectors of one write, a prefix of them in
    /// order, any of them otherwise.
    #[test]
    fn a_crash_keeps_what_was_synced() {
        let dir = Path::new("d");
        let (mut seen_torn, mut seen_reordered, mut seen_gap) = (false, false, false);
        for seed in 0..500 {
            let mut disk = SimStorage::new();
            let mut file = disk.create(&dir.join("f")).unwrap();
            file.write_at(0, b"synced").unwrap();
            file.sync().unwrap();
            disk.sync_dir(dir).unwrap();
            // Two writes of three sectors each, the first starting mid-sector.
            file.write_at(6, &[1; 1530]).unwrap();
            file.write_at(1536, &[2; 1536]).unwrap();
            let model = if seed % 2 == 0 {
                CrashModel::InOrder
            } else {
                CrashModel::AnyOrder
            };
            let mut after = disk.crash(&mut SplitMix64::new(seed), model);
            let bytes = contents(&mut after, "d/f").expect("a synced file survives");
            assert!(bytes.starts_with(b"synced"), "{bytes:?}");
            let sector = |i: usize| bytes.get(i * 512..(i + 1) * 512).map(|s| s[511]);
            let first: Vec<_> = (0..3).map(|i| sector(i) == Some(1)).collect();
            let second: Vec<_> = (3..6).map(|i| sector(i) == Some(2)).collect();
            // Every sector is all or nothing.
            for i in 0..6 {
                if let Some(range) = bytes.get((i * 512).max(6)..(i + 1) * 512) {
                    assert!(
                        range.windows(2).all(|w| w[0] == w[1]),
                        "sector {i} is split"
                    );
                }
            }
            if model == CrashModel::InOrder {
                assert!(
                    second.iter().all(|&s| !s) || first.iter().all(|&s| s),
                    "in order, the second write survives only after the first"
                );
                // Within a write, sectors survive as a prefix.
                for write in [&first, &second] {
                    assert!(write.windows(2).all(|w| w[0] || !w[1]), "{write:?}");
                }
            }
            seen_reordered |= second.iter().all(|&s| s) && !first.iter().all(|&s| s);
            seen_torn |= first.iter().any(|&s| s) && !first.iter().all(|&s| s);
            seen_gap |= first[0] && !first[1] && first[2];
        }
        assert!(seen_torn && seen_reordered && seen_gap);
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
