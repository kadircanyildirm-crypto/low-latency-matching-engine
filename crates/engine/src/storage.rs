//! The file operations the journal and the snapshot store need, behind a trait, so tests can
//! swap the disk for a simulated one that crashes.
//!
//! [`FsStorage`] is the real file system. [`SimStorage`] keeps files in memory and records
//! which writes and directory changes have been made durable by a sync. Its
//! [`crash`](SimStorage::crash) returns the disk as a power failure could leave it: every
//! synced byte, plus any part of what was written since, down to a torn single write.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use orderbook::workload::SplitMix64;

/// Directory and file operations.
pub trait Storage {
    /// An open file.
    type File: StorageFile;

    /// Creates `dir` and its parents if they do not exist.
    fn create_dir_all(&mut self, dir: &Path) -> io::Result<()>;
    /// Names of the files in `dir`.
    fn list(&mut self, dir: &Path) -> io::Result<Vec<String>>;
    /// Creates an empty file at `path`, replacing any file there, open for reading and
    /// writing.
    fn create(&mut self, path: &Path) -> io::Result<Self::File>;
    /// Opens the existing file at `path` for reading and writing.
    fn open(&mut self, path: &Path) -> io::Result<Self::File>;
    /// Moves `from` to `to`, replacing any file there.
    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()>;
    /// Deletes the file at `path`.
    fn remove(&mut self, path: &Path) -> io::Result<()>;
    /// Makes the creations, renames and removals in `dir` durable.
    fn sync_dir(&mut self, dir: &Path) -> io::Result<()>;
}

/// Positional reads and writes on an open file.
pub trait StorageFile {
    /// Length in bytes.
    fn size(&mut self) -> io::Result<u64>;
    /// Grows the file with zeros, or cuts it, to `len` bytes.
    fn set_len(&mut self, len: u64) -> io::Result<()>;
    /// Fills `buf` from `offset`; an error if the file ends first.
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()>;
    /// Writes all of `buf` at `offset`.
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<()>;
    /// Makes everything written so far durable (`fdatasync`, `FlushFileBuffers`).
    fn sync(&mut self) -> io::Result<()>;
}

/// The real file system.
#[derive(Clone, Copy, Debug, Default)]
pub struct FsStorage;

/// A file of [`FsStorage`].
#[derive(Debug)]
pub struct FsFile {
    file: std::fs::File,
    /// Where the OS file cursor is, if known: writes at that offset skip the seek, so
    /// appending costs one system call.
    cursor: Option<u64>,
}

impl FsFile {
    fn seek(&mut self, offset: u64) -> io::Result<()> {
        if self.cursor != Some(offset) {
            self.cursor = None;
            self.file.seek(SeekFrom::Start(offset))?;
        }
        Ok(())
    }
}

impl Storage for FsStorage {
    type File = FsFile;

    fn create_dir_all(&mut self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn list(&mut self, dir: &Path) -> io::Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_owned());
                }
            }
        }
        Ok(names)
    }

    fn create(&mut self, path: &Path) -> io::Result<FsFile> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(FsFile {
            file,
            cursor: Some(0),
        })
    }

    fn open(&mut self, path: &Path) -> io::Result<FsFile> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        Ok(FsFile {
            file,
            cursor: Some(0),
        })
    }

    fn rename(&mut self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove(&mut self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    /// On Unix, syncs the directory itself, which makes its entries durable. Windows has no
    /// portable way to open a directory for that; NTFS journals its metadata, and the
    /// journal never relies on a rename being durable before a later write is (see
    /// DESIGN.md).
    fn sync_dir(&mut self, dir: &Path) -> io::Result<()> {
        if cfg!(unix) {
            std::fs::File::open(dir)?.sync_all()?;
        }
        Ok(())
    }
}

impl StorageFile for FsFile {
    fn size(&mut self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        self.seek(offset)?;
        let result = self.file.read_exact(buf);
        self.cursor = result.is_ok().then_some(offset + buf.len() as u64);
        result
    }

    fn write_at(&mut self, offset: u64, buf: &[u8]) -> io::Result<()> {
        self.seek(offset)?;
        let result = self.file.write_all(buf);
        self.cursor = result.is_ok().then_some(offset + buf.len() as u64);
        result
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
}

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
