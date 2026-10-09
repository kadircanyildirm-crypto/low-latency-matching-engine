//! The file operations the journal and the snapshot store need, behind a trait, so tests can
//! swap the disk for a simulated one that crashes.
//!
//! [`FsStorage`] is the real file system. [`SimStorage`](crate::sim::SimStorage) keeps files
//! in memory and can lose power.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Directory and file operations. Clones are handles on the same storage: the journal and
/// the snapshots each use one, possibly on different threads.
pub trait Storage: Clone {
    /// An open file.
    type File: StorageFile;
    /// An exclusive hold on a directory, released when dropped.
    type Lock;

    /// Takes the exclusive lock on `dir`, or returns `None` if someone else holds it.
    fn lock(&mut self, dir: &Path) -> io::Result<Option<Self::Lock>>;

    /// Creates `dir` and its parents if they do not exist, durably.
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
    type Lock = std::fs::File;

    /// An OS lock on the file `LOCK` in `dir` (`flock`, `LockFileEx`). The OS releases it
    /// when the process exits, however it exits, so a crash never leaves the directory
    /// locked.
    fn lock(&mut self, dir: &Path) -> io::Result<Option<std::fs::File>> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("LOCK"))?;
        // Through the trait: newer standard libraries have an inherent `try_lock` of
        // their own, which the minimum supported Rust version lacks.
        match fs4::FileExt::try_lock(&file) {
            Ok(()) => Ok(Some(file)),
            Err(fs4::TryLockError::WouldBlock) => Ok(None),
            Err(fs4::TryLockError::Error(error)) => Err(error),
        }
    }

    /// Syncs the parent of every directory it creates, so the new directory itself survives
    /// a power failure.
    fn create_dir_all(&mut self, dir: &Path) -> io::Result<()> {
        if dir.as_os_str().is_empty() || dir.is_dir() {
            return Ok(());
        }
        if let Some(parent) = dir.parent() {
            self.create_dir_all(parent)?;
        }
        std::fs::create_dir(dir)?;
        let parent = dir.parent().filter(|parent| !parent.as_os_str().is_empty());
        self.sync_dir(parent.unwrap_or(Path::new(".")))
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

    /// Syncs the directory itself, which makes its entries durable: `fsync` on the
    /// directory on Unix, and on Windows `FlushFileBuffers` on a handle to it, which needs
    /// write access and `FILE_FLAG_BACKUP_SEMANTICS` to open.
    fn sync_dir(&mut self, dir: &Path) -> io::Result<()> {
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(dir)?
                .sync_all()
        }
        #[cfg(not(windows))]
        {
            std::fs::File::open(dir)?.sync_all()
        }
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
