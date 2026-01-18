//! PostgreSQL-native local file storage implementation.
//!
//! This module provides a local file storage implementation that uses PostgreSQL's
//! internal Virtual File Descriptor (VFD) system via `PathNameOpenFile`, `FileRead`,
//! `FileWrite`, and `FileClose` functions. This integration provides:
//!
//! - **Resource Owner Integration**: File handles are automatically registered with
//!   PostgreSQL's ResourceOwner system, ensuring automatic cleanup on transaction
//!   abort or backend exit.
//! - **VFD Pool Management**: PostgreSQL's VFD system manages file descriptor limits
//!   transparently, automatically closing/reopening files as needed.
//! - **Consistent Error Handling**: Uses PostgreSQL's error reporting mechanisms.

use std::any::Any;
use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use pgrx::pg_sys;

use iceberg_lite::Result;
use iceberg_lite::io::{FileMetadata, FileRead, FileWrite, Storage};

/// Local file storage implementation using PostgreSQL's VFD system.
///
/// This implementation wraps PostgreSQL's internal file I/O functions to provide
/// seamless integration with PostgreSQL's resource management infrastructure.
#[derive(Debug, Default)]
pub struct LocalStorage;

impl Storage for LocalStorage {
    fn exists(&self, path: &str) -> Result<bool> {
        Ok(Path::new(path).exists())
    }

    fn delete(&self, path: &str) -> Result<()> {
        match fs::remove_file(path) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn remove_dir_all(&self, path: &str) -> Result<()> {
        let p = Path::new(path);
        if p.exists() {
            if p.is_dir() {
                fs::remove_dir_all(p)?;
            } else {
                fs::remove_file(p)?;
            }
        }
        Ok(())
    }

    fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let metadata = fs::metadata(path)?;
        Ok(FileMetadata {
            size: metadata.len(),
        })
    }

    fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let reader = PgFileRead::open(path)?;
        Ok(Box::new(reader))
    }

    fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        // Create parent directories if they don't exist
        if let Some(parent) = Path::new(path).parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }
        let writer = PgFileWrite::open(path)?;
        Ok(Box::new(writer))
    }

    fn initialize(&mut self, _props: HashMap<String, String>) -> Result<()> {
        Ok(())
    }

    fn scheme(&self) -> &str {
        "file"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
struct VfdOwner(pg_sys::File);

impl Drop for VfdOwner {
    fn drop(&mut self) {
        // SAFETY: fd is valid and FileClose handles cleanup properly
        unsafe {
            pg_sys::FileClose(self.0);
        }
    }
}

/// PostgreSQL VFD-backed file reader.
///
/// Wraps a PostgreSQL virtual file descriptor for reading operations. The file
/// handle is automatically registered with the current ResourceOwner and will
/// be closed when the ResourceOwner is released (e.g., at transaction end).
#[derive(Debug)]
pub struct PgFileRead {
    /// Path to the file (for error reporting)
    path: String,
    /// PostgreSQL virtual file descriptor owner
    file: Arc<VfdOwner>,
    /// Total file size in bytes
    size: i64,
}

// Note: PgFileRead is intentionally NOT Send/Sync.
// PostgreSQL's VFD system is thread-local and bound to the current backend.
// Using VFD handles across threads is undefined behavior.

impl PgFileRead {
    /// Open a file for reading using PostgreSQL's VFD system.
    ///
    /// # Arguments
    /// * `path` - Path to the file to open
    ///
    /// # Returns
    /// A new `PgFileRead` instance on success, or an error if the file cannot be opened.
    pub fn open(path: &str) -> io::Result<Self> {
        let c_path = CString::new(path)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let file =
            unsafe { pg_sys::PathNameOpenFile(c_path.as_ptr(), libc::O_RDONLY) };
        if file < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to open file '{}': {}", path, err),
            ));
        }

        let size = unsafe { pg_sys::FileSize(file) };
        if size < 0 {
            unsafe { pg_sys::FileClose(file) };
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to get size of file '{}': {}", path, err),
            ));
        }

        Ok(Self {
            path: path.to_string(),
            file: Arc::new(VfdOwner(file)),
            size: size as i64,
        })
    }

    /// Read bytes from a specific offset in the file.
    fn read_at(&self, offset: i64, len: usize) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];

        let result = unsafe {
            pg_sys::FileRead(
                self.file.0,
                buffer.as_mut_ptr() as *mut std::ffi::c_void,
                len,
                offset as pg_sys::off_t,
                pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_READ as u32,
            )
        };

        if result < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!(
                    "failed to read from file '{}' at offset {}: {}",
                    self.path, offset, err
                ),
            ));
        }

        buffer.truncate(result as usize);
        Ok(buffer)
    }
}

impl FileRead for PgFileRead {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        // Validate range boundaries
        if range.start > range.end {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("invalid range: start {} > end {}", range.start, range.end),
            )
            .into());
        }
        if range.end > self.size as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "range end {} exceeds file size {} for '{}'",
                    range.end, self.size, self.path
                ),
            )
            .into());
        }

        let start = range.start as i64;
        let len = (range.end - range.start) as usize;
        let buffer = self.read_at(start, len)?;
        Ok(Bytes::from(buffer))
    }

    fn read_all(&self) -> Result<Bytes> {
        self.read_range(0..self.size as u64)
    }

    fn try_clone(&self) -> io::Result<Box<dyn FileRead>> {
        Ok(Box::new(Self {
            path: self.path.clone(),
            file: self.file.clone(),
            size: self.size,
        }))
    }
}

impl Read for PgFileRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let fd = unsafe { pg_sys::FileGetRawDesc(self.file.0) };
        if fd < 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("invalid file descriptor for file '{}'", self.path),
            ));
        }

        let ret = unsafe {
            libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len() as _)
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to read from file '{}': {}", self.path, err),
            ));
        }

        Ok(ret as usize)
    }
}

impl Seek for PgFileRead {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let fd = unsafe { pg_sys::FileGetRawDesc(self.file.0) };
        if fd < 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("invalid file descriptor for file '{}'", self.path),
            ));
        }

        let (whence, offset) = match pos {
            SeekFrom::Start(off) => (libc::SEEK_SET, off as i64),
            SeekFrom::End(off) => (libc::SEEK_END, off),
            SeekFrom::Current(off) => (libc::SEEK_CUR, off),
        };

        let new_pos = unsafe { libc::lseek(fd, offset as libc::off_t, whence) };
        if new_pos < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to seek in file '{}': {}", self.path, err),
            ));
        }

        Ok(new_pos as u64)
    }
}

/// PostgreSQL VFD-backed file writer.
///
/// Wraps a PostgreSQL virtual file descriptor for writing operations. The file
/// handle is automatically registered with the current ResourceOwner and will
/// be closed when the ResourceOwner is released.
pub struct PgFileWrite {
    /// Path to the file (for error reporting)
    path: String,
    /// PostgreSQL virtual file descriptor
    file: pg_sys::File,
    /// Current write position in the file
    position: i64,
}

// Note: PgFileWrite is intentionally NOT Send/Sync.
// PostgreSQL's VFD system is thread-local and bound to the current backend.
// Using VFD handles across threads is undefined behavior.

impl PgFileWrite {
    /// Open a file for writing using PostgreSQL's VFD system.
    ///
    /// Creates the file if it doesn't exist and truncates it if it does.
    ///
    /// # Arguments
    /// * `path` - Path to the file to open or create
    ///
    /// # Returns
    /// A new `PgFileWrite` instance on success, or an error if the file cannot be opened.
    pub fn open(path: &str) -> io::Result<Self> {
        let c_path = CString::new(path)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let file = unsafe {
            pg_sys::PathNameOpenFile(
                c_path.as_ptr(),
                (libc::O_WRONLY
                    | libc::O_CREAT
                    | libc::O_TRUNC
                    | pg_sys::PG_BINARY as i32) as i32,
            )
        };
        if file < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to open file '{}' for writing: {}", path, err),
            ));
        }

        Ok(Self {
            path: path.to_string(),
            file,
            position: 0,
        })
    }
}

impl Write for PgFileWrite {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let result = unsafe {
            pg_sys::FileWrite(
                self.file,
                buf.as_ptr() as *const std::ffi::c_void,
                buf.len(),
                self.position as pg_sys::off_t,
                pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_WRITE as u32,
            )
        };

        if result < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to write to file '{}': {}", self.path, err),
            ));
        }

        self.position += result as i64;
        Ok(result as usize)
    }

    fn flush(&mut self) -> io::Result<()> {
        // SAFETY: file is valid
        let ret = unsafe {
            pg_sys::FileSync(
                self.file,
                pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_SYNC as u32,
            )
        };

        if ret < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to sync file '{}': {}", self.path, err),
            ));
        }

        Ok(())
    }
}

impl FileWrite for PgFileWrite {
    fn close(&mut self) -> Result<()> {
        self.flush()?;
        // Note: The file will be closed in Drop, but we can explicitly
        // sync here if needed before the final close.
        Ok(())
    }
}

impl Drop for PgFileWrite {
    fn drop(&mut self) {
        // SAFETY: file is valid and FileClose handles cleanup properly
        unsafe {
            pg_sys::FileClose(self.file);
        }
    }
}
