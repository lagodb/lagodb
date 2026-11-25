//! PostgreSQL VFD-backed file reader.

use std::ffi::CString;
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;
use std::rc::Rc;

use bytes::Bytes;
use iceberg_lite::Result;
use iceberg_lite::io::FileRead;
use lagodb_core::backend::OwnedVfd;
use pgrx::pg_sys;

#[derive(Debug)]
struct SharedReadFile {
    vfd: OwnedVfd,
    path: Box<str>,
}

/// PostgreSQL VFD-backed file reader.
///
/// Wraps a PostgreSQL virtual file descriptor for reading operations. The file
/// handle and diagnostic path are shared via [`SharedReadFile`] so cloning a
/// reader performs no path allocation, and the VFD is closed when the last
/// reader drops. The surrounding executor state is responsible for dropping the
/// reader during normal completion and PostgreSQL error unwinding.
#[derive(Debug)]
pub(super) struct LocalFileReader {
    shared: Rc<SharedReadFile>,
    /// Total file size in bytes
    size: i64,
    /// Logical cursor used by the std::io::Read/Seek implementation.
    position: i64,
}

// SAFETY: this private adapter implements iceberg-lite `FileRead`, which
// requires `Send + Sync`. The inner `Rc<SharedReadFile>` and `OwnedVfd` remain
// intentionally `!Send + !Sync` so `Send + Sync` is never an accidental
// compiler-derived property and cloning pays no atomic refcount cost; every
// open, `try_clone`, read, and `Drop` runs inside a PostgreSQL-owned serial
// scan on the owning backend thread.
unsafe impl Send for LocalFileReader {}
// SAFETY: the serial iterator never invokes one reader concurrently; shared
// access exists only for the upstream `FileRead` trait surface.
unsafe impl Sync for LocalFileReader {}

impl LocalFileReader {
    /// Open a file for reading using PostgreSQL's VFD system.
    pub(super) fn open(path: &str) -> io::Result<Self> {
        let c_path = CString::new(path)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let Some(vfd) = OwnedVfd::open(&c_path, libc::O_RDONLY) else {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!("failed to open file '{}': {}", path, err),
            ));
        };

        let size = vfd.size();
        if size < 0 {
            let err = io::Error::last_os_error();
            drop(vfd);
            return Err(io::Error::new(
                err.kind(),
                format!("failed to get size of file '{}': {}", path, err),
            ));
        }

        Ok(Self {
            shared: Rc::new(SharedReadFile {
                vfd,
                path: path.into(),
            }),
            size,
            position: 0,
        })
    }

    #[inline]
    pub(super) fn size(&self) -> u64 {
        self.size as u64
    }

    #[inline]
    fn path(&self) -> &str {
        &self.shared.path
    }

    fn read_into_at(&self, offset: i64, buf: &mut [u8]) -> io::Result<usize> {
        let result = self.shared.vfd.read_at(
            buf,
            offset as pg_sys::off_t,
            pg_sys::WaitEventIO::WAIT_EVENT_DATA_FILE_READ,
        );

        if result < 0 {
            let err = io::Error::last_os_error();
            return Err(io::Error::new(
                err.kind(),
                format!(
                    "failed to read from file '{}' at offset {}: {}",
                    self.path(),
                    offset,
                    err
                ),
            ));
        }

        Ok(result as usize)
    }

    /// Read exactly `len` bytes at `offset`. `FileRead` may return fewer bytes
    /// than requested, so repeat until the range is complete; end of file
    /// before that means the file is shorter than the size recorded at open.
    fn read_exact_at(&self, offset: i64, len: usize) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];
        let mut filled = 0;
        while filled < len {
            let position = offset + filled as i64;
            let read = self.read_into_at(position, &mut buffer[filled..])?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "file '{}' ended at offset {} before range {}..{} was read",
                        self.path(),
                        position,
                        offset,
                        offset + len as i64
                    ),
                ));
            }
            filled += read;
        }
        Ok(buffer)
    }
}

impl FileRead for LocalFileReader {
    fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
        if range.start > range.end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid range: start {} > end {}", range.start, range.end),
            )
            .into());
        }
        if range.end > self.size as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "range end {} exceeds file size {} for '{}'",
                    range.end,
                    self.size,
                    self.path()
                ),
            )
            .into());
        }

        let start = range.start as i64;
        let len = (range.end - range.start) as usize;
        let buffer = self.read_exact_at(start, len)?;
        Ok(Bytes::from(buffer))
    }

    fn read_all(&self) -> Result<Bytes> {
        self.read_range(0..self.size as u64)
    }

    fn try_clone(&self) -> io::Result<Box<dyn FileRead>> {
        Ok(Box::new(Self {
            shared: Rc::clone(&self.shared),
            size: self.size,
            position: self.position,
        }))
    }
}

impl Read for LocalFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let bytes_read = self.read_into_at(self.position, buf)?;
        if bytes_read > 0 {
            let bytes_read = i64::try_from(bytes_read).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("read length is too large for file '{}'", self.path()),
                )
            })?;
            self.position =
                self.position.checked_add(bytes_read).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("read position overflow for file '{}'", self.path()),
                    )
                })?;
        }
        Ok(bytes_read)
    }
}

impl Seek for LocalFileReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(off) => i64::try_from(off).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("seek offset {} is too large for '{}'", off, self.path()),
                )
            })?,
            SeekFrom::End(off) => self.size.checked_add(off).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("seek offset overflows for '{}'", self.path()),
                )
            })?,
            SeekFrom::Current(off) => {
                self.position.checked_add(off).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("seek offset overflows for '{}'", self.path()),
                    )
                })?
            }
        };

        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid seek before start of file '{}'", self.path()),
            ));
        }

        self.position = new_pos;
        Ok(new_pos as u64)
    }
}
